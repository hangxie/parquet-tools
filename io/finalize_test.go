package io

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinalizeWriter(t *testing.T) {
	const schema = `{"Tag":"name=root","Fields":[{"Tag":"name=value, type=BYTE_ARRAY, convertedtype=UTF8"}]}`
	ctx := context.Background()
	writeErr := errors.New("pipeline failed")

	testCases := map[string]struct {
		writeErr   error
		wantErr    error
		wantExists bool
	}{
		"success": {wantExists: true},
		"failure": {writeErr: writeErr, wantErr: writeErr},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			pw, err := NewJSONWriter(ctx, "out.parquet", WriteOption{}, schema)
			require.NoError(t, err)
			require.NoError(t, pw.WriteWithContext(ctx, `{"value":"alpha"}`))

			err = FinalizeWriter(ctx, &pw.ParquetWriter, "out.parquet", tc.writeErr)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			data, err := os.ReadFile("out.parquet")
			if !tc.wantExists {
				require.True(t, os.IsNotExist(err))
				return
			}
			require.NoError(t, err)
			require.Equal(t, []byte("PAR1"), data[:4])
			require.Equal(t, []byte("PAR1"), data[len(data)-4:])
		})
	}
}

func TestDiscardDestination(t *testing.T) {
	// Remote URIs have no delete through the writer interface and must not panic.
	DiscardDestination("s3://bucket/prefix/object.parquet")
	DiscardDestination("http://[::1")

	path := filepath.Join(t.TempDir(), "partial.parquet")
	require.NoError(t, os.WriteFile(path, []byte("PAR1partial"), 0o600))
	DiscardDestination(path)
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
}
