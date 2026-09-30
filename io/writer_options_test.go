package io

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/hangxie/parquet-go/v3/types"
	"github.com/stretchr/testify/require"
)

func TestParseValueMode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		want    types.ValueMode
		invalid bool
	}{
		{"", types.ValueModeInterpreted, false}, {"interpreted", types.ValueModeInterpreted, false}, {"raw", types.ValueModeRaw, false}, {"auto", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseValueMode(tc.name)
			if tc.invalid {
				require.ErrorContains(t, err, "invalid value mode")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestWriterValueModeRejectsInvalidMode(t *testing.T) {
	_, err := NewJSONWriter(context.Background(), filepath.Join(t.TempDir(), "output.parquet"), WriteOption{ValueMode: types.ValueMode(99)}, `{"Tag":"name=root","Fields":[{"Tag":"name=Value, type=INT64"}]}`)
	require.ErrorContains(t, err, "unsupported value mode")
}
