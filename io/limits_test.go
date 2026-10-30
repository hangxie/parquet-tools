package io

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildLimitOptions(t *testing.T) {
	testCases := map[string]struct {
		option  ReadOption
		numOpts int
		errMsg  string
	}{
		"defaults":           {option: ReadOption{}, numOpts: 0},
		"all-set":            {option: ReadOption{MaxPageSize: 1024, MaxDecompressedSize: 2048, MaxDecodedBatchBytes: 4096}, numOpts: 3},
		"no-limit":           {option: ReadOption{MaxPageSize: -1, MaxDecompressedSize: -1, MaxDecodedBatchBytes: -1}, numOpts: 3},
		"only-batch":         {option: ReadOption{MaxDecodedBatchBytes: 512 << 20}, numOpts: 1},
		"bad-page-size":      {option: ReadOption{MaxPageSize: -2}, errMsg: "invalid --max-page-size -2"},
		"bad-decompressed":   {option: ReadOption{MaxDecompressedSize: -10}, errMsg: "invalid --max-decompressed-size -10"},
		"bad-decoded-batch":  {option: ReadOption{MaxDecodedBatchBytes: -3}, errMsg: "invalid --max-decoded-batch-bytes -3"},
		"bad-after-good-one": {option: ReadOption{MaxPageSize: 1, MaxDecodedBatchBytes: -2}, errMsg: "invalid --max-decoded-batch-bytes"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			opts, err := buildLimitOptions(tc.option)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Len(t, opts, tc.numOpts)
		})
	}
}

func TestBuildReaderOptionsRejectsBadLimit(t *testing.T) {
	_, err := buildReaderOptions(ReadOption{MaxPageSize: -2})
	require.ErrorContains(t, err, "invalid --max-page-size")
}

func TestLimitFlagHint(t *testing.T) {
	sentinel := errors.New("decompressed data size exceeds configured limit (see reader.WithMaxDecompressedSize)")
	testCases := map[string]struct {
		err  error
		want string
	}{
		"page-size": {
			err:  fmt.Errorf("read page: %w", errors.New("page size 300 exceeds limit 10 (see reader.WithMaxPageSize)")),
			want: "read page: page size 300 exceeds limit 10 (see --max-page-size)",
		},
		"decompressed": {
			err:  fmt.Errorf("uncompress SNAPPY: %w", sentinel),
			want: "uncompress SNAPPY: decompressed data size exceeds configured limit (see --max-decompressed-size)",
		},
		"decoded-batch": {
			err:  errors.New("byte array of 9 bytes exceeds remaining batch budget of 2 bytes under the 2-byte limit (see reader.WithMaxDecodedBatchBytes)"),
			want: "byte array of 9 bytes exceeds remaining batch budget of 2 bytes under the 2-byte limit (see --max-decoded-batch-bytes)",
		},
		"unrelated": {
			err:  errors.New("no such file or directory"),
			want: "no such file or directory",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			got := LimitFlagHint(tc.err)
			require.EqualError(t, got, tc.want)
			require.ErrorIs(t, got, tc.err)
		})
	}

	t.Run("wrapped-sentinel", func(t *testing.T) {
		require.ErrorIs(t, LimitFlagHint(fmt.Errorf("read: %w", sentinel)), sentinel)
	})
	t.Run("nil", func(t *testing.T) {
		require.NoError(t, LimitFlagHint(nil))
	})
}
