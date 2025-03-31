package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

func Test_SizeCmd_Run_error(t *testing.T) {
	testCases := map[string]struct {
		cmd    SizeCmd
		errMsg string
	}{
		"non-existent-file": {SizeCmd{URI: "file/does/not/exist"}, "failed to open local"},
		"invalid-query":     {SizeCmd{Query: "invalid", URI: "../testdata/all-types.parquet"}, "unknown query type"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.NotNil(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func Test_SizeCmd_Run_good(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    SizeCmd
		stdout string
	}{
		"raw":               {SizeCmd{rOpt, "raw", false, "../testdata/all-types.parquet"}, "18482\n"},
		"raw-json":          {SizeCmd{rOpt, "raw", true, "../testdata/all-types.parquet"}, `{"Raw":18482}` + "\n"},
		"uncompressed":      {SizeCmd{rOpt, "uncompressed", false, "../testdata/all-types.parquet"}, "27158\n"},
		"uncompressed-json": {SizeCmd{rOpt, "uncompressed", true, "../testdata/all-types.parquet"}, `{"Uncompressed":27158}` + "\n"},
		"footer":            {SizeCmd{rOpt, "footer", false, "../testdata/all-types.parquet"}, "6674\n"},
		"footer-json":       {SizeCmd{rOpt, "footer", true, "../testdata/all-types.parquet"}, `{"Footer":6674}` + "\n"},
		"all":               {SizeCmd{rOpt, "all", false, "../testdata/all-types.parquet"}, "18482 27158 6674\n"},
		"all-json":          {SizeCmd{rOpt, "all", true, "../testdata/all-types.parquet"}, `{"Raw":18482,"Uncompressed":27158,"Footer":6674}` + "\n"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			stdout, stderr := captureStdoutStderr(func() {
				require.Nil(t, tc.cmd.Run())
			})
			require.Equal(t, tc.stdout, stdout)
			require.Equal(t, "", stderr)
		})
	}
}
