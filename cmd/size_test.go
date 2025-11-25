package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
)

func TestSizeCmd(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    SizeCmd
		stdout string
		errMsg string
	}{
		// error cases
		"non-existent-file": {cmd: SizeCmd{URI: "file/does/not/exist"}, errMsg: "no such file or directory"},
		"invalid-query":     {cmd: SizeCmd{Query: "invalid", URI: "../testdata/good.parquet"}, errMsg: "unknown query type"},
		// good cases
		"raw":               {cmd: SizeCmd{ReadOption: rOpt, Query: "raw", JSON: false, URI: "../testdata/good.parquet"}, stdout: "588\n"},
		"raw-json":          {cmd: SizeCmd{ReadOption: rOpt, Query: "raw", JSON: true, URI: "../testdata/good.parquet"}, stdout: `{"Raw":588}` + "\n"},
		"uncompressed":      {cmd: SizeCmd{ReadOption: rOpt, Query: "uncompressed", JSON: false, URI: "../testdata/good.parquet"}, stdout: "438\n"},
		"uncompressed-json": {cmd: SizeCmd{ReadOption: rOpt, Query: "uncompressed", JSON: true, URI: "../testdata/good.parquet"}, stdout: `{"Uncompressed":438}` + "\n"},
		"footer":            {cmd: SizeCmd{ReadOption: rOpt, Query: "footer", JSON: false, URI: "../testdata/good.parquet"}, stdout: "335\n"},
		"footer-json":       {cmd: SizeCmd{ReadOption: rOpt, Query: "footer", JSON: true, URI: "../testdata/good.parquet"}, stdout: `{"Footer":335}` + "\n"},
		"all":               {cmd: SizeCmd{ReadOption: rOpt, Query: "all", JSON: false, URI: "../testdata/good.parquet"}, stdout: "588 438 335\n"},
		"all-json":          {cmd: SizeCmd{ReadOption: rOpt, Query: "all", JSON: true, URI: "../testdata/good.parquet"}, stdout: `{"Raw":588,"Uncompressed":438,"Footer":335}` + "\n"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if tc.errMsg == "" {
				t.Parallel()
			}
			if tc.errMsg != "" {
				err := tc.cmd.Run()
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				stdout, stderr := captureStdoutStderr(func() {
					require.NoError(t, tc.cmd.Run())
				})
				require.Equal(t, tc.stdout, stdout)
				require.Equal(t, "", stderr)
			}
		})
	}
}

func BenchmarkSizeCmd(b *testing.B) {
	savedStdout, savedStderr := os.Stdout, os.Stderr
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0o666)
	if err != nil {
		panic(err)
	}
	os.Stdout = devNull
	defer func() {
		os.Stdout, os.Stderr = savedStdout, savedStderr
		_ = devNull.Close()
	}()

	cmd := SizeCmd{
		ReadOption: pio.ReadOption{},
		Query:      "all",
		URI:        "../build/benchmark.parquet",
	}
	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}
