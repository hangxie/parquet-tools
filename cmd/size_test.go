package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
)

func Test_SizeCmd_Run_error(t *testing.T) {
	testCases := map[string]struct {
		cmd    SizeCmd
		errMsg string
	}{
		"non-existent-file": {SizeCmd{URI: "file/does/not/exist"}, "no such file or directory"},
		"invalid-query":     {SizeCmd{Query: "invalid", URI: "../testdata/good.parquet"}, "unknown query type"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.Error(t, err)
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
		"raw":               {SizeCmd{ReadOption: rOpt, Query: "raw", JSON: false, URI: "../testdata/good.parquet"}, "588\n"},
		"raw-json":          {SizeCmd{ReadOption: rOpt, Query: "raw", JSON: true, URI: "../testdata/good.parquet"}, `{"Raw":588}` + "\n"},
		"uncompressed":      {SizeCmd{ReadOption: rOpt, Query: "uncompressed", JSON: false, URI: "../testdata/good.parquet"}, "438\n"},
		"uncompressed-json": {SizeCmd{ReadOption: rOpt, Query: "uncompressed", JSON: true, URI: "../testdata/good.parquet"}, `{"Uncompressed":438}` + "\n"},
		"footer":            {SizeCmd{ReadOption: rOpt, Query: "footer", JSON: false, URI: "../testdata/good.parquet"}, "323\n"},
		"footer-json":       {SizeCmd{ReadOption: rOpt, Query: "footer", JSON: true, URI: "../testdata/good.parquet"}, `{"Footer":323}` + "\n"},
		"all":               {SizeCmd{ReadOption: rOpt, Query: "all", JSON: false, URI: "../testdata/good.parquet"}, "588 438 323\n"},
		"all-json":          {SizeCmd{ReadOption: rOpt, Query: "all", JSON: true, URI: "../testdata/good.parquet"}, `{"Raw":588,"Uncompressed":438,"Footer":323}` + "\n"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			stdout, stderr := captureStdoutStderr(func() {
				require.Nil(t, tc.cmd.Run())
			})
			require.Equal(t, tc.stdout, stdout)
			require.Equal(t, "", stderr)
		})
	}
}

func Benchmark_SizeCmd_Run(b *testing.B) {
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
