package size

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/internal/testutils"

	pio "github.com/hangxie/parquet-tools/io"
)

const (
	encFooterKey = "MDEyMzQ1Njc4OTAxMjM0NQ=="
	encDoubleKey = "MTIzNDU2Nzg5MDEyMzQ1MA=="
	encFloatKey  = "MTIzNDU2Nzg5MDEyMzQ1MQ=="
	encAADPrefix = "dGVzdGVy"
	encWrongKey  = "d3Jvbmd3cm9uZ3dyb25nMQ=="
)

func TestCmd(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    Cmd
		stdout string
		errMsg string
	}{
		// error cases
		"non-existent-file":     {cmd: Cmd{URI: "file/does/not/exist"}, errMsg: "no such file or directory"},
		"invalid-query":         {cmd: Cmd{Query: "invalid", URI: "../../testdata/good.parquet"}, errMsg: "unknown query type"},
		"encrypted-no-key":      {cmd: Cmd{Query: "raw", URI: "../../testdata/encrypted-footer.parquet"}, errMsg: "decryption key required for footer"},
		"encrypted-wrong-key":   {cmd: Cmd{ReadOption: pio.ReadOption{FooterKey: encWrongKey}, Query: "raw", URI: "../../testdata/encrypted-footer.parquet"}, errMsg: "decrypt"},
		"encrypted-missing-col": {cmd: Cmd{ReadOption: pio.ReadOption{FooterKey: encFooterKey}, Query: "raw", URI: "../../testdata/encrypted-columns.parquet"}, errMsg: "decryption key required for column"},
		// good cases
		"raw":               {cmd: Cmd{ReadOption: rOpt, Query: "raw", JSON: false, URI: "../../testdata/good.parquet"}, stdout: "588\n"},
		"raw-json":          {cmd: Cmd{ReadOption: rOpt, Query: "raw", JSON: true, URI: "../../testdata/good.parquet"}, stdout: `{"Raw":588}` + "\n"},
		"uncompressed":      {cmd: Cmd{ReadOption: rOpt, Query: "uncompressed", JSON: false, URI: "../../testdata/good.parquet"}, stdout: "438\n"},
		"uncompressed-json": {cmd: Cmd{ReadOption: rOpt, Query: "uncompressed", JSON: true, URI: "../../testdata/good.parquet"}, stdout: `{"Uncompressed":438}` + "\n"},
		"footer":            {cmd: Cmd{ReadOption: rOpt, Query: "footer", JSON: false, URI: "../../testdata/good.parquet"}, stdout: "335\n"},
		"footer-json":       {cmd: Cmd{ReadOption: rOpt, Query: "footer", JSON: true, URI: "../../testdata/good.parquet"}, stdout: `{"Footer":335}` + "\n"},
		"all":               {cmd: Cmd{ReadOption: rOpt, Query: "all", JSON: false, URI: "../../testdata/good.parquet"}, stdout: "588 438 335\n"},
		"all-json":          {cmd: Cmd{ReadOption: rOpt, Query: "all", JSON: true, URI: "../../testdata/good.parquet"}, stdout: `{"Raw":588,"Uncompressed":438,"Footer":335}` + "\n"},
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
				stdout, stderr := testutils.CaptureStdoutStderr(func() {
					require.NoError(t, tc.cmd.Run())
				})
				require.Equal(t, tc.stdout, stdout)
				require.Equal(t, "", stderr)
			}
		})
	}
}

func TestCmdEncrypted(t *testing.T) {
	encReadOption := pio.ReadOption{
		FooterKey:  encFooterKey,
		ColumnKeys: []string{"double_field=" + encDoubleKey, "float_field=" + encFloatKey},
	}
	testCases := map[string]Cmd{
		"footer":  {ReadOption: encReadOption, Query: "raw", URI: "../../testdata/encrypted-footer.parquet"},
		"columns": {ReadOption: encReadOption, Query: "raw", URI: "../../testdata/encrypted-columns.parquet"},
		"uniform": {ReadOption: pio.ReadOption{FooterKey: encFooterKey}, Query: "raw", URI: "../../testdata/uniform-encryption.parquet"},
		"aad": {
			ReadOption: pio.ReadOption{
				FooterKey:  encFooterKey,
				ColumnKeys: []string{"double_field=" + encDoubleKey, "float_field=" + encFloatKey},
				AADPrefix:  encAADPrefix,
			},
			Query: "raw",
			URI:   "../../testdata/encrypted-aad.parquet",
		},
	}

	for name, cmd := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			stdout, stderr := testutils.CaptureStdoutStderr(func() {
				require.NoError(t, cmd.Run())
			})
			require.NotEmpty(t, stdout)
			require.Equal(t, "", stderr)
		})
	}
}

func BenchmarkSizeCmd(b *testing.B) {
	savedStdout, savedStderr := os.Stdout, os.Stderr
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0o666)
	if err != nil {
		b.Fatal(err)
	}
	os.Stdout = devNull
	defer func() {
		os.Stdout, os.Stderr = savedStdout, savedStderr
		_ = devNull.Close()
	}()

	cmd := Cmd{
		ReadOption: pio.ReadOption{},
		Query:      "all",
		URI:        "../../build/benchmark.parquet",
	}

	// Warm up the Go runtime before actual benchmark
	for range 10 {
		_ = cmd.Run()
	}

	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}
