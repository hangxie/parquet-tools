package rowcount

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
	t.Run("non-existent", func(t *testing.T) {
		cmd := &Cmd{}
		cmd.URI = "file/does/not/exist"

		err := cmd.Run()
		require.Error(t, err)
		require.Contains(t, err.Error(), "no such file or directory")
	})

	t.Run("good", func(t *testing.T) {
		cmd := &Cmd{}
		cmd.URI = "../../testdata/good.parquet"

		stdout, stderr := testutils.CaptureStdoutStderr(func() {
			require.Nil(t, cmd.Run())
		})
		require.Equal(t, "3\n", stdout)
		require.Equal(t, "", stderr)
	})
}

func TestCmdEncrypted(t *testing.T) {
	encReadOption := pio.ReadOption{
		FooterKey:  encFooterKey,
		ColumnKeys: []string{"double_field=" + encDoubleKey, "float_field=" + encFloatKey},
	}
	testCases := map[string]struct {
		cmd    Cmd
		stdout string
		errMsg string
	}{
		"footer": {
			cmd:    Cmd{ReadOption: encReadOption, URI: "../../testdata/encrypted-footer.parquet"},
			stdout: "50\n",
		},
		"columns": {
			cmd:    Cmd{ReadOption: encReadOption, URI: "../../testdata/encrypted-columns.parquet"},
			stdout: "50\n",
		},
		"aad": {
			cmd:    Cmd{ReadOption: pio.ReadOption{FooterKey: encFooterKey, ColumnKeys: []string{"double_field=" + encDoubleKey, "float_field=" + encFloatKey}, AADPrefix: encAADPrefix}, URI: "../../testdata/encrypted-aad.parquet"},
			stdout: "50\n",
		},
		"uniform": {
			cmd:    Cmd{ReadOption: pio.ReadOption{FooterKey: encFooterKey}, URI: "../../testdata/uniform-encryption.parquet"},
			stdout: "50\n",
		},
		"no-key": {
			cmd:    Cmd{ReadOption: pio.ReadOption{}, URI: "../../testdata/encrypted-footer.parquet"},
			errMsg: "decryption key required for footer",
		},
		"wrong-key": {
			cmd:    Cmd{ReadOption: pio.ReadOption{FooterKey: encWrongKey}, URI: "../../testdata/encrypted-footer.parquet"},
			errMsg: "decrypt",
		},
		// Mixed plaintext/encrypted: row count only needs row-group metadata, so a footer
		// key alone is enough even when some column keys are missing.
		"footer-only-mixed": {
			cmd:    Cmd{ReadOption: pio.ReadOption{FooterKey: encFooterKey}, URI: "../../testdata/encrypted-columns.parquet"},
			stdout: "50\n",
		},
		// Plaintext-signed footer in encrypted-columns.parquet does not require any key for row count.
		"no-key-mixed": {
			cmd:    Cmd{ReadOption: pio.ReadOption{}, URI: "../../testdata/encrypted-columns.parquet"},
			stdout: "50\n",
		},
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
				return
			}
			stdout, stderr := testutils.CaptureStdoutStderr(func() {
				require.NoError(t, tc.cmd.Run())
			})
			require.Equal(t, tc.stdout, stdout)
			require.Equal(t, "", stderr)
		})
	}
}

func BenchmarkRowCountCmd(b *testing.B) {
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
