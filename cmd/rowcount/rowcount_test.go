package rowcount

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/internal/testutils"

	pio "github.com/hangxie/parquet-tools/io"
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

func BenchmarkRowCountCmd(b *testing.B) {
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

	cmd := Cmd{
		ReadOption: pio.ReadOption{},
		URI:        "../../build/benchmark.parquet",
	}
	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}
