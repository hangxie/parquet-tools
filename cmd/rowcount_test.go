package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

func Test_RowCountCmd_Run_non_existent(t *testing.T) {
	cmd := &RowCountCmd{}
	cmd.URI = "file/does/not/exist"

	err := cmd.Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "no such file or directory")
}

func Test_RowCountCmd_Run_good(t *testing.T) {
	cmd := &RowCountCmd{}
	cmd.URI = "../testdata/good.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	require.Equal(t, "3\n", stdout)
	require.Equal(t, "", stderr)
}

func Benchmark_RowCountCmd_Run(b *testing.B) {
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

	cmd := RowCountCmd{
		ReadOption: pio.ReadOption{},
		URI:        "../build/benchmark.parquet",
	}
	b.Run("default", func(b *testing.B) {
		require.NoError(b, cmd.Run())
	})
}
