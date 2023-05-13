package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_RowCountCmd_Run_non_existent(t *testing.T) {
	cmd := &RowCountCmd{}
	cmd.URI = "file/does/not/exist"

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), string("failed to open local file"))
}

func Test_RowCountCmd_Run_good(t *testing.T) {
	cmd := &RowCountCmd{}
	cmd.URI = "../testdata/good.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, "3\n", stdout)
	require.Equal(t, "", stderr)
}
