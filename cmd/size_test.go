package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_SizeCmd_Run_non_existent_file(t *testing.T) {
	cmd := &SizeCmd{}
	cmd.URI = "file/does/not/exist"

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open local")
}

func Test_SizeCmd_Run_invalid_query(t *testing.T) {
	cmd := &SizeCmd{}
	cmd.Query = "invalid"
	cmd.URI = "../testdata/all-types.parquet"

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unknown query type")
}

func Test_SizeCmd_Run_good_raw(t *testing.T) {
	cmd := &SizeCmd{}
	cmd.Query = "raw"
	cmd.URI = "../testdata/all-types.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, "18533\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_raw_json(t *testing.T) {
	cmd := &SizeCmd{}
	cmd.Query = "raw"
	cmd.JSON = true
	cmd.URI = "../testdata/all-types.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, `{"Raw":18533}`+"\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_uncompressed(t *testing.T) {
	cmd := &SizeCmd{}
	cmd.Query = "uncompressed"
	cmd.URI = "../testdata/all-types.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, "27169\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_uncompressed_json(t *testing.T) {
	cmd := &SizeCmd{}
	cmd.Query = "uncompressed"
	cmd.JSON = true
	cmd.URI = "../testdata/all-types.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, `{"Uncompressed":27169}`+"\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_footer(t *testing.T) {
	cmd := &SizeCmd{}
	cmd.Query = "footer"
	cmd.URI = "../testdata/all-types.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, "6674\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_footer_json(t *testing.T) {
	cmd := &SizeCmd{}
	cmd.Query = "footer"
	cmd.JSON = true
	cmd.URI = "../testdata/all-types.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, `{"Footer":6674}`+"\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_all(t *testing.T) {
	cmd := &SizeCmd{}
	cmd.Query = "all"
	cmd.URI = "../testdata/all-types.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, "18533 27169 6674\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_all_json(t *testing.T) {
	cmd := &SizeCmd{}
	cmd.Query = "all"
	cmd.JSON = true
	cmd.URI = "../testdata/all-types.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, `{"Raw":18533,"Uncompressed":27169,"Footer":6674}`+"\n", stdout)
	require.Equal(t, "", stderr)
}
