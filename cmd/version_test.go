package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func setupTest() {
	version = "the-version"
	build = "the-build"
	gitHash = "the-hash"
}

func Test_VersionCmd_Run_good_plain(t *testing.T) {
	setupTest()
	cmd := &VersionCmd{}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	require.Equal(t, "the-version\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_VersionCmd_Run_good_plain_with_build_time(t *testing.T) {
	setupTest()
	cmd := &VersionCmd{}
	cmd.BuildTime = true

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	require.Equal(t, "the-version\nthe-build\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_VersionCmd_Run_good_plain_with_git_hash(t *testing.T) {
	setupTest()
	cmd := &VersionCmd{}
	cmd.GitHash = true

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	require.Equal(t, "the-version\nthe-hash\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_VersionCmd_Run_good_json(t *testing.T) {
	setupTest()
	cmd := &VersionCmd{}
	cmd.JSON = true

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	require.Equal(t, `{"Version":"the-version"}`+"\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_VersionCmd_Run_good_json_with_build_time(t *testing.T) {
	setupTest()
	cmd := &VersionCmd{}
	cmd.JSON = true
	cmd.BuildTime = true

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	require.Equal(t, `{"Version":"the-version","BuildTime":"the-build"}`+"\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_VersionCmd_Run_good_json_with_build_time_and_git_hash(t *testing.T) {
	setupTest()
	cmd := &VersionCmd{}
	cmd.JSON = true
	cmd.BuildTime = true
	cmd.GitHash = true

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	require.Equal(t, `{"Version":"the-version","BuildTime":"the-build","GitHash":"the-hash"}`+"\n", stdout)
	require.Equal(t, "", stderr)
}
