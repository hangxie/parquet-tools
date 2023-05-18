package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_VersionCmd_Run_good_plain(t *testing.T) {
	cmd := &VersionCmd{}
	version = "the-version"
	build = "the-build"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	require.Equal(t, "the-version\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_VersionCmd_Run_good_plain_with_build_time(t *testing.T) {
	cmd := &VersionCmd{
		BuildTime: true,
	}
	version = "the-version"
	build = "the-build"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	require.Equal(t, "the-version\nthe-build\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_VersionCmd_Run_good_json(t *testing.T) {
	cmd := &VersionCmd{
		JSON: true,
	}
	version = "the-version"
	build = "the-build"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	require.Equal(t, `{"Version":"the-version"}`+"\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_VersionCmd_Run_good_json_with_build_time(t *testing.T) {
	cmd := &VersionCmd{
		JSON:      true,
		BuildTime: true,
	}
	version = "the-version"
	build = "the-build"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	require.Equal(t, `{"Version":"the-version","BuildTime":"the-build"}`+"\n", stdout)
	require.Equal(t, "", stderr)
}
