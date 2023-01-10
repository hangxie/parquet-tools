package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_VersionCmd_Run_invalid_call(t *testing.T) {
	cmd := &VersionCmd{}
	err := cmd.Run(nil)
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "cannot retrieve build information")
}

func Test_VersionCmd_Run_good_plain(t *testing.T) {
	cmd := &VersionCmd{}
	ctx := Context{
		Version: "the-version",
		Build:   "the-build",
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&ctx))
	})
	require.Equal(t, "the-version\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_VersionCmd_Run_good_plain_with_build_time(t *testing.T) {
	cmd := &VersionCmd{
		BuildTime: true,
	}
	ctx := Context{
		Version: "the-version",
		Build:   "the-build",
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&ctx))
	})
	require.Equal(t, "the-version\nthe-build\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_VersionCmd_Run_good_json(t *testing.T) {
	cmd := &VersionCmd{
		JSON: true,
	}
	ctx := Context{
		Version: "the-version",
		Build:   "the-build",
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&ctx))
	})
	require.Equal(t, `{"Version":"the-version"}`+"\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_VersionCmd_Run_good_json_with_build_time(t *testing.T) {
	cmd := &VersionCmd{
		JSON:      true,
		BuildTime: true,
	}
	ctx := Context{
		Version: "the-version",
		Build:   "the-build",
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&ctx))
	})
	require.Equal(t, `{"Version":"the-version","BuildTime":"the-build"}`+"\n", stdout)
	require.Equal(t, "", stderr)
}
