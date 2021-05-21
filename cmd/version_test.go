package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_VersionCmd_Run_panic(t *testing.T) {
	cmd := &VersionCmd{}
	assert.Panics(t, func() { assert.NotNil(t, cmd.Run(nil)) })
	ctx := Context{}
	assert.NotPanics(t, func() { assert.Nil(t, cmd.Run(&ctx)) })
}

func Test_VersionCmd_Run_good_plain(t *testing.T) {
	cmd := &VersionCmd{}
	ctx := Context{
		Version: "the-version",
		Build:   "the-build",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&ctx))
	})
	assert.Equal(t, stdout, "the-version\n")
	assert.Equal(t, stderr, "")
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
		assert.Nil(t, cmd.Run(&ctx))
	})
	assert.Equal(t, stdout, "the-version\nthe-build\n")
	assert.Equal(t, stderr, "")
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
		assert.Nil(t, cmd.Run(&ctx))
	})
	assert.Equal(t, stdout, `{"Version":"the-version"}`+"\n")
	assert.Equal(t, stderr, "")
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
		assert.Nil(t, cmd.Run(&ctx))
	})
	assert.Equal(t, stdout, `{"Version":"the-version","BuildTime":"the-build"}`+"\n")
	assert.Equal(t, stderr, "")
}
