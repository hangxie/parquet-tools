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

func Test_VersionCmd_Run_good(t *testing.T) {
	cmd := &VersionCmd{}
	ctx := Context{
		Version: "the-version",
		Build:   "the-build",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&ctx))
	})
	assert.Equal(t, stdout, "Version: the-version\nBuild Time: the-build\n")
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
	assert.Equal(t, stdout, `{"Version":"the-version","BuildTime":"the-build"}`+"\n")
	assert.Equal(t, stderr, "")
}
