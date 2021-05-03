package cmd

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_VersionCmd_Run_panic(t *testing.T) {
	cmd := &VersionCmd{}
	assert.Panics(t, func() { cmd.Run(nil) })
	ctx := Context{}
	assert.NotPanics(t, func() { cmd.Run(&ctx) })
}

func Test_VersionCmd_Run_good(t *testing.T) {
	cmd := &VersionCmd{}
	ctx := Context{
		Version: "the-version",
		Build:   "the-build",
	}

	savedStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	cmd.Run(&ctx)
	w.Close()
	out, _ := ioutil.ReadAll(r)
	os.Stdout = savedStdout

	assert.Equal(t, string(out), "Version: the-version\nBuild Time: the-build\n")
}
