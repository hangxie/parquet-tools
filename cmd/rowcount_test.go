package cmd

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_RowCountCmd_Run_non_existent(t *testing.T) {
	cmd := &RowCountCmd{
		CommonOption: CommonOption{
			URI: "path/to/non-existent/file",
		},
	}
	ctx := Context{}

	err := cmd.Run(&ctx)
	assert.NotEqual(t, err, nil)
	assert.Contains(t, err.Error(), string("failed to open local file"))
}

func Test_RowCountCmd_Run_good(t *testing.T) {
	cmd := &RowCountCmd{
		CommonOption: CommonOption{
			URI: "file://./testdata/good.parquet",
		},
	}
	ctx := Context{}

	savedStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	cmd.Run(&ctx)
	w.Close()
	out, _ := ioutil.ReadAll(r)
	os.Stdout = savedStdout

	assert.Equal(t, string(out), "4\n")
}
