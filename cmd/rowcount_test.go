package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_RowCountCmd_Run_non_existent(t *testing.T) {
	cmd := &RowCountCmd{
		CommonOption: CommonOption{
			URI: "file/does/not/exist",
		},
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), string("failed to open local file"))
}

func Test_RowCountCmd_Run_good(t *testing.T) {
	cmd := &RowCountCmd{
		CommonOption: CommonOption{
			URI: "testdata/good.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, "4\n", stdout)
	assert.Equal(t, "", stderr)
}
