package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SizeCmd_Run_non_existent_file(t *testing.T) {
	cmd := &SizeCmd{
		CommonOption: CommonOption{
			URI: "file/does/not/exist",
		},
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open local")
}

func Test_SizeCmd_Run_invalid_query(t *testing.T) {
	cmd := &SizeCmd{
		Query: "invalid",
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unknown query type")
}

func Test_SizeCmd_Run_good_raw(t *testing.T) {
	cmd := &SizeCmd{
		Query: "raw",
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, "10120\n")
	assert.Equal(t, stderr, "")
}

func Test_SizeCmd_Run_good_raw_json(t *testing.T) {
	cmd := &SizeCmd{
		Query: "raw",
		JSON:  true,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Raw":10120}`+"\n")
	assert.Equal(t, stderr, "")
}

func Test_SizeCmd_Run_good_uncompressed(t *testing.T) {
	cmd := &SizeCmd{
		Query: "uncompressed",
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, "10829\n")
	assert.Equal(t, stderr, "")
}

func Test_SizeCmd_Run_good_uncompressed_json(t *testing.T) {
	cmd := &SizeCmd{
		Query: "uncompressed",
		JSON:  true,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Uncompressed":10829}`+"\n")
	assert.Equal(t, stderr, "")
}

func Test_SizeCmd_Run_good_footer(t *testing.T) {
	cmd := &SizeCmd{
		Query: "footer",
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, "4416\n")
	assert.Equal(t, stderr, "")
}

func Test_SizeCmd_Run_good_footer_json(t *testing.T) {
	cmd := &SizeCmd{
		Query: "footer",
		JSON:  true,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Footer":4416}`+"\n")
	assert.Equal(t, stderr, "")
}

func Test_SizeCmd_Run_good_all(t *testing.T) {
	cmd := &SizeCmd{
		Query: "all",
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, "10120 10829 4416\n")
	assert.Equal(t, stderr, "")
}

func Test_SizeCmd_Run_good_all_json(t *testing.T) {
	cmd := &SizeCmd{
		Query: "all",
		JSON:  true,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Raw":10120,"Uncompressed":10829,"Footer":4416}`+"\n")
	assert.Equal(t, stderr, "")
}
