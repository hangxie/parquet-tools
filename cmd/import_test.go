package cmd

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ImportCmd_Run_good(t *testing.T) {
	testFile := os.TempDir() + "/import.parquet"
	os.Remove(testFile)
	cmd := &ImportCmd{
		Source: "testdata/csv.source",
		Schema: "testdata/csv.schema",
		Format: "csv",
		CommonOption: CommonOption{
			URI: testFile,
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})

	assert.Equal(t, stdout, "")
	assert.Equal(t, stderr, "")

	_, err := os.Stat(testFile)
	assert.Nil(t, err)
}

func Test_ImportCmd_Run_bad_schema_file(t *testing.T) {
	cmd := &ImportCmd{
		Schema: "file/does/not/exist",
		Format: "csv",
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to load schema from")
}

func Test_ImportCmd_Run_invalid_format(t *testing.T) {
	cmd := &ImportCmd{
		Schema: "testdata/csv.schema",
		Format: "random",
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "is not a recognized source format")
}

func Test_ImportCmd_importCSV_invalid_uri(t *testing.T) {
	cmd := &ImportCmd{}

	err := cmd.importCSV("testdata/csv.source", "://uri", "")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_ImportCmd_importCSV_non_existent_source(t *testing.T) {
	cmd := &ImportCmd{}

	// non-existent source
	err := cmd.importCSV("some-source", "s3://target", "")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open CSV file")
}

func Test_ImportCmd_importCSV_fail_to_write(t *testing.T) {
	// fail to write
	cmd := &ImportCmd{}
	schema, _ := ioutil.ReadFile("testdata/csv.schema")

	err := cmd.importCSV("testdata/csv.source", "s3://target", string(schema))
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to close Parquet file")
}

func Test_ImportCmd_importCSV_good(t *testing.T) {
	cmd := &ImportCmd{}
	schema, _ := ioutil.ReadFile("testdata/csv.schema")

	err := cmd.importCSV("testdata/csv.source", os.TempDir()+"/import-csv.parquet", string(schema))
	assert.Nil(t, err)
}

func Test_ImportCmd_importJson_good(t *testing.T) {
	cmd := &ImportCmd{
		Schema: "testdata/csv.schema",
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, "To be implemented.\n")
	assert.Equal(t, stderr, "")
}
