package cmd

import (
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
		cmd.Run(&Context{})
	})

	assert.Equal(t, stdout, "")
	assert.Equal(t, stderr, "")

	_, err := os.Stat(testFile)
	assert.Nil(t, err)
}

func Test_ImportCmd_Run_bad_schema_file(t *testing.T) {
	cmd := &ImportCmd{
		Schema: "testdata/does-not-exist",
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

func Test_importCSV(t *testing.T) {
	cmd := &ImportCmd{}
	var err error
	schema := `
name=Id, type=INT64
name=Name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY
name=Age, type=INT32
name=Temperature, type=FLOAT
name=Vaccinated, type=BOOLEAN
`

	// invalid target
	err = cmd.importCSV("some-source", "bad://target", "")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unknown location scheme")

	// invalid schema
	assert.Panics(t, func() {
		cmd.importCSV("some-source", "s3://target", "bad schema")
	})

	// non-existent source
	err = cmd.importCSV("some-source", "s3://target", "")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open CSV file")

	// fail to write
	err = cmd.importCSV("testdata/csv.source", "s3://target", schema)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to close Parquet file")
}

func Test_importJson(t *testing.T) {
	cmd := &ImportCmd{
		Schema: "testdata/csv.schema",
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		cmd.Run(&Context{})
	})
	assert.Equal(t, stdout, "TBD\n")
	assert.Equal(t, stderr, "")
}
