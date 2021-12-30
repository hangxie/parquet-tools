package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ImportCmd_Run_CSV_good(t *testing.T) {
	testFile := os.TempDir() + "/import-csv.parquet"
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

func Test_ImportCmd_Run_JSON_good(t *testing.T) {
	testFile := os.TempDir() + "/import-json.parquet"
	os.Remove(testFile)
	cmd := &ImportCmd{
		Source: "testdata/json.source",
		Schema: "testdata/json.schema",
		Format: "json",
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

func Test_ImportCmd_Run_invalid_format(t *testing.T) {
	cmd := &ImportCmd{
		Schema: "testdata/csv.schema",
		Format: "random",
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "is not a recognized source format")
}

func Test_ImportCmd_importCSV_bad_schema_file(t *testing.T) {
	cmd := &ImportCmd{
		Schema: "file/does/not/exist",
		Format: "csv",
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to load schema from")
}

func Test_ImportCmd_importCSV_invalid_uri(t *testing.T) {
	cmd := &ImportCmd{
		Format: "csv",
		Schema: "testdata/csv.schema",
		Source: "testdata/csv.source",
		CommonOption: CommonOption{
			URI: "://uri",
		},
	}

	err := cmd.importCSV()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_ImportCmd_importCSV_non_existent_source(t *testing.T) {
	cmd := &ImportCmd{
		Format: "csv",
		Schema: "testdata/csv.schema",
		Source: "file/does/not/exist",
		CommonOption: CommonOption{
			URI: "s3://target",
		},
	}

	// non-existent source
	err := cmd.importCSV()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open CSV file")
}

func Test_ImportCmd_importCSV_fail_to_write(t *testing.T) {
	// fail to write
	cmd := &ImportCmd{
		Format: "csv",
		Schema: "testdata/csv.schema",
		Source: "testdata/csv.source",
		CommonOption: CommonOption{
			URI: "s3://target",
		},
	}

	err := cmd.importCSV()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to close Parquet file")
}

func Test_ImportCmd_importCSV_good(t *testing.T) {
	cmd := &ImportCmd{
		Format: "csv",
		Schema: "testdata/csv.schema",
		Source: "testdata/csv.source",
		CommonOption: CommonOption{
			URI: os.TempDir() + "/import-csv.parquet",
		},
	}

	err := cmd.importCSV()
	assert.Nil(t, err)
}

func Test_ImportCmd_importJSON_bad_schema_file(t *testing.T) {
	cmd := &ImportCmd{
		Schema: "file/does/not/exist",
		Format: "json",
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to load schema from")
}

func Test_ImportCmd_importJSON_invalid_uri(t *testing.T) {
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/json.schema",
		Source: "testdata/json.source",
		CommonOption: CommonOption{
			URI: "://uri",
		},
	}

	err := cmd.importJSON()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_ImportCmd_importJSON_non_existent_source(t *testing.T) {
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/json.schema",
		Source: "file/does/not/exist",
		CommonOption: CommonOption{
			URI: "s3://target",
		},
	}

	// non-existent source
	err := cmd.importJSON()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to load source from")
}

func Test_ImportCmd_importJSON_fail_to_write(t *testing.T) {
	// fail to write
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/json.schema",
		Source: "testdata/json.source",
		CommonOption: CommonOption{
			URI: "s3://target",
		},
	}

	err := cmd.importJSON()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to close Parquet file")
}

func Test_ImportCmd_importJSON_invalid_schema(t *testing.T) {
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/csv.schema",
		Source: "testdata/json.source",
		CommonOption: CommonOption{
			URI: "s3://target",
		},
	}

	err := cmd.importJSON()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "is not a valid schema JSON")
}

func Test_ImportCmd_importJSON_invalid_source(t *testing.T) {
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/json.schema",
		Source: "testdata/csv.source",
		CommonOption: CommonOption{
			URI: "s3://target",
		},
	}

	err := cmd.importJSON()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid JSON string")
}

func Test_ImportCmd_importJSON_schema_mismatch(t *testing.T) {
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/json.schema",
		Source: "testdata/json.bad-source",
		CommonOption: CommonOption{
			URI: "s3://target",
		},
	}

	err := cmd.importJSON()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to close Parquet writer")
}

func Test_ImportCmd_importJSON_good(t *testing.T) {
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/json.schema",
		Source: "testdata/json.source",
		CommonOption: CommonOption{
			URI: os.TempDir() + "/import-csv.parquet",
		},
	}

	err := cmd.importJSON()
	assert.Nil(t, err)
}

func Test_ImportCmd_importJSONL_bad_schema_file(t *testing.T) {
	cmd := &ImportCmd{
		Schema: "file/does/not/exist",
		Format: "jsonl",
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to load schema from")
}

func Test_ImportCmd_importJSONL_invalid_uri(t *testing.T) {
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/jsonl.schema",
		Source: "testdata/jsonl.source",
		CommonOption: CommonOption{
			URI: "://uri",
		},
	}

	err := cmd.importJSONL()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_ImportCmd_importJSONL_non_existent_source(t *testing.T) {
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/jsonl.schema",
		Source: "file/does/not/exist",
		CommonOption: CommonOption{
			URI: "s3://target",
		},
	}

	// non-existent source
	err := cmd.importJSONL()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open source file")
}

func Test_ImportCmd_importJSONL_fail_to_write(t *testing.T) {
	// fail to write
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/jsonl.schema",
		Source: "testdata/jsonl.source",
		CommonOption: CommonOption{
			URI: "s3://target",
		},
	}

	err := cmd.importJSONL()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to close Parquet file")
}

func Test_ImportCmd_importJSONL_invalid_schema(t *testing.T) {
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/csv.schema",
		Source: "testdata/jsonl.source",
		CommonOption: CommonOption{
			URI: "s3://target",
		},
	}

	err := cmd.importJSONL()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "is not a valid schema JSON")
}

func Test_ImportCmd_importJSONL_invalid_source(t *testing.T) {
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/jsonl.schema",
		Source: "testdata/csv.source",
		CommonOption: CommonOption{
			URI: "s3://target",
		},
	}

	err := cmd.importJSONL()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid JSON string")
}

func Test_ImportCmd_importJSONL_schema_mismatch(t *testing.T) {
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/jsonl.schema",
		Source: "testdata/jsonl.bad-source",
		CommonOption: CommonOption{
			URI: "s3://target",
		},
	}

	err := cmd.importJSONL()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to close Parquet writer")
}

func Test_ImportCmd_importJSONL_good(t *testing.T) {
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/jsonl.schema",
		Source: "testdata/jsonl.source",
		CommonOption: CommonOption{
			URI: os.TempDir() + "/import-csv.parquet",
		},
	}

	err := cmd.importJSONL()
	assert.Nil(t, err)
}
