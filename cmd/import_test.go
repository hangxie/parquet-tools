package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ImportCmd_Run_CSV_good(t *testing.T) {
	testFile := os.TempDir() + "/import-csv.parquet"
	os.Remove(testFile)
	cmd := &ImportCmd{
		Source: "testdata/csv.source",
		Schema: "testdata/csv.schema",
		Format: "csv",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: testFile,
			},
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})

	require.Equal(t, "", stdout)
	require.Equal(t, "", stderr)

	_, err := os.Stat(testFile)
	require.Nil(t, err)
}

func Test_ImportCmd_Run_CSV_skip_header_good(t *testing.T) {
	testFile := os.TempDir() + "/import-csv.parquet"
	os.Remove(testFile)
	cmd := &ImportCmd{
		Source:     "testdata/csv-with-header.source",
		Schema:     "testdata/csv.schema",
		Format:     "csv",
		SkipHeader: true,
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: testFile,
			},
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})

	require.Equal(t, "", stdout)
	require.Equal(t, "", stderr)

	_, err := os.Stat(testFile)
	require.Nil(t, err)
}

func Test_ImportCmd_Run_JSON_good(t *testing.T) {
	testFile := os.TempDir() + "/import-json.parquet"
	os.Remove(testFile)
	cmd := &ImportCmd{
		Source: "testdata/json.source",
		Schema: "testdata/json.schema",
		Format: "json",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: testFile,
			},
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})

	require.Equal(t, "", stdout)
	require.Equal(t, "", stderr)

	_, err := os.Stat(testFile)
	require.Nil(t, err)
}

func Test_ImportCmd_Run_invalid_format(t *testing.T) {
	cmd := &ImportCmd{
		Schema: "testdata/csv.schema",
		Format: "random",
	}

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "is not a recognized source format")
}

func Test_ImportCmd_importCSV_bad_schema_file(t *testing.T) {
	cmd := &ImportCmd{
		Schema: "file/does/not/exist",
		Format: "csv",
	}

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to load schema from")
}

func Test_ImportCmd_importCSV_invalid_uri(t *testing.T) {
	cmd := &ImportCmd{
		Format: "csv",
		Schema: "testdata/csv.schema",
		Source: "testdata/csv.source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "://uri",
			},
		},
	}

	err := cmd.importCSV()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_ImportCmd_importCSV_non_existent_source(t *testing.T) {
	cmd := &ImportCmd{
		Format: "csv",
		Schema: "testdata/csv.schema",
		Source: "file/does/not/exist",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "s3://target",
			},
		},
	}

	// non-existent source
	err := cmd.importCSV()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open CSV file")
}

func Test_ImportCmd_importCSV_fail_to_write(t *testing.T) {
	// fail to write
	cmd := &ImportCmd{
		Format: "csv",
		Schema: "testdata/csv.schema",
		Source: "testdata/csv.source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "s3://target",
			},
		},
	}

	err := cmd.importCSV()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to close Parquet file")
}

func Test_ImportCmd_importCSV_good(t *testing.T) {
	cmd := &ImportCmd{
		Format: "csv",
		Schema: "testdata/csv.schema",
		Source: "testdata/csv.source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: os.TempDir() + "/import-csv.parquet",
			},
		},
	}

	err := cmd.importCSV()
	require.Nil(t, err)

	reader, err := newParquetFileReader(ReadOption{CommonOption: cmd.CommonOption})
	require.Nil(t, err)
	require.Equal(t, reader.GetNumRows(), int64(7))
}

func Test_ImportCmd_importJSON_bad_schema_file(t *testing.T) {
	cmd := &ImportCmd{
		Schema: "file/does/not/exist",
		Format: "json",
	}

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to load schema from")
}

func Test_ImportCmd_importJSON_invalid_uri(t *testing.T) {
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/json.schema",
		Source: "testdata/json.source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "://uri",
			},
		},
	}

	err := cmd.importJSON()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_ImportCmd_importJSON_non_existent_source(t *testing.T) {
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/json.schema",
		Source: "file/does/not/exist",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "s3://target",
			},
		},
	}

	// non-existent source
	err := cmd.importJSON()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to load source from")
}

func Test_ImportCmd_importJSON_fail_to_write(t *testing.T) {
	// fail to write
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/json.schema",
		Source: "testdata/json.source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "s3://target",
			},
		},
	}

	err := cmd.importJSON()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to close Parquet file")
}

func Test_ImportCmd_importJSON_invalid_schema(t *testing.T) {
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/csv.schema",
		Source: "testdata/json.source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "s3://target",
			},
		},
	}

	err := cmd.importJSON()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "is not a valid schema JSON")
}

func Test_ImportCmd_importJSON_invalid_source(t *testing.T) {
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/json.schema",
		Source: "testdata/csv.source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "s3://target",
			},
		},
	}

	err := cmd.importJSON()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "invalid JSON string")
}

func Test_ImportCmd_importJSON_schema_mismatch(t *testing.T) {
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/json.schema",
		Source: "testdata/json.bad-source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "s3://target",
			},
		},
	}

	err := cmd.importJSON()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to close Parquet")
}

func Test_ImportCmd_importJSON_good(t *testing.T) {
	cmd := &ImportCmd{
		Format: "json",
		Schema: "testdata/json.schema",
		Source: "testdata/json.source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: os.TempDir() + "/import-csv.parquet",
			},
		},
	}

	err := cmd.importJSON()
	require.Nil(t, err)
}

func Test_ImportCmd_importJSONL_bad_schema_file(t *testing.T) {
	cmd := &ImportCmd{
		Schema: "file/does/not/exist",
		Format: "jsonl",
	}

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to load schema from")
}

func Test_ImportCmd_importJSONL_invalid_uri(t *testing.T) {
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/jsonl.schema",
		Source: "testdata/jsonl.source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "://uri",
			},
		},
	}

	err := cmd.importJSONL()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_ImportCmd_importJSONL_non_existent_source(t *testing.T) {
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/jsonl.schema",
		Source: "file/does/not/exist",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "s3://target",
			},
		},
	}

	// non-existent source
	err := cmd.importJSONL()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open source file")
}

func Test_ImportCmd_importJSONL_fail_to_write(t *testing.T) {
	// fail to write
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/jsonl.schema",
		Source: "testdata/jsonl.source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "s3://target",
			},
		},
	}

	err := cmd.importJSONL()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to close Parquet file")
}

func Test_ImportCmd_importJSONL_invalid_schema(t *testing.T) {
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/csv.schema",
		Source: "testdata/jsonl.source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "s3://target",
			},
		},
	}

	err := cmd.importJSONL()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "is not a valid schema JSON")
}

func Test_ImportCmd_importJSONL_invalid_source(t *testing.T) {
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/jsonl.schema",
		Source: "testdata/csv.source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "s3://target",
			},
		},
	}

	err := cmd.importJSONL()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "invalid JSON string")
}

func Test_ImportCmd_importJSONL_schema_mismatch(t *testing.T) {
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/jsonl.schema",
		Source: "testdata/jsonl.bad-source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: "s3://target",
			},
		},
	}

	err := cmd.importJSONL()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to close Parquet writer")
}

func Test_ImportCmd_importJSONL_good(t *testing.T) {
	cmd := &ImportCmd{
		Format: "jsonl",
		Schema: "testdata/jsonl.schema",
		Source: "testdata/jsonl.source",
		WriteOption: WriteOption{
			CommonOption: CommonOption{
				URI: os.TempDir() + "/import-csv.parquet",
			},
		},
	}

	err := cmd.importJSONL()
	require.Nil(t, err)
}
