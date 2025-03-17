package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/internal"
)

func Test_ImportCmd_Run_CSV_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	testFile := filepath.Join(tempDir, "import-csv.parquet")
	_ = os.Remove(testFile)
	cmd := &ImportCmd{}
	cmd.Source = "../testdata/csv.source"
	cmd.Schema = "../testdata/csv.schema"
	cmd.Format = "csv"
	cmd.URI = testFile
	cmd.Compression = "SNAPPY"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})

	require.Equal(t, "", stdout)
	require.Equal(t, "", stderr)

	_, err := os.Stat(testFile)
	require.Nil(t, err)
}

func Test_ImportCmd_Run_CSV_skip_header_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	testFile := filepath.Join(tempDir, "import-csv.parquet")
	_ = os.Remove(testFile)
	cmd := &ImportCmd{}
	cmd.Source = "../testdata/csv-with-header.source"
	cmd.Schema = "../testdata/csv.schema"
	cmd.Format = "csv"
	cmd.SkipHeader = true
	cmd.URI = testFile
	cmd.Compression = "ZSTD"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})

	require.Equal(t, "", stdout)
	require.Equal(t, "", stderr)

	_, err := os.Stat(testFile)
	require.Nil(t, err)
}

func Test_ImportCmd_Run_JSON_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	testFile := filepath.Join(tempDir, "import-json.parquet")
	_ = os.Remove(testFile)
	cmd := &ImportCmd{}
	cmd.Source = "../testdata/json.source"
	cmd.Schema = "../testdata/json.schema"
	cmd.Format = "json"
	cmd.URI = testFile
	cmd.Compression = "ZSTD"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})

	require.Equal(t, "", stdout)
	require.Equal(t, "", stderr)

	_, err := os.Stat(testFile)
	require.Nil(t, err)

	// verify jsonSchema
	type jsonSchema struct {
		Tag    string
		Fields []interface{}
	}
	sourceSchemaBuf, _ := os.ReadFile(cmd.Schema)
	reader, err := internal.NewParquetFileReader(testFile, internal.ReadOption{})
	require.Nil(t, err)
	schema, err := internal.NewSchemaTree(reader, internal.SchemaOption{})
	require.Nil(t, err)

	var sourceSchema jsonSchema
	_ = json.Unmarshal(sourceSchemaBuf, &sourceSchema)
	var targetSchema jsonSchema
	_ = json.Unmarshal([]byte(schema.JSONSchema()), &targetSchema)

	// top level tag can be different
	require.Equal(t, sourceSchema.Fields, targetSchema.Fields)
	_ = os.Remove(testFile)
}

func Test_ImportCmd_Run_invalid_format(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Schema = "../testdata/csv.schema"
	cmd.Format = "random"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "is not a recognized source format")
}

func Test_ImportCmd_Run_invalid_compression(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	testFile := filepath.Join(tempDir, "import-json.parquet")
	_ = os.Remove(testFile)
	cmd := &ImportCmd{}
	cmd.Source = "../testdata/json.source"
	cmd.Schema = "../testdata/json.schema"
	cmd.Format = "json"
	cmd.URI = testFile
	cmd.Compression = "foobar"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "not a valid CompressionCodec string")
}

func Test_ImportCmd_importCSV_bad_schema_file(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Schema = "file/does/not/exist"
	cmd.Format = "csv"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to load schema from")
}

func Test_ImportCmd_importCSV_invalid_uri(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "csv"
	cmd.Schema = "../testdata/csv.schema"
	cmd.Source = "../testdata/csv.source"
	cmd.URI = "://uri"

	err := cmd.importCSV()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_ImportCmd_importCSV_non_existent_source(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "csv"
	cmd.Schema = "../testdata/csv.schema"
	cmd.Source = "file/does/not/exist"
	cmd.URI = "s3://target"

	// non-existent source
	err := cmd.importCSV()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open CSV file")
}

func Test_ImportCmd_importCSV_fail_to_write(t *testing.T) {
	// fail to write
	cmd := &ImportCmd{}
	cmd.Format = "csv"
	cmd.Schema = "../testdata/csv.schema"
	cmd.Source = "../testdata/csv.source"
	cmd.URI = "s3://target"
	cmd.Compression = "LZ4"

	err := cmd.importCSV()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to close Parquet file")
}

func Test_ImportCmd_importCSV_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &ImportCmd{}
	cmd.Format = "csv"
	cmd.Schema = "../testdata/csv.schema"
	cmd.Source = "../testdata/csv.source"
	cmd.URI = filepath.Join(tempDir, "import-csv.parquet")
	cmd.Compression = "LZ4_RAW"

	err := cmd.importCSV()
	require.Nil(t, err)

	reader, err := internal.NewParquetFileReader(cmd.URI, internal.ReadOption{})
	require.Nil(t, err)
	require.Equal(t, reader.GetNumRows(), int64(7))
}

func Test_ImportCmd_importJSON_bad_schema_file(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Schema = "file/does/not/exist"
	cmd.Format = "json"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to load schema from")
}

func Test_ImportCmd_importJSON_invalid_uri(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "json"
	cmd.Schema = "../testdata/json.schema"
	cmd.Source = "../testdata/json.source"
	cmd.URI = "://uri"

	err := cmd.importJSON()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_ImportCmd_importJSON_non_existent_source(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "json"
	cmd.Schema = "../testdata/json.schema"
	cmd.Source = "file/does/not/exist"
	cmd.URI = "s3://target"

	// non-existent source
	err := cmd.importJSON()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to load source from")
}

func Test_ImportCmd_importJSON_fail_to_write(t *testing.T) {
	// fail to write
	cmd := &ImportCmd{}
	cmd.Format = "json"
	cmd.Schema = "../testdata/json.schema"
	cmd.Source = "../testdata/json.source"
	cmd.URI = "s3://target"
	cmd.Compression = "ZSTD"

	err := cmd.importJSON()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to close Parquet file")
}

func Test_ImportCmd_importJSON_invalid_schema(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "json"
	cmd.Schema = "../testdata/csv.schema"
	cmd.Source = "../testdata/json.source"
	cmd.URI = "s3://target"

	err := cmd.importJSON()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "is not a valid schema JSON")
}

func Test_ImportCmd_importJSON_invalid_source(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "json"
	cmd.Schema = "../testdata/json.schema"
	cmd.Source = "../testdata/csv.source"
	cmd.URI = "s3://target"

	err := cmd.importJSON()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "invalid JSON string")
}

func Test_ImportCmd_importJSON_schema_mismatch(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "json"
	cmd.Schema = "../testdata/json.schema"
	cmd.Source = "../testdata/json.bad-source"
	cmd.URI = "s3://target"
	cmd.Compression = "UNCOMPRESSED"

	err := cmd.importJSON()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to close Parquet")
}

func Test_ImportCmd_importJSON_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &ImportCmd{}
	cmd.Format = "json"
	cmd.Schema = "../testdata/json.schema"
	cmd.Source = "../testdata/json.source"
	cmd.URI = filepath.Join(tempDir, "import-csv.parquet")
	cmd.Compression = "GZIP"

	err := cmd.importJSON()
	require.Nil(t, err)
}

func Test_ImportCmd_importJSONL_bad_schema_file(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Schema = "file/does/not/exist"
	cmd.Format = "jsonl"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to load schema from")
}

func Test_ImportCmd_importJSONL_invalid_uri(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "jsonl"
	cmd.Schema = "../testdata/jsonl.schema"
	cmd.Source = "../testdata/jsonl.source"
	cmd.URI = "://uri"

	err := cmd.importJSONL()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_ImportCmd_importJSONL_non_existent_source(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "jsonl"
	cmd.Schema = "../testdata/jsonl.schema"
	cmd.Source = "file/does/not/exist"
	cmd.URI = "s3://target"

	// non-existent source
	err := cmd.importJSONL()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open source file")
}

func Test_ImportCmd_importJSONL_fail_to_write(t *testing.T) {
	// fail to write
	cmd := &ImportCmd{}
	cmd.Format = "jsonl"
	cmd.Schema = "../testdata/jsonl.schema"
	cmd.Source = "../testdata/jsonl.source"
	cmd.URI = "s3://target"
	cmd.Compression = "GZIP"

	err := cmd.importJSONL()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to close Parquet file")
}

func Test_ImportCmd_importJSONL_invalid_schema(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "jsonl"
	cmd.Schema = "../testdata/csv.schema"
	cmd.Source = "../testdata/jsonl.source"
	cmd.URI = "s3://target"

	err := cmd.importJSONL()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "is not a valid schema JSON")
}

func Test_ImportCmd_importJSONL_invalid_source(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "jsonl"
	cmd.Schema = "../testdata/jsonl.schema"
	cmd.Source = "../testdata/csv.source"
	cmd.URI = "s3://target"
	cmd.Compression = "SNAPPY"

	err := cmd.importJSONL()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "invalid JSON string")
}

func Test_ImportCmd_importJSONL_schema_mismatch(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "jsonl"
	cmd.Schema = "../testdata/jsonl.schema"
	cmd.Source = "../testdata/jsonl.bad-source"
	cmd.URI = "s3://target"
	cmd.Compression = "UNCOMPRESSED"

	err := cmd.importJSONL()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to close Parquet")
}

func Test_ImportCmd_importJSONL_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &ImportCmd{}
	cmd.Format = "jsonl"
	cmd.Schema = "../testdata/jsonl.schema"
	cmd.Source = "../testdata/jsonl.source"
	cmd.URI = filepath.Join(tempDir, "import-csv.parquet")
	cmd.Compression = "LZ4"

	err := cmd.importJSONL()
	require.Nil(t, err)
}

func Test_ImportCmd_importCSV_int96(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "csv"
	cmd.URI = "/something/does/not/matter"
	cmd.Schema = "../testdata/int96-csv.schema"
	cmd.Source = "/something/does/not/matter"
	cmd.Compression = "LZ4"
	err := cmd.importCSV()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "import does not support INT96 type")
}

func Test_ImportCmd_importJSON_int96(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "json"
	cmd.URI = "/something/does/not/matter"
	cmd.Schema = "../testdata/int96-json.schema"
	cmd.Source = "/something/does/not/matter"
	cmd.Compression = "LZ4"
	err := cmd.importJSON()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "import does not support INT96 type")
}

func Test_ImportCmd_importJSONL_int96(t *testing.T) {
	cmd := &ImportCmd{}
	cmd.Format = "jsonl"
	cmd.URI = "/something/does/not/matter"
	cmd.Schema = "../testdata/int96-json.schema"
	cmd.Source = "/something/does/not/matter"
	cmd.Compression = "LZ4"
	err := cmd.importJSONL()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "import does not support INT96 type")
}
