package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
	pschema "github.com/hangxie/parquet-tools/internal/schema"
)

func Test_ImportCmd_Run_error(t *testing.T) {
	wOpt := pio.WriteOption{Compression: "SNAPPY"}
	tempDir, _ := os.MkdirTemp(os.TempDir(), "import-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	testCases := map[string]struct {
		cmd    ImportCmd
		errMsg string
	}{
		"write-format":      {ImportCmd{wOpt, "src", "random", "../testdata/csv.schema", false, tempDir + "/tgt"}, "is not a recognized source format"},
		"write-compression": {ImportCmd{pio.WriteOption{Compression: "foobar"}, "../testdata/json.source", "json", "../testdata/json.schema", false, tempDir + "/tgt"}, "not a valid CompressionCodec string"},

		"csv-schema-file": {ImportCmd{wOpt, "does/not/exist", "csv", "schema", false, tempDir + "/tgt"}, "failed to load schema from"},
		"csv-source-file": {ImportCmd{wOpt, "file/does/not/exist", "csv", "../testdata/csv.schema", false, tempDir + "/tgt"}, "failed to open CSV file"},
		"csv-target-file": {ImportCmd{wOpt, "../testdata/csv.source", "csv", "../testdata/csv.schema", false, "://uri"}, "unable to parse file location"},
		"csv-schema":      {ImportCmd{wOpt, "../testdata/csv.source", "csv", "../testdata/json.schema", false, tempDir + "/tgt"}, "expect 'key=value' but got '{'"},
		"csv-source":      {ImportCmd{wOpt, "../testdata/json.source", "csv", "../testdata/csv.schema", false, tempDir + "/tgt"}, "failed to write [[{]] to parquet"},
		"csv-target":      {ImportCmd{wOpt, "../testdata/csv.source", "csv", "../testdata/csv.schema", false, "s3://target"}, "failed to close Parquet file"},
		"csv-int96":       {ImportCmd{wOpt, "../testdata/csv.source", "csv", "../testdata/int96-csv.schema", false, tempDir + "/tgt"}, "import does not support INT96 type"},

		"json-schema-file":     {ImportCmd{wOpt, "does/not/exist", "json", "schema", false, tempDir + "/tgt"}, "failed to load schema from"},
		"json-source-file":     {ImportCmd{wOpt, "file/does/not/exist", "json", "../testdata/json.schema", false, tempDir + "/tgt"}, "failed to load source from"},
		"json-target-file":     {ImportCmd{wOpt, "../testdata/json.source", "json", "../testdata/json.schema", false, "://uri"}, "unable to parse file location"},
		"json-schema":          {ImportCmd{wOpt, "../testdata/json.source", "json", "../testdata/csv.schema", false, tempDir + "/tgt"}, "is not a valid schema JSON"},
		"json-source":          {ImportCmd{wOpt, "../testdata/csv.source", "json", "../testdata/json.schema", false, tempDir + "/tgt"}, "invalid JSON string:"},
		"json-target":          {ImportCmd{wOpt, "../testdata/json.source", "json", "../testdata/json.schema", false, "s3://target"}, "failed to close Parquet file"},
		"json-schema-mismatch": {ImportCmd{wOpt, "../testdata/json.bad-source", "json", "../testdata/json.schema", false, tempDir + "/tgt"}, "failed to close Parquet writer"},
		"json-int96":           {ImportCmd{wOpt, "src", "json", "../testdata/int96-json.schema", false, tempDir + "/tgt"}, "import does not support INT96 type"},

		"jsonl-schema-file":     {ImportCmd{wOpt, "does/not/exist", "jsonl", "schema", false, tempDir + "/tgt"}, "failed to load schema from"},
		"jsonl-source-file":     {ImportCmd{wOpt, "file/does/not/exist", "jsonl", "../testdata/jsonl.schema", false, tempDir + "/tgt"}, "failed to open source file"},
		"jsonl-target-file":     {ImportCmd{wOpt, "../testdata/jsonl.source", "jsonl", "../testdata/jsonl.schema", false, "://uri"}, "unable to parse file location"},
		"jsonl-schema":          {ImportCmd{wOpt, "../testdata/jsonl.source", "jsonl", "../testdata/csv.schema", false, tempDir + "/tgt"}, "is not a valid schema JSON"},
		"jsonl-source":          {ImportCmd{wOpt, "../testdata/csv.source", "jsonl", "../testdata/jsonl.schema", false, tempDir + "/tgt"}, "invalid JSON string:"},
		"jsonl-target":          {ImportCmd{wOpt, "../testdata/jsonl.source", "jsonl", "../testdata/jsonl.schema", false, "s3://target"}, "failed to close Parquet file"},
		"jsonl-schema-mismatch": {ImportCmd{wOpt, "../testdata/jsonl.source", "jsonl", "../testdata/json.schema", false, tempDir + "/tgt"}, "failed to close Parquet writer"},
		"jsonl-int96":           {ImportCmd{wOpt, "src", "jsonl", "../testdata/int96-json.schema", false, tempDir + "/tgt"}, "import does not support INT96 type"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.NotNil(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func Test_ImportCmd_Run_CSV_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "import-test")
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
	tempDir, _ := os.MkdirTemp(os.TempDir(), "import-test")
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
	tempDir, _ := os.MkdirTemp(os.TempDir(), "import-test")
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
	reader, err := pio.NewParquetFileReader(testFile, pio.ReadOption{})
	require.Nil(t, err)
	schema, err := pschema.NewSchemaTree(reader, pschema.SchemaOption{})
	require.Nil(t, err)

	var sourceSchema jsonSchema
	_ = json.Unmarshal(sourceSchemaBuf, &sourceSchema)
	var targetSchema jsonSchema
	_ = json.Unmarshal([]byte(schema.JSONSchema()), &targetSchema)

	// top level tag can be different
	require.Equal(t, sourceSchema.Fields, targetSchema.Fields)
	_ = os.Remove(testFile)
}

func Test_ImportCmd_importCSV_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "import-test")
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

	reader, err := pio.NewParquetFileReader(cmd.URI, pio.ReadOption{})
	require.Nil(t, err)
	require.Equal(t, reader.GetNumRows(), int64(7))
}

func Test_ImportCmd_importJSON_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "import-test")
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

func Test_ImportCmd_importJSONL_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "import-test")
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
