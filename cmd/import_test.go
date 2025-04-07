package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
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

func Test_ImportCmd_Run_good(t *testing.T) {
	wOpt := pio.WriteOption{Compression: "SNAPPY"}
	testCases := map[string]struct {
		cmd      ImportCmd
		rowCount int64
	}{
		"csv-wo-header": {ImportCmd{wOpt, "csv.source", "csv", "csv.schema", false, ""}, 7},
		"csv-w-header":  {ImportCmd{wOpt, "csv-with-header.source", "csv", "csv.schema", true, ""}, 7},
		"json":          {ImportCmd{wOpt, "json.source", "json", "json.schema", false, ""}, 1},
		"jsonl":         {ImportCmd{wOpt, "jsonl.source", "jsonl", "jsonl.schema", false, ""}, 7},
	}

	tempDir, _ := os.MkdirTemp(os.TempDir(), "import-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tc.cmd.Source = "../testdata/" + tc.cmd.Source
			tc.cmd.Schema = "../testdata/" + tc.cmd.Schema
			tc.cmd.URI = filepath.Join(tempDir, "import-"+name+".parquet")

			err := tc.cmd.Run()
			require.Nil(t, err)

			reader, err := pio.NewParquetFileReader(tc.cmd.URI, pio.ReadOption{})
			require.Nil(t, err)
			require.Equal(t, reader.GetNumRows(), tc.rowCount)
		})
	}
}
