package cmd

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

func Test_ImportCmd_Run_error(t *testing.T) {
	wOpt := pio.WriteOption{Compression: "SNAPPY"}
	tempDir := t.TempDir()

	testCases := map[string]struct {
		cmd    ImportCmd
		errMsg string
	}{
		"write-format":          {ImportCmd{WriteOption: wOpt, Source: "src", Format: "random", Schema: "../testdata/csv.schema", SkipHeader: false, URI: "dummy"}, "is not a recognized source format"},
		"write-compression":     {ImportCmd{WriteOption: pio.WriteOption{Compression: "foobar"}, Source: "../testdata/json.source", Format: "json", Schema: "../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "not a valid CompressionCodec string"},
		"csv-schema-file":       {ImportCmd{WriteOption: wOpt, Source: "does/not/exist", Format: "csv", Schema: "schema", SkipHeader: false, URI: "dummy"}, "failed to load schema from"},
		"csv-source-file":       {ImportCmd{WriteOption: wOpt, Source: "file/does/not/exist", Format: "csv", Schema: "../testdata/csv.schema", SkipHeader: false, URI: "dummy"}, "failed to open CSV file"},
		"csv-target-file":       {ImportCmd{WriteOption: wOpt, Source: "../testdata/csv.source", Format: "csv", Schema: "../testdata/csv.schema", SkipHeader: false, URI: "://uri"}, "unable to parse file location"},
		"csv-schema":            {ImportCmd{WriteOption: wOpt, Source: "../testdata/csv.source", Format: "csv", Schema: "../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "expect 'key=value' but got '{'"},
		"csv-source":            {ImportCmd{WriteOption: wOpt, Source: "../testdata/json.source", Format: "csv", Schema: "../testdata/csv.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "failed to write [[{]] to parquet"},
		"csv-target":            {ImportCmd{WriteOption: wOpt, Source: "../testdata/csv.source", Format: "csv", Schema: "../testdata/csv.schema", SkipHeader: false, URI: "s3://target"}, "failed to close Parquet file"},
		"csv-int96":             {ImportCmd{WriteOption: wOpt, Source: "../testdata/csv.source", Format: "csv", Schema: "../testdata/int96-csv.schema", SkipHeader: false, URI: filepath.Join(tempDir, "tgt")}, "import does not support INT96 type"},
		"json-schema-file":      {ImportCmd{WriteOption: wOpt, Source: "does/not/exist", Format: "json", Schema: "schema", SkipHeader: false, URI: "dummy"}, "failed to load schema from"},
		"json-source-file":      {ImportCmd{WriteOption: wOpt, Source: "file/does/not/exist", Format: "json", Schema: "../testdata/json.schema", SkipHeader: false, URI: "dummy"}, "failed to load source from"},
		"json-target-file":      {ImportCmd{WriteOption: wOpt, Source: "../testdata/json.source", Format: "json", Schema: "../testdata/json.schema", SkipHeader: false, URI: "://uri"}, "unable to parse file location"},
		"json-schema":           {ImportCmd{WriteOption: wOpt, Source: "../testdata/json.source", Format: "json", Schema: "../testdata/csv.schema", SkipHeader: false, URI: "dummy"}, "is not a valid schema JSON"},
		"json-source":           {ImportCmd{WriteOption: wOpt, Source: "../testdata/csv.source", Format: "json", Schema: "../testdata/json.schema", SkipHeader: false, URI: "dummy"}, "invalid JSON string:"},
		"json-target":           {ImportCmd{WriteOption: wOpt, Source: "../testdata/json.source", Format: "json", Schema: "../testdata/json.schema", SkipHeader: false, URI: "s3://target"}, "failed to close Parquet file"},
		"json-schema-mismatch":  {ImportCmd{WriteOption: wOpt, Source: "../testdata/json.bad-source", Format: "json", Schema: "../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "failed to close Parquet writer"},
		"json-int96":            {ImportCmd{WriteOption: wOpt, Source: "src", Format: "json", Schema: "../testdata/int96-json.schema", SkipHeader: false, URI: "dummy"}, "import does not support INT96 type"},
		"jsonl-schema-file":     {ImportCmd{WriteOption: wOpt, Source: "does/not/exist", Format: "jsonl", Schema: "schema", SkipHeader: false, URI: "dummy"}, "failed to load schema from"},
		"jsonl-source-file":     {ImportCmd{WriteOption: wOpt, Source: "file/does/not/exist", Format: "jsonl", Schema: "../testdata/jsonl.schema", SkipHeader: false, URI: "dummy"}, "failed to open source file"},
		"jsonl-target-file":     {ImportCmd{WriteOption: wOpt, Source: "../testdata/jsonl.source", Format: "jsonl", Schema: "../testdata/jsonl.schema", SkipHeader: false, URI: "://uri"}, "unable to parse file location"},
		"jsonl-schema":          {ImportCmd{WriteOption: wOpt, Source: "../testdata/jsonl.source", Format: "jsonl", Schema: "../testdata/csv.schema", SkipHeader: false, URI: "dummy"}, "is not a valid schema JSON"},
		"jsonl-source":          {ImportCmd{WriteOption: wOpt, Source: "../testdata/csv.source", Format: "jsonl", Schema: "../testdata/jsonl.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "invalid JSON string:"},
		"jsonl-target":          {ImportCmd{WriteOption: wOpt, Source: "../testdata/jsonl.source", Format: "jsonl", Schema: "../testdata/jsonl.schema", SkipHeader: false, URI: "s3://target"}, "failed to close Parquet file"},
		"jsonl-schema-mismatch": {ImportCmd{WriteOption: wOpt, Source: "../testdata/jsonl.source", Format: "jsonl", Schema: "../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "failed to close Parquet writer"},
		"jsonl-int96":           {ImportCmd{WriteOption: wOpt, Source: "src", Format: "jsonl", Schema: "../testdata/int96-json.schema", SkipHeader: false, URI: "dummy"}, "import does not support INT96 type"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := tc.cmd.Run()
			require.Error(t, err)
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
		"csv-wo-header": {ImportCmd{WriteOption: wOpt, Source: "csv.source", Format: "csv", Schema: "csv.schema", SkipHeader: false, URI: ""}, 7},
		"csv-w-header":  {ImportCmd{WriteOption: wOpt, Source: "csv-with-header.source", Format: "csv", Schema: "csv.schema", SkipHeader: true, URI: ""}, 7},
		"json":          {ImportCmd{WriteOption: wOpt, Source: "json.source", Format: "json", Schema: "json.schema", SkipHeader: false, URI: ""}, 1},
		"jsonl":         {ImportCmd{WriteOption: wOpt, Source: "jsonl.source", Format: "jsonl", Schema: "jsonl.schema", SkipHeader: false, URI: ""}, 7},
	}

	tempDir := t.TempDir()

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tc.cmd.Source = filepath.Join("../testdata", tc.cmd.Source)
			tc.cmd.Schema = filepath.Join("../testdata", tc.cmd.Schema)
			tc.cmd.URI = filepath.Join(tempDir, "import-"+name+".parquet")

			err := tc.cmd.Run()
			require.NoError(t, err)

			reader, err := pio.NewParquetFileReader(tc.cmd.URI, pio.ReadOption{})
			require.NoError(t, err)
			require.Equal(t, reader.GetNumRows(), tc.rowCount)
		})
	}
}
