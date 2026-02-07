package importcmd

import (
	"fmt"
	"path/filepath"
	"testing"

	parquetSource "github.com/hangxie/parquet-go/v2/source"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
)

type mockParquetFileWriter struct {
	closeFunc func() error
}

func (m *mockParquetFileWriter) Write(p []byte) (int, error) { return len(p), nil }
func (m *mockParquetFileWriter) Close() error                { return m.closeFunc() }
func (m *mockParquetFileWriter) Create(_ string) (parquetSource.ParquetFileWriter, error) {
	return nil, fmt.Errorf("not implemented")
}

func TestCmd(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		wOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		tempDir := t.TempDir()

		testCases := map[string]struct {
			cmd    Cmd
			errMsg string
		}{
			"write-format":          {Cmd{WriteOption: wOpt, Source: "src", Format: "random", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "dummy"}, "is not a recognized source format"},
			"write-compression":     {Cmd{WriteOption: pio.WriteOption{Compression: "foobar"}, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "not a valid CompressionCodec string"},
			"csv-schema-file":       {Cmd{WriteOption: wOpt, Source: "does/not/exist", Format: "csv", Schema: "schema", SkipHeader: false, URI: "dummy"}, "failed to load schema from"},
			"csv-source-file":       {Cmd{WriteOption: wOpt, Source: "file/does/not/exist", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "dummy"}, "failed to open CSV file"},
			"csv-target-file":       {Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "://uri"}, "unable to parse file location"},
			"csv-schema":            {Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "csv", Schema: "../../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "expect 'key=value' but got '{'"},
			"csv-source":            {Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "ow 0 has less than 39 fields"},
			"csv-target":            {Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "s3://target"}, "failed to close Parquet file"},
			"json-schema-file":      {Cmd{WriteOption: wOpt, Source: "does/not/exist", Format: "json", Schema: "schema", SkipHeader: false, URI: "dummy"}, "failed to load schema from"},
			"json-source-file":      {Cmd{WriteOption: wOpt, Source: "file/does/not/exist", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "dummy"}, "failed to load source from"},
			"json-target-file":      {Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "://uri"}, "unable to parse file location"},
			"json-schema":           {Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "dummy"}, "is not a valid schema JSON"},
			"json-source":           {Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "dummy"}, "invalid JSON string:"},
			"json-target":           {Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "s3://target"}, "failed to close Parquet file"},
			"json-schema-mismatch":  {Cmd{WriteOption: wOpt, Source: "../../testdata/json.bad-source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "failed to close Parquet writer"},
			"jsonl-schema-file":     {Cmd{WriteOption: wOpt, Source: "does/not/exist", Format: "jsonl", Schema: "schema", SkipHeader: false, URI: "dummy"}, "failed to load schema from"},
			"jsonl-source-file":     {Cmd{WriteOption: wOpt, Source: "file/does/not/exist", Format: "jsonl", Schema: "../../testdata/jsonl.schema", SkipHeader: false, URI: "dummy"}, "failed to open source file"},
			"jsonl-target-file":     {Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "jsonl", Schema: "../../testdata/jsonl.schema", SkipHeader: false, URI: "://uri"}, "unable to parse file location"},
			"jsonl-schema":          {Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "jsonl", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "dummy"}, "is not a valid schema JSON"},
			"jsonl-source":          {Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "jsonl", Schema: "../../testdata/jsonl.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "invalid JSON string:"},
			"jsonl-target":          {Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "jsonl", Schema: "../../testdata/jsonl.schema", SkipHeader: false, URI: "s3://target"}, "failed to close Parquet file"},
			"jsonl-schema-mismatch": {Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "jsonl", Schema: "../../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "failed to close Parquet writer"},
		}

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				err := tc.cmd.Run()
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			})
		}
	})

	t.Run("good", func(t *testing.T) {
		wOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		pOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 2,
		}
		testCases := map[string]struct {
			cmd      Cmd
			rowCount int64
		}{
			"csv-wo-header":  {Cmd{WriteOption: wOpt, Source: "csv.source", Format: "csv", Schema: "csv.schema", SkipHeader: false, URI: ""}, 10},
			"csv-w-header":   {Cmd{WriteOption: wOpt, Source: "csv-with-header.source", Format: "csv", Schema: "csv.schema", SkipHeader: true, URI: ""}, 10},
			"json":           {Cmd{WriteOption: wOpt, Source: "json.source", Format: "json", Schema: "json.schema", SkipHeader: false, URI: ""}, 1},
			"jsonl":          {Cmd{WriteOption: wOpt, Source: "jsonl.source", Format: "jsonl", Schema: "jsonl.schema", SkipHeader: false, URI: ""}, 10},
			"csv-parallel":   {Cmd{WriteOption: pOpt, Source: "csv.source", Format: "csv", Schema: "csv.schema", SkipHeader: false, URI: ""}, 10},
			"json-parallel":  {Cmd{WriteOption: pOpt, Source: "json.source", Format: "json", Schema: "json.schema", SkipHeader: false, URI: ""}, 1},
			"jsonl-parallel": {Cmd{WriteOption: pOpt, Source: "jsonl.source", Format: "jsonl", Schema: "jsonl.schema", SkipHeader: false, URI: ""}, 10},
		}

		tempDir := t.TempDir()

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				tc.cmd.Source = filepath.Join("../../testdata", tc.cmd.Source)
				tc.cmd.Schema = filepath.Join("../../testdata", tc.cmd.Schema)
				tc.cmd.URI = filepath.Join(tempDir, "import-"+name+".parquet")

				err := tc.cmd.Run()
				require.NoError(t, err)

				reader, err := pio.NewParquetFileReader(tc.cmd.URI, pio.ReadOption{})
				require.NoError(t, err)
				require.Equal(t, tc.rowCount, reader.GetNumRows())
			})
		}
	})
}

func TestCloseWriter(t *testing.T) {
	cmd := Cmd{}

	t.Run("success", func(t *testing.T) {
		mock := &mockParquetFileWriter{closeFunc: func() error { return nil }}
		err := cmd.closeWriter(mock)
		require.NoError(t, err)
	})

	t.Run("non-retryable-error", func(t *testing.T) {
		mock := &mockParquetFileWriter{closeFunc: func() error { return fmt.Errorf("some other error") }}
		err := cmd.closeWriter(mock)
		require.Error(t, err)
		require.Contains(t, err.Error(), "some other error")
	})

	t.Run("retry-then-success", func(t *testing.T) {
		callCount := 0
		mock := &mockParquetFileWriter{
			closeFunc: func() error {
				callCount++
				if callCount <= 1 {
					return fmt.Errorf("replication in progress")
				}
				return nil
			},
		}
		err := cmd.closeWriter(mock)
		require.NoError(t, err)
		require.Equal(t, 2, callCount)
	})
}
