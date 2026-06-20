package importcmd

import (
	"fmt"
	"path/filepath"
	"testing"

	parquetSource "github.com/hangxie/parquet-go/v3/source"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/cat"
	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
	pio "github.com/hangxie/parquet-tools/io"
)

const (
	importEncryptionFooterKey = "MDEyMzQ1Njc4OTAxMjM0NQ=="
	importEncryptionColumnKey = "MTIzNDU2Nzg5MDEyMzQ1MA=="
)

type mockParquetFileWriter struct {
	closeFunc func() error
}

func (m *mockParquetFileWriter) Write(p []byte) (int, error) { return len(p), nil }
func (m *mockParquetFileWriter) Close() error                { return m.closeFunc() }
func (m *mockParquetFileWriter) Create(_ string) (parquetSource.ParquetFileWriter, error) {
	return nil, fmt.Errorf("not implemented")
}

func importTestCatCmd(uri string, option pio.ReadOption) cat.Cmd {
	return cat.Cmd{
		ReadOption:   option,
		ReadPageSize: 1000,
		SampleRatio:  1.0,
		Format:       "json",
		GeoFormat:    "geojson",
		URI:          uri,
	}
}

func TestCmd(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		wOpt := pio.WriteOption{
			CompressionCodec: "SNAPPY",
			PageSize:         1024 * 1024,
			RowGroupSize:     128 * 1024 * 1024,
		}
		tempDir := t.TempDir()

		testCases := map[string]struct {
			cmd    Cmd
			errMsg string
		}{
			"write-format":          {Cmd{WriteOption: wOpt, Source: "src", Format: "random", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "dummy"}, "is not a recognized source format"},
			"write-compression":     {Cmd{WriteOption: pio.WriteOption{CompressionCodec: "foobar"}, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "not a valid CompressionCodec string"},
			"csv-schema-file":       {Cmd{WriteOption: wOpt, Source: "does/not/exist", Format: "csv", Schema: "schema", SkipHeader: false, URI: "dummy"}, "failed to load schema from"},
			"csv-source-file":       {Cmd{WriteOption: wOpt, Source: "file/does/not/exist", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "dummy"}, "failed to open CSV file"},
			"csv-target-file":       {Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "://uri"}, "unable to parse file location"},
			"csv-schema":            {Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "csv", Schema: "../../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "expect 'key=value' but got '{'"},
			"csv-source":            {Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "failed to read CSV record from"},
			"csv-malformed":         {Cmd{WriteOption: wOpt, Source: "../../testdata/csv-malformed.source", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "failed to read CSV record from"},
			"csv-target":            {Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "s3://target"}, "failed to close Parquet file"},
			"json-schema-file":      {Cmd{WriteOption: wOpt, Source: "does/not/exist", Format: "json", Schema: "schema", SkipHeader: false, URI: "dummy"}, "failed to load schema from"},
			"json-source-file":      {Cmd{WriteOption: wOpt, Source: "file/does/not/exist", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "dummy"}, "failed to load source from"},
			"json-target-file":      {Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "://uri"}, "unable to parse file location"},
			"json-schema":           {Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "dummy"}, "is not a valid schema JSON"},
			"json-source":           {Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "dummy"}, "is not a valid JSON array"},
			"json-source-not-array": {Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "dummy"}, "is not a valid JSON array"},
			"json-target":           {Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "s3://target"}, "failed to close Parquet file"},
			"json-schema-mismatch":  {Cmd{WriteOption: wOpt, Source: "../../testdata/json.bad-source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "failed to close Parquet writer"},
			"jsonl-schema-file":     {Cmd{WriteOption: wOpt, Source: "does/not/exist", Format: "jsonl", Schema: "schema", SkipHeader: false, URI: "dummy"}, "failed to load schema from"},
			"jsonl-source-file":     {Cmd{WriteOption: wOpt, Source: "file/does/not/exist", Format: "jsonl", Schema: "../../testdata/jsonl.schema", SkipHeader: false, URI: "dummy"}, "failed to open source file"},
			"jsonl-target-file":     {Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "jsonl", Schema: "../../testdata/jsonl.schema", SkipHeader: false, URI: "://uri"}, "unable to parse file location"},
			"jsonl-schema":          {Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "jsonl", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "dummy"}, "is not a valid schema JSON"},
			"jsonl-source":          {Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "jsonl", Schema: "../../testdata/jsonl.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "invalid JSON string:"},
			"jsonl-target":          {Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "jsonl", Schema: "../../testdata/jsonl.schema", SkipHeader: false, URI: "s3://target"}, "failed to close Parquet file"},
			"jsonl-schema-mismatch": {Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "jsonl", Schema: "../../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "failed to close Parquet writer"},
			"csv-unknown-not-nil":   {Cmd{WriteOption: wOpt, Source: "../../testdata/unknown-type-bad.csv", Format: "csv", Schema: "../../testdata/unknown-type-csv.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")}, "UNKNOWN column"},
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
			CompressionCodec: "SNAPPY",
			PageSize:         1024 * 1024,
			RowGroupSize:     128 * 1024 * 1024,
		}
		testCases := map[string]struct {
			cmd      Cmd
			rowCount int64
		}{
			"csv-wo-header": {Cmd{WriteOption: wOpt, Source: "csv.source", Format: "csv", Schema: "csv.schema", SkipHeader: false, URI: ""}, 10},
			"csv-w-header":  {Cmd{WriteOption: wOpt, Source: "csv-with-header.source", Format: "csv", Schema: "csv.schema", SkipHeader: true, URI: ""}, 10},
			"json":          {Cmd{WriteOption: wOpt, Source: "json.source", Format: "json", Schema: "json.schema", SkipHeader: false, URI: ""}, 1},
			"jsonl":         {Cmd{WriteOption: wOpt, Source: "jsonl.source", Format: "jsonl", Schema: "jsonl.schema", SkipHeader: false, URI: ""}, 10},
			"json-unknown":  {Cmd{WriteOption: wOpt, Source: "unknown-type.source", Format: "json", Schema: "unknown-type.schema", SkipHeader: false, URI: ""}, 3},
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

func TestCmdEncryption(t *testing.T) {
	source := filepath.Join("..", "..", "testdata", "csv.source")
	schema := filepath.Join("..", "..", "testdata", "csv.schema")
	tempDir := t.TempDir()

	plainURI := filepath.Join(tempDir, "plain.parquet")
	plainCmd := Cmd{
		WriteOption: pio.WriteOption{
			CompressionCodec: "SNAPPY",
			PageSize:         1024 * 1024,
			RowGroupSize:     128 * 1024 * 1024,
		},
		Source: source,
		Format: "csv",
		Schema: schema,
		URI:    plainURI,
	}
	require.NoError(t, plainCmd.Run())
	wantOutput := testutils.CommandStdout(t, importTestCatCmd(plainURI, pio.ReadOption{}))

	testCases := []struct {
		name        string
		writeOption pio.WriteOption
		readOption  pio.ReadOption
		footerMagic string
	}{
		{
			name: "encrypted-footer",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  importEncryptionFooterKey,
			},
			readOption:  pio.ReadOption{FooterKey: importEncryptionFooterKey},
			footerMagic: "PARE",
		},
		{
			name: "encrypted-footer-ctr-algorithm",
			writeOption: pio.WriteOption{
				CompressionCodec:    "SNAPPY",
				PageSize:            1024 * 1024,
				RowGroupSize:        128 * 1024 * 1024,
				WriterFooterKey:     importEncryptionFooterKey,
				EncryptionAlgorithm: "AES-GCM-CTR-V1",
			},
			readOption:  pio.ReadOption{FooterKey: importEncryptionFooterKey},
			footerMagic: "PARE",
		},
		{
			name: "encrypted-footer-column-keys",
			writeOption: pio.WriteOption{
				CompressionCodec:    "SNAPPY",
				PageSize:            1024 * 1024,
				RowGroupSize:        128 * 1024 * 1024,
				WriterFooterKey:     importEncryptionFooterKey,
				WriterColumnKeys:    []string{"Bool=" + importEncryptionColumnKey},
				DataPageVersion:     2,
				EncryptionAlgorithm: "AES-GCM-V1",
			},
			readOption: pio.ReadOption{
				FooterKey:  importEncryptionFooterKey,
				ColumnKeys: []string{"Bool=" + importEncryptionColumnKey},
			},
			footerMagic: "PARE",
		},
		{
			name: "plaintext-footer-column-keys",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  importEncryptionFooterKey,
				WriterColumnKeys: []string{"Bool=" + importEncryptionColumnKey},
				PlaintextFooter:  true,
				DataPageVersion:  2,
			},
			readOption: pio.ReadOption{
				FooterKey:  importEncryptionFooterKey,
				ColumnKeys: []string{"Bool=" + importEncryptionColumnKey},
			},
			footerMagic: "PAR1",
		},
		{
			name: "encrypted-footer-sentinel-column",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  importEncryptionFooterKey,
				WriterColumnKeys: []string{"Bool=@footer-key"},
			},
			readOption:  pio.ReadOption{FooterKey: importEncryptionFooterKey},
			footerMagic: "PARE",
		},
		{
			name: "encrypted-footer-encrypt-all-columns",
			writeOption: pio.WriteOption{
				CompressionCodec:  "SNAPPY",
				PageSize:          1024 * 1024,
				RowGroupSize:      128 * 1024 * 1024,
				WriterFooterKey:   importEncryptionFooterKey,
				EncryptAllColumns: true,
			},
			readOption:  pio.ReadOption{FooterKey: importEncryptionFooterKey},
			footerMagic: "PARE",
		},
		{
			name: "plaintext-footer-encrypt-all-columns",
			writeOption: pio.WriteOption{
				CompressionCodec:  "SNAPPY",
				PageSize:          1024 * 1024,
				RowGroupSize:      128 * 1024 * 1024,
				WriterFooterKey:   importEncryptionFooterKey,
				EncryptAllColumns: true,
				PlaintextFooter:   true,
			},
			readOption:  pio.ReadOption{FooterKey: importEncryptionFooterKey},
			footerMagic: "PAR1",
		},
		{
			name: "plaintext-footer-sentinel-column",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  importEncryptionFooterKey,
				WriterColumnKeys: []string{"Bool=@footer-key"},
				PlaintextFooter:  true,
			},
			readOption:  pio.ReadOption{FooterKey: importEncryptionFooterKey},
			footerMagic: "PAR1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			uri := filepath.Join(tempDir, tc.name+".parquet")
			cmd := Cmd{
				WriteOption: tc.writeOption,
				Source:      source,
				Format:      "csv",
				Schema:      schema,
				URI:         uri,
			}
			require.NoError(t, cmd.Run())
			require.Equal(t, tc.footerMagic, testutils.ParquetFooterMagic(t, uri))
			require.Equal(t, wantOutput, testutils.CommandStdout(t, importTestCatCmd(uri, tc.readOption)))
		})
	}
}

func TestCmdEncryptionErrors(t *testing.T) {
	tempDir := t.TempDir()

	testCases := []struct {
		name        string
		writeOption pio.WriteOption
		errMsg      string
	}{
		{
			name: "missing-footer-key",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterColumnKeys: []string{"Bool=" + importEncryptionColumnKey},
			},
			errMsg: "--writer-footer-key is required",
		},
		{
			name: "bad-base64",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  "not base64",
			},
			errMsg: "invalid base64 writer footer key",
		},
		{
			name: "wrong-key-size",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  "MTIzNDU=",
			},
			errMsg: "writer footer key must be 16, 24, or 32 bytes",
		},
		{
			name: "missing-column-key-path",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  importEncryptionFooterKey,
				WriterColumnKeys: []string{"Missing=" + importEncryptionColumnKey},
			},
			errMsg: "writer column key path [Missing] not found in schema",
		},
		{
			name: "duplicate-column-key-path",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  importEncryptionFooterKey,
				WriterColumnKeys: []string{
					"Bool=" + importEncryptionColumnKey,
					"Bool=" + importEncryptionColumnKey,
				},
			},
			errMsg: "duplicate writer column key path [Bool]",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := Cmd{
				WriteOption: tc.writeOption,
				Source:      filepath.Join("..", "..", "testdata", "csv.source"),
				Format:      "csv",
				Schema:      filepath.Join("..", "..", "testdata", "csv.schema"),
				URI:         filepath.Join(tempDir, tc.name+".parquet"),
			}
			err := cmd.Run()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

// TestCmdEncryptionEncryptAllColumns proves that --encrypt-all-columns
// actually encrypts unlisted columns. With --plaintext-footer the file's
// footer can be read without keys, so the discriminator is whether reading
// column data succeeds with no keys: without the flag, columns are plaintext
// and the read succeeds; with the flag, columns are footer-key encrypted and
// the read must fail.
func TestCmdEncryptionEncryptAllColumns(t *testing.T) {
	source := filepath.Join("..", "..", "testdata", "csv.source")
	schema := filepath.Join("..", "..", "testdata", "csv.schema")
	tempDir := t.TempDir()

	runImport := func(t *testing.T, name string, option pio.WriteOption) string {
		t.Helper()
		option.CompressionCodec = "SNAPPY"
		option.PageSize = 1024 * 1024
		option.RowGroupSize = 128 * 1024 * 1024
		uri := filepath.Join(tempDir, name+".parquet")
		cmd := Cmd{
			WriteOption: option,
			Source:      source,
			Format:      "csv",
			Schema:      schema,
			URI:         uri,
		}
		require.NoError(t, cmd.Run())
		return uri
	}

	catNoKeysErr := func(t *testing.T, uri string) error {
		t.Helper()
		var err error
		_, _ = testutils.CaptureStdoutStderr(func() {
			err = importTestCatCmd(uri, pio.ReadOption{}).Run()
		})
		return err
	}

	t.Run("default-mixed-no-column-keys-allows-no-key-read", func(t *testing.T) {
		uri := runImport(t, "default-mixed", pio.WriteOption{
			WriterFooterKey: importEncryptionFooterKey,
			PlaintextFooter: true,
			WriterColumnKeys: []string{
				// At least one encrypted column is required for --plaintext-footer.
				// Use the sentinel so this test does not depend on a column key.
				"Bool=@footer-key",
			},
		})
		// All columns except Bool are plaintext; Bool is encrypted with the
		// footer key. cat without keys must fail on Bool but the failure
		// proves the unlisted columns are at least readable up to that point.
		err := catNoKeysErr(t, uri)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decryption key required")
	})

	t.Run("encrypt-all-columns-blocks-no-key-read", func(t *testing.T) {
		uri := runImport(t, "encrypt-all", pio.WriteOption{
			WriterFooterKey:   importEncryptionFooterKey,
			EncryptAllColumns: true,
			PlaintextFooter:   true,
		})
		err := catNoKeysErr(t, uri)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decryption key required")
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
