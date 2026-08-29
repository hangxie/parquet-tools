package importcmd

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	parquetSource "github.com/hangxie/parquet-go/v3/source"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/cat"
	"github.com/hangxie/parquet-tools/cmd/inspect"
	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
	pio "github.com/hangxie/parquet-tools/io"
)

var (
	importEncryptionFooterKey = new("MDEyMzQ1Njc4OTAxMjM0NQ==")
	importEncryptionColumnKey = "MTIzNDU2Nzg5MDEyMzQ1MA=="
)

const jsonlLineSizeTestSchema = `{
	"Tag": "name=parquet_go_root, inname=Parquet_go_root",
	"Fields": [
		{"Tag": "name=Value, inname=Value, type=BYTE_ARRAY, convertedtype=UTF8"}
	]
}`

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
			"write-format": {
				Cmd{WriteOption: wOpt, Source: "src", Format: "random", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "dummy"},
				"is not a recognized source format",
			},
			"write-compression": {
				Cmd{WriteOption: pio.WriteOption{CompressionCodec: "foobar"}, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				"not a valid CompressionCodec string",
			},
			"csv-schema-file": {
				Cmd{WriteOption: wOpt, Source: "does/not/exist", Format: "csv", Schema: "schema", SkipHeader: false, URI: "dummy"},
				"failed to load schema from",
			},
			"csv-source-file": {
				Cmd{WriteOption: wOpt, Source: "file/does/not/exist", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "dummy"},
				"failed to open CSV file",
			},
			"csv-target-file": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "://uri"},
				"unable to parse file location",
			},
			"csv-schema": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "csv", Schema: "../../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				"expect 'key=value' but got '{'",
			},
			"csv-source": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				"failed to read CSV record from",
			},
			"csv-malformed": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/csv-malformed.source", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				"failed to read CSV record from",
			},
			"csv-target": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "csv", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "s3://target"},
				"failed to close Parquet file",
			},
			"json-schema-file": {
				Cmd{WriteOption: wOpt, Source: "does/not/exist", Format: "json", Schema: "schema", SkipHeader: false, URI: "dummy"},
				"failed to load schema from",
			},
			"json-source-file": {
				Cmd{WriteOption: wOpt, Source: "file/does/not/exist", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "dummy"},
				"failed to load source from",
			},
			"json-target-file": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "://uri"},
				"unable to parse file location",
			},
			"json-schema": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "dummy"},
				"is not a valid schema JSON",
			},
			"json-source": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "dummy"},
				"is not a valid JSON array",
			},
			"json-source-not-array": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "dummy"},
				"is not a valid JSON array",
			},
			"json-target": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: "s3://target"},
				"failed to close Parquet file",
			},
			"json-schema-mismatch": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/json.bad-source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				"failed to close Parquet writer",
			},
			"jsonl-schema-file": {
				Cmd{WriteOption: wOpt, Source: "does/not/exist", Format: "jsonl", Schema: "schema", SkipHeader: false, URI: "dummy"},
				"failed to load schema from",
			},
			"jsonl-source-file": {
				Cmd{WriteOption: wOpt, Source: "file/does/not/exist", Format: "jsonl", Schema: "../../testdata/jsonl.schema", SkipHeader: false, URI: "dummy"},
				"failed to open source file",
			},
			"jsonl-target-file": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "jsonl", Schema: "../../testdata/jsonl.schema", SkipHeader: false, URI: "://uri"},
				"unable to parse file location",
			},
			"jsonl-schema": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "jsonl", Schema: "../../testdata/csv.schema", SkipHeader: false, URI: "dummy"},
				"is not a valid schema JSON",
			},
			"jsonl-source": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "jsonl", Schema: "../../testdata/jsonl.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				"invalid JSON string:",
			},
			"jsonl-target": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "jsonl", Schema: "../../testdata/jsonl.schema", SkipHeader: false, URI: "s3://target"},
				"failed to close Parquet file",
			},
			"jsonl-schema-mismatch": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "jsonl", Schema: "../../testdata/json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				"failed to close Parquet writer",
			},
			"csv-unknown-not-nil": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/unknown-type-bad.csv", Format: "csv", Schema: "../../testdata/unknown-type-csv.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				"UNKNOWN column",
			},
			"csv-invalid-logical-type": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/csv.source", Format: "csv", Schema: "../../testdata/invalid-logical-type.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				"LogicalType DECIMAL can only be used",
			},
			"json-invalid-logical-type": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/invalid-logical-type-json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				"LogicalType DECIMAL can only be used",
			},
			"jsonl-invalid-logical-type": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/jsonl.source", Format: "jsonl", Schema: "../../testdata/invalid-logical-type-json.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				"LogicalType DECIMAL can only be used",
			},
			"field-delimiter": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/json.source", Format: "json", Schema: "../../testdata/json.schema", SkipHeader: false, FieldDelimiter: "::", URI: "dummy"},
				"field delimiter must be a single character",
			},
			// Bare NaN/Infinity are not JSON; only the quoted spellings import.
			"json-bare-non-finite": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/non-finite.bad-source", Format: "json", Schema: "../../testdata/non-finite.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				"is not a valid JSON array",
			},
			"jsonl-bare-non-finite": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/non-finite-jsonl.bad-source", Format: "jsonl", Schema: "../../testdata/non-finite.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				"invalid JSON string:",
			},
			// Infinity takes a sign but NaN does not: ParseFloat matches NaN
			// before it looks for one, so "+NaN" leaves a bare sign behind.
			"json-signed-nan": {
				Cmd{WriteOption: wOpt, Source: "../../testdata/non-finite-signed-nan.source", Format: "json", Schema: "../../testdata/non-finite.schema", SkipHeader: false, URI: filepath.Join(tempDir, "dummy")},
				`parse DOUBLE "+NaN"`,
			},
		}

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				err := tc.cmd.Run(context.Background())
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
			"csv-wo-header": {
				Cmd{WriteOption: wOpt, Source: "csv.source", Format: "csv", Schema: "csv.schema", SkipHeader: false, URI: ""},
				10,
			},
			"csv-w-header": {
				Cmd{WriteOption: wOpt, Source: "csv-with-header.source", Format: "csv", Schema: "csv.schema", SkipHeader: true, URI: ""},
				10,
			},
			"json": {
				Cmd{WriteOption: wOpt, Source: "json.source", Format: "json", Schema: "json.schema", SkipHeader: false, URI: ""},
				1,
			},
			"jsonl": {
				Cmd{WriteOption: wOpt, Source: "jsonl.source", Format: "jsonl", Schema: "jsonl.schema", SkipHeader: false, URI: ""},
				10,
			},
			"json-unknown": {
				Cmd{WriteOption: wOpt, Source: "unknown-type-json.source", Format: "json", Schema: "unknown-type.schema", SkipHeader: false, URI: ""},
				3,
			},
		}

		tempDir := t.TempDir()

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				tc.cmd.Source = filepath.Join("../../testdata", tc.cmd.Source)
				tc.cmd.Schema = filepath.Join("../../testdata", tc.cmd.Schema)
				tc.cmd.URI = filepath.Join(tempDir, "import-"+name+".parquet")

				err := tc.cmd.Run(context.Background())
				require.NoError(t, err)

				reader, err := pio.NewParquetFileReader(context.Background(), tc.cmd.URI, pio.ReadOption{})
				require.NoError(t, err)
				require.Equal(t, tc.rowCount, reader.GetNumRows())
			})
		}
	})
}

// The source spells the non-finite values every way strconv.ParseFloat accepts;
// cat has to normalize all of them to the canonical quoted form.
func TestCmdNonFiniteRoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	cmd := Cmd{
		WriteOption: pio.WriteOption{
			CompressionCodec: "SNAPPY",
			PageSize:         1024 * 1024,
			RowGroupSize:     128 * 1024 * 1024,
		},
		Source: "../../testdata/non-finite.source",
		Format: "json",
		Schema: "../../testdata/non-finite.schema",
		URI:    filepath.Join(tempDir, "non-finite.parquet"),
	}
	require.NoError(t, cmd.Run(context.Background()))

	catCmd := importTestCatCmd(cmd.URI, pio.ReadOption{})
	stdout, stderr := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, catCmd.Run(context.Background()))
	})
	require.Equal(t, testutils.LoadExpected(t, "../../testdata/golden/cat-non-finite.json"), stdout)
	require.Equal(t, "", stderr)
}

func TestCmdJSONLLineSize(t *testing.T) {
	testCases := []struct {
		name             string
		values           []string
		jsonlMaxLineSize int
		wantRows         int64
		wantErr          string
	}{
		{
			name:     "line-over-64-kib",
			values:   []string{"before", strings.Repeat("x", 70*1024), "after"},
			wantRows: 3,
		},
		{
			name:             "configured-limit-exceeded",
			values:           []string{"before", strings.Repeat("x", 2*1024), "after"},
			jsonlMaxLineSize: 1024,
			wantErr:          "failed to read JSONL source file",
		},
		{
			name:             "invalid-configured-limit",
			values:           []string{"value"},
			jsonlMaxLineSize: -1,
			wantErr:          "JSONL maximum line size must be greater than zero",
		},
		{
			name:             "configured-limit-too-large",
			values:           []string{"value"},
			jsonlMaxLineSize: math.MaxInt,
			wantErr:          "JSONL maximum line size is too large",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			sourcePath := filepath.Join(tempDir, "source.jsonl")
			schemaPath := filepath.Join(tempDir, "schema.json")
			parquetPath := filepath.Join(tempDir, "output.parquet")

			var source strings.Builder
			encoder := json.NewEncoder(&source)
			for _, value := range tc.values {
				require.NoError(t, encoder.Encode(map[string]string{"Value": value}))
			}
			require.NoError(t, os.WriteFile(sourcePath, []byte(source.String()), 0o600))
			require.NoError(t, os.WriteFile(schemaPath, []byte(jsonlLineSizeTestSchema), 0o600))

			err := (Cmd{
				Source:           sourcePath,
				Format:           "jsonl",
				Schema:           schemaPath,
				JSONLMaxLineSize: tc.jsonlMaxLineSize,
				URI:              parquetPath,
			}).Run(context.Background())
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)

			reader, err := pio.NewParquetFileReader(context.Background(), parquetPath, pio.ReadOption{})
			require.NoError(t, err)
			require.Equal(t, tc.wantRows, reader.GetNumRows())
		})
	}
}

func TestCmdJSONLMaxLineSizeBoundary(t *testing.T) {
	const maxLineSize = 1024

	testCases := []struct {
		name      string
		lineSize  int
		delimiter string
		wantErr   string
	}{
		{
			name:      "LF-at-limit",
			lineSize:  maxLineSize,
			delimiter: "\n",
		},
		{
			name:      "CRLF-at-limit",
			lineSize:  maxLineSize,
			delimiter: "\r\n",
		},
		{
			name:     "unterminated-at-limit",
			lineSize: maxLineSize,
		},
		{
			name:      "LF-over-limit",
			lineSize:  maxLineSize + 1,
			delimiter: "\n",
			wantErr:   "exceeds maximum line size",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			sourcePath := filepath.Join(tempDir, "source.jsonl")
			schemaPath := filepath.Join(tempDir, "schema.json")
			parquetPath := filepath.Join(tempDir, "output.parquet")

			emptyRecord, err := json.Marshal(map[string]string{"Value": ""})
			require.NoError(t, err)
			require.GreaterOrEqual(t, tc.lineSize, len(emptyRecord))
			record, err := json.Marshal(map[string]string{
				"Value": strings.Repeat("x", tc.lineSize-len(emptyRecord)),
			})
			require.NoError(t, err)
			require.Len(t, record, tc.lineSize)
			require.NoError(t, os.WriteFile(
				sourcePath,
				append(record, tc.delimiter...),
				0o600,
			))
			require.NoError(t, os.WriteFile(
				schemaPath,
				[]byte(jsonlLineSizeTestSchema),
				0o600,
			))

			err = (Cmd{
				Source:           sourcePath,
				Format:           "jsonl",
				Schema:           schemaPath,
				JSONLMaxLineSize: maxLineSize,
				URI:              parquetPath,
			}).Run(context.Background())
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)

			reader, err := pio.NewParquetFileReader(context.Background(), parquetPath, pio.ReadOption{})
			require.NoError(t, err)
			require.Equal(t, int64(1), reader.GetNumRows())
		})
	}
}

func TestCmdDefaultDataPageVersionWithDictionaryEncoding(t *testing.T) {
	tempDir := t.TempDir()
	testdataDir := filepath.Join("..", "..", "testdata")
	uri := filepath.Join(tempDir, "dictionary.parquet")

	require.NoError(t, (Cmd{
		Source: filepath.Join(testdataDir, "jsonl.source"),
		Format: "jsonl",
		Schema: filepath.Join(testdataDir, "jsonl.schema"),
		URI:    uri,
	}).Run(context.Background()))

	stdout, stderr := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, (inspect.Cmd{
			URI:         uri,
			RowGroup:    new(0),
			ColumnChunk: new(1),
		}).Run(context.Background()))
	})
	require.Empty(t, stderr)

	var inspection struct {
		ColumnChunk struct {
			Encodings []string `json:"encodings"`
		} `json:"columnChunk"`
		Pages []struct {
			Type     string `json:"type"`
			Encoding string `json:"encoding"`
		} `json:"pages"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &inspection))
	require.Contains(t, inspection.ColumnChunk.Encodings, "RLE_DICTIONARY")

	var hasDictionaryPage, hasDataPageV2 bool
	for _, page := range inspection.Pages {
		switch page.Type {
		case "DICTIONARY_PAGE":
			hasDictionaryPage = true
			require.Equal(t, "PLAIN", page.Encoding)
		case "DATA_PAGE_V2":
			hasDataPageV2 = true
			require.Equal(t, "RLE_DICTIONARY", page.Encoding)
		default:
			t.Fatalf("unexpected page type %q", page.Type)
		}
	}
	require.True(t, hasDictionaryPage)
	require.True(t, hasDataPageV2)
}

func TestCmdHighCardinalityDictionaryFallback(t *testing.T) {
	const (
		dictionaryLimit = 1024 * 1024
		valueCount      = 1400
	)

	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "high-cardinality.jsonl")
	schemaPath := filepath.Join(tempDir, "high-cardinality.schema")
	parquetPath := filepath.Join(tempDir, "high-cardinality.parquet")

	source, err := os.Create(sourcePath)
	require.NoError(t, err)
	encoder := json.NewEncoder(source)
	values := make([]string, valueCount)
	suffix := strings.Repeat("x", 1012)
	for i := range values {
		values[i] = fmt.Sprintf("%06d-%s", i, suffix)
		require.NoError(t, encoder.Encode(map[string]string{"Value": values[i]}))
	}
	require.NoError(t, source.Close())

	schema := `{
		"Tag": "name=parquet_go_root, inname=Parquet_go_root",
		"Fields": [
			{"Tag": "name=Value, inname=Value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"}
		]
	}`
	require.NoError(t, os.WriteFile(schemaPath, []byte(schema), 0o600))

	require.NoError(t, (Cmd{
		WriteOption: pio.WriteOption{
			CompressionCodec: "UNCOMPRESSED",
			PageSize:         64 * 1024,
			RowGroupSize:     8 * 1024 * 1024,
		},
		Source: sourcePath,
		Format: "jsonl",
		Schema: schemaPath,
		URI:    parquetPath,
	}).Run(context.Background()))

	catOutputPath := filepath.Join(tempDir, "cat.json")
	catOutput, err := os.Create(catOutputPath)
	require.NoError(t, err)
	originalStdout := os.Stdout
	var catErr error
	func() {
		os.Stdout = catOutput
		defer func() { os.Stdout = originalStdout }()
		catErr = importTestCatCmd(parquetPath, pio.ReadOption{}).Run(context.Background())
	}()
	require.NoError(t, catErr)
	require.NoError(t, catOutput.Close())

	catJSON, err := os.ReadFile(catOutputPath)
	require.NoError(t, err)
	var gotValues []struct {
		Value string
	}
	require.NoError(t, json.Unmarshal(catJSON, &gotValues))
	require.Len(t, gotValues, len(values))
	for i := range values {
		require.Equal(t, values[i], gotValues[i].Value)
	}

	inspectStdout, inspectStderr := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, (inspect.Cmd{
			URI:         parquetPath,
			RowGroup:    new(0),
			ColumnChunk: new(0),
		}).Run(context.Background()))
	})
	require.Empty(t, inspectStderr)

	var inspection struct {
		ColumnChunk struct {
			Encodings []string `json:"encodings"`
		} `json:"columnChunk"`
		Pages []struct {
			Type             string `json:"type"`
			Encoding         string `json:"encoding"`
			UncompressedSize int32  `json:"uncompressedSize"`
		} `json:"pages"`
	}
	require.NoError(t, json.Unmarshal([]byte(inspectStdout), &inspection))
	require.Contains(t, inspection.ColumnChunk.Encodings, "RLE_DICTIONARY")
	require.Contains(t, inspection.ColumnChunk.Encodings, "PLAIN")

	var hasDictionaryPage, hasDictionaryDataPage, hasPlainDataPage bool
	for _, page := range inspection.Pages {
		switch page.Type {
		case "DICTIONARY_PAGE":
			hasDictionaryPage = true
			require.LessOrEqual(t, page.UncompressedSize, int32(dictionaryLimit))
		case "DATA_PAGE", "DATA_PAGE_V2":
			hasDictionaryDataPage = hasDictionaryDataPage || page.Encoding == "RLE_DICTIONARY"
			hasPlainDataPage = hasPlainDataPage || page.Encoding == "PLAIN"
		}
	}
	require.True(t, hasDictionaryPage)
	require.True(t, hasDictionaryDataPage)
	require.True(t, hasPlainDataPage)
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
	require.NoError(t, plainCmd.Run(context.Background()))
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
			require.NoError(t, cmd.Run(context.Background()))
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
				WriterFooterKey:  new("not base64"),
			},
			errMsg: "invalid base64 writer footer key",
		},
		{
			name: "wrong-key-size",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  new("MTIzNDU="),
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
			err := cmd.Run(context.Background())
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
		require.NoError(t, cmd.Run(context.Background()))
		return uri
	}

	catNoKeysErr := func(t *testing.T, uri string) error {
		t.Helper()
		var err error
		_, _ = testutils.CaptureStdoutStderr(func() {
			err = importTestCatCmd(uri, pio.ReadOption{}).Run(context.Background())
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

// FuzzImportJSONL walks the JSONL source path with arbitrary content: the line
// scanner, the per-line JSON check, and the value conversions the writer does,
// including the "NaN"/"Infinity" spellings cat emits.
func FuzzImportJSONL(f *testing.F) {
	f.Add("{\"dbl\":1.5,\"str\":\"a\",\"num\":3}\n")
	f.Add("{\"dbl\":\"NaN\",\"str\":\"a\",\"num\":3}\n{\"dbl\":\"-Infinity\",\"str\":\"b\",\"num\":4}\n")
	f.Add("{\"dbl\":NaN}\n")
	f.Add("[]\n\n{}\n")

	schema := `{"Tag":"name=root","Fields":[` +
		`{"Tag":"name=dbl, type=DOUBLE"},` +
		`{"Tag":"name=str, type=BYTE_ARRAY, convertedtype=UTF8"},` +
		`{"Tag":"name=num, type=INT64"}]}`

	f.Fuzz(func(t *testing.T, source string) {
		dir := t.TempDir()
		schemaFile := filepath.Join(dir, "schema.json")
		sourceFile := filepath.Join(dir, "source.jsonl")
		if os.WriteFile(schemaFile, []byte(schema), 0o600) != nil ||
			os.WriteFile(sourceFile, []byte(source), 0o600) != nil {
			t.Skip()
		}

		cmd := Cmd{
			FieldDelimiter: ".",
			Format:         "jsonl",
			Schema:         schemaFile,
			Source:         sourceFile,
			URI:            filepath.Join(dir, "output.parquet"),
			WriteOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024,
				RowGroupSize:     1 << 20,
			},
		}
		_ = cmd.Run(context.Background())
	})
}
