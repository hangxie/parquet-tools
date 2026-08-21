package cat

import (
	"context"
	"encoding/binary"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
	pio "github.com/hangxie/parquet-tools/io"
)

func writeTruncatedColumnChunk(t *testing.T) string {
	t.Helper()

	const sourcePath = "../../testdata/good.parquet"
	data, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), 8)

	fileReader, err := pio.NewParquetFileReader(context.Background(), sourcePath, pio.ReadOption{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fileReader.PFile.Close()) })

	tempDir := t.TempDir()
	column := fileReader.Footer.RowGroups[0].Columns[0]
	chunkStart := column.MetaData.DataPageOffset
	if column.MetaData.DictionaryPageOffset != nil {
		chunkStart = *column.MetaData.DictionaryPageOffset
	}
	chunkEnd := chunkStart + column.MetaData.TotalCompressedSize
	require.LessOrEqual(t, chunkEnd, int64(len(data)))

	// Store one real three-row chunk in an external file ending immediately after
	// its page, then make its metadata declare two additional values.
	chunkPath := filepath.Join(tempDir, "truncated-column-chunk.bin")
	externalChunk := append(make([]byte, chunkStart), data[chunkStart:chunkEnd]...)
	require.NoError(t, os.WriteFile(chunkPath, externalChunk, 0o600))
	column.FilePath = new(chunkPath)
	column.MetaData.NumValues += 2

	fileReader.Footer.NumRows += 2
	rowGroup := fileReader.Footer.RowGroups[0]
	rowGroup.NumRows += 2

	serializer := thrift.NewTSerializer()
	serializer.Protocol = thrift.NewTCompactProtocolFactoryConf(
		&thrift.TConfiguration{},
	).GetProtocol(serializer.Transport)
	footer, err := serializer.Write(context.Background(), fileReader.Footer)
	require.NoError(t, err)

	oldFooterSize := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	bodyEnd := len(data) - oldFooterSize - 8
	require.GreaterOrEqual(t, bodyEnd, 4)

	truncated := append([]byte(nil), data[:bodyEnd]...)
	truncated = append(truncated, footer...)
	truncated = binary.LittleEndian.AppendUint32(truncated, uint32(len(footer)))
	truncated = append(truncated, data[len(data)-4:]...)

	path := filepath.Join(tempDir, "truncated-column-chunk.parquet")
	require.NoError(t, os.WriteFile(path, truncated, 0o600))
	return path
}

var (
	encFooterKey = new("MDEyMzQ1Njc4OTAxMjM0NQ==")
	encDoubleKey = "MTIzNDU2Nzg5MDEyMzQ1MA=="
	encFloatKey  = "MTIzNDU2Nzg5MDEyMzQ1MQ=="
	encAADPrefix = new("dGVzdGVy")
	encWrongKey  = "d3Jvbmd3cm9uZ3dyb25nMQ=="
)

func TestCmd(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    Cmd
		golden string
		errMsg string
	}{
		// error cases
		"non-existent-file": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "file/does/not/exist"},
			errMsg: "no such file or directory",
		},
		"parquet-1481": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "../../testdata/PARQUET-1481.parquet"},
			errMsg: "unknown parquet type: <UNSET>",
		},
		"arrow-rs-gh-6229": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "../../testdata/ARROW-RS-GH-6229-LEVELS.parquet"},
			errMsg: "data page value count 21 exceeds column chunk total 1",
		},
		"invalid-read-page-size": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 0, SampleRatio: 0.5, Format: "json", NoHeader: false, URI: "does/not/matter", Concurrent: true},
			errMsg: "invalid read page size",
		},
		"invalid-skip-size": {
			cmd:    Cmd{ReadOption: rOpt, Skip: -10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "json", NoHeader: false, URI: "does/not/matter"},
			errMsg: "invalid skip -10",
		},
		"sampling-too-high": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 2.0, Format: "json", NoHeader: false, URI: "does/not/matter", Concurrent: true},
			errMsg: "invalid sampling",
		},
		"sampling-too-low": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: -0.5, Format: "json", NoHeader: false, URI: "does/not/matter"},
			errMsg: "invalid sampling",
		},
		"invalid-format": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "foobar", NoHeader: false, URI: "does/not/matter"},
			errMsg: "unknown format: [foobar]",
		},
		"fail-on-int96": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "json", NoHeader: true, URI: "../../testdata/all-types.parquet", FailOnInt96: true},
			errMsg: "type INT96 which is not supported",
		},
		"nested-schema-csv": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "csv", NoHeader: true, URI: "../../testdata/all-types.parquet"},
			errMsg: "field [Variant] is not scalar type",
		},
		"nested-schema-tsv": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "tsv", NoHeader: true, URI: "../../testdata/all-types.parquet"},
			errMsg: "field [Variant] is not scalar type",
		},
		"geospatial-csv": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "csv", NoHeader: true, URI: "../../testdata/geospatial.parquet"},
			errMsg: "field [Geometry] is not scalar type",
		},
		"geospatial-tsv": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "tsv", NoHeader: true, URI: "../../testdata/geospatial.parquet"},
			errMsg: "field [Geometry] is not scalar type",
		},
		"nan-json-error": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "../../testdata/nan.parquet"},
			errMsg: "json: unsupported value: NaN",
		},
		"arrow-gh-41321": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "../../testdata/ARROW-GH-41321.parquet"},
			errMsg: "failed to cat",
		},
		"concurrent-non-existent": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "file/does/not/exist", Concurrent: true},
			errMsg: "no such file or directory",
		},
		"concurrent-nan-json": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "../../testdata/nan.parquet", Concurrent: true},
			errMsg: "json: unsupported value: NaN",
		},
		"concurrent-int96": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "json", NoHeader: true, URI: "../../testdata/all-types.parquet", Concurrent: true, FailOnInt96: true},
			errMsg: "type INT96 which is not supported",
		},
		"concurrent-nested-csv": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "csv", NoHeader: true, URI: "../../testdata/all-types.parquet", Concurrent: true},
			errMsg: "is not scalar type",
		},
		"encrypted-footer-no-key": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 1, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", URI: "../../testdata/encrypted-footer.parquet"},
			errMsg: "decryption key required for footer",
		},
		"encrypted-footer-wrong-key": {
			cmd:    Cmd{ReadOption: pio.ReadOption{FooterKey: &encWrongKey}, Skip: 0, Limit: 1, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", URI: "../../testdata/encrypted-footer.parquet"},
			errMsg: "decrypt",
		},
		"encrypted-columns-missing-col-key": {
			cmd:    Cmd{ReadOption: pio.ReadOption{FooterKey: encFooterKey}, Skip: 0, Limit: 1, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", URI: "../../testdata/encrypted-columns.parquet"},
			errMsg: "decryption key required for column",
		},
		"encrypted-aad-missing": {
			cmd:    Cmd{ReadOption: pio.ReadOption{FooterKey: encFooterKey, ColumnKeys: []string{"double_field=" + encDoubleKey, "float_field=" + encFloatKey}}, Skip: 0, Limit: 1, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", URI: "../../testdata/encrypted-aad.parquet"},
			errMsg: "AAD prefix",
		},

		// good cases
		"default": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet"},
			golden: "cat-good-json.json",
		},
		"limit-0": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet"},
			golden: "cat-good-json.json",
		},
		"limit-2": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 2, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet"},
			golden: "cat-good-json-limit-2.json",
		},
		"skip-2": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 2, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet", Concurrent: true},
			golden: "cat-good-json-skip-2.json",
		},
		"skip-all": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 20, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet"},
			golden: "empty-json.txt",
		},
		"sampling-0": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 0.0, Format: "json", NoHeader: false, URI: "good.parquet"},
			golden: "empty-json.txt",
		},
		"empty": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "empty.parquet"},
			golden: "empty-json.txt",
		},
		"jsonl": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: false, URI: "good.parquet"},
			golden: "cat-good-jsonl.jsonl",
		},
		"csv": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "csv", NoHeader: false, URI: "good.parquet"},
			golden: "cat-good-csv.txt",
		},
		"csv-no-header": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "csv", NoHeader: true, URI: "good.parquet"},
			golden: "cat-good-csv-no-header.txt",
		},
		"tsv": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "tsv", NoHeader: false, URI: "good.parquet"},
			golden: "cat-good-tsv.txt",
		},
		"tsv-no-header": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "tsv", NoHeader: true, URI: "good.parquet"},
			golden: "cat-good-tsv-no-header.txt",
		},
		"all-types": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "all-types.parquet"},
			golden: "cat-all-types.jsonl",
		},
		"geospatial-hex": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", GeoFormat: "hex", NoHeader: true, URI: "geospatial.parquet"},
			golden: "cat-geospatial-hex.jsonl",
		},
		"geospatial-base64": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", GeoFormat: "base64", NoHeader: true, URI: "geospatial.parquet"},
			golden: "cat-geospatial-base64.jsonl",
		},
		"geospatial-geojson": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "geospatial.parquet"},
			golden: "cat-geospatial-geojson.jsonl",
		},
		"old-style-list": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "old-style-list.parquet"},
			golden: "cat-old-style-list.jsonl",
		},
		"multi-row-groups": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "row-group.parquet"},
			golden: "cat-row-group.jsonl",
		},
		"skip-uneven-row-groups": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 3, Limit: 1, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "row-group.parquet"},
			golden: "cat-row-group-skip-3.jsonl",
		},
		"skip-across-uneven-row-groups": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 18, Limit: 1, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "row-group.parquet"},
			golden: "cat-row-group-skip-18.jsonl",
		},
		"dict-page": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "dict-page.parquet"},
			golden: "cat-dict-page.jsonl",
		},
		"high-compression": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 1, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "high-compression.parquet"},
			golden: "cat-high-compression.jsonl",
		},
		"unknown-type": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "unknown-type.parquet"},
			golden: "cat-unknown-type.jsonl",
		},
		"unknown-type-raw": {
			cmd:    Cmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "unknown-type.parquet", RawUnknown: true},
			golden: "cat-unknown-type-raw.jsonl",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if tc.errMsg == "" {
				t.Parallel()
			}
			cmd := tc.cmd
			if tc.golden != "" {
				cmd.URI = "file://../../testdata/" + tc.cmd.URI
			}
			if tc.errMsg != "" {
				err := cmd.Run(context.Background())
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				stdout, stderr := testutils.CaptureStdoutStderr(func() {
					require.NoError(t, cmd.Run(context.Background()))
				})
				require.Equal(t, testutils.LoadExpected(t, "../../testdata/golden/"+tc.golden), stdout)
				require.Equal(t, "", stderr)
			}
		})
	}
}

func TestCmdEncrypted(t *testing.T) {
	columnKeys := []string{"double_field=" + encDoubleKey, "float_field=" + encFloatKey}
	testCases := map[string]struct {
		readOption pio.ReadOption
		uri        string
	}{
		"columns": {
			readOption: pio.ReadOption{
				FooterKey:  encFooterKey,
				ColumnKeys: columnKeys,
			},
			uri: "file://../../testdata/encrypted-columns.parquet",
		},
		"footer": {
			readOption: pio.ReadOption{
				FooterKey:  encFooterKey,
				ColumnKeys: columnKeys,
			},
			uri: "file://../../testdata/encrypted-footer.parquet",
		},
		"aad": {
			readOption: pio.ReadOption{
				FooterKey:  encFooterKey,
				ColumnKeys: columnKeys,
				AADPrefix:  encAADPrefix,
			},
			uri: "file://../../testdata/encrypted-aad.parquet",
		},
		"uniform": {
			readOption: pio.ReadOption{
				FooterKey: encFooterKey,
			},
			uri: "file://../../testdata/uniform-encryption.parquet",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cmd := Cmd{
				ReadOption:   tc.readOption,
				Limit:        1,
				ReadPageSize: 10,
				SampleRatio:  1.0,
				Format:       "jsonl",
				URI:          tc.uri,
			}

			stdout, stderr := testutils.CaptureStdoutStderr(func() {
				require.NoError(t, cmd.Run(context.Background()))
			})
			require.Contains(t, stdout, `"double_field"`)
			require.Contains(t, stdout, `"float_field"`)
			require.Equal(t, "", stderr)
		})
	}
}

func TestCmdTruncatedColumnChunk(t *testing.T) {
	cmd := Cmd{
		ReadPageSize: 10,
		SampleRatio:  1.0,
		Format:       "jsonl",
		URI:          writeTruncatedColumnChunk(t),
	}

	var runErr error
	stdout, stderr := testutils.CaptureStdoutStderr(func() {
		runErr = cmd.Run(context.Background())
	})

	require.ErrorContains(t, runErr, "truncated column chunk")
	require.Empty(t, strings.TrimSpace(stdout))
	require.Empty(t, stderr)
}

func TestCmdEncoder(t *testing.T) {
	rOpt := pio.ReadOption{}

	testCases := map[string]struct {
		setup       func(t *testing.T) (context.Context, context.CancelFunc, chan any, chan string, *reader.ParquetReader, []string)
		wantErr     bool
		errContains string
	}{
		"context-cancelled-in-main-loop": {
			setup: func(t *testing.T) (context.Context, context.CancelFunc, chan any, chan string, *reader.ParquetReader, []string) {
				// Create a context that we can cancel
				ctx, cancel := context.WithCancel(context.Background())

				// Create channels
				rowChan := make(chan any, 10)
				outputChan := make(chan string, 10)

				// Open a test parquet file
				fileReader, err := pio.NewParquetFileReader(context.Background(), "file://../../testdata/good.parquet", rOpt)
				require.NoError(t, err)

				// Populate rowChan with some data, then cancel context
				rows, err := fileReader.ReadByNumberWithContext(context.Background(), 5)
				require.NoError(t, err)

				// Send one row and then cancel
				rowChan <- rows[0]
				cancel() // Cancel the context immediately

				return ctx, cancel, rowChan, outputChan, fileReader, nil
			},
			wantErr:     true,
			errContains: "context canceled",
		},
		"context-cancelled-before-send": {
			setup: func(t *testing.T) (context.Context, context.CancelFunc, chan any, chan string, *reader.ParquetReader, []string) {
				// Create a context that we can cancel
				ctx, cancel := context.WithCancel(context.Background())

				// Create channels
				rowChan := make(chan any, 10)
				outputChan := make(chan string, 1) // Small buffer to test the send path

				// Open a test parquet file
				fileReader, err := pio.NewParquetFileReader(context.Background(), "file://../../testdata/good.parquet", rOpt)
				require.NoError(t, err)

				// Populate rowChan with data
				rows, err := fileReader.ReadByNumberWithContext(context.Background(), 5)
				require.NoError(t, err)

				// Fill output channel to block sending
				outputChan <- "blocking"

				// Send row to process
				rowChan <- rows[0]

				// Cancel before the encoder can send to outputChan
				cancel()

				return ctx, cancel, rowChan, outputChan, fileReader, nil
			},
			wantErr:     true,
			errContains: "context canceled",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel, rowChan, outputChan, fileReader, fieldList := tc.setup(t)
			defer cancel()
			defer func() { _ = fileReader.PFile.Close() }()

			cmd := Cmd{
				Format: "json",
			}

			err := cmd.encoder(ctx, rowChan, outputChan, fileReader.SchemaHandler, fieldList, nil)

			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// cloneCountingFile counts Clone calls, which only the per-column page header
// reads make, so a nonzero count means cat paid for encoding information.
type cloneCountingFile struct {
	source.ParquetFileReader
	clones *atomic.Int64
}

func (f *cloneCountingFile) Clone() (source.ParquetFileReader, error) {
	f.clones.Add(1)
	cloned, err := f.ParquetFileReader.Clone()
	if err != nil {
		return nil, err
	}
	return &cloneCountingFile{ParquetFileReader: cloned, clones: f.clones}, nil
}

func TestOutputRowsSkipsPageEncoding(t *testing.T) {
	for _, format := range []string{"json", "csv"} {
		t.Run(format, func(t *testing.T) {
			fileReader, err := pio.NewParquetFileReader(context.Background(), "../../testdata/good.parquet", pio.ReadOption{})
			require.NoError(t, err)
			defer func() { require.NoError(t, fileReader.PFile.Close()) }()

			clones := new(atomic.Int64)
			fileReader.PFile = &cloneCountingFile{ParquetFileReader: fileReader.PFile, clones: clones}

			cmd := Cmd{Limit: 1, ReadPageSize: 10, SampleRatio: 1.0, Format: format}
			stdout, stderr := testutils.CaptureStdoutStderr(func() {
				require.NoError(t, cmd.outputRows(context.Background(), fileReader))
			})
			require.NotEmpty(t, strings.TrimSpace(stdout))
			require.Empty(t, stderr)
			require.Zero(t, clones.Load(), "cat does not use page encoding, it should not read data page headers")
		})
	}
}

func TestNullifyUnknownColumnsIgnoresNonMapRow(t *testing.T) {
	nullifyUnknownCols("row", map[string]struct{}{"unknown": {}})
}

func TestCmdEncoderRejectsUnsupportedFormat(t *testing.T) {
	fileReader, err := pio.NewParquetFileReader(context.Background(), "file://../../testdata/good.parquet", pio.ReadOption{})
	require.NoError(t, err)
	defer func() { require.NoError(t, fileReader.PFile.Close()) }()

	rows, err := fileReader.ReadByNumberWithContext(context.Background(), 1)
	require.NoError(t, err)
	rowChan := make(chan any, 1)
	rowChan <- rows[0]

	err = (Cmd{Format: "unsupported"}).encoder(
		context.Background(), rowChan, make(chan string, 1), fileReader.SchemaHandler, nil, nil,
	)
	require.ErrorContains(t, err, "unsupported format: [unsupported]")
}

func TestValuesToCSVReturnsWriterError(t *testing.T) {
	builder := new(strings.Builder)
	writer := csv.NewWriter(builder)
	writer.Comma = '\n'

	line, err := (&Cmd{}).valuesToCSV([]string{"value"}, builder, writer)
	require.Empty(t, line)
	require.ErrorContains(t, err, "invalid field or comment delimiter")
}

func BenchmarkCatCmd(b *testing.B) {
	// savedStdout, savedStderr := os.Stdout, os.Stderr
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0o666)
	if err != nil {
		b.Fatal(err)
	}
	os.Stdout = devNull
	defer func() {
		// os.Stdout, os.Stderr = savedStdout, savedStderr
		// _ = devNull.Close()
	}()

	cmd := Cmd{
		ReadOption:   pio.ReadOption{},
		ReadPageSize: 1000,
		SampleRatio:  1.0,
		Format:       "jsonl",
		URI:          "../../build/benchmark.parquet",
	}
	// Warm up the Go runtime before actual benchmark
	for range 10 {
		_ = cmd.Run(context.Background())
	}

	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run(context.Background()))
		}
	})

	cmd.Concurrent = true
	b.Run("concurrent", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run(context.Background()))
		}
	})

	cmd.Format = "csv"
	cmd.URI = "../../build/flat.parquet"
	cmd.Concurrent = true
	b.Run("csv", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run(context.Background()))
		}
	})
}

func TestMapToStrList(t *testing.T) {
	testCases := map[string]struct {
		flatValues map[string]any
		fieldList  []string
		expected   []string
	}{
		"all-present": {
			flatValues: map[string]any{"a": "x", "b": int64(2), "c": nil},
			fieldList:  []string{"a", "b", "c"},
			expected:   []string{"x", "2", ""},
		},
		"reordered": {
			flatValues: map[string]any{"a": "x", "b": "y"},
			fieldList:  []string{"b", "a"},
			expected:   []string{"y", "x"},
		},
		// output slice must be sized by the header field list, not the row map;
		// a row map with fewer keys than fields must not panic.
		"fewer-map-keys": {
			flatValues: map[string]any{"a": "x"},
			fieldList:  []string{"a", "b", "c"},
			expected:   []string{"x", "", ""},
		},
		"empty-field-list": {
			flatValues: map[string]any{"a": "x"},
			fieldList:  []string{},
			expected:   []string{},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, mapToStrList(tc.flatValues, tc.fieldList))
		})
	}
}
