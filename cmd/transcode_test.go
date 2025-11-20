package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
)

func Test_TranscodeCmd_Run_error(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{Compression: "SNAPPY"}
	tempDir := t.TempDir()

	testCases := map[string]struct {
		cmd    TranscodeCmd
		errMsg string
	}{
		"pagesize-too-small":  {TranscodeCmd{DataPageVersion: 1, ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 0, Source: "../testdata/good.parquet", URI: "dummy"}, "invalid read page size"},
		"source-non-existent": {TranscodeCmd{DataPageVersion: 1, ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "does/not/exist", URI: "dummy"}, "no such file or directory"},
		"source-not-parquet":  {TranscodeCmd{DataPageVersion: 1, ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../testdata/not-a-parquet-file", URI: "dummy"}, "failed to read from"},
		"target-file":         {TranscodeCmd{DataPageVersion: 1, ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../testdata/good.parquet", URI: "://uri"}, "unable to parse file location"},
		"target-compression":  {TranscodeCmd{DataPageVersion: 1, ReadOption: rOpt, WriteOption: pio.WriteOption{}, ReadPageSize: 10, Source: "../testdata/good.parquet", URI: filepath.Join(tempDir, "dummy")}, "not a valid CompressionCode"},
		"target-write":        {TranscodeCmd{DataPageVersion: 1, ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../testdata/good.parquet", URI: "s3://target"}, "failed to close"},
		"invalid-encoding":    {TranscodeCmd{DataPageEncoding: "INVALID_ENCODING", DataPageVersion: 1, ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../testdata/good.parquet", URI: filepath.Join(tempDir, "dummy")}, "invalid encoding"},
		"deprecated-encoding": {TranscodeCmd{DataPageEncoding: "PLAIN_DICTIONARY", DataPageVersion: 1, ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../testdata/good.parquet", URI: filepath.Join(tempDir, "dummy")}, "PLAIN_DICTIONARY encoding is deprecated"},
		"fail-on-int96":       {TranscodeCmd{FailOnInt96: true, DataPageVersion: 1, ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../testdata/all-types.parquet", URI: filepath.Join(tempDir, "dummy")}, "has type INT96 which is not supported"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func Test_TranscodeCmd_Run_good(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		source          string
		compression     string
		dataPageVersion int32
		omitStats       string
		encoding        string
		rowCount        int64
	}{
		"good-gzip":           {"good.parquet", "GZIP", 1, "", "", 3},
		"good-zstd":           {"good.parquet", "ZSTD", 1, "", "", 3},
		"good-uncompressed":   {"good.parquet", "UNCOMPRESSED", 1, "", "", 3},
		"good-lz4":            {"good.parquet", "LZ4", 1, "", "", 3},
		"all-types-gzip":      {"all-types.parquet", "GZIP", 1, "", "", 10},
		"all-types-zstd":      {"all-types.parquet", "ZSTD", 1, "", "", 10},
		"empty-gzip":          {"empty.parquet", "GZIP", 1, "", "", 0},
		"good-v2":             {"good.parquet", "SNAPPY", 2, "", "", 3},
		"all-types-v2-zstd":   {"all-types.parquet", "ZSTD", 2, "", "", 10},
		"good-stats-true":     {"good.parquet", "SNAPPY", 1, "true", "", 3},
		"good-stats-false":    {"good.parquet", "SNAPPY", 1, "false", "", 3},
		"good-encoding-plain": {"good.parquet", "SNAPPY", 1, "", "PLAIN", 3},
		"good-encoding-rle":   {"good.parquet", "SNAPPY", 1, "", "RLE", 3},
		"good-all-options":    {"good.parquet", "ZSTD", 2, "true", "PLAIN", 3},
	}
	tempDir := t.TempDir()

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			wOpt := pio.WriteOption{Compression: tc.compression}
			cmd := TranscodeCmd{
				DataPageVersion:  tc.dataPageVersion,
				DataPageEncoding: tc.encoding,
				OmitStats:        tc.omitStats,
				ReadOption:       rOpt,
				WriteOption:      wOpt,
				ReadPageSize:     10,
				Source:           filepath.Join("..", "testdata", tc.source),
				URI:              filepath.Join(tempDir, name+".parquet"),
			}
			err := cmd.Run()
			require.NoError(t, err)

			// Verify the output file exists and has the correct row count
			reader, err := pio.NewParquetFileReader(cmd.URI, rOpt)
			require.NoError(t, err)
			rowCount := reader.GetNumRows()
			_ = reader.PFile.Close()
			require.Equal(t, tc.rowCount, rowCount)

			// Verify the file size (compression should make a difference)
			fileInfo, err := os.Stat(cmd.URI)
			require.NoError(t, err)
			require.Greater(t, fileInfo.Size(), int64(0))
		})
	}
}

func Test_TranscodeCmd_Run_verify_data(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{Compression: "ZSTD"}
	tempDir := t.TempDir()

	// Transcode a file
	cmd := TranscodeCmd{
		DataPageVersion: 1,
		ReadOption:      rOpt,
		WriteOption:     wOpt,
		ReadPageSize:    100,
		Source:          "../testdata/good.parquet",
		URI:             filepath.Join(tempDir, "transcoded.parquet"),
	}
	err := cmd.Run()
	require.NoError(t, err)

	// Verify the data is the same by using cat command
	catOriginal := CatCmd{
		ReadOption:   rOpt,
		ReadPageSize: 1000,
		SampleRatio:  1.0,
		Format:       "json",
		GeoFormat:    "geojson",
		URI:          "../testdata/good.parquet",
	}
	catTranscoded := CatCmd{
		ReadOption:   rOpt,
		ReadPageSize: 1000,
		SampleRatio:  1.0,
		Format:       "json",
		GeoFormat:    "geojson",
		URI:          cmd.URI,
	}

	originalOutput, _ := captureStdoutStderr(func() {
		require.NoError(t, catOriginal.Run())
	})
	transcodedOutput, _ := captureStdoutStderr(func() {
		require.NoError(t, catTranscoded.Run())
	})

	require.Equal(t, originalOutput, transcodedOutput)
}

func Test_TranscodeCmd_isEncodingCompatible(t *testing.T) {
	cmd := TranscodeCmd{}

	testCases := []struct {
		encoding string
		dataType string
		expected bool
	}{
		// PLAIN works with all types
		{"PLAIN", "INT32", true},
		{"PLAIN", "INT64", true},
		{"PLAIN", "BYTE_ARRAY", true},
		{"PLAIN", "BOOLEAN", true},
		{"PLAIN", "FLOAT", true},
		{"PLAIN", "DOUBLE", true},

		// Empty type (struct/group) should not accept encoding
		{"PLAIN", "", false},
		{"RLE", "", false},

		// Integer type encodings
		{"RLE", "INT32", true},
		{"RLE", "INT64", true},
		{"BIT_PACKED", "INT32", true},
		{"BIT_PACKED", "INT64", true},
		{"DELTA_BINARY_PACKED", "INT32", true},
		{"DELTA_BINARY_PACKED", "INT64", true},
		{"RLE_DICTIONARY", "INT32", true},
		{"RLE_DICTIONARY", "INT64", true},

		// Integer encodings should not work with other types
		{"DELTA_BINARY_PACKED", "BYTE_ARRAY", false},
		{"DELTA_BINARY_PACKED", "FLOAT", false},
		{"BIT_PACKED", "BYTE_ARRAY", false},
		{"BIT_PACKED", "FLOAT", false},

		// Byte array type encodings
		{"RLE", "BYTE_ARRAY", true},
		{"RLE", "FIXED_LEN_BYTE_ARRAY", true},
		{"DELTA_LENGTH_BYTE_ARRAY", "BYTE_ARRAY", true},
		{"DELTA_LENGTH_BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY", false},
		{"DELTA_BYTE_ARRAY", "BYTE_ARRAY", true},
		{"RLE_DICTIONARY", "BYTE_ARRAY", true},

		// Byte array encodings should not work with other types
		{"DELTA_LENGTH_BYTE_ARRAY", "INT32", false},
		{"DELTA_BYTE_ARRAY", "FLOAT", false},

		// Boolean encodings
		{"RLE", "BOOLEAN", true},
		{"BIT_PACKED", "BOOLEAN", true},

		// Float/Double encodings
		{"RLE_DICTIONARY", "FLOAT", true},
		{"RLE_DICTIONARY", "DOUBLE", true},
		{"BYTE_STREAM_SPLIT", "FLOAT", true},
		{"BYTE_STREAM_SPLIT", "DOUBLE", true},

		// Float-specific encodings should not work with other types
		{"BYTE_STREAM_SPLIT", "INT32", false},
		{"BYTE_STREAM_SPLIT", "BYTE_ARRAY", false},
		{"BYTE_STREAM_SPLIT", "BOOLEAN", false},

		// Case insensitivity
		{"plain", "int32", true},
		{"PlAiN", "InT32", true},
		{"rle", "boolean", true},

		// Unknown encodings
		{"UNKNOWN_ENCODING", "INT32", false},
		{"INVALID", "BYTE_ARRAY", false},

		// Unknown data types
		{"RLE", "INT96", false},
		{"PLAIN", "UNKNOWN_TYPE", true}, // PLAIN works with all types
	}

	for _, tc := range testCases {
		t.Run(tc.encoding+"_"+tc.dataType, func(t *testing.T) {
			result := cmd.isEncodingCompatible(tc.encoding, tc.dataType)
			require.Equal(t, tc.expected, result, "encoding=%s, type=%s", tc.encoding, tc.dataType)
		})
	}
}

func Test_TranscodeCmd_Run_schema_modification(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{Compression: "SNAPPY"}
	tempDir := t.TempDir()

	testCases := []struct {
		name             string
		cmd              TranscodeCmd
		expectedEncoding string
	}{
		{
			name: "schema with no modifications",
			cmd: TranscodeCmd{
				DataPageVersion: 1,
				ReadOption:      rOpt,
				WriteOption:     wOpt,
				ReadPageSize:    10,
				Source:          "../testdata/good.parquet",
				URI:             filepath.Join(tempDir, "no-mods.parquet"),
			},
			expectedEncoding: "",
		},
		{
			name: "schema modification with INT96 detection",
			cmd: TranscodeCmd{
				DataPageVersion:  1,
				DataPageEncoding: "PLAIN",
				ReadOption:       rOpt,
				WriteOption:      wOpt,
				ReadPageSize:     10,
				Source:           "../testdata/all-types.parquet",
				URI:              filepath.Join(tempDir, "with-int96.parquet"),
			},
			expectedEncoding: "PLAIN",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.NoError(t, err)

			// Verify output file
			reader, err := pio.NewParquetFileReader(tc.cmd.URI, rOpt)
			require.NoError(t, err)
			require.Greater(t, reader.GetNumRows(), int64(0))
			_ = reader.PFile.Close()
		})
	}
}

func Test_TranscodeCmd_Run_with_different_page_sizes(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{Compression: "SNAPPY"}
	tempDir := t.TempDir()

	pageSizes := []int{1, 5, 10, 100, 1000}
	for _, pageSize := range pageSizes {
		t.Run(fmt.Sprintf("pagesize-%d", pageSize), func(t *testing.T) {
			cmd := TranscodeCmd{
				DataPageVersion: 1,
				ReadOption:      rOpt,
				WriteOption:     wOpt,
				ReadPageSize:    pageSize,
				Source:          "../testdata/all-types.parquet",
				URI:             filepath.Join(tempDir, fmt.Sprintf("pagesize-%d.parquet", pageSize)),
			}
			err := cmd.Run()
			require.NoError(t, err)

			// Verify the data is correct
			reader, err := pio.NewParquetFileReader(cmd.URI, rOpt)
			require.NoError(t, err)
			require.Equal(t, int64(10), reader.GetNumRows())
			_ = reader.PFile.Close()
		})
	}
}

func Test_TranscodeCmd_Run_edge_cases(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{Compression: "SNAPPY"}
	tempDir := t.TempDir()

	testCases := []struct {
		name      string
		cmd       TranscodeCmd
		expectErr bool
		errMsg    string
	}{
		{
			name: "empty file",
			cmd: TranscodeCmd{
				DataPageVersion: 1,
				ReadOption:      rOpt,
				WriteOption:     wOpt,
				ReadPageSize:    10,
				Source:          "../testdata/empty.parquet",
				URI:             filepath.Join(tempDir, "empty-out.parquet"),
			},
			expectErr: false,
		},
		{
			name: "large page size",
			cmd: TranscodeCmd{
				DataPageVersion: 1,
				ReadOption:      rOpt,
				WriteOption:     wOpt,
				ReadPageSize:    10000,
				Source:          "../testdata/good.parquet",
				URI:             filepath.Join(tempDir, "large-page.parquet"),
			},
			expectErr: false,
		},
		{
			name: "all encoding and stats options",
			cmd: TranscodeCmd{
				DataPageVersion:  2,
				DataPageEncoding: "PLAIN",
				OmitStats:        "false",
				ReadOption:       rOpt,
				WriteOption:      wOpt,
				ReadPageSize:     10,
				Source:           "../testdata/good.parquet",
				URI:              filepath.Join(tempDir, "all-opts.parquet"),
			},
			expectErr: false,
		},
		{
			name: "multiple compression types",
			cmd: TranscodeCmd{
				DataPageVersion: 1,
				ReadOption:      rOpt,
				WriteOption:     pio.WriteOption{Compression: "LZ4_RAW"},
				ReadPageSize:    10,
				Source:          "../testdata/good.parquet",
				URI:             filepath.Join(tempDir, "lz4-raw.parquet"),
			},
			expectErr: false,
		},
		{
			name: "encoding with byte array fields",
			cmd: TranscodeCmd{
				DataPageVersion:  1,
				DataPageEncoding: "DELTA_BYTE_ARRAY",
				ReadOption:       rOpt,
				WriteOption:      wOpt,
				ReadPageSize:     10,
				Source:           "../testdata/good.parquet",
				URI:              filepath.Join(tempDir, "delta-byte-array.parquet"),
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cmd.Run()
			if tc.expectErr {
				require.Error(t, err)
				if tc.errMsg != "" {
					require.Contains(t, err.Error(), tc.errMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Benchmark_TranscodeCmd_Run(b *testing.B) {
	savedStdout, savedStderr := os.Stdout, os.Stderr
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0o666)
	if err != nil {
		panic(err)
	}
	os.Stdout = devNull
	defer func() {
		os.Stdout, os.Stderr = savedStdout, savedStderr
		_ = devNull.Close()
	}()

	tempDir := b.TempDir()
	cmd := TranscodeCmd{
		DataPageVersion: 1,
		ReadOption:      pio.ReadOption{},
		WriteOption:     pio.WriteOption{Compression: "ZSTD"},
		ReadPageSize:    1000,
		Source:          "../build/benchmark.parquet",
		URI:             filepath.Join(tempDir, "transcoded.parquet"),
	}
	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}
