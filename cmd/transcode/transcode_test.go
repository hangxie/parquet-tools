package transcode

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/cat"
	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
	"github.com/hangxie/parquet-tools/cmd/schema"

	pio "github.com/hangxie/parquet-tools/io"
)

func TestCmd(t *testing.T) {
	t.Run("error", testCmdError)
	t.Run("good", testCmdGood)
	t.Run("verify-data", testCmdVerifyData)
	t.Run("schema-modification", testCmdSchemaModification)
	t.Run("page-sizes", testCmdPageSizes)
	t.Run("edge-cases", testCmdEdgeCases)
	t.Run("field-encoding", testCmdFieldEncoding)
	t.Run("field-compression", testCmdFieldCompression)
	t.Run("field-encoding-and-compression", testCmdFieldEncodingAndCompression)
	t.Run("preserves-encodings-override", testCmdPreservesEncodingsOverride)
	t.Run("overrides-encoding-when-specified", testCmdOverridesEncodingWhenSpecified)
	t.Run("preserves-encodings-with-compression-change", testCmdPreservesEncodingsWithCompressionChange)
	t.Run("preserves-encodings-with-data-page-version-change", testCmdPreservesEncodingsWithDataPageVersionChange)
}

func testCmdError(t *testing.T) {
	rOpt := pio.ReadOption{}
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
		"pagesize-too-small":  {Cmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 0, Source: "../../testdata/good.parquet", URI: "dummy"}, "invalid read page size"},
		"source-non-existent": {Cmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "does/not/exist", URI: "dummy"}, "no such file or directory"},
		"source-not-parquet":  {Cmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../../testdata/not-a-parquet-file", URI: "dummy"}, "failed to read from"},
		"target-file":         {Cmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../../testdata/good.parquet", URI: "://uri"}, "unable to parse file location"},
		"target-write":        {Cmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../../testdata/good.parquet", URI: "s3://target"}, "failed to close"},
		"fail-on-int96":       {Cmd{FailOnInt96: true, ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../../testdata/all-types.parquet", URI: filepath.Join(tempDir, "dummy")}, "has type INT96 which is not supported"},
		"target-compression": {Cmd{ReadOption: rOpt, WriteOption: pio.WriteOption{
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}, ReadPageSize: 10, Source: "../../testdata/good.parquet", URI: filepath.Join(tempDir, "dummy")}, "not a valid CompressionCode"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func testCmdGood(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		source          string
		compression     string
		dataPageVersion int32
		omitStats       string
		rowCount        int64
	}{
		"good-gzip":         {"good.parquet", "GZIP", 1, "", 3},
		"good-zstd":         {"good.parquet", "ZSTD", 1, "", 3},
		"good-uncompressed": {"good.parquet", "UNCOMPRESSED", 1, "", 3},
		"good-lz4":          {"good.parquet", "LZ4", 1, "", 3},
		"good-brotli":       {"good.parquet", "BROTLI", 1, "", 3},
		"all-types-gzip":    {"all-types.parquet", "GZIP", 1, "", 5},
		"all-types-zstd":    {"all-types.parquet", "ZSTD", 1, "", 5},
		"all-types-brotli":  {"all-types.parquet", "BROTLI", 1, "", 5},
		"empty-gzip":        {"empty.parquet", "GZIP", 1, "", 0},
		"good-v2":           {"good.parquet", "SNAPPY", 2, "", 3},
		"all-types-v2-zstd": {"all-types.parquet", "ZSTD", 2, "", 5},
		"good-v2-brotli":    {"good.parquet", "BROTLI", 2, "", 3},
		"good-stats-true":   {"good.parquet", "SNAPPY", 1, "true", 3},
		"good-stats-false":  {"good.parquet", "SNAPPY", 1, "false", 3},
		"good-all-options":  {"good.parquet", "ZSTD", 2, "true", 3},
	}
	tempDir := t.TempDir()

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			wOpt := pio.WriteOption{
				Compression:     tc.compression,
				DataPageVersion: tc.dataPageVersion,
				PageSize:        1024 * 1024,
				RowGroupSize:    128 * 1024 * 1024,
				ParallelNumber:  0,
			}
			cmd := Cmd{
				OmitStats:    tc.omitStats,
				ReadOption:   rOpt,
				WriteOption:  wOpt,
				ReadPageSize: 10,
				Source:       filepath.Join("..", "..", "testdata", tc.source),
				URI:          filepath.Join(tempDir, name+".parquet"),
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

			// Verify schema structure remains the same
			require.True(t, testutils.HasSameSchema(cmd.Source, cmd.URI, false, true))
		})
	}
}

func testCmdVerifyData(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{
		Compression:     "ZSTD",
		DataPageVersion: 1,
		PageSize:        1024 * 1024,
		RowGroupSize:    128 * 1024 * 1024,
		ParallelNumber:  0,
	}
	tempDir := t.TempDir()

	// Transcode a file
	cmd := Cmd{
		ReadOption:   rOpt,
		WriteOption:  wOpt,
		ReadPageSize: 100,
		Source:       "../../testdata/good.parquet",
		URI:          filepath.Join(tempDir, "transcoded.parquet"),
	}
	err := cmd.Run()
	require.NoError(t, err)

	// Verify the data is the same by using cat command
	catOriginal := cat.Cmd{
		ReadOption:   rOpt,
		ReadPageSize: 1000,
		SampleRatio:  1.0,
		Format:       "json",
		GeoFormat:    "geojson",
		URI:          "../../testdata/good.parquet",
	}
	catTranscoded := cat.Cmd{
		ReadOption:   rOpt,
		ReadPageSize: 1000,
		SampleRatio:  1.0,
		Format:       "json",
		GeoFormat:    "geojson",
		URI:          cmd.URI,
	}

	originalOutput, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, catOriginal.Run())
	})
	transcodedOutput, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, catTranscoded.Run())
	})

	require.Equal(t, originalOutput, transcodedOutput)
	require.True(t, testutils.HasSameSchema(cmd.Source, cmd.URI, false, true))
}

func testCmdSchemaModification(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{
		Compression:     "SNAPPY",
		DataPageVersion: 1,
		PageSize:        1024 * 1024,
		RowGroupSize:    128 * 1024 * 1024,
		ParallelNumber:  0,
	}
	tempDir := t.TempDir()

	cmd := Cmd{
		ReadOption:   rOpt,
		WriteOption:  wOpt,
		ReadPageSize: 10,
		Source:       "../../testdata/good.parquet",
		URI:          filepath.Join(tempDir, "no-mods.parquet"),
	}

	err := cmd.Run()
	require.NoError(t, err)

	// Verify output file
	reader, err := pio.NewParquetFileReader(cmd.URI, rOpt)
	require.NoError(t, err)
	defer func() {
		_ = reader.PFile.Close()
	}()
	require.Equal(t, int64(3), reader.GetNumRows())
	require.True(t, testutils.HasSameSchema(cmd.Source, cmd.URI, false, true))
}

func testCmdPageSizes(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{
		Compression:     "SNAPPY",
		DataPageVersion: 1,
		PageSize:        1024 * 1024,
		RowGroupSize:    128 * 1024 * 1024,
		ParallelNumber:  0,
	}
	tempDir := t.TempDir()

	pageSizes := []int{1, 5, 10, 100, 1000}
	for _, pageSize := range pageSizes {
		t.Run(fmt.Sprintf("pagesize-%d", pageSize), func(t *testing.T) {
			cmd := Cmd{
				ReadOption:   rOpt,
				WriteOption:  wOpt,
				ReadPageSize: pageSize,
				Source:       "../../testdata/all-types.parquet",
				URI:          filepath.Join(tempDir, fmt.Sprintf("pagesize-%d.parquet", pageSize)),
			}
			err := cmd.Run()
			require.NoError(t, err)

			// Verify the data is correct
			reader, err := pio.NewParquetFileReader(cmd.URI, rOpt)
			require.NoError(t, err)
			require.Equal(t, int64(5), reader.GetNumRows())
			_ = reader.PFile.Close()
		})
	}
}

func testCmdEdgeCases(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{
		Compression:     "SNAPPY",
		DataPageVersion: 1,
		PageSize:        1024 * 1024,
		RowGroupSize:    128 * 1024 * 1024,
		ParallelNumber:  0,
	}
	wOptV2 := pio.WriteOption{
		Compression:     "SNAPPY",
		DataPageVersion: 2,
		PageSize:        1024 * 1024,
		RowGroupSize:    128 * 1024 * 1024,
		ParallelNumber:  0,
	}
	tempDir := t.TempDir()

	testCases := []struct {
		name   string
		cmd    Cmd
		errMsg string
	}{
		{
			name: "empty file",
			cmd: Cmd{
				ReadOption:   rOpt,
				WriteOption:  wOpt,
				ReadPageSize: 10,
				Source:       "../../testdata/empty.parquet",
				URI:          filepath.Join(tempDir, "empty-out.parquet"),
			},
		},
		{
			name: "large page size",
			cmd: Cmd{
				ReadOption:   rOpt,
				WriteOption:  wOpt,
				ReadPageSize: 10000,
				Source:       "../../testdata/good.parquet",
				URI:          filepath.Join(tempDir, "large-page.parquet"),
			},
		},
		{
			name: "all stats options",
			cmd: Cmd{
				OmitStats:    "false",
				ReadOption:   rOpt,
				WriteOption:  wOptV2,
				ReadPageSize: 10,
				Source:       "../../testdata/good.parquet",
				URI:          filepath.Join(tempDir, "all-opts.parquet"),
			},
		},
		{
			name: "multiple compression types",
			cmd: Cmd{
				ReadOption: rOpt,
				WriteOption: pio.WriteOption{
					Compression:    "LZ4_RAW",
					PageSize:       1024 * 1024,
					RowGroupSize:   128 * 1024 * 1024,
					ParallelNumber: 0,
				},
				ReadPageSize: 10,
				Source:       "../../testdata/good.parquet",
				URI:          filepath.Join(tempDir, "lz4-raw.parquet"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cmd.Run()
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func testCmdFieldEncoding(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{
		Compression:     "SNAPPY",
		DataPageVersion: 1,
		PageSize:        1024 * 1024,
		RowGroupSize:    128 * 1024 * 1024,
		ParallelNumber:  0,
	}
	tempDir := t.TempDir()

	testCases := []struct {
		name            string
		source          string
		fieldEncoding   []string
		errMsg          string
		dataPageVersion int32
	}{
		{
			name:            "single field encoding",
			source:          "good.parquet",
			fieldEncoding:   []string{"shoe_brand=DELTA_BYTE_ARRAY"},
			dataPageVersion: 2,
		},
		{
			name:            "multiple field encodings",
			source:          "good.parquet",
			fieldEncoding:   []string{"shoe_brand=DELTA_BYTE_ARRAY", "shoe_name=PLAIN"},
			dataPageVersion: 2,
		},
		{
			name:            "field encoding with global encoding",
			source:          "good.parquet",
			fieldEncoding:   []string{"shoe_brand=DELTA_LENGTH_BYTE_ARRAY"},
			dataPageVersion: 2,
		},
		{
			name:          "incompatible encoding fails",
			source:        "good.parquet",
			fieldEncoding: []string{"shoe_brand=RLE"}, // RLE only works with BOOLEAN, not BYTE_ARRAY
			errMsg:        "not compatible with field",
		},
		{
			name:          "invalid field encoding format",
			source:        "good.parquet",
			fieldEncoding: []string{"invalid-format"},
			errMsg:        "invalid field encoding format",
		},
		{
			name:          "invalid encoding name",
			source:        "good.parquet",
			fieldEncoding: []string{"shoe_brand=NOT_A_REAL_ENCODING"},
			errMsg:        "invalid encoding",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := wOpt
			if tc.dataPageVersion != 0 {
				opts.DataPageVersion = tc.dataPageVersion
			}
			cmd := Cmd{
				FieldEncoding: tc.fieldEncoding,
				ReadOption:    rOpt,
				WriteOption:   opts,
				ReadPageSize:  10,
				Source:        filepath.Join("..", "..", "testdata", tc.source),
				URI:           filepath.Join(tempDir, tc.name+".parquet"),
			}

			err := cmd.Run()

			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)

				// Verify output file exists and has correct row count
				reader, err := pio.NewParquetFileReader(cmd.URI, rOpt)
				require.NoError(t, err)
				require.Equal(t, int64(3), reader.GetNumRows())
				_ = reader.PFile.Close()
			}
		})
	}
}

func testCmdFieldCompression(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{
		Compression:     "SNAPPY",
		DataPageVersion: 1,
		PageSize:        1024 * 1024,
		RowGroupSize:    128 * 1024 * 1024,
		ParallelNumber:  0,
	}
	tempDir := t.TempDir()

	testCases := []struct {
		name             string
		source           string
		fieldCompression []string
		errMsg           string
	}{
		{
			name:             "single field compression",
			source:           "good.parquet",
			fieldCompression: []string{"shoe_brand=ZSTD"},
		},
		{
			name:             "multiple field compressions",
			source:           "good.parquet",
			fieldCompression: []string{"shoe_brand=ZSTD", "shoe_name=GZIP"},
		},
		{
			name:             "uncompressed field",
			source:           "good.parquet",
			fieldCompression: []string{"shoe_brand=UNCOMPRESSED"},
		},
		{
			name:             "all compression codecs",
			source:           "good.parquet",
			fieldCompression: []string{"shoe_brand=SNAPPY"},
		},
		{
			name:             "invalid field compression format",
			source:           "good.parquet",
			fieldCompression: []string{"invalid-format"},
			errMsg:           "invalid field compression format",
		},
		{
			name:             "invalid compression codec",
			source:           "good.parquet",
			fieldCompression: []string{"shoe_brand=NOT_A_REAL_CODEC"},
			errMsg:           "invalid compression codec",
		},
		{
			name:             "empty field path",
			source:           "good.parquet",
			fieldCompression: []string{"=ZSTD"},
			errMsg:           "empty field path",
		},
		{
			name:             "empty compression codec",
			source:           "good.parquet",
			fieldCompression: []string{"shoe_brand="},
			errMsg:           "empty compression codec",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := Cmd{
				FieldCompression: tc.fieldCompression,
				ReadOption:       rOpt,
				WriteOption:      wOpt,
				ReadPageSize:     10,
				Source:           filepath.Join("..", "..", "testdata", tc.source),
				URI:              filepath.Join(tempDir, tc.name+".parquet"),
			}

			err := cmd.Run()

			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)

				// Verify output file exists and has correct row count
				reader, err := pio.NewParquetFileReader(cmd.URI, rOpt)
				require.NoError(t, err)
				require.Equal(t, int64(3), reader.GetNumRows())
				_ = reader.PFile.Close()
			}
		})
	}
}

func testCmdFieldEncodingAndCompression(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{
		Compression:     "SNAPPY",
		DataPageVersion: 1,
		PageSize:        1024 * 1024,
		RowGroupSize:    128 * 1024 * 1024,
		ParallelNumber:  0,
	}
	tempDir := t.TempDir()

	// Test combining field-encoding and field-compression
	wOpt.DataPageVersion = 2
	cmd := Cmd{
		FieldEncoding:    []string{"shoe_brand=DELTA_BYTE_ARRAY"},
		FieldCompression: []string{"shoe_name=ZSTD"},
		ReadOption:       rOpt,
		WriteOption:      wOpt,
		ReadPageSize:     10,
		Source:           filepath.Join("..", "..", "testdata", "good.parquet"),
		URI:              filepath.Join(tempDir, "combined.parquet"),
	}

	err := cmd.Run()
	require.NoError(t, err)

	// Verify output file exists and has correct row count
	reader, err := pio.NewParquetFileReader(cmd.URI, rOpt)
	require.NoError(t, err)
	require.Equal(t, int64(3), reader.GetNumRows())
	_ = reader.PFile.Close()
}

func testCmdPreservesEncodingsOverride(t *testing.T) {
	rOpt := pio.ReadOption{}
	tempDir := t.TempDir()

	// First, create a test file with specific encodings
	testFile := "../../testdata/all-types.parquet"

	// Get the schema from the source file
	schemaCmd := schema.Cmd{
		Format:     "go",
		ReadOption: rOpt,
		URI:        testFile,
	}
	originalSchema, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, schemaCmd.Run())
	})

	// Transcode without specifying field encodings
	transcodedFile := filepath.Join(tempDir, "transcoded-preserved.parquet")
	cmd := Cmd{
		ReadOption: rOpt,
		WriteOption: pio.WriteOption{
			Compression:     "ZSTD",
			DataPageVersion: 1,
			PageSize:        1024 * 1024,
			RowGroupSize:    128 * 1024 * 1024,
			ParallelNumber:  0,
		},
		ReadPageSize: 10,
		Source:       testFile,
		URI:          transcodedFile,
	}
	err := cmd.Run()
	require.NoError(t, err)

	// Get schema from transcoded file
	schemaCmd = schema.Cmd{
		Format:     "go",
		ReadOption: rOpt,
		URI:        transcodedFile,
	}
	transcodedSchema, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, schemaCmd.Run())
	})

	// Schemas should match (encodings preserved)
	require.Equal(t, originalSchema, transcodedSchema,
		"Encodings should be preserved when transcoding without field-encoding overrides")

	// Verify data integrity
	catOriginal := cat.Cmd{
		ReadOption:   rOpt,
		ReadPageSize: 1000,
		SampleRatio:  1.0,
		Format:       "json",
		GeoFormat:    "geojson",
		URI:          testFile,
	}
	catTranscoded := cat.Cmd{
		ReadOption:   rOpt,
		ReadPageSize: 1000,
		SampleRatio:  1.0,
		Format:       "json",
		GeoFormat:    "geojson",
		URI:          transcodedFile,
	}

	originalOutput, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, catOriginal.Run())
	})
	transcodedOutput, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, catTranscoded.Run())
	})

	require.Equal(t, originalOutput, transcodedOutput)
}

func testCmdOverridesEncodingWhenSpecified(t *testing.T) {
	rOpt := pio.ReadOption{}
	tempDir := t.TempDir()

	// Transcode with explicit field encoding override
	transcodedFile := filepath.Join(tempDir, "transcoded-override.parquet")

	// Use good.parquet which has BYTE_ARRAY fields
	// Override one of the fields to use DELTA_BYTE_ARRAY
	cmd := Cmd{
		FieldEncoding: []string{"shoe_name=DELTA_BYTE_ARRAY"},
		ReadOption:    rOpt,
		WriteOption: pio.WriteOption{
			Compression:     "ZSTD",
			DataPageVersion: 2,
			PageSize:        1024 * 1024,
			RowGroupSize:    128 * 1024 * 1024,
			ParallelNumber:  0,
		},
		ReadPageSize: 10,
		Source:       "../../testdata/good.parquet",
		URI:          transcodedFile,
	}
	err := cmd.Run()
	require.NoError(t, err)

	// Get schema from transcoded file
	schemaCmd := schema.Cmd{
		Format:     "go",
		ReadOption: rOpt,
		URI:        transcodedFile,
	}
	transcodedSchema, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, schemaCmd.Run())
	})

	// Verify the encoding was overridden for shoe_name
	require.Contains(t, transcodedSchema, "encoding=DELTA_BYTE_ARRAY")

	// Verify data integrity by checking row count and basic data
	reader, err := pio.NewParquetFileReader(transcodedFile, rOpt)
	require.NoError(t, err)
	require.Equal(t, int64(3), reader.GetNumRows())
	_ = reader.PFile.Close()

	// Verify data using cat
	catTranscoded := cat.Cmd{
		ReadOption:   rOpt,
		ReadPageSize: 1000,
		SampleRatio:  1.0,
		Format:       "json",
		GeoFormat:    "geojson",
		URI:          transcodedFile,
	}
	catOriginal := cat.Cmd{
		ReadOption:   rOpt,
		ReadPageSize: 1000,
		SampleRatio:  1.0,
		Format:       "json",
		GeoFormat:    "geojson",
		URI:          "../../testdata/good.parquet",
	}

	transcodedOutput, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, catTranscoded.Run())
	})
	originalOutput, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, catOriginal.Run())
	})

	require.Equal(t, originalOutput, transcodedOutput)
}

func testCmdPreservesEncodingsWithCompressionChange(t *testing.T) {
	rOpt := pio.ReadOption{}
	tempDir := t.TempDir()

	// First, create a test file with specific encodings
	testFile := "../../testdata/all-types.parquet"

	// Get the schema from the source file
	schemaCmd := schema.Cmd{
		Format:     "go",
		ReadOption: rOpt,
		URI:        testFile,
	}
	originalSchema, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, schemaCmd.Run())
	})

	// Transcode with different compression but preserve encodings
	transcodedFile := filepath.Join(tempDir, "transcoded-gzip.parquet")
	cmd := Cmd{
		ReadOption: rOpt,
		WriteOption: pio.WriteOption{
			Compression:     "GZIP",
			DataPageVersion: 1,
			PageSize:        1024 * 1024,
			RowGroupSize:    128 * 1024 * 1024,
			ParallelNumber:  0,
		},
		ReadPageSize: 10,
		Source:       testFile,
		URI:          transcodedFile,
	}
	err := cmd.Run()
	require.NoError(t, err)

	// Get schemas
	schemaCmd = schema.Cmd{
		Format:     "go",
		ReadOption: rOpt,
		URI:        transcodedFile,
	}
	transcodedSchema, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, schemaCmd.Run())
	})

	// Encodings should be preserved even with different compression
	require.Equal(t, originalSchema, transcodedSchema)
}

func testCmdPreservesEncodingsWithDataPageVersionChange(t *testing.T) {
	rOpt := pio.ReadOption{}
	tempDir := t.TempDir()

	// Transcode from v1 to v2 and preserve encodings (except PLAIN_DICTIONARY)
	transcodedFile := filepath.Join(tempDir, "transcoded-v2.parquet")
	cmd := Cmd{
		ReadOption: rOpt,
		WriteOption: pio.WriteOption{
			Compression:     "SNAPPY",
			DataPageVersion: 2,
			PageSize:        1024 * 1024,
			RowGroupSize:    128 * 1024 * 1024,
			ParallelNumber:  0,
		},
		ReadPageSize: 10,
		Source:       "../../testdata/all-types.parquet",
		URI:          transcodedFile,
	}
	err := cmd.Run()
	require.NoError(t, err)

	// Verify file was created and data is correct
	reader, err := pio.NewParquetFileReader(transcodedFile, rOpt)
	require.NoError(t, err)
	require.Equal(t, int64(5), reader.GetNumRows())
	_ = reader.PFile.Close()

	// Note: PLAIN_DICTIONARY should NOT be in v2 files
	schemaCmd := schema.Cmd{
		Format:     "go",
		ReadOption: rOpt,
		URI:        transcodedFile,
	}
	transcodedSchema, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, schemaCmd.Run())
	})

	// Should not contain PLAIN_DICTIONARY in v2
	require.NotContains(t, transcodedSchema, "PLAIN_DICTIONARY")
}

func TestCmdIsEncodingCompatible(t *testing.T) {
	cmd := Cmd{}

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
		{"RLE", "INT32", false},
		{"RLE", "INT64", false},
		{"BIT_PACKED", "INT32", false},
		{"BIT_PACKED", "INT64", false},
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
		// Per parquet-go: RLE is NOT supported for BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY
		{"RLE", "BYTE_ARRAY", false},
		{"RLE", "FIXED_LEN_BYTE_ARRAY", false},
		{"DELTA_LENGTH_BYTE_ARRAY", "BYTE_ARRAY", true},
		{"DELTA_LENGTH_BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY", false},
		{"DELTA_BYTE_ARRAY", "BYTE_ARRAY", true},
		{"DELTA_BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY", true},
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

		// Per parquet-go: BYTE_STREAM_SPLIT supports FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY
		{"BYTE_STREAM_SPLIT", "INT32", true},
		{"BYTE_STREAM_SPLIT", "INT64", true},
		{"BYTE_STREAM_SPLIT", "FIXED_LEN_BYTE_ARRAY", true},
		{"BYTE_STREAM_SPLIT", "BYTE_ARRAY", false},
		{"BYTE_STREAM_SPLIT", "BOOLEAN", false},

		// PLAIN_DICTIONARY (v1 only, but compatibility check doesn't validate page version)
		{"PLAIN_DICTIONARY", "INT32", true},
		{"PLAIN_DICTIONARY", "INT64", true},
		{"PLAIN_DICTIONARY", "BYTE_ARRAY", true},
		{"PLAIN_DICTIONARY", "BOOLEAN", true},
		{"PLAIN_DICTIONARY", "FLOAT", true},
		{"PLAIN_DICTIONARY", "DOUBLE", true},
		{"PLAIN_DICTIONARY", "FIXED_LEN_BYTE_ARRAY", true},

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
		t.Run(tc.encoding+"-"+tc.dataType, func(t *testing.T) {
			result := cmd.isEncodingCompatible(tc.encoding, tc.dataType)
			require.Equal(t, tc.expected, result, "encoding=%s, type=%s", tc.encoding, tc.dataType)
		})
	}
}

func TestCmdParseFieldEncodings(t *testing.T) {
	testCases := []struct {
		name            string
		dataPageVersion int32
		fieldEncoding   []string
		expected        map[string]string
		errMsg          string
	}{
		{
			name:            "empty input",
			dataPageVersion: 1,
			fieldEncoding:   []string{},
			expected:        map[string]string{},
		},
		{
			name:            "single field encoding",
			dataPageVersion: 1,
			fieldEncoding:   []string{"shoe_brand=PLAIN"},
			expected:        map[string]string{"shoe_brand": "PLAIN"},
		},
		{
			name:            "multiple field encodings",
			dataPageVersion: 1,
			fieldEncoding:   []string{"shoe_brand=PLAIN", "shoe_name=PLAIN"},
			expected:        map[string]string{"shoe_brand": "PLAIN", "shoe_name": "PLAIN"},
		},
		{
			name:            "nested field path",
			dataPageVersion: 1,
			fieldEncoding:   []string{"parent.child.leaf=RLE"},
			expected:        map[string]string{"parent.child.leaf": "RLE"},
		},
		{
			name:            "case insensitive encoding",
			dataPageVersion: 1,
			fieldEncoding:   []string{"field=plain"},
			expected:        map[string]string{"field": "PLAIN"},
		},
		{
			name:            "encoding with whitespace",
			dataPageVersion: 1,
			fieldEncoding:   []string{"  field  =  RLE  "},
			expected:        map[string]string{"field": "RLE"},
		},
		{
			name:            "missing equals sign",
			dataPageVersion: 1,
			fieldEncoding:   []string{"fieldPLAIN"},
			errMsg:          "invalid field encoding format",
		},
		{
			name:            "empty field path",
			dataPageVersion: 1,
			fieldEncoding:   []string{"=PLAIN"},
			errMsg:          "empty field path",
		},
		{
			name:            "empty encoding",
			dataPageVersion: 1,
			fieldEncoding:   []string{"field="},
			errMsg:          "empty encoding",
		},
		{
			name:            "invalid encoding",
			dataPageVersion: 1,
			fieldEncoding:   []string{"field=INVALID_ENCODING"},
			errMsg:          "invalid encoding",
		},
		{
			name:            "plain_dictionary allowed in v1",
			dataPageVersion: 1,
			fieldEncoding:   []string{"field=PLAIN_DICTIONARY"},
			expected:        map[string]string{"field": "PLAIN_DICTIONARY"},
		},
		{
			name:            "plain_dictionary not allowed in v2",
			dataPageVersion: 2,
			fieldEncoding:   []string{"field=PLAIN_DICTIONARY"},
			errMsg:          "PLAIN_DICTIONARY encoding is only allowed with data page version 1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := Cmd{
				FieldEncoding: tc.fieldEncoding,
				WriteOption: pio.WriteOption{
					Compression:     "SNAPPY",
					DataPageVersion: tc.dataPageVersion,
					PageSize:        1024 * 1024,
					RowGroupSize:    128 * 1024 * 1024,
					ParallelNumber:  0,
				},
			}
			result, err := cmd.parseFieldEncodings()

			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestCmd_getAllowedEncodings(t *testing.T) {
	cmd := Cmd{}

	testCases := []struct {
		name              string
		dataType          string
		expectedEncodings []string
	}{
		{
			name:     "BOOLEAN",
			dataType: "BOOLEAN",
			expectedEncodings: []string{
				"PLAIN", "BIT_PACKED", "PLAIN_DICTIONARY", "RLE", "RLE_DICTIONARY",
			},
		},
		{
			name:     "BYTE_ARRAY",
			dataType: "BYTE_ARRAY",
			expectedEncodings: []string{
				"PLAIN", "DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY", "PLAIN_DICTIONARY", "RLE_DICTIONARY",
			},
		},
		{
			name:     "INT32",
			dataType: "INT32",
			expectedEncodings: []string{
				"PLAIN", "BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED", "PLAIN_DICTIONARY", "RLE_DICTIONARY",
			},
		},
		{
			name:     "INT64",
			dataType: "INT64",
			expectedEncodings: []string{
				"PLAIN", "BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED", "PLAIN_DICTIONARY", "RLE_DICTIONARY",
			},
		},
		{
			name:     "FLOAT",
			dataType: "FLOAT",
			expectedEncodings: []string{
				"PLAIN", "BYTE_STREAM_SPLIT", "PLAIN_DICTIONARY", "RLE_DICTIONARY",
			},
		},
		{
			name:     "DOUBLE",
			dataType: "DOUBLE",
			expectedEncodings: []string{
				"PLAIN", "BYTE_STREAM_SPLIT", "PLAIN_DICTIONARY", "RLE_DICTIONARY",
			},
		},
		{
			name:     "FIXED_LEN_BYTE_ARRAY",
			dataType: "FIXED_LEN_BYTE_ARRAY",
			expectedEncodings: []string{
				"PLAIN", "BYTE_STREAM_SPLIT", "DELTA_BYTE_ARRAY", "PLAIN_DICTIONARY", "RLE_DICTIONARY",
			},
		},
		{
			name:     "unknown type",
			dataType: "UNKNOWN_TYPE",
			expectedEncodings: []string{
				"PLAIN",
			},
		},
		{
			name:     "lowercase input",
			dataType: "byte_array",
			expectedEncodings: []string{
				"PLAIN", "DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY", "PLAIN_DICTIONARY", "RLE_DICTIONARY",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := cmd.getAllowedEncodings(tc.dataType)
			require.Equal(t, tc.expectedEncodings, result)
		})
	}
}

func TestCmdParseFieldCompressions(t *testing.T) {
	testCases := []struct {
		name             string
		fieldCompression []string
		expected         map[string]string
		errMsg           string
	}{
		{
			name:             "empty input",
			fieldCompression: []string{},
			expected:         map[string]string{},
		},
		{
			name:             "single field compression",
			fieldCompression: []string{"shoe_brand=SNAPPY"},
			expected:         map[string]string{"shoe_brand": "SNAPPY"},
		},
		{
			name:             "multiple field compressions",
			fieldCompression: []string{"shoe_brand=SNAPPY", "shoe_name=ZSTD"},
			expected:         map[string]string{"shoe_brand": "SNAPPY", "shoe_name": "ZSTD"},
		},
		{
			name:             "nested field path",
			fieldCompression: []string{"parent.child.leaf=GZIP"},
			expected:         map[string]string{"parent.child.leaf": "GZIP"},
		},
		{
			name:             "case insensitive codec",
			fieldCompression: []string{"field=snappy"},
			expected:         map[string]string{"field": "SNAPPY"},
		},
		{
			name:             "codec with whitespace",
			fieldCompression: []string{"  field  =  ZSTD  "},
			expected:         map[string]string{"field": "ZSTD"},
		},
		{
			name:             "all valid codecs",
			fieldCompression: []string{"f1=UNCOMPRESSED", "f2=SNAPPY", "f3=GZIP", "f4=LZ4", "f5=LZ4_RAW", "f6=ZSTD", "f7=BROTLI"},
			expected: map[string]string{
				"f1": "UNCOMPRESSED",
				"f2": "SNAPPY",
				"f3": "GZIP",
				"f4": "LZ4",
				"f5": "LZ4_RAW",
				"f6": "ZSTD",
				"f7": "BROTLI",
			},
		},
		{
			name:             "missing equals sign",
			fieldCompression: []string{"fieldSNAPPY"},
			errMsg:           "invalid field compression format",
		},
		{
			name:             "empty field path",
			fieldCompression: []string{"=SNAPPY"},
			errMsg:           "empty field path",
		},
		{
			name:             "empty compression codec",
			fieldCompression: []string{"field="},
			errMsg:           "empty compression codec",
		},
		{
			name:             "invalid compression codec",
			fieldCompression: []string{"field=INVALID_CODEC"},
			errMsg:           "invalid compression codec",
		},
		{
			name:             "LZO not supported",
			fieldCompression: []string{"field=LZO"},
			errMsg:           "invalid compression codec",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := Cmd{
				FieldCompression: tc.fieldCompression,
			}
			result, err := cmd.parseFieldCompressions()

			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func BenchmarkTranscodeCmd(b *testing.B) {
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
	cmd := Cmd{
		ReadOption: pio.ReadOption{},
		WriteOption: pio.WriteOption{
			Compression:     "ZSTD",
			DataPageVersion: 1,
			PageSize:        1024 * 1024,
			RowGroupSize:    128 * 1024 * 1024,
			ParallelNumber:  0,
		},
		ReadPageSize: 1000,
		Source:       "../../build/benchmark.parquet",
		URI:          filepath.Join(tempDir, "transcoded.parquet"),
	}
	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}
