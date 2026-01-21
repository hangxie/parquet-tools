package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
)

func TestTranscodeCmd(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		tempDir := t.TempDir()

		testCases := map[string]struct {
			cmd    TranscodeCmd
			errMsg string
		}{
			"pagesize-too-small":  {TranscodeCmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 0, Source: "../testdata/good.parquet", URI: "dummy"}, "invalid read page size"},
			"source-non-existent": {TranscodeCmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "does/not/exist", URI: "dummy"}, "no such file or directory"},
			"source-not-parquet":  {TranscodeCmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../testdata/not-a-parquet-file", URI: "dummy"}, "failed to read from"},
			"target-file":         {TranscodeCmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../testdata/good.parquet", URI: "://uri"}, "unable to parse file location"},
			"target-write":        {TranscodeCmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../testdata/good.parquet", URI: "s3://target"}, "failed to close"},
			"fail-on-int96":       {TranscodeCmd{FailOnInt96: true, ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../testdata/all-types.parquet", URI: filepath.Join(tempDir, "dummy")}, "has type INT96 which is not supported"},
			"target-compression": {TranscodeCmd{ReadOption: rOpt, WriteOption: pio.WriteOption{
				PageSize:       1024 * 1024,
				RowGroupSize:   128 * 1024 * 1024,
				ParallelNumber: 0,
			}, ReadPageSize: 10, Source: "../testdata/good.parquet", URI: filepath.Join(tempDir, "dummy")}, "not a valid CompressionCode"},
		}

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				err := tc.cmd.Run()
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			})
		}
	})

	t.Run("good", func(t *testing.T) {
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
			"all-types-gzip":    {"all-types.parquet", "GZIP", 1, "", 10},
			"all-types-zstd":    {"all-types.parquet", "ZSTD", 1, "", 10},
			"all-types-brotli":  {"all-types.parquet", "BROTLI", 1, "", 10},
			"empty-gzip":        {"empty.parquet", "GZIP", 1, "", 0},
			"good-v2":           {"good.parquet", "SNAPPY", 2, "", 3},
			"all-types-v2-zstd": {"all-types.parquet", "ZSTD", 2, "", 10},
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
				cmd := TranscodeCmd{
					OmitStats:    tc.omitStats,
					ReadOption:   rOpt,
					WriteOption:  wOpt,
					ReadPageSize: 10,
					Source:       filepath.Join("..", "testdata", tc.source),
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
				require.True(t, hasSameSchema(cmd.Source, cmd.URI, false, true))
			})
		}
	})

	t.Run("verify-data", func(t *testing.T) {
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
		cmd := TranscodeCmd{
			ReadOption:   rOpt,
			WriteOption:  wOpt,
			ReadPageSize: 100,
			Source:       "../testdata/good.parquet",
			URI:          filepath.Join(tempDir, "transcoded.parquet"),
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
		require.True(t, hasSameSchema(cmd.Source, cmd.URI, false, true))
	})

	t.Run("schema-modification", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:     "SNAPPY",
			DataPageVersion: 1,
			PageSize:        1024 * 1024,
			RowGroupSize:    128 * 1024 * 1024,
			ParallelNumber:  0,
		}
		tempDir := t.TempDir()

		cmd := TranscodeCmd{
			ReadOption:   rOpt,
			WriteOption:  wOpt,
			ReadPageSize: 10,
			Source:       "../testdata/good.parquet",
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
		require.True(t, hasSameSchema(cmd.Source, cmd.URI, false, true))
	})

	t.Run("page-sizes", func(t *testing.T) {
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
				cmd := TranscodeCmd{
					ReadOption:   rOpt,
					WriteOption:  wOpt,
					ReadPageSize: pageSize,
					Source:       "../testdata/all-types.parquet",
					URI:          filepath.Join(tempDir, fmt.Sprintf("pagesize-%d.parquet", pageSize)),
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
	})

	t.Run("edge-cases", func(t *testing.T) {
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
			cmd    TranscodeCmd
			errMsg string
		}{
			{
				name: "empty file",
				cmd: TranscodeCmd{
					ReadOption:   rOpt,
					WriteOption:  wOpt,
					ReadPageSize: 10,
					Source:       "../testdata/empty.parquet",
					URI:          filepath.Join(tempDir, "empty-out.parquet"),
				},
			},
			{
				name: "large page size",
				cmd: TranscodeCmd{
					ReadOption:   rOpt,
					WriteOption:  wOpt,
					ReadPageSize: 10000,
					Source:       "../testdata/good.parquet",
					URI:          filepath.Join(tempDir, "large-page.parquet"),
				},
			},
			{
				name: "all stats options",
				cmd: TranscodeCmd{
					OmitStats:    "false",
					ReadOption:   rOpt,
					WriteOption:  wOptV2,
					ReadPageSize: 10,
					Source:       "../testdata/good.parquet",
					URI:          filepath.Join(tempDir, "all-opts.parquet"),
				},
			},
			{
				name: "multiple compression types",
				cmd: TranscodeCmd{
					ReadOption: rOpt,
					WriteOption: pio.WriteOption{
						Compression:    "LZ4_RAW",
						PageSize:       1024 * 1024,
						RowGroupSize:   128 * 1024 * 1024,
						ParallelNumber: 0,
					},
					ReadPageSize: 10,
					Source:       "../testdata/good.parquet",
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
	})

	t.Run("field-encoding", func(t *testing.T) {
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
			name          string
			source        string
			fieldEncoding []string
			errMsg        string
		}{
			{
				name:          "single field encoding",
				source:        "good.parquet",
				fieldEncoding: []string{"shoe_brand=DELTA_BYTE_ARRAY"},
			},
			{
				name:          "multiple field encodings",
				source:        "good.parquet",
				fieldEncoding: []string{"shoe_brand=DELTA_BYTE_ARRAY", "shoe_name=PLAIN"},
			},
			{
				name:          "field encoding with global encoding",
				source:        "good.parquet",
				fieldEncoding: []string{"shoe_brand=DELTA_LENGTH_BYTE_ARRAY"},
			},
			{
				name:          "incompatible encoding fails",
				source:        "good.parquet",
				fieldEncoding: []string{"shoe_brand=DELTA_BINARY_PACKED"}, // DELTA_BINARY_PACKED only works with INT32/INT64, not BYTE_ARRAY
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
				cmd := TranscodeCmd{
					FieldEncoding: tc.fieldEncoding,
					ReadOption:    rOpt,
					WriteOption:   wOpt,
					ReadPageSize:  10,
					Source:        filepath.Join("..", "testdata", tc.source),
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
	})

	t.Run("field-compression", func(t *testing.T) {
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
				cmd := TranscodeCmd{
					FieldCompression: tc.fieldCompression,
					ReadOption:       rOpt,
					WriteOption:      wOpt,
					ReadPageSize:     10,
					Source:           filepath.Join("..", "testdata", tc.source),
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
	})

	t.Run("field-encoding-and-compression", func(t *testing.T) {
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
		cmd := TranscodeCmd{
			FieldEncoding:    []string{"shoe_brand=DELTA_BYTE_ARRAY"},
			FieldCompression: []string{"shoe_name=ZSTD"},
			ReadOption:       rOpt,
			WriteOption:      wOpt,
			ReadPageSize:     10,
			Source:           filepath.Join("..", "testdata", "good.parquet"),
			URI:              filepath.Join(tempDir, "combined.parquet"),
		}

		err := cmd.Run()
		require.NoError(t, err)

		// Verify output file exists and has correct row count
		reader, err := pio.NewParquetFileReader(cmd.URI, rOpt)
		require.NoError(t, err)
		require.Equal(t, int64(3), reader.GetNumRows())
		_ = reader.PFile.Close()
	})

	t.Run("encoding-preservation", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:     "SNAPPY",
			DataPageVersion: 1,
			PageSize:        1024 * 1024,
			RowGroupSize:    128 * 1024 * 1024,
			ParallelNumber:  0,
		}
		tempDir := t.TempDir()

		// First, create a test file with specific encodings
		createTestFileWithEncodings := func(filename string) string {
			testFilePath := filepath.Join(tempDir, filename)
			// Use the schema command to create a test file with known encodings
			// We'll use the testdata generation approach
			sourceFile := "../testdata/all-types.parquet"

			// Create initial transcode
			cmd := TranscodeCmd{
				ReadOption:   rOpt,
				WriteOption:  wOpt,
				ReadPageSize: 10,
				Source:       sourceFile,
				URI:          testFilePath,
			}
			err := cmd.Run()
			require.NoError(t, err)
			return testFilePath
		}

		testFile := createTestFileWithEncodings("source-with-encodings.parquet")

		// Get the schema from the source file
		schemaCmd := SchemaCmd{
			Format:     "go",
			ReadOption: rOpt,
			URI:        testFile,
		}
		originalSchema, _ := captureStdoutStderr(func() {
			require.NoError(t, schemaCmd.Run())
		})

		t.Run("preserves_encodings_without_override", func(t *testing.T) {
			// Transcode without specifying field encodings
			transcodedFile := filepath.Join(tempDir, "transcoded-preserved.parquet")
			cmd := TranscodeCmd{
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
			schemaCmd := SchemaCmd{
				Format:     "go",
				ReadOption: rOpt,
				URI:        transcodedFile,
			}
			transcodedSchema, _ := captureStdoutStderr(func() {
				require.NoError(t, schemaCmd.Run())
			})

			// Schemas should match (encodings preserved)
			require.Equal(t, originalSchema, transcodedSchema,
				"Encodings should be preserved when transcoding without field-encoding overrides")

			// Verify data integrity
			catOriginal := CatCmd{
				ReadOption:   rOpt,
				ReadPageSize: 1000,
				SampleRatio:  1.0,
				Format:       "json",
				GeoFormat:    "geojson",
				URI:          testFile,
			}
			catTranscoded := CatCmd{
				ReadOption:   rOpt,
				ReadPageSize: 1000,
				SampleRatio:  1.0,
				Format:       "json",
				GeoFormat:    "geojson",
				URI:          transcodedFile,
			}

			originalOutput, _ := captureStdoutStderr(func() {
				require.NoError(t, catOriginal.Run())
			})
			transcodedOutput, _ := captureStdoutStderr(func() {
				require.NoError(t, catTranscoded.Run())
			})

			require.Equal(t, originalOutput, transcodedOutput,
				"Data should be identical after transcoding")
		})

		t.Run("overrides_encoding_when_specified", func(t *testing.T) {
			// Transcode with explicit field encoding override
			transcodedFile := filepath.Join(tempDir, "transcoded-override.parquet")

			// Use good.parquet which has BYTE_ARRAY fields
			// Override one of the fields to use DELTA_BYTE_ARRAY
			cmd := TranscodeCmd{
				FieldEncoding: []string{"shoe_name=DELTA_BYTE_ARRAY"},
				ReadOption:    rOpt,
				WriteOption: pio.WriteOption{
					Compression:     "ZSTD",
					DataPageVersion: 1,
					PageSize:        1024 * 1024,
					RowGroupSize:    128 * 1024 * 1024,
					ParallelNumber:  0,
				},
				ReadPageSize: 10,
				Source:       "../testdata/good.parquet",
				URI:          transcodedFile,
			}
			err := cmd.Run()
			require.NoError(t, err)

			// Get schema from transcoded file
			schemaCmd := SchemaCmd{
				Format:     "go",
				ReadOption: rOpt,
				URI:        transcodedFile,
			}
			transcodedSchema, _ := captureStdoutStderr(func() {
				require.NoError(t, schemaCmd.Run())
			})

			// Verify the encoding was overridden for shoe_name
			require.Contains(t, transcodedSchema, "encoding=DELTA_BYTE_ARRAY",
				"Field encoding should be overridden when explicitly specified")

			// Verify data integrity by checking row count and basic data
			reader, err := pio.NewParquetFileReader(transcodedFile, rOpt)
			require.NoError(t, err)
			require.Equal(t, int64(3), reader.GetNumRows())
			_ = reader.PFile.Close()

			// Verify data using cat
			catTranscoded := CatCmd{
				ReadOption:   rOpt,
				ReadPageSize: 1000,
				SampleRatio:  1.0,
				Format:       "json",
				GeoFormat:    "geojson",
				URI:          transcodedFile,
			}
			catOriginal := CatCmd{
				ReadOption:   rOpt,
				ReadPageSize: 1000,
				SampleRatio:  1.0,
				Format:       "json",
				GeoFormat:    "geojson",
				URI:          "../testdata/good.parquet",
			}

			transcodedOutput, _ := captureStdoutStderr(func() {
				require.NoError(t, catTranscoded.Run())
			})
			originalOutput, _ := captureStdoutStderr(func() {
				require.NoError(t, catOriginal.Run())
			})

			require.Equal(t, originalOutput, transcodedOutput,
				"Data should be identical even with different encoding")
		})

		t.Run("preserves_encodings_with_compression_change", func(t *testing.T) {
			// Transcode with different compression but preserve encodings
			transcodedFile := filepath.Join(tempDir, "transcoded-gzip.parquet")
			cmd := TranscodeCmd{
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
			schemaCmd := SchemaCmd{
				Format:     "go",
				ReadOption: rOpt,
				URI:        transcodedFile,
			}
			transcodedSchema, _ := captureStdoutStderr(func() {
				require.NoError(t, schemaCmd.Run())
			})

			// Encodings should be preserved even with different compression
			require.Equal(t, originalSchema, transcodedSchema,
				"Encodings should be preserved when only changing compression")
		})

		t.Run("preserves_encodings_with_data_page_version_change", func(t *testing.T) {
			// Transcode from v1 to v2 and preserve encodings (except PLAIN_DICTIONARY)
			transcodedFile := filepath.Join(tempDir, "transcoded-v2.parquet")
			cmd := TranscodeCmd{
				ReadOption: rOpt,
				WriteOption: pio.WriteOption{
					Compression:     "SNAPPY",
					DataPageVersion: 2,
					PageSize:        1024 * 1024,
					RowGroupSize:    128 * 1024 * 1024,
					ParallelNumber:  0,
				},
				ReadPageSize: 10,
				Source:       "../testdata/all-types.parquet",
				URI:          transcodedFile,
			}
			err := cmd.Run()
			require.NoError(t, err)

			// Verify file was created and data is correct
			reader, err := pio.NewParquetFileReader(transcodedFile, rOpt)
			require.NoError(t, err)
			require.Equal(t, int64(10), reader.GetNumRows())
			_ = reader.PFile.Close()

			// Note: PLAIN_DICTIONARY should NOT be in v2 files
			schemaCmd := SchemaCmd{
				Format:     "go",
				ReadOption: rOpt,
				URI:        transcodedFile,
			}
			transcodedSchema, _ := captureStdoutStderr(func() {
				require.NoError(t, schemaCmd.Run())
			})

			// Should not contain PLAIN_DICTIONARY in v2
			require.NotContains(t, transcodedSchema, "PLAIN_DICTIONARY",
				"PLAIN_DICTIONARY should not appear in v2 data pages")
		})
	})
}

func TestTranscodeCmdIsEncodingCompatible(t *testing.T) {
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
		// Per parquet-go: RLE is NOT supported for BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY
		{"RLE", "BYTE_ARRAY", false},
		{"RLE", "FIXED_LEN_BYTE_ARRAY", false},
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
		t.Run(tc.encoding+"_"+tc.dataType, func(t *testing.T) {
			result := cmd.isEncodingCompatible(tc.encoding, tc.dataType)
			require.Equal(t, tc.expected, result, "encoding=%s, type=%s", tc.encoding, tc.dataType)
		})
	}
}

func TestTranscodeCmdParseFieldEncodings(t *testing.T) {
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
			fieldEncoding:   []string{"shoe_brand=PLAIN", "shoe_name=DELTA_BYTE_ARRAY"},
			expected:        map[string]string{"shoe_brand": "PLAIN", "shoe_name": "DELTA_BYTE_ARRAY"},
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
			cmd := TranscodeCmd{
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

func TestTranscodeCmd_getAllowedEncodings(t *testing.T) {
	cmd := TranscodeCmd{}

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
				"PLAIN", "BIT_PACKED", "BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED", "PLAIN_DICTIONARY", "RLE", "RLE_DICTIONARY",
			},
		},
		{
			name:     "INT64",
			dataType: "INT64",
			expectedEncodings: []string{
				"PLAIN", "BIT_PACKED", "BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED", "PLAIN_DICTIONARY", "RLE", "RLE_DICTIONARY",
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
				"PLAIN", "BYTE_STREAM_SPLIT", "PLAIN_DICTIONARY", "RLE_DICTIONARY",
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

func TestTranscodeCmdParseFieldCompressions(t *testing.T) {
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
			cmd := TranscodeCmd{
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
	cmd := TranscodeCmd{
		ReadOption: pio.ReadOption{},
		WriteOption: pio.WriteOption{
			Compression:     "ZSTD",
			DataPageVersion: 1,
			PageSize:        1024 * 1024,
			RowGroupSize:    128 * 1024 * 1024,
			ParallelNumber:  0,
		},
		ReadPageSize: 1000,
		Source:       "../build/benchmark.parquet",
		URI:          filepath.Join(tempDir, "transcoded.parquet"),
	}
	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}
