package merge

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/cat"
	"github.com/hangxie/parquet-tools/cmd/internal/testutils"

	pio "github.com/hangxie/parquet-tools/io"
)

func TestCmd(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		wOptNoCompression := pio.WriteOption{
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		tempDir := t.TempDir()

		testCases := map[string]struct {
			cmd    Cmd
			errMsg string
		}{
			"pagesize-too-small":  {Cmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: true, ReadPageSize: 0, Source: []string{"src"}, URI: "dummy"}, "invalid read page size"},
			"source-need-more":    {Cmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: false, ReadPageSize: 10, Source: []string{"../../testdata/good.parquet"}, URI: "dummy"}, "needs at least 2 source files"},
			"source-non-existent": {Cmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: true, ReadPageSize: 10, Source: []string{"does/not/exist1", "does/not/exist2"}, URI: "dummy"}, "no such file or directory"},
			"source-not-parquet":  {Cmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: false, ReadPageSize: 10, Source: []string{"../../testdata/not-a-parquet-file", "../../testdata/not-a-parquet-file"}, URI: "dummy"}, "failed to read from"},
			"source-diff-schema":  {Cmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: true, ReadPageSize: 10, Source: []string{"../../testdata/good.parquet", "../../testdata/empty.parquet"}, URI: "dummy"}, "does not have same schema"},
			"target-file":         {Cmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: false, ReadPageSize: 10, Source: []string{"../../testdata/good.parquet", "../../testdata/good.parquet"}, URI: "://uri"}, "unable to parse file location"},
			"target-compression":  {Cmd{ReadOption: rOpt, WriteOption: wOptNoCompression, Concurrent: false, FailOnInt96: true, ReadPageSize: 10, Source: []string{"../../testdata/good.parquet", "../../testdata/good.parquet"}, URI: filepath.Join(tempDir, "dummy")}, "not a valid CompressionCode"},
			"target-write":        {Cmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: false, ReadPageSize: 10, Source: []string{"../../testdata/good.parquet", "../../testdata/good.parquet"}, URI: "s3://target"}, "failed to close"},
			"int96":               {Cmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: true, FailOnInt96: true, ReadPageSize: 10, Source: []string{"../../testdata/all-types.parquet", "../../testdata/all-types.parquet"}, URI: "dummy"}, "type INT96 which is not supported"},
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
		wOpt := pio.WriteOption{
			Compression:     "SNAPPY",
			PageSize:        1024 * 1024,
			RowGroupSize:    128 * 1024 * 1024,
			ParallelNumber:  0,
			DataPageVersion: 1,
		}
		testCases := map[string]struct {
			cmd      Cmd
			rowCount int64
		}{
			"good":      {Cmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: true, FailOnInt96: false, ReadPageSize: 10, Source: []string{"good.parquet", "good.parquet"}, URI: ""}, 6},
			"all-types": {Cmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: false, ReadPageSize: 10, Source: []string{"all-types.parquet", "all-types.parquet"}, URI: ""}, 10},
			"empty":     {Cmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: true, FailOnInt96: false, ReadPageSize: 10, Source: []string{"empty.parquet", "empty.parquet"}, URI: ""}, 0},
			"top-tag":   {Cmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: false, ReadPageSize: 10, Source: []string{"top-level-tag1.parquet", "top-level-tag2.parquet"}, URI: ""}, 6},
		}
		tempDir := t.TempDir()

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				for i := range tc.cmd.Source {
					tc.cmd.Source[i] = filepath.Join("..", "..", "testdata", tc.cmd.Source[i])
				}
				tc.cmd.URI = filepath.Join(tempDir, name+".parquet")
				err := tc.cmd.Run()
				require.NoError(t, err)

				reader, _ := pio.NewParquetFileReader(tc.cmd.URI, rOpt)
				rowCount := reader.GetNumRows()
				_ = reader.PFile.Close()
				require.Equal(t, tc.rowCount, rowCount)

				require.True(t, testutils.HasSameSchema(tc.cmd.Source[0], tc.cmd.URI, false, false))
			})
		}
	})

	t.Run("repeat", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:     "SNAPPY",
			PageSize:        1024 * 1024,
			RowGroupSize:    128 * 1024 * 1024,
			ParallelNumber:  0,
			DataPageVersion: 1,
		}
		tempDir := t.TempDir()
		source := "../../testdata/all-types.parquet"

		cmd := Cmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: true, FailOnInt96: false, ReadPageSize: 10, Source: []string{source, source}, URI: ""}
		cmd.URI = filepath.Join(tempDir, "1.parquet")
		require.Nil(t, cmd.Run())

		reader, _ := pio.NewParquetFileReader(cmd.URI, rOpt)
		rowCount := reader.GetNumRows()
		_ = reader.PFile.Close()
		require.Equal(t, int64(10), rowCount)
		require.True(t, testutils.HasSameSchema(source, cmd.URI, false, false))

		cmd.Source = []string{cmd.URI, source}
		cmd.URI = filepath.Join(tempDir, "2.parquet")
		require.Nil(t, cmd.Run())

		reader, _ = pio.NewParquetFileReader(cmd.URI, rOpt)
		rowCount = reader.GetNumRows()
		_ = reader.PFile.Close()
		require.Equal(t, int64(15), rowCount)
		require.True(t, testutils.HasSameSchema(source, cmd.URI, false, false))

		cmd.Source = []string{cmd.URI, source}
		cmd.URI = filepath.Join(tempDir, "3.parquet")
		require.Nil(t, cmd.Run())

		reader, _ = pio.NewParquetFileReader(cmd.URI, rOpt)
		rowCount = reader.GetNumRows()
		_ = reader.PFile.Close()
		require.Equal(t, int64(20), rowCount)
		require.True(t, testutils.HasSameSchema(source, cmd.URI, false, false))
	})

	t.Run("optional-list", func(t *testing.T) {
		tempDir := t.TempDir()
		resultFile := filepath.Join(tempDir, "ut.parquet")
		mergeCmd := Cmd{
			ReadOption: pio.ReadOption{},
			WriteOption: pio.WriteOption{
				Compression:    "SNAPPY",
				PageSize:       1024 * 1024,
				RowGroupSize:   128 * 1024 * 1024,
				ParallelNumber: 0,
			},
			ReadPageSize: 11000,
			Source:       []string{"../../testdata/optional-fields.parquet", "../../testdata/optional-fields.parquet"},
			URI:          resultFile,
		}

		err := mergeCmd.Run()
		require.NoError(t, err)

		require.True(t, testutils.HasSameSchema("../../testdata/optional-fields.parquet", resultFile, false, false))

		catCmd := cat.Cmd{
			ReadOption:   pio.ReadOption{},
			ReadPageSize: 1000,
			SampleRatio:  1.0,
			Format:       "json",
			GeoFormat:    "geojson",
			URI:          resultFile,
		}
		stdout, _ := testutils.CaptureStdoutStderr(func() {
			require.NoError(t, catCmd.Run())
		})
		require.Equal(t, testutils.LoadExpected(t, "../../testdata/golden/merge-optional-fields-json.json"), stdout)
	})
}

func BenchmarkMergeCmd(b *testing.B) {
	savedStdout, savedStderr := os.Stdout, os.Stderr
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0o666)
	if err != nil {
		b.Fatal(err)
	}
	os.Stdout = devNull
	defer func() {
		os.Stdout, os.Stderr = savedStdout, savedStderr
		_ = devNull.Close()
	}()

	cmd := Cmd{
		ReadOption: pio.ReadOption{},
		WriteOption: pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		},
		ReadPageSize: 1000,
		Source:       slices.Repeat([]string{"../../build/benchmark.parquet"}, 3),
		URI:          "../../build/merged.parquet",
	}

	// Warm up the Go runtime before actual benchmark
	for range 10 {
		_ = cmd.Run()
	}

	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
	cmd.Concurrent = true
	b.Run("concurrent", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}
