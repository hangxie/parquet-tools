package cmd

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
)

func TestMergeCmd(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{Compression: "SNAPPY"}
		tempDir := t.TempDir()

		testCases := map[string]struct {
			cmd    MergeCmd
			errMsg string
		}{
			"pagesize-too-small":  {MergeCmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: true, ReadPageSize: 0, Source: []string{"src"}, URI: "dummy"}, "invalid read page size"},
			"source-need-more":    {MergeCmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: false, ReadPageSize: 10, Source: []string{"../testdata/good.parquet"}, URI: "dummy"}, "needs at least 2 source files"},
			"source-non-existent": {MergeCmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: true, ReadPageSize: 10, Source: []string{"does/not/exist1", "does/not/exist2"}, URI: "dummy"}, "no such file or directory"},
			"source-not-parquet":  {MergeCmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: false, ReadPageSize: 10, Source: []string{"../testdata/not-a-parquet-file", "../testdata/not-a-parquet-file"}, URI: "dummy"}, "failed to read from"},
			"source-diff-schema":  {MergeCmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: true, ReadPageSize: 10, Source: []string{"../testdata/good.parquet", "../testdata/empty.parquet"}, URI: "dummy"}, "does not have same schema"},
			"target-file":         {MergeCmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: false, ReadPageSize: 10, Source: []string{"../testdata/good.parquet", "../testdata/good.parquet"}, URI: "://uri"}, "unable to parse file location"},
			"target-compression":  {MergeCmd{ReadOption: rOpt, WriteOption: pio.WriteOption{}, Concurrent: false, FailOnInt96: true, ReadPageSize: 10, Source: []string{"../testdata/good.parquet", "../testdata/good.parquet"}, URI: filepath.Join(tempDir, "dummy")}, "not a valid CompressionCode"},
			"target-write":        {MergeCmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: false, ReadPageSize: 10, Source: []string{"../testdata/good.parquet", "../testdata/good.parquet"}, URI: "s3://target"}, "failed to close"},
			"int96":               {MergeCmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: true, FailOnInt96: true, ReadPageSize: 10, Source: []string{"../testdata/all-types.parquet", "../testdata/all-types.parquet"}, URI: "dummy"}, "type INT96 which is not supported"},
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
		wOpt := pio.WriteOption{Compression: "SNAPPY"}
		testCases := map[string]struct {
			cmd      MergeCmd
			rowCount int64
		}{
			"good":      {MergeCmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: true, FailOnInt96: false, ReadPageSize: 10, Source: []string{"good.parquet", "good.parquet"}, URI: ""}, 6},
			"all-types": {MergeCmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: false, ReadPageSize: 10, Source: []string{"all-types.parquet", "all-types.parquet"}, URI: ""}, 20},
			"empty":     {MergeCmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: true, FailOnInt96: false, ReadPageSize: 10, Source: []string{"empty.parquet", "empty.parquet"}, URI: ""}, 0},
			"top-tag":   {MergeCmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: false, FailOnInt96: false, ReadPageSize: 10, Source: []string{"top-level-tag1.parquet", "top-level-tag2.parquet"}, URI: ""}, 6},
		}
		tempDir := t.TempDir()

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				for i := range tc.cmd.Source {
					tc.cmd.Source[i] = filepath.Join("..", "testdata", tc.cmd.Source[i])
				}
				tc.cmd.URI = filepath.Join(tempDir, name+".parquet")
				err := tc.cmd.Run()
				require.NoError(t, err)

				reader, _ := pio.NewParquetFileReader(tc.cmd.URI, rOpt)
				rowCount := reader.GetNumRows()
				_ = reader.PFile.Close()
				require.Equal(t, tc.rowCount, rowCount)
			})
		}
	})

	t.Run("repeat", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{Compression: "SNAPPY"}
		tempDir := t.TempDir()
		source := "../testdata/all-types.parquet"

		cmd := MergeCmd{ReadOption: rOpt, WriteOption: wOpt, Concurrent: true, FailOnInt96: false, ReadPageSize: 10, Source: []string{source, source}, URI: ""}
		cmd.URI = filepath.Join(tempDir, "1.parquet")
		require.Nil(t, cmd.Run())

		reader, _ := pio.NewParquetFileReader(cmd.URI, rOpt)
		rowCount := reader.GetNumRows()
		_ = reader.PFile.Close()
		require.Equal(t, int64(20), rowCount)

		cmd.Source = []string{cmd.URI, source}
		cmd.URI = filepath.Join(tempDir, "2.parquet")
		require.Nil(t, cmd.Run())

		reader, _ = pio.NewParquetFileReader(cmd.URI, rOpt)
		rowCount = reader.GetNumRows()
		_ = reader.PFile.Close()
		require.Equal(t, int64(30), rowCount)

		cmd.Source = []string{cmd.URI, source}
		cmd.URI = filepath.Join(tempDir, "3.parquet")
		require.Nil(t, cmd.Run())

		reader, _ = pio.NewParquetFileReader(cmd.URI, rOpt)
		rowCount = reader.GetNumRows()
		_ = reader.PFile.Close()
		require.Equal(t, int64(40), rowCount)
	})

	t.Run("optional-list", func(t *testing.T) {
		tempDir := t.TempDir()
		resultFile := filepath.Join(tempDir, "ut.parquet")
		mergeCmd := MergeCmd{
			ReadOption:   pio.ReadOption{},
			WriteOption:  pio.WriteOption{Compression: "SNAPPY"},
			ReadPageSize: 11000,
			Source:       []string{"../testdata/optional-fields.parquet", "../testdata/optional-fields.parquet"},
			URI:          resultFile,
		}

		err := mergeCmd.Run()
		require.NoError(t, err)

		catCmd := CatCmd{
			ReadOption:   pio.ReadOption{},
			ReadPageSize: 1000,
			SampleRatio:  1.0,
			Format:       "json",
			GeoFormat:    "geojson",
			URI:          resultFile,
		}
		stdout, _ := captureStdoutStderr(func() {
			require.NoError(t, catCmd.Run())
		})
		require.Equal(t, loadExpected(t, "../testdata/golden/merge-optional-fields-json.json"), stdout)
	})
}

func BenchmarkMergeCmd(b *testing.B) {
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

	cmd := MergeCmd{
		ReadOption:   pio.ReadOption{},
		WriteOption:  pio.WriteOption{Compression: "SNAPPY"},
		ReadPageSize: 1000,
		Source:       slices.Repeat([]string{"../build/benchmark.parquet"}, 3),
		URI:          "../build/merged.parquet",
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
