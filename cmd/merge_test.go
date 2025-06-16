package cmd

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

func Test_MergeCmd_Run_error(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{Compression: "SNAPPY"}
	tempDir := t.TempDir()

	testCases := map[string]struct {
		cmd    MergeCmd
		errMsg string
	}{
		"pagesize-too-small":  {MergeCmd{rOpt, wOpt, 0, []string{"src"}, "dummy", false, true}, "invalid read page size"},
		"source-need-more":    {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/good.parquet"}, "dummy", false, false}, "needs at least 2 source files"},
		"source-non-existent": {MergeCmd{rOpt, wOpt, 10, []string{"does/not/exist1", "does/not/exist2"}, "dummy", false, true}, "no such file or directory"},
		"source-not-parquet":  {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/not-a-parquet-file", "../testdata/not-a-parquet-file"}, "dummy", false, false}, "failed to read from"},
		"source-diff-schema":  {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/good.parquet", "../testdata/empty.parquet"}, "dummy", false, true}, "does not have same schema"},
		"target-file":         {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/good.parquet", "../testdata/good.parquet"}, "://uri", false, false}, "unable to parse file location"},
		"target-compression":  {MergeCmd{rOpt, pio.WriteOption{}, 10, []string{"../testdata/good.parquet", "../testdata/good.parquet"}, filepath.Join(tempDir, "dummy"), false, true}, "not a valid CompressionCode"},
		"target-write":        {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/good.parquet", "../testdata/good.parquet"}, "s3://target", false, false}, "failed to close"},
		"int96":               {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/all-types.parquet", "../testdata/all-types.parquet"}, "dummy", true, true}, "type INT96 which is not supported"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func Test_MergeCmd_Run_good(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{Compression: "SNAPPY"}
	testCases := map[string]struct {
		cmd      MergeCmd
		rowCount int64
	}{
		"good":      {MergeCmd{rOpt, wOpt, 10, []string{"good.parquet", "good.parquet"}, "", false, true}, 6},
		"all-types": {MergeCmd{rOpt, wOpt, 10, []string{"all-types.parquet", "all-types.parquet"}, "", false, false}, 20},
		"empty":     {MergeCmd{rOpt, wOpt, 10, []string{"empty.parquet", "empty.parquet"}, "", false, true}, 0},
		"top-tag":   {MergeCmd{rOpt, wOpt, 10, []string{"top-level-tag1.parquet", "top-level-tag2.parquet"}, "", false, false}, 6},
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
}

func Test_MergeCmd_Run_good_repeat(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{Compression: "SNAPPY"}
	tempDir := t.TempDir()
	source := "../testdata/all-types.parquet"

	cmd := MergeCmd{rOpt, wOpt, 10, []string{source, source}, "", false, true}
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
}

func Benchmark_MergeCmd_Run(b *testing.B) {
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

func Test_MergeCmd_Run_optional_list(t *testing.T) {
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
		URI:          resultFile,
	}
	stdout, _ := captureStdoutStderr(func() {
		require.NoError(t, catCmd.Run())
	})
	require.Equal(t, loadExpected(t, "../testdata/golden/merge-optional-fields-json.json"), stdout)
}
