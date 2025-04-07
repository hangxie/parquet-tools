package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

func Test_MergeCmd_Run_error(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{Compression: "SNAPPY"}
	tempDir, _ := os.MkdirTemp(os.TempDir(), "merge-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	testCases := map[string]struct {
		cmd    MergeCmd
		errMsg string
	}{
		"pagesize-too-small":  {MergeCmd{rOpt, wOpt, 0, []string{"src"}, "dummy", false}, "invalid read page size"},
		"source-need-more":    {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/good.parquet"}, "dummy", false}, "needs at least 2 source files"},
		"source-non-existent": {MergeCmd{rOpt, wOpt, 10, []string{"does/not/exist1", "does/not/exist2"}, "dummy", false}, "no such file or directory"},
		"source-not-parquet":  {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/not-a-parquet-file", "../testdata/not-a-parquet-file"}, "dummy", false}, "failed to read from"},
		"source-diff-schema":  {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/good.parquet", "../testdata/empty.parquet"}, "dummy", false}, "does not have same schema"},
		"target-file":         {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/good.parquet", "../testdata/good.parquet"}, "://uri", false}, "unable to parse file location"},
		"target-compression":  {MergeCmd{rOpt, pio.WriteOption{}, 10, []string{"../testdata/good.parquet", "../testdata/good.parquet"}, filepath.Join(tempDir, "dummy"), false}, "not a valid CompressionCode"},
		"target-write":        {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/good.parquet", "../testdata/good.parquet"}, "s3://target", false}, "failed to close"},
		"int96":               {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/all-types.parquet", "../testdata/all-types.parquet"}, "dummy", true}, "type INT96 which is not supported"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.NotNil(t, err)
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
		"good":    {MergeCmd{rOpt, wOpt, 10, []string{"good.parquet", "good.parquet"}, "", false}, 6},
		"empty":   {MergeCmd{rOpt, wOpt, 10, []string{"empty.parquet", "empty.parquet"}, "", false}, 0},
		"top-tag": {MergeCmd{rOpt, wOpt, 10, []string{"top-level-tag1.parquet", "top-level-tag2.parquet"}, "", false}, 6},
	}
	tempDir, _ := os.MkdirTemp(os.TempDir(), "merge-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for i := range tc.cmd.Source {
				tc.cmd.Source[i] = filepath.Join("..", "testdata", tc.cmd.Source[i])
			}
			tc.cmd.URI = filepath.Join(tempDir, name+".parquet")
			err := tc.cmd.Run()
			require.Nil(t, err)

			reader, _ := pio.NewParquetFileReader(tc.cmd.URI, rOpt)
			rowCount := reader.GetNumRows()
			require.Equal(t, rowCount, tc.rowCount)
		})
	}
}
