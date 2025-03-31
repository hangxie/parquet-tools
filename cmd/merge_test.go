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
		"pagesize-too-small":  {MergeCmd{rOpt, wOpt, 0, []string{"src"}, tempDir + "/tgt", false}, "invalid read page size"},
		"source-need-more":    {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/good.parquet"}, tempDir + "/tgt", false}, "needs at least 2 source files"},
		"source-non-existent": {MergeCmd{rOpt, wOpt, 10, []string{"does/not/exist1", "does/not/exist2"}, tempDir + "/tgt", false}, "no such file or directory"},
		"source-not-parquet":  {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/not-a-parquet-file", "../testdata/not-a-parquet-file"}, tempDir + "/tgt", false}, "failed to read from"},
		"source-diff-schema":  {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/good.parquet", "../testdata/empty.parquet"}, tempDir + "/tgt", false}, "does not have same schema"},
		"target-file":         {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/good.parquet", "../testdata/good.parquet"}, "://uri", false}, "unable to parse file location"},
		"target-compression":  {MergeCmd{rOpt, pio.WriteOption{}, 10, []string{"../testdata/good.parquet", "../testdata/good.parquet"}, tempDir + "/tgt", false}, "not a valid CompressionCode"},
		"target-write":        {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/good.parquet", "../testdata/good.parquet"}, "s3://target", false}, "failed to close"},
		"int96":               {MergeCmd{rOpt, wOpt, 10, []string{"../testdata/all-types.parquet", "../testdata/all-types.parquet"}, tempDir + "/tgt", true}, "type INT96 which is not supported"},
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
	tempDir, _ := os.MkdirTemp(os.TempDir(), "merge-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &MergeCmd{}
	cmd.ReadPageSize = 10
	cmd.Source = []string{
		"../testdata/good.parquet",
		"../testdata/good.parquet",
	}
	cmd.URI = filepath.Join(tempDir, "import-csv.parquet")
	cmd.Compression = "SNAPPY"

	require.Nil(t, cmd.Run())

	reader, _ := pio.NewParquetFileReader(cmd.URI, pio.ReadOption{})
	rowCount := reader.GetNumRows()
	require.Equal(t, rowCount, int64(6))

	_ = os.Remove(cmd.URI)
}

func Test_MergeCmd_Run_diff_top_level_tag(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "merge-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &MergeCmd{}
	cmd.ReadPageSize = 10
	cmd.Source = []string{
		"../testdata/top-level-tag1.parquet",
		"../testdata/top-level-tag2.parquet",
	}
	cmd.URI = filepath.Join(tempDir, "top-level-tag.parquet")
	cmd.Compression = "SNAPPY"

	err := cmd.Run()
	require.Nil(t, err)

	reader, _ := pio.NewParquetFileReader(cmd.URI, pio.ReadOption{})
	rowCount := reader.GetNumRows()
	require.Equal(t, rowCount, int64(6))

	_ = os.Remove(cmd.URI)
}
