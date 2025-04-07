package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

func Test_SplitCmd_Run_error(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{Compression: "SNAPPY"}
	tw := TrunkWriter{}
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	testCases := map[string]struct {
		cmd    SplitCmd
		errMsg string
	}{
		"page-size":   {SplitCmd{rOpt, wOpt, 0, "", 0, 0, false, "dummy", tw}, "invalid read page size"},
		"no-count":    {SplitCmd{rOpt, wOpt, 1000, "", 0, 0, false, "dummy", tw}, "needs either --file-count or --record-count"},
		"sorce-file":  {SplitCmd{rOpt, wOpt, 1000, "does/not/exist", 0, 10, false, "dummy", tw}, "failed to open"},
		"int96":       {SplitCmd{rOpt, wOpt, 1000, "../testdata/all-types.parquet", 0, 10, true, "dummy", tw}, "has type INT96 which is not supporte"},
		"target-file": {SplitCmd{rOpt, wOpt, 1000, "../testdata/good.parquet", 0, 2, false, "dummy://%d.parquet", tw}, "unknown location scheme"},
		"first-write": {SplitCmd{rOpt, wOpt, 1000, "../testdata/good.parquet", 0, 1, false, "s3://target/%d.parquet", tw}, "failed to close"},
		"last-write":  {SplitCmd{rOpt, wOpt, 1000, "../testdata/good.parquet", 0, 3, false, "s3://target/%d.parquet", tw}, "failed to close"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.NotNil(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func Test_SplitCmd_Run_good(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{Compression: "SNAPPY"}
	testCases := map[string]struct {
		cmd    SplitCmd
		result map[string]int64
	}{
		"recordcount": {
			SplitCmd{rOpt, wOpt, 1000, "all-types.parquet", 0, 3, false, "ut-%d.parquet", TrunkWriter{}},
			map[string]int64{"ut-0.parquet": 3, "ut-1.parquet": 3, "ut-2.parquet": 3, "ut-3.parquet": 1},
		},
		"fileount": {
			SplitCmd{rOpt, wOpt, 1000, "all-types.parquet", 3, 0, false, "ut-%d.parquet", TrunkWriter{}},
			map[string]int64{"ut-0.parquet": 4, "ut-1.parquet": 3, "ut-2.parquet": 3},
		},
		"one-result-recordcount": {
			SplitCmd{rOpt, wOpt, 1000, "all-types.parquet", 0, 20, false, "ut-%d.parquet", TrunkWriter{}},
			map[string]int64{"ut-0.parquet": 10},
		},
		"one-result-filecount": {
			SplitCmd{rOpt, wOpt, 1000, "all-types.parquet", 1, 0, false, "ut-%d.parquet", TrunkWriter{}},
			map[string]int64{"ut-0.parquet": 10},
		},
		"empty-recordcount": {
			SplitCmd{rOpt, wOpt, 1000, "empty.parquet", 0, 3, false, "ut-%d.parquet", TrunkWriter{}},
			map[string]int64{},
		},
		"empty-filecount": {
			SplitCmd{rOpt, wOpt, 1000, "empty.parquet", 3, 0, false, "ut-%d.parquet", TrunkWriter{}},
			map[string]int64{},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
			defer func() {
				_ = os.RemoveAll(tempDir)
			}()

			tc.cmd.URI = filepath.Join("../testdata", tc.cmd.URI)
			tc.cmd.NameFormat = filepath.Join(tempDir, tc.cmd.NameFormat)
			err := tc.cmd.Run()
			require.Nil(t, err)
			files, _ := os.ReadDir(tempDir)
			require.Equal(t, len(files), len(tc.result))

			for _, file := range files {
				rowCount, ok := tc.result[file.Name()]
				require.True(t, ok)
				reader, err := pio.NewParquetFileReader(filepath.Join(tempDir, file.Name()), rOpt)
				require.Nil(t, err)
				require.NotNil(t, reader)
				require.Equal(t, reader.GetNumRows(), rowCount)
				_ = reader.PFile.Close()
			}
		})
	}
}
