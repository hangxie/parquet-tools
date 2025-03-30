package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/internal"
)

func Test_SplitCmd_Run_error(t *testing.T) {
	rOpt := internal.ReadOption{}
	wOpt := internal.WriteOption{Compression: "SNAPPY"}
	tw := TrunkWriter{}
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	testCases := map[string]struct {
		cmd    SplitCmd
		errMsg string
	}{
		"page-size":   {SplitCmd{rOpt, wOpt, 0, "", 0, 0, false, "", tw}, "invalid read page size"},
		"no-count":    {SplitCmd{rOpt, wOpt, 1000, "", 0, 0, false, "", tw}, "needs either --file-count or --record-count"},
		"sorce-file":  {SplitCmd{rOpt, wOpt, 1000, "does/not/exist", 0, 10, false, tempDir + "/%d.parquet", tw}, "failed to open"},
		"int96":       {SplitCmd{rOpt, wOpt, 1000, "../testdata/all-types.parquet", 0, 10, true, tempDir + "/%d.parquet", tw}, "has type INT96 which is not supporte"},
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

func Test_SplitCmd_Run_good_with_recordcount(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &SplitCmd{}
	cmd.URI = "../testdata/all-types.parquet"
	cmd.WriteOption.Compression = "SNAPPY"
	cmd.ReadPageSize = 1000
	cmd.RecordCount = 3
	cmd.NameFormat = filepath.Join(tempDir, "unit-test-%d.parquet")

	err := cmd.Run()
	require.Nil(t, err)
	for i := 0; i < 4; i++ {
		reader, err := internal.NewParquetFileReader(fmt.Sprintf(cmd.NameFormat, i), internal.ReadOption{})
		require.Nil(t, err)
		require.NotNil(t, reader)
		if i == 3 {
			// last file contains 1 record
			require.Equal(t, reader.GetNumRows(), int64(1))
		} else {
			/// all other files contains 3 records
			require.Equal(t, reader.GetNumRows(), int64(3))
		}
		_ = reader.PFile.Close()
	}
}

func Test_SplitCmd_Run_good_with_filecount(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &SplitCmd{}
	cmd.URI = "../testdata/all-types.parquet"
	cmd.WriteOption.Compression = "SNAPPY"
	cmd.ReadPageSize = 1000
	cmd.FileCount = 3
	cmd.NameFormat = filepath.Join(tempDir, "unit-test-%d.parquet")

	err := cmd.Run()
	require.Nil(t, err)
	for i := 0; i < 3; i++ {
		reader, err := internal.NewParquetFileReader(fmt.Sprintf(cmd.NameFormat, i), internal.ReadOption{})
		require.Nil(t, err)
		require.NotNil(t, reader)
		if i == 0 {
			// first file contains 4 records
			require.Equal(t, reader.GetNumRows(), int64(4))
		} else {
			/// all other files contains 3 records
			require.Equal(t, reader.GetNumRows(), int64(3))
		}
		_ = reader.PFile.Close()
	}
}
