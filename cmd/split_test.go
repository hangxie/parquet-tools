package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/internal"
)

func Test_SplitCmd_Run_wrong_params(t *testing.T) {
	cmd := &SplitCmd{}

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "invalid read page size")

	cmd.ReadPageSize = 1000
	err = cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "needs either --file-count or --record-count")
}

func Test_SplitCmd_Run_failed_to_open_for_read(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &SplitCmd{}
	cmd.URI = "file/does/not/exist"
	cmd.ReadPageSize = 1000
	cmd.RecordCount = 10
	cmd.NameFormat = filepath.Join(tempDir, "unit-test-%d.parquet")

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open")
}

func Test_SplitCmd_Run_failed_with_int96(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &SplitCmd{}
	cmd.URI = "../testdata/all-types.parquet"
	cmd.FailOnInt96 = true
	cmd.ReadPageSize = 1000
	cmd.RecordCount = 10
	cmd.NameFormat = filepath.Join(tempDir, "unit-test-%d.parquet")

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to load schema")
}

func Test_SplitCmd_Run_failed_to_open_for_write(t *testing.T) {
	cmd := &SplitCmd{}
	cmd.URI = "../testdata/all-types.parquet"
	cmd.WriteOption.Compression = "SNAPPY"
	cmd.ReadPageSize = 1000
	cmd.RecordCount = 10
	cmd.NameFormat = "dummy://unit-test-%d.parquet"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unknown location scheme")
}

func Test_SplitCmd_Run_failed_to_close_for_write(t *testing.T) {
	cmd := &SplitCmd{}
	cmd.URI = "../testdata/all-types.parquet"
	cmd.WriteOption.Compression = "SNAPPY"
	cmd.ReadPageSize = 1000
	cmd.RecordCount = 10

	// failed to close last parquet file
	cmd.NameFormat = "s3://daylight-openstreetmap/unit-test-%d.parquet"
	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to close [s3:")

	// failed to close first parquet file
	cmd.RecordCount = 3
	cmd.NameFormat = "s3://daylight-openstreetmap/unit-test-%d.parquet"
	err = cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to close [s3:")
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
		reader.PFile.Close()
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
		reader.PFile.Close()
	}
}
