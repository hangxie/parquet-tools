package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/internal"
)

func Test_MergeCmd_Run_pagesize_too_small(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "merge-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &MergeCmd{}
	cmd.ReadPageSize = 0
	cmd.Sources = []string{
		"../testdata/good.parquet",
		"../testdata/good.parquet",
	}
	cmd.URI = filepath.Join(tempDir, "import-csv.parquet")
	cmd.Compression = "SNAPPY"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "invalid read page size")

	_ = os.Remove(cmd.URI)
}

func Test_MergeCmd_Run_need_more_sources(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "merge-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &MergeCmd{}
	cmd.ReadPageSize = 10
	cmd.Sources = []string{
		"../testdata/good.parquet",
	}
	cmd.URI = filepath.Join(tempDir, "import-csv.parquet")
	cmd.Compression = "SNAPPY"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "needs at least 2 sources files")

	_ = os.Remove(cmd.URI)
}

func Test_MergeCmd_Run_nonexistent_source(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "merge-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &MergeCmd{}
	cmd.ReadPageSize = 10
	cmd.Sources = []string{
		"/path/to/nowhere/file1",
		"/path/to/nowhere/file2",
	}
	cmd.URI = filepath.Join(tempDir, "import-csv.parquet")
	cmd.Compression = "SNAPPY"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "no such file or directory")

	_ = os.Remove(cmd.URI)
}

func Test_MergeCmd_Run_invalid_source(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "merge-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &MergeCmd{}
	cmd.ReadPageSize = 10
	cmd.Sources = []string{
		"../testdata/not-a-parquet-file",
		"../testdata/not-a-parquet-file",
	}
	cmd.URI = filepath.Join(tempDir, "import-csv.parquet")
	cmd.Compression = "SNAPPY"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to read from")

	_ = os.Remove(cmd.URI)
}

func Test_MergeCmd_Run_source_schema_not_match(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "merge-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &MergeCmd{}
	cmd.ReadPageSize = 10
	cmd.Sources = []string{
		"../testdata/good.parquet",
		"../testdata/empty.parquet",
	}
	cmd.URI = filepath.Join(tempDir, "import-csv.parquet")
	cmd.Compression = "SNAPPY"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "does not have same schema")

	_ = os.Remove(cmd.URI)
}

func Test_MergeCmd_Run_invalid_target(t *testing.T) {
	cmd := &MergeCmd{}
	cmd.ReadPageSize = 10
	cmd.Sources = []string{
		"../testdata/good.parquet",
		"../testdata/good.parquet",
	}
	cmd.URI = "://uri"
	cmd.Compression = "SNAPPY"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")

	_ = os.Remove(cmd.URI)
}

func Test_MergeCmd_Run_failed_to_write_stop(t *testing.T) {
	cmd := &MergeCmd{}
	cmd.ReadPageSize = 10
	cmd.Sources = []string{
		"../testdata/good.parquet",
		"../testdata/good.parquet",
	}
	cmd.URI = "s3://aws"
	cmd.Compression = "SNAPPY"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "operation error S3: PutObject")

	_ = os.Remove(cmd.URI)
}

func Test_MergeCmd_Run_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "merge-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &MergeCmd{}
	cmd.ReadPageSize = 10
	cmd.Sources = []string{
		"../testdata/good.parquet",
		"../testdata/good.parquet",
	}
	cmd.URI = filepath.Join(tempDir, "import-csv.parquet")
	cmd.Compression = "SNAPPY"

	require.Nil(t, cmd.Run())

	reader, _ := internal.NewParquetFileReader(cmd.URI, internal.ReadOption{})
	rowCount := reader.GetNumRows()
	require.Equal(t, rowCount, int64(6))

	_ = os.Remove(cmd.URI)
}

func Test_MergeCmd_Run_fail_on_int96(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "merge-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	cmd := &MergeCmd{}
	cmd.ReadPageSize = 10
	cmd.Sources = []string{
		"../testdata/all-types.parquet",
		"../testdata/all-types.parquet",
	}
	cmd.URI = filepath.Join(tempDir, "import-csv.parquet")
	cmd.Compression = "SNAPPY"
	cmd.FailOnInt96 = true

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "type INT96 which is not supporte")

	_ = os.Remove(cmd.URI)
}
