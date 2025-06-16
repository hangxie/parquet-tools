package cmd

import (
	"fmt"
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

	testCases := map[string]struct {
		cmd    SplitCmd
		errMsg string
	}{
		"page-size":   {SplitCmd{rOpt, wOpt, 0, "", 0, 0, false, "dummy", tw}, "invalid read page size"},
		"no-count":    {SplitCmd{rOpt, wOpt, 1000, "", 0, 0, false, "dummy", tw}, "needs either --file-count or --record-count"},
		"name-format": {SplitCmd{rOpt, wOpt, 1000, "", 0, 10, false, "ut-%%parquet", tw}, "lack of useable verb"},
		"source-file": {SplitCmd{rOpt, wOpt, 1000, "does/not/exist", 0, 10, false, "%d", tw}, "failed to open"},
		"int96":       {SplitCmd{rOpt, wOpt, 1000, "../testdata/all-types.parquet", 0, 10, true, "%d", tw}, "has type INT96 which is not supported"},
		"target-file": {SplitCmd{rOpt, wOpt, 1000, "../testdata/good.parquet", 0, 2, false, "dummy://%d.parquet", tw}, "unknown location scheme"},
		"first-write": {SplitCmd{rOpt, wOpt, 1000, "../testdata/good.parquet", 0, 1, false, "s3://target/%d.parquet", tw}, "failed to close"},
		"last-write":  {SplitCmd{rOpt, wOpt, 1000, "../testdata/good.parquet", 0, 3, false, "s3://target/%d.parquet", tw}, "failed to close"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.Error(t, err)
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
		"record-count": {
			SplitCmd{rOpt, wOpt, 1000, "all-types.parquet", 0, 3, false, "ut-%d.parquet", TrunkWriter{}},
			map[string]int64{"ut-0.parquet": 3, "ut-1.parquet": 3, "ut-2.parquet": 3, "ut-3.parquet": 1},
		},
		"file-count": {
			SplitCmd{rOpt, wOpt, 1000, "all-types.parquet", 3, 0, false, "ut-%d.parquet", TrunkWriter{}},
			map[string]int64{"ut-0.parquet": 4, "ut-1.parquet": 3, "ut-2.parquet": 3},
		},
		"one-result-record-count": {
			SplitCmd{rOpt, wOpt, 1000, "all-types.parquet", 0, 20, false, "ut-%d.parquet", TrunkWriter{}},
			map[string]int64{"ut-0.parquet": 10},
		},
		"one-result-filecount": {
			SplitCmd{rOpt, wOpt, 1000, "all-types.parquet", 1, 0, false, "ut-%d.parquet", TrunkWriter{}},
			map[string]int64{"ut-0.parquet": 10},
		},
		"empty-record-count": {
			SplitCmd{rOpt, wOpt, 1000, "empty.parquet", 0, 3, false, "ut-%d.parquet", TrunkWriter{}},
			map[string]int64{},
		},
		"empty-file-count": {
			SplitCmd{rOpt, wOpt, 1000, "empty.parquet", 3, 0, false, "ut-%d.parquet", TrunkWriter{}},
			map[string]int64{},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tempDir := t.TempDir()

			tc.cmd.URI = filepath.Join("../testdata", tc.cmd.URI)
			tc.cmd.NameFormat = filepath.Join(tempDir, tc.cmd.NameFormat)
			err := tc.cmd.Run()
			require.NoError(t, err)
			files, _ := os.ReadDir(tempDir)
			require.Equal(t, len(files), len(tc.result))

			for _, file := range files {
				rowCount, ok := tc.result[file.Name()]
				require.True(t, ok)
				reader, err := pio.NewParquetFileReader(filepath.Join(tempDir, file.Name()), rOpt)
				require.NoError(t, err)
				require.NotNil(t, reader)
				require.Equal(t, reader.GetNumRows(), rowCount)
				_ = reader.PFile.Close()
			}
		})
	}
}

func Test_SplitCmd_Run_optional_fields(t *testing.T) {
	tempDir := t.TempDir()
	splitCmd := SplitCmd{
		ReadOption:   pio.ReadOption{},
		WriteOption:  pio.WriteOption{Compression: "SNAPPY"},
		ReadPageSize: 11000,
		URI:          "../testdata/optional-fields.parquet",
		FileCount:    1,
		NameFormat:   filepath.Join(tempDir, "ut-%d.parquet"),
	}

	err := splitCmd.Run()
	require.NoError(t, err)
	files, _ := os.ReadDir(tempDir)
	require.Equal(t, 1, len(files))

	catCmd := CatCmd{
		ReadOption:   pio.ReadOption{},
		ReadPageSize: 1000,
		SampleRatio:  1.0,
		Format:       "json",
		URI:          filepath.Join(tempDir, files[0].Name()),
	}
	stdout, _ := captureStdoutStderr(func() {
		require.NoError(t, catCmd.Run())
	})
	require.Equal(t, loadExpected(t, "../testdata/golden/split-optional-fields-json.json"), stdout)
}

func Test_checkNameFormat(t *testing.T) {
	testCases := map[string]error{
		// good
		"%b":   nil,
		"%3d":  nil,
		"%03o": nil,
		"%x":   nil,
		"%3X":  nil,
		// bad
		"foobar":  fmt.Errorf("lack of useable ver"),
		"%%":      fmt.Errorf("lack of useable ver"),
		"%s":      fmt.Errorf("is not an allowed format verb"),
		"%0.7f":   fmt.Errorf("is not an allowed format verb"),
		"%d-%02x": fmt.Errorf("has more than one useable verb"),
	}

	for name, expected := range testCases {
		t.Run(name, func(t *testing.T) {
			err := checkNameFormat(name)
			if expected != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), expected.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
