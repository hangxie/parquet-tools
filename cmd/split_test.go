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
		"page-size":   {SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "", ReadPageSize: 0, RecordCount: 0, URI: "dummy", current: tw}, "invalid read page size"},
		"no-count":    {SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "", ReadPageSize: 1000, RecordCount: 0, URI: "dummy", current: tw}, "needs either --file-count or --record-count"},
		"name-format": {SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "ut-%%parquet", ReadPageSize: 1000, RecordCount: 10, URI: "", current: tw}, "lack of useable verb"},
		"source-file": {SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "%d", ReadPageSize: 1000, RecordCount: 10, URI: "does/not/exist", current: tw}, "failed to open"},
		"int96":       {SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: true, FileCount: 0, NameFormat: "%d", ReadPageSize: 1000, RecordCount: 10, URI: "../testdata/all-types.parquet", current: tw}, "has type INT96 which is not supported"},
		"target-file": {SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "dummy://%d.parquet", ReadPageSize: 1000, RecordCount: 2, URI: "../testdata/good.parquet", current: tw}, "unknown location scheme"},
		"first-write": {SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "s3://target/%d.parquet", ReadPageSize: 1000, RecordCount: 1, URI: "../testdata/good.parquet", current: tw}, "failed to close"},
		"last-write":  {SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "s3://target/%d.parquet", ReadPageSize: 1000, RecordCount: 3, URI: "../testdata/good.parquet", current: tw}, "failed to close"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
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
			SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 3, URI: "all-types.parquet", current: TrunkWriter{}},
			map[string]int64{"ut-0.parquet": 3, "ut-1.parquet": 3, "ut-2.parquet": 3, "ut-3.parquet": 1},
		},
		"file-count": {
			SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 3, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 0, URI: "all-types.parquet", current: TrunkWriter{}},
			map[string]int64{"ut-0.parquet": 4, "ut-1.parquet": 3, "ut-2.parquet": 3},
		},
		"one-result-record-count": {
			SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 20, URI: "all-types.parquet", current: TrunkWriter{}},
			map[string]int64{"ut-0.parquet": 10},
		},
		"one-result-filecount": {
			SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 1, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 0, URI: "all-types.parquet", current: TrunkWriter{}},
			map[string]int64{"ut-0.parquet": 10},
		},
		"empty-record-count": {
			SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 3, URI: "empty.parquet", current: TrunkWriter{}},
			map[string]int64{},
		},
		"empty-file-count": {
			SplitCmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 3, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 0, URI: "empty.parquet", current: TrunkWriter{}},
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
