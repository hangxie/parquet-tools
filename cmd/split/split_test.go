package split

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/cat"
	"github.com/hangxie/parquet-tools/cmd/internal/testutils"

	pio "github.com/hangxie/parquet-tools/io"
)

func TestCmd(t *testing.T) {
	rOpt := pio.ReadOption{}
	wOpt := pio.WriteOption{
		Compression:    "SNAPPY",
		PageSize:       1024 * 1024,
		RowGroupSize:   128 * 1024 * 1024,
		ParallelNumber: 0,
	}
	tw := trunkWriter{}
	testCases := map[string]struct {
		cmd    Cmd
		result map[string]int64
		errMsg string
	}{
		// error cases
		"page-size":   {cmd: Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "", ReadPageSize: 0, RecordCount: 0, URI: "dummy", current: tw}, errMsg: "invalid read page size"},
		"no-count":    {cmd: Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "", ReadPageSize: 1000, RecordCount: 0, URI: "dummy", current: tw}, errMsg: "needs either --file-count or --record-count"},
		"name-format": {cmd: Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "ut-%%parquet", ReadPageSize: 1000, RecordCount: 10, URI: "", current: tw}, errMsg: "lack of useable verb"},
		"source-file": {cmd: Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "%d", ReadPageSize: 1000, RecordCount: 10, URI: "does/not/exist", current: tw}, errMsg: "failed to open"},
		"int96":       {cmd: Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: true, FileCount: 0, NameFormat: "%d", ReadPageSize: 1000, RecordCount: 10, URI: "../../testdata/all-types.parquet", current: tw}, errMsg: "has type INT96 which is not supported"},
		"target-file": {cmd: Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "dummy://%d.parquet", ReadPageSize: 1000, RecordCount: 2, URI: "../../testdata/good.parquet", current: tw}, errMsg: "unknown location scheme"},
		"first-write": {cmd: Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "s3://target/%d.parquet", ReadPageSize: 1000, RecordCount: 1, URI: "../../testdata/good.parquet", current: tw}, errMsg: "failed to close"},
		"last-write":  {cmd: Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "s3://target/%d.parquet", ReadPageSize: 1000, RecordCount: 3, URI: "../../testdata/good.parquet", current: tw}, errMsg: "failed to close"},
		// good cases - URI will be prefixed with "../../testdata/"
		"record-count": {
			cmd:    Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 2, URI: "all-types.parquet", current: trunkWriter{}},
			result: map[string]int64{"ut-0.parquet": 2, "ut-1.parquet": 2, "ut-2.parquet": 1},
		},
		"file-count": {
			cmd:    Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 2, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 0, URI: "all-types.parquet", current: trunkWriter{}},
			result: map[string]int64{"ut-0.parquet": 3, "ut-1.parquet": 2},
		},
		"one-result-record-count": {
			cmd:    Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 20, URI: "all-types.parquet", current: trunkWriter{}},
			result: map[string]int64{"ut-0.parquet": 5},
		},
		"one-result-filecount": {
			cmd:    Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 1, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 0, URI: "all-types.parquet", current: trunkWriter{}},
			result: map[string]int64{"ut-0.parquet": 5},
		},
		"empty-record-count": {
			cmd:    Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 0, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 3, URI: "empty.parquet", current: trunkWriter{}},
			result: map[string]int64{},
		},
		"empty-file-count": {
			cmd:    Cmd{ReadOption: rOpt, WriteOption: wOpt, FailOnInt96: false, FileCount: 3, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 0, URI: "empty.parquet", current: trunkWriter{}},
			result: map[string]int64{},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cmd := tc.cmd
			if tc.errMsg != "" {
				err := cmd.Run()
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				t.Parallel()
				tempDir := t.TempDir()
				cmd.URI = filepath.Join("../../testdata", cmd.URI)
				cmd.NameFormat = filepath.Join(tempDir, cmd.NameFormat)
				err := cmd.Run()
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

					require.True(t, testutils.HasSameSchema(cmd.URI, filepath.Join(tempDir, file.Name())))
				}
			}
		})
	}

	t.Run("optional-fields", func(t *testing.T) {
		tempDir := t.TempDir()
		splitCmd := Cmd{
			ReadOption: pio.ReadOption{},
			WriteOption: pio.WriteOption{
				Compression:    "SNAPPY",
				PageSize:       1024 * 1024,
				RowGroupSize:   128 * 1024 * 1024,
				ParallelNumber: 0,
			},
			ReadPageSize: 11000,
			URI:          "../../testdata/optional-fields.parquet",
			FileCount:    1,
			NameFormat:   filepath.Join(tempDir, "ut-%d.parquet"),
		}

		err := splitCmd.Run()
		require.NoError(t, err)
		files, _ := os.ReadDir(tempDir)
		require.Equal(t, 1, len(files))

		catCmd := cat.Cmd{
			ReadOption:   pio.ReadOption{},
			ReadPageSize: 1000,
			SampleRatio:  1.0,
			Format:       "json",
			URI:          filepath.Join(tempDir, files[0].Name()),
		}
		stdout, _ := testutils.CaptureStdoutStderr(func() {
			require.NoError(t, catCmd.Run())
		})
		require.Equal(t, testutils.LoadExpected(t, "../../testdata/golden/split-optional-fields-json.json"), stdout)
	})
}

func TestCheckNameFormat(t *testing.T) {
	testCases := map[string]error{
		// good
		"%b":    nil,
		"%3d":   nil,
		"%03o":  nil,
		"%x":    nil,
		"%3X":   nil,
		"%05d":  nil,
		"%08x":  nil,
		"%012o": nil,
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
