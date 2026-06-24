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
	tw := trunkWriter{}
	testCases := map[string]struct {
		cmd    Cmd
		result map[string]int64
		errMsg string
	}{
		// error cases
		"page-size": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: false, FileCount: 0, NameFormat: "", ReadPageSize: 0, RecordCount: 0, URI: "dummy", current: tw},
			errMsg: "invalid read page size",
		},
		"no-count": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: false, FileCount: 0, NameFormat: "", ReadPageSize: 1000, RecordCount: 0, URI: "dummy", current: tw},
			errMsg: "needs either --file-count or --record-count",
		},
		"name-format": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: false, FileCount: 0, NameFormat: "ut-%%parquet", ReadPageSize: 1000, RecordCount: 10, URI: "", current: tw},
			errMsg: "lack of useable verb",
		},
		"source-file": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: false, FileCount: 0, NameFormat: "%d", ReadPageSize: 1000, RecordCount: 10, URI: "does/not/exist", current: tw},
			errMsg: "failed to open",
		},
		"int96": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: true, FileCount: 0, NameFormat: "%d", ReadPageSize: 1000, RecordCount: 10, URI: "../../testdata/all-types.parquet", current: tw},
			errMsg: "has type INT96 which is not supported",
		},
		"target-file": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: false, FileCount: 0, NameFormat: "dummy://%d.parquet", ReadPageSize: 1000, RecordCount: 2, URI: "../../testdata/good.parquet", current: tw},
			errMsg: "unknown location scheme",
		},
		"first-write": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: false, FileCount: 0, NameFormat: "s3://target/%d.parquet", ReadPageSize: 1000, RecordCount: 1, URI: "../../testdata/good.parquet", current: tw},
			errMsg: "failed to close",
		},
		"last-write": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: false, FileCount: 0, NameFormat: "s3://target/%d.parquet", ReadPageSize: 1000, RecordCount: 3, URI: "../../testdata/good.parquet", current: tw},
			errMsg: "failed to close",
		},
		// good cases - URI will be prefixed with "../../testdata/"
		"record-count": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: false, FileCount: 0, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 2, URI: "all-types.parquet", current: trunkWriter{}},
			result: map[string]int64{"ut-0.parquet": 2, "ut-1.parquet": 2, "ut-2.parquet": 1},
		},
		"file-count": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: false, FileCount: 2, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 0, URI: "all-types.parquet", current: trunkWriter{}},
			result: map[string]int64{"ut-0.parquet": 3, "ut-1.parquet": 2},
		},
		"one-result-record-count": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: false, FileCount: 0, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 20, URI: "all-types.parquet", current: trunkWriter{}},
			result: map[string]int64{"ut-0.parquet": 5},
		},
		"one-result-filecount": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: false, FileCount: 1, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 0, URI: "all-types.parquet", current: trunkWriter{}},
			result: map[string]int64{"ut-0.parquet": 5},
		},
		"empty-record-count": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: false, FileCount: 0, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 3, URI: "empty.parquet", current: trunkWriter{}},
			result: map[string]int64{},
		},
		"empty-file-count": {
			cmd:    Cmd{ReadOption: rOpt, FailOnInt96: false, FileCount: 3, NameFormat: "ut-%d.parquet", ReadPageSize: 1000, RecordCount: 0, URI: "empty.parquet", current: trunkWriter{}},
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
			ReadOption:   pio.ReadOption{},
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

var (
	splitEncryptionFooterKey = new("MDEyMzQ1Njc4OTAxMjM0NQ==")
	splitEncryptionColumnKey = "MTIzNDU2Nzg5MDEyMzQ1MA=="
)

func TestCmdEncryption(t *testing.T) {
	source := filepath.Join("..", "..", "testdata", "good.parquet")

	testCases := []struct {
		name        string
		writeOption pio.WriteOption
		readOption  pio.ReadOption
		footerMagic string
	}{
		{
			name: "encrypted-footer",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  splitEncryptionFooterKey,
			},
			readOption:  pio.ReadOption{FooterKey: splitEncryptionFooterKey},
			footerMagic: "PARE",
		},
		{
			name: "encrypted-footer-column-keys",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  splitEncryptionFooterKey,
				WriterColumnKeys: []string{"shoe_name=" + splitEncryptionColumnKey},
			},
			readOption: pio.ReadOption{
				FooterKey:  splitEncryptionFooterKey,
				ColumnKeys: []string{"shoe_name=" + splitEncryptionColumnKey},
			},
			footerMagic: "PARE",
		},
		{
			name: "plaintext-footer-column-keys",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  splitEncryptionFooterKey,
				WriterColumnKeys: []string{"shoe_name=" + splitEncryptionColumnKey},
				PlaintextFooter:  true,
			},
			readOption: pio.ReadOption{
				FooterKey:  splitEncryptionFooterKey,
				ColumnKeys: []string{"shoe_name=" + splitEncryptionColumnKey},
			},
			footerMagic: "PAR1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			cmd := Cmd{
				ReadOption:   pio.ReadOption{},
				WriteOption:  tc.writeOption,
				ReadPageSize: 1000,
				RecordCount:  2,
				NameFormat:   filepath.Join(tempDir, "part-%d.parquet"),
				URI:          source,
			}
			require.NoError(t, cmd.Run())

			files, err := os.ReadDir(tempDir)
			require.NoError(t, err)
			require.Equal(t, 2, len(files))

			var totalRows int64
			for _, file := range files {
				path := filepath.Join(tempDir, file.Name())
				require.Equal(t, tc.footerMagic, testutils.ParquetFooterMagic(t, path))
				reader, err := pio.NewParquetFileReader(path, tc.readOption)
				require.NoError(t, err)
				totalRows += reader.GetNumRows()
				_ = reader.PFile.Close()
			}
			require.Equal(t, int64(3), totalRows)
		})
	}
}

func TestCmdEncryptionErrors(t *testing.T) {
	source := filepath.Join("..", "..", "testdata", "good.parquet")

	testCases := []struct {
		name        string
		writeOption pio.WriteOption
		errMsg      string
	}{
		{
			name: "missing-footer-key",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterColumnKeys: []string{"shoe_name=" + splitEncryptionColumnKey},
			},
			errMsg: "--writer-footer-key is required",
		},
		{
			name: "bad-base64",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  new("not base64"),
			},
			errMsg: "invalid base64 writer footer key",
		},
		{
			name: "wrong-key-size",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  new("MTIzNDU="),
			},
			errMsg: "writer footer key must be 16, 24, or 32 bytes",
		},
		{
			name: "missing-column-key-path",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  splitEncryptionFooterKey,
				WriterColumnKeys: []string{"missing=" + splitEncryptionColumnKey},
			},
			errMsg: "writer column key path [missing] not found in schema",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			cmd := Cmd{
				ReadOption:   pio.ReadOption{},
				WriteOption:  tc.writeOption,
				ReadPageSize: 1000,
				RecordCount:  2,
				NameFormat:   filepath.Join(tempDir, "part-%d.parquet"),
				URI:          source,
			}
			err := cmd.Run()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
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
