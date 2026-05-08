package meta

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"

	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
	pio "github.com/hangxie/parquet-tools/io"
)

const (
	encFooterKey = "MDEyMzQ1Njc4OTAxMjM0NQ=="
	encDoubleKey = "MTIzNDU2Nzg5MDEyMzQ1MA=="
	encFloatKey  = "MTIzNDU2Nzg5MDEyMzQ1MQ=="
	encAADPrefix = "dGVzdGVy"
	encWrongKey  = "d3Jvbmd3cm9uZ3dyb25nMQ=="
)

func TestSortingToString(t *testing.T) {
	testCases := map[string]struct {
		sortingColumns []*parquet.SortingColumn
		columnIndex    int
		expected       *string
	}{
		"nil-sorting-columns": {
			sortingColumns: nil,
			columnIndex:    0,
			expected:       nil,
		},
		"empty-sorting-columns": {
			sortingColumns: []*parquet.SortingColumn{},
			columnIndex:    0,
			expected:       nil,
		},
		"column-not-found": {
			sortingColumns: []*parquet.SortingColumn{
				{ColumnIdx: 1, Descending: false},
				{ColumnIdx: 2, Descending: true},
			},
			columnIndex: 0,
			expected:    nil,
		},
		"ascending-column-found": {
			sortingColumns: []*parquet.SortingColumn{
				{ColumnIdx: 0, Descending: false},
				{ColumnIdx: 1, Descending: true},
			},
			columnIndex: 0,
			expected:    new("ASC"),
		},
		"descending-column-found": {
			sortingColumns: []*parquet.SortingColumn{
				{ColumnIdx: 0, Descending: false},
				{ColumnIdx: 1, Descending: true},
			},
			columnIndex: 1,
			expected:    new("DESC"),
		},
		"multiple-columns-first-match": {
			sortingColumns: []*parquet.SortingColumn{
				{ColumnIdx: 2, Descending: true},
				{ColumnIdx: 1, Descending: false},
				{ColumnIdx: 2, Descending: false}, // Should not be reached due to first match
			},
			columnIndex: 2,
			expected:    new("DESC"),
		},
		"column-index-conversion": {
			sortingColumns: []*parquet.SortingColumn{
				{ColumnIdx: 42, Descending: false},
			},
			columnIndex: 42,
			expected:    new("ASC"),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := sortingToString(tc.sortingColumns, tc.columnIndex)

			if tc.expected == nil {
				require.Nil(t, result)
			} else {
				require.NotNil(t, result)
				require.Equal(t, *tc.expected, *result)
			}
		})
	}
}

func TestCmd(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    Cmd
		golden string
		errMsg string
	}{
		// error cases
		"non-existent":           {cmd: Cmd{ReadOption: rOpt, URI: "file/does/not/exist"}, errMsg: "no such file or directory"},
		"encrypted-no-key":       {cmd: Cmd{ReadOption: rOpt, URI: "../../testdata/encrypted-footer.parquet"}, errMsg: "decryption key required for footer"},
		"encrypted-wrong-key":    {cmd: Cmd{ReadOption: pio.ReadOption{FooterKey: encWrongKey}, URI: "../../testdata/encrypted-footer.parquet"}, errMsg: "decrypt"},
		"encrypted-missing-col":  {cmd: Cmd{ReadOption: pio.ReadOption{FooterKey: encFooterKey}, URI: "../../testdata/encrypted-columns.parquet"}, errMsg: "decryption key required for column"},
		"peek-key-not-encrypted": {cmd: Cmd{ReadOption: rOpt, ShowKeyMetadata: true, URI: "../../testdata/good.parquet"}, errMsg: "file is not encrypted"},
		// --show-key-metadata flag: show key_metadata hints so users can retrieve the right key from KMS
		"enc-no-key-footer":     {cmd: Cmd{ReadOption: rOpt, ShowKeyMetadata: true, URI: "encrypted-footer.parquet"}, golden: "meta-enc-no-key-footer-raw.json"},
		"enc-no-key-columns":    {cmd: Cmd{ReadOption: rOpt, ShowKeyMetadata: true, URI: "encrypted-columns.parquet"}, golden: "meta-enc-no-key-columns-raw.json"},
		"enc-no-key-uniform":    {cmd: Cmd{ReadOption: rOpt, ShowKeyMetadata: true, URI: "uniform-encryption.parquet"}, golden: "meta-enc-no-key-uniform-raw.json"},
		"enc-no-key-aad":        {cmd: Cmd{ReadOption: rOpt, ShowKeyMetadata: true, URI: "encrypted-aad.parquet"}, golden: "meta-enc-no-key-aad-raw.json"},
		"encrypted-aad-missing": {cmd: Cmd{ReadOption: pio.ReadOption{FooterKey: encFooterKey, ColumnKeys: []string{"double_field=" + encDoubleKey, "float_field=" + encFloatKey}}, URI: "../../testdata/encrypted-aad.parquet"}, errMsg: "AAD prefix"},
		"no-int96":              {cmd: Cmd{ReadOption: rOpt, FailOnInt96: true, URI: "../../testdata/all-types.parquet"}, errMsg: "type INT96 which is not supported"},
		"nan-json-error":        {cmd: Cmd{ReadOption: rOpt, URI: "../../testdata/nan.parquet"}, errMsg: "json: unsupported value: NaN"},
		"arrow-gh-41317":        {cmd: Cmd{ReadOption: rOpt, URI: "../../testdata/ARROW-GH-41317.parquet"}, errMsg: "schema node not found for column path"},
		// good cases - URI will be prefixed with "../../testdata/"
		"raw":          {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet"}, golden: "meta-good-raw.json"},
		"nil-stat":     {cmd: Cmd{ReadOption: rOpt, URI: "nil-statistics.parquet"}, golden: "meta-nil-statistics-raw.json"},
		"sorting-col":  {cmd: Cmd{ReadOption: rOpt, URI: "sorting-col.parquet"}, golden: "meta-sorting-col-raw.json"},
		"all-types":    {cmd: Cmd{ReadOption: rOpt, URI: "all-types.parquet"}, golden: "meta-all-types-raw.json"},
		"geospatial":   {cmd: Cmd{ReadOption: rOpt, URI: "geospatial.parquet"}, golden: "meta-geospatial-raw.json"},
		"row-group":    {cmd: Cmd{ReadOption: rOpt, URI: "row-group.parquet"}, golden: "meta-row-group-raw.json"},
		"bloom-filter": {cmd: Cmd{ReadOption: rOpt, URI: "bloom-filter.parquet"}, golden: "meta-bloom-filter-raw.json"},
		// encrypted cases: column-level keys with KeyMetadata, uniform footer-key encryption,
		// and footer with both column-level keys and file-level FooterKeyMetadata
		"enc-columns": {
			cmd: Cmd{
				ReadOption: pio.ReadOption{
					FooterKey:  encFooterKey,
					ColumnKeys: []string{"double_field=" + encDoubleKey, "float_field=" + encFloatKey},
				},
				URI: "encrypted-columns.parquet",
			},
			golden: "meta-enc-columns-raw.json",
		},
		"enc-uniform": {
			cmd:    Cmd{ReadOption: pio.ReadOption{FooterKey: encFooterKey}, URI: "uniform-encryption.parquet"},
			golden: "meta-enc-uniform-raw.json",
		},
		"enc-footer": {
			cmd: Cmd{
				ReadOption: pio.ReadOption{
					FooterKey:  encFooterKey,
					ColumnKeys: []string{"double_field=" + encDoubleKey, "float_field=" + encFloatKey},
				},
				URI: "encrypted-footer.parquet",
			},
			golden: "meta-enc-footer-raw.json",
		},
		"enc-aad": {
			cmd: Cmd{
				ReadOption: pio.ReadOption{
					FooterKey:  encFooterKey,
					ColumnKeys: []string{"double_field=" + encDoubleKey, "float_field=" + encFloatKey},
					AADPrefix:  encAADPrefix,
				},
				URI: "encrypted-aad.parquet",
			},
			golden: "meta-enc-aad-raw.json",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if tc.errMsg == "" {
				t.Parallel()
			}
			cmd := tc.cmd
			if tc.golden != "" {
				cmd.URI = "../../testdata/" + tc.cmd.URI
			}
			if tc.errMsg != "" {
				err := cmd.Run()
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				stdout, stderr := testutils.CaptureStdoutStderr(func() {
					require.NoError(t, cmd.Run())
				})
				require.Equal(t, testutils.LoadExpected(t, "../../testdata/golden/"+tc.golden), stdout)
				require.Equal(t, "", stderr)
			}
		})
	}
}

func BenchmarkMetaCmd(b *testing.B) {
	savedStdout, savedStderr := os.Stdout, os.Stderr
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0o666)
	if err != nil {
		b.Fatal(err)
	}
	os.Stdout = devNull
	defer func() {
		os.Stdout, os.Stderr = savedStdout, savedStderr
		_ = devNull.Close()
	}()

	cmd := Cmd{
		ReadOption: pio.ReadOption{},
		URI:        "../../build/benchmark.parquet",
	}

	// Warm up the Go runtime before actual benchmark
	for range 10 {
		_ = cmd.Run()
	}

	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}
