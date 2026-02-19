package meta

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"

	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
	pio "github.com/hangxie/parquet-tools/io"
)

func TestRetrieveValue(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		testCases := map[string]struct {
			pType  parquet.Type
			errMsg string
		}{
			"int32":   {parquet.Type_INT32, "failed to read data as INT32"},
			"int64":   {parquet.Type_INT64, "failed to read data as INT64"},
			"float":   {parquet.Type_FLOAT, "failed to read data as FLOAT"},
			"double":  {parquet.Type_DOUBLE, "failed to read data as DOUBLE"},
			"boolean": {parquet.Type_BOOLEAN, "failed to read data as BOOLEAN"},
		}
		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				msg := retrieveValue([]byte{}, tc.pType)
				require.Contains(t, msg, tc.errMsg)
			})
		}
	})

	t.Run("numeric", func(t *testing.T) {
		testCases := map[string]struct {
			pType  parquet.Type
			value  []byte
			expect any
		}{
			"nil-boolean":       {parquet.Type_BOOLEAN, nil, nil},
			"nil-int32":         {parquet.Type_INT32, nil, nil},
			"nil-int64":         {parquet.Type_INT64, nil, nil},
			"nil-float":         {parquet.Type_FLOAT, nil, nil},
			"nil-double":        {parquet.Type_DOUBLE, nil, nil},
			"nil-bytearr":       {parquet.Type_BYTE_ARRAY, nil, nil},
			"nil-fixed-bytearr": {parquet.Type_BYTE_ARRAY, nil, nil},
			"boolean-true":      {parquet.Type_BOOLEAN, []byte{1}, true},
			"boolean-false":     {parquet.Type_BOOLEAN, []byte{0}, false},
			"int32=9":           {parquet.Type_INT32, []byte{9, 0, 0, 0}, int32(9)},
			"int32=-5":          {parquet.Type_INT32, []byte{251, 255, 255, 255}, int32(-5)},
			"int64=9":           {parquet.Type_INT64, []byte{9, 0, 0, 0, 0, 0, 0, 0}, int64(9)},
			"int64=-5":          {parquet.Type_INT64, []byte{251, 255, 255, 255, 255, 255, 255, 255}, int64(-5)},
			"float=-2.5":        {parquet.Type_FLOAT, []byte{0, 0, 32, 192}, float32(-2.5)},
			"float=2":           {parquet.Type_FLOAT, []byte{0, 0, 0, 64}, float32(2)},
			"double=-2.5":       {parquet.Type_DOUBLE, []byte{0, 0, 0, 0, 0, 0, 4, 192}, float64(-2.5)},
			"double=2":          {parquet.Type_DOUBLE, []byte{0, 0, 0, 0, 0, 0, 0, 64}, float64(2)},
		}

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				result := retrieveValue(tc.value, tc.pType)
				require.Equal(t, tc.expect, result)
			})
		}
	})

	t.Run("byte-array", func(t *testing.T) {
		testCases := map[string]struct {
			pType  parquet.Type
			value  []byte
			expect any
		}{
			"nil-byte-array":       {parquet.Type_BYTE_ARRAY, nil, nil},
			"nil-fixed-byte-array": {parquet.Type_BYTE_ARRAY, nil, nil},
			"byte-array":           {parquet.Type_BYTE_ARRAY, []byte("ab"), "ab"},
			"fixed-byte-array":     {parquet.Type_FIXED_LEN_BYTE_ARRAY, []byte("ab"), "ab"},
		}

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				result := retrieveValue(tc.value, tc.pType)
				require.Equal(t, tc.expect, result)
			})
			t.Run(name+"-base64", func(t *testing.T) {
				result := retrieveValue(tc.value, tc.pType)
				require.Equal(t, tc.expect, result)
			})
		}
	})
}

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
		"non-existent":   {cmd: Cmd{ReadOption: rOpt, URI: "file/does/not/exist"}, errMsg: "no such file or directory"},
		"no-int96":       {cmd: Cmd{ReadOption: rOpt, FailOnInt96: true, URI: "../../testdata/all-types.parquet"}, errMsg: "type INT96 which is not supported"},
		"nan-json-error": {cmd: Cmd{ReadOption: rOpt, URI: "../../testdata/nan.parquet"}, errMsg: "json: unsupported value: NaN"},
		"arrow-gh-41317": {cmd: Cmd{ReadOption: rOpt, URI: "../../testdata/ARROW-GH-41317.parquet"}, errMsg: "schema node not found for column path"},
		// good cases - URI will be prefixed with "../../testdata/"
		"raw":          {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet"}, golden: "meta-good-raw.json"},
		"nil-stat":     {cmd: Cmd{ReadOption: rOpt, URI: "nil-statistics.parquet"}, golden: "meta-nil-statistics-raw.json"},
		"sorting-col":  {cmd: Cmd{ReadOption: rOpt, URI: "sorting-col.parquet"}, golden: "meta-sorting-col-raw.json"},
		"all-types":    {cmd: Cmd{ReadOption: rOpt, URI: "all-types.parquet"}, golden: "meta-all-types-raw.json"},
		"geospatial":   {cmd: Cmd{ReadOption: rOpt, URI: "geospatial.parquet"}, golden: "meta-geospatial-raw.json"},
		"row-group":    {cmd: Cmd{ReadOption: rOpt, URI: "row-group.parquet"}, golden: "meta-row-group-raw.json"},
		"bloom-filter": {cmd: Cmd{ReadOption: rOpt, URI: "bloom-filter.parquet"}, golden: "meta-bloom-filter-raw.json"},
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
