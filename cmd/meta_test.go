package cmd

import (
	"os"
	"testing"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

func Test_retrieveValue_error(t *testing.T) {
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
	c := &MetaCmd{}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			msg := c.retrieveValue([]byte{}, tc.pType)
			require.Equal(t, tc.errMsg, msg)
		})
	}
}

func Test_retrieveValue_numeric(t *testing.T) {
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
			c := &MetaCmd{}
			result := c.retrieveValue(tc.value, tc.pType)
			require.Equal(t, tc.expect, result)
		})
	}
}

func Test_retrieveValue_byte_array(t *testing.T) {
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
		c := &MetaCmd{}
		t.Run(name, func(t *testing.T) {
			result := c.retrieveValue(tc.value, tc.pType)
			require.Equal(t, tc.expect, result)
		})
		t.Run(name+"-base64", func(t *testing.T) {
			result := c.retrieveValue(tc.value, tc.pType)
			require.Equal(t, tc.expect, result)
		})
	}
}

func Test_sortingToString(t *testing.T) {
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
			expected:    common.ToPtr("ASC"),
		},
		"descending-column-found": {
			sortingColumns: []*parquet.SortingColumn{
				{ColumnIdx: 0, Descending: false},
				{ColumnIdx: 1, Descending: true},
			},
			columnIndex: 1,
			expected:    common.ToPtr("DESC"),
		},
		"multiple-columns-first-match": {
			sortingColumns: []*parquet.SortingColumn{
				{ColumnIdx: 2, Descending: true},
				{ColumnIdx: 1, Descending: false},
				{ColumnIdx: 2, Descending: false}, // Should not be reached due to first match
			},
			columnIndex: 2,
			expected:    common.ToPtr("DESC"),
		},
		"column-index-conversion": {
			sortingColumns: []*parquet.SortingColumn{
				{ColumnIdx: 42, Descending: false},
			},
			columnIndex: 42,
			expected:    common.ToPtr("ASC"),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := sortingToString(tc.sortingColumns, tc.columnIndex)

			if tc.expected == nil {
				require.Nil(t, result, "Expected nil result for %s", name)
			} else {
				require.NotNil(t, result, "Expected non-nil result for %s", name)
				require.Equal(t, *tc.expected, *result, "Expected %s but got %s", *tc.expected, *result)
			}
		})
	}
}

func Test_MetaCmd_Run_error(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    MetaCmd
		errMsg string
	}{
		"non-existent":   {MetaCmd{ReadOption: rOpt, Base64: false, FailOnInt96: false, URI: "file/does/not/exist"}, "no such file or directory"},
		"no-int96":       {MetaCmd{ReadOption: rOpt, Base64: false, FailOnInt96: true, URI: "../testdata/all-types.parquet"}, "type INT96 which is not supported"},
		"nan-json-error": {MetaCmd{ReadOption: rOpt, Base64: false, FailOnInt96: false, URI: "../testdata/nan.parquet"}, "json: unsupported value: NaN"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func Test_MetaCmd_Run_good(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    MetaCmd
		golden string
	}{
		"raw":         {MetaCmd{ReadOption: rOpt, Base64: false, FailOnInt96: false, URI: "good.parquet"}, "meta-good-raw.json"},
		"nil-stat":    {MetaCmd{ReadOption: rOpt, Base64: false, FailOnInt96: false, URI: "nil-statistics.parquet"}, "meta-nil-statistics-raw.json"},
		"sorting-col": {MetaCmd{ReadOption: rOpt, Base64: false, FailOnInt96: false, URI: "sorting-col.parquet"}, "meta-sorting-col-raw.json"},
		"all-types":   {MetaCmd{ReadOption: rOpt, Base64: false, FailOnInt96: false, URI: "all-types.parquet"}, "meta-all-types-raw.json"},
		"geospatial":  {MetaCmd{ReadOption: rOpt, Base64: false, FailOnInt96: false, URI: "geospatial.parquet"}, "meta-geospatial-raw.json"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tc.cmd.URI = "../testdata/" + tc.cmd.URI
			tc.golden = "../testdata/golden/" + tc.golden
			stdout, stderr := captureStdoutStderr(func() {
				require.Nil(t, tc.cmd.Run())
			})
			expected := loadExpected(t, tc.golden)
			require.Equal(t, expected, stdout)
			require.Equal(t, "", stderr)
		})
	}
}

func Benchmark_MetaCmd_Run(b *testing.B) {
	savedStdout, savedStderr := os.Stdout, os.Stderr
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0o666)
	if err != nil {
		panic(err)
	}
	os.Stdout = devNull
	defer func() {
		os.Stdout, os.Stderr = savedStdout, savedStderr
		_ = devNull.Close()
	}()

	cmd := MetaCmd{
		ReadOption: pio.ReadOption{},
		URI:        "../build/benchmark.parquet",
	}
	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}
