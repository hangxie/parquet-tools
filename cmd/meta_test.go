package cmd

import (
	"encoding/base64"
	"testing"

	"github.com/hangxie/parquet-go/parquet"
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
			msg := c.retrieveValue([]byte{}, tc.pType, false)
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
			result := c.retrieveValue(tc.value, tc.pType, false)
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
			result := c.retrieveValue(tc.value, tc.pType, false)
			require.Equal(t, tc.expect, result)
		})
		b64 := tc.expect
		if b64 != nil {
			b64 = base64.StdEncoding.EncodeToString([]byte(tc.expect.(string)))
		}
		t.Run(name+"-base64", func(t *testing.T) {
			result := c.retrieveValue(tc.value, tc.pType, true)
			require.Equal(t, b64, result)
		})
	}
}

func Test_MetaCmd_Run_error(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    MetaCmd
		errMsg string
	}{
		"non-existent": {MetaCmd{rOpt, false, "file/does/not/exist", false, ""}, "no such file or directory"},
		"no-int96":     {MetaCmd{rOpt, false, "../testdata/all-types.parquet", true, ""}, "type INT96 which is not supported"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.NotNil(t, err)
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
		"base64":       {MetaCmd{rOpt, true, "good.parquet", false, ""}, "meta-good-base64.json"},
		"raw":          {MetaCmd{rOpt, false, "good.parquet", false, ""}, "meta-good-raw.json"},
		"nil-stat":     {MetaCmd{rOpt, false, "nil-statistics.parquet", false, ""}, "meta-nil-statistics-raw.json"},
		"sorting-col":  {MetaCmd{rOpt, true, "sorting-col.parquet", false, ""}, "meta-sorting-col-base64.json"},
		"RI-scalar":    {MetaCmd{rOpt, false, "reinterpret-scalar.parquet", false, ""}, "meta-reinterpret-scalar-raw.json"},
		"RI-pointer":   {MetaCmd{rOpt, false, "reinterpret-pointer.parquet", false, ""}, "meta-reinterpret-pointer-raw.json"},
		"RI-list":      {MetaCmd{rOpt, false, "reinterpret-list.parquet", false, ""}, "meta-reinterpret-list-raw.json"},
		"RI-map-key":   {MetaCmd{rOpt, false, "reinterpret-map-key.parquet", false, ""}, "meta-reinterpret-map-key-raw.json"},
		"RI-map-value": {MetaCmd{rOpt, false, "reinterpret-map-value.parquet", false, ""}, "meta-reinterpret-map-value-raw.json"},
		"RI-composite": {MetaCmd{rOpt, false, "reinterpret-composite.parquet", false, ""}, "meta-reinterpret-composite-raw.json"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
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
