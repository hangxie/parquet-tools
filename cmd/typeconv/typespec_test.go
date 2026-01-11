package typeconv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTypeSpec(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected *TypeSpec
		errMsg   string
	}{
		// Valid cases
		"int64-none": {
			input:    "INT64:NONE",
			expected: &TypeSpec{Primitive: "INT64", Logical: "NONE"},
		},
		"int32-none": {
			input:    "INT32:NONE",
			expected: &TypeSpec{Primitive: "INT32", Logical: "NONE"},
		},
		"float-none": {
			input:    "FLOAT:NONE",
			expected: &TypeSpec{Primitive: "FLOAT", Logical: "NONE"},
		},
		"double-none": {
			input:    "DOUBLE:NONE",
			expected: &TypeSpec{Primitive: "DOUBLE", Logical: "NONE"},
		},
		"byte-array-none": {
			input:    "BYTE_ARRAY:NONE",
			expected: &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "NONE"},
		},
		"byte-array-string": {
			input:    "BYTE_ARRAY:STRING",
			expected: &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "STRING"},
		},
		"int64-timestamp-nanos": {
			input:    "INT64:TIMESTAMP_NANOS",
			expected: &TypeSpec{Primitive: "INT64", Logical: "TIMESTAMP_NANOS"},
		},
		"int64-timestamp-micros": {
			input:    "INT64:TIMESTAMP_MICROS",
			expected: &TypeSpec{Primitive: "INT64", Logical: "TIMESTAMP_MICROS"},
		},
		"int64-timestamp-millis": {
			input:    "INT64:TIMESTAMP_MILLIS",
			expected: &TypeSpec{Primitive: "INT64", Logical: "TIMESTAMP_MILLIS"},
		},
		"int32-decimal": {
			input:    "INT32:DECIMAL(9,2)",
			expected: &TypeSpec{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
		},
		"int64-decimal": {
			input:    "INT64:DECIMAL(18,4)",
			expected: &TypeSpec{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 4},
		},
		"flba-decimal": {
			input:    "FIXED_LEN_BYTE_ARRAY(16):DECIMAL(38,10)",
			expected: &TypeSpec{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 16, Logical: "DECIMAL", Precision: 38, Scale: 10},
		},
		"byte-array-decimal": {
			input:    "BYTE_ARRAY:DECIMAL(50,20)",
			expected: &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "DECIMAL", Precision: 50, Scale: 20},
		},
		"int32-date": {
			input:    "INT32:DATE",
			expected: &TypeSpec{Primitive: "INT32", Logical: "DATE"},
		},
		"lowercase-input": {
			input:    "int64:timestamp_nanos",
			expected: &TypeSpec{Primitive: "INT64", Logical: "TIMESTAMP_NANOS"},
		},
		"mixed-case-input": {
			input:    "Int64:Decimal(18,2)",
			expected: &TypeSpec{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 2},
		},
		"flba-none": {
			input:    "FIXED_LEN_BYTE_ARRAY(8):NONE",
			expected: &TypeSpec{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 8, Logical: "NONE"},
		},

		// Invalid cases - format errors
		"empty-string": {
			input:  "",
			errMsg: "empty type specification",
		},
		"missing-colon": {
			input:  "INT64",
			errMsg: "must be PRIMITIVE:LOGICAL format",
		},
		"missing-primitive": {
			input:  ":NONE",
			errMsg: "missing primitive type",
		},
		"missing-logical": {
			input:  "INT64:",
			errMsg: "missing logical type",
		},

		// Invalid cases - unknown types
		"unknown-primitive": {
			input:  "INT128:NONE",
			errMsg: "unknown primitive type",
		},
		"unknown-logical": {
			input:  "INT64:UNKNOWN",
			errMsg: "unknown logical type",
		},

		// Invalid cases - FIXED_LEN_BYTE_ARRAY
		"flba-missing-length": {
			input:  "FIXED_LEN_BYTE_ARRAY:NONE",
			errMsg: "FIXED_LEN_BYTE_ARRAY requires length",
		},
		"flba-zero-length": {
			input:  "FIXED_LEN_BYTE_ARRAY(0):NONE",
			errMsg: "must be positive",
		},
		"flba-negative-length": {
			input:  "FIXED_LEN_BYTE_ARRAY(-1):NONE",
			errMsg: "unknown primitive type",
		},

		// Invalid cases - DECIMAL
		"decimal-missing-params": {
			input:  "INT64:DECIMAL",
			errMsg: "DECIMAL requires precision and scale",
		},
		"decimal-scale-exceeds-precision": {
			input:  "INT64:DECIMAL(5,10)",
			errMsg: "scale (10) cannot exceed precision (5)",
		},
		"decimal-negative-scale": {
			input:  "INT64:DECIMAL(10,-1)",
			errMsg: "unknown logical type",
		},
		"decimal-zero-precision": {
			input:  "INT64:DECIMAL(0,0)",
			errMsg: "precision must be positive",
		},
		"decimal-precision-exceeds-int32": {
			input:  "INT32:DECIMAL(10,2)",
			errMsg: "precision 10 exceeds maximum 9",
		},
		"decimal-precision-exceeds-int64": {
			input:  "INT64:DECIMAL(19,2)",
			errMsg: "precision 19 exceeds maximum 18",
		},

		// Invalid cases - type combinations
		"timestamp-wrong-primitive": {
			input:  "INT32:TIMESTAMP_NANOS",
			errMsg: "TIMESTAMP_NANOS requires INT64",
		},
		"string-wrong-primitive": {
			input:  "INT64:STRING",
			errMsg: "STRING requires BYTE_ARRAY",
		},
		"date-wrong-primitive": {
			input:  "INT64:DATE",
			errMsg: "DATE requires INT32",
		},
		"decimal-wrong-primitive": {
			input:  "FLOAT:DECIMAL(10,2)",
			errMsg: "DECIMAL requires INT32, INT64, BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := ParseTypeSpec(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, tc.expected.Primitive, result.Primitive)
				require.Equal(t, tc.expected.PrimitiveLen, result.PrimitiveLen)
				require.Equal(t, tc.expected.Logical, result.Logical)
				require.Equal(t, tc.expected.Precision, result.Precision)
				require.Equal(t, tc.expected.Scale, result.Scale)
			}
		})
	}
}

func TestTypeSpecString(t *testing.T) {
	tests := map[string]struct {
		ts       TypeSpec
		expected string
	}{
		"int64-none": {
			ts:       TypeSpec{Primitive: "INT64", Logical: "NONE"},
			expected: "INT64:NONE",
		},
		"int64-timestamp": {
			ts:       TypeSpec{Primitive: "INT64", Logical: "TIMESTAMP_NANOS"},
			expected: "INT64:TIMESTAMP_NANOS",
		},
		"int64-decimal": {
			ts:       TypeSpec{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 2},
			expected: "INT64:DECIMAL(18,2)",
		},
		"flba-decimal": {
			ts:       TypeSpec{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 16, Logical: "DECIMAL", Precision: 38, Scale: 10},
			expected: "FIXED_LEN_BYTE_ARRAY(16):DECIMAL(38,10)",
		},
		"byte-array-string": {
			ts:       TypeSpec{Primitive: "BYTE_ARRAY", Logical: "STRING"},
			expected: "BYTE_ARRAY:STRING",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := tc.ts.String()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestMaxDecimalPrecision(t *testing.T) {
	tests := map[string]struct {
		ts       TypeSpec
		expected int
	}{
		"int32": {
			ts:       TypeSpec{Primitive: "INT32"},
			expected: 9,
		},
		"int64": {
			ts:       TypeSpec{Primitive: "INT64"},
			expected: 18,
		},
		"flba-4": {
			ts:       TypeSpec{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 4},
			expected: 9, // (4*8-1)*0.30103 = 9.33
		},
		"flba-8": {
			ts:       TypeSpec{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 8},
			expected: 18, // (8*8-1)*0.30103 = 18.96
		},
		"flba-16": {
			ts:       TypeSpec{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 16},
			expected: 38, // (16*8-1)*0.30103 = 38.22
		},
		"byte-array": {
			ts:       TypeSpec{Primitive: "BYTE_ARRAY"},
			expected: 1000000,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := tc.ts.maxDecimalPrecision()
			require.Equal(t, tc.expected, result)
		})
	}
}
