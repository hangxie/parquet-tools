package typeconv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildConverter(t *testing.T) {
	tests := map[string]struct {
		source   SourceTypeInfo
		target   *TypeSpec
		input    any
		expected any
		errMsg   string
	}{
		"int32-to-int64": {
			source:   SourceTypeInfo{Primitive: "INT32", Logical: "NONE"},
			target:   &TypeSpec{Primitive: "INT64", Logical: "NONE"},
			input:    int32(42),
			expected: int64(42),
		},
		"int64-to-int32": {
			source:   SourceTypeInfo{Primitive: "INT64", Logical: "NONE"},
			target:   &TypeSpec{Primitive: "INT32", Logical: "NONE"},
			input:    int64(42),
			expected: int32(42),
		},
		"float-to-double": {
			source:   SourceTypeInfo{Primitive: "FLOAT", Logical: "NONE"},
			target:   &TypeSpec{Primitive: "DOUBLE", Logical: "NONE"},
			input:    float32(3.14),
			expected: float64(float32(3.14)),
		},
		"byte-array-to-string": {
			source:   SourceTypeInfo{Primitive: "BYTE_ARRAY", Logical: "NONE"},
			target:   &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "STRING"},
			input:    "hello",
			expected: "hello",
		},
		"null-passthrough": {
			source:   SourceTypeInfo{Primitive: "INT32", Logical: "NONE"},
			target:   &TypeSpec{Primitive: "INT64", Logical: "NONE"},
			input:    nil,
			expected: nil,
		},
		"invalid-conversion": {
			source: SourceTypeInfo{Primitive: "BYTE_ARRAY", Logical: "STRING"},
			target: &TypeSpec{Primitive: "INT64", Logical: "NONE"},
			errMsg: "is not supported",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			conv, err := BuildConverter(tc.source, tc.target)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, conv)

			result, err := conv(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildConverterInt96(t *testing.T) {
	source := SourceTypeInfo{Primitive: "INT96", Logical: "NONE"}

	tests := map[string]struct {
		target *TypeSpec
		errMsg string
	}{
		"to-timestamp-nanos": {
			target: &TypeSpec{Primitive: "INT64", Logical: "TIMESTAMP_NANOS"},
		},
		"to-timestamp-micros": {
			target: &TypeSpec{Primitive: "INT64", Logical: "TIMESTAMP_MICROS"},
		},
		"to-timestamp-millis": {
			target: &TypeSpec{Primitive: "INT64", Logical: "TIMESTAMP_MILLIS"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			conv, err := BuildConverter(source, tc.target)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, conv)
		})
	}
}

func TestBuildConverterDecimal(t *testing.T) {
	tests := map[string]struct {
		source   SourceTypeInfo
		target   *TypeSpec
		input    any
		expected any
		errMsg   string
	}{
		"int32-decimal-to-int64-decimal": {
			source:   SourceTypeInfo{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
			target:   &TypeSpec{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 2},
			input:    int32(12345),
			expected: int64(12345),
		},
		"decimal-scale-increase": {
			source:   SourceTypeInfo{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
			target:   &TypeSpec{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 4},
			input:    int32(12345),   // 123.45
			expected: int64(1234500), // 123.4500
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			conv, err := BuildConverter(tc.source, tc.target)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, conv)

			result, err := conv(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestIsNumericType(t *testing.T) {
	tests := map[string]struct {
		primitive string
		expected  bool
	}{
		"int32":      {primitive: "INT32", expected: true},
		"int64":      {primitive: "INT64", expected: true},
		"float":      {primitive: "FLOAT", expected: true},
		"double":     {primitive: "DOUBLE", expected: true},
		"byte-array": {primitive: "BYTE_ARRAY", expected: false},
		"fixed-len":  {primitive: "FIXED_LEN_BYTE_ARRAY", expected: false},
		"int96":      {primitive: "INT96", expected: false},
		"boolean":    {primitive: "BOOLEAN", expected: false},
		"unknown":    {primitive: "UNKNOWN", expected: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := isNumericType(tc.primitive)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestIsBinaryType(t *testing.T) {
	tests := map[string]struct {
		primitive string
		expected  bool
	}{
		"byte-array": {primitive: "BYTE_ARRAY", expected: true},
		"fixed-len":  {primitive: "FIXED_LEN_BYTE_ARRAY", expected: true},
		"int32":      {primitive: "INT32", expected: false},
		"int64":      {primitive: "INT64", expected: false},
		"float":      {primitive: "FLOAT", expected: false},
		"double":     {primitive: "DOUBLE", expected: false},
		"int96":      {primitive: "INT96", expected: false},
		"boolean":    {primitive: "BOOLEAN", expected: false},
		"unknown":    {primitive: "UNKNOWN", expected: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := isBinaryType(tc.primitive)
			require.Equal(t, tc.expected, result)
		})
	}
}
