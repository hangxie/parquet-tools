package typeconv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildBinaryConverter(t *testing.T) {
	tests := map[string]struct {
		source   SourceTypeInfo
		target   *TypeSpec
		hasError bool
	}{
		"byte-array-to-flba": {
			source:   SourceTypeInfo{Primitive: "BYTE_ARRAY"},
			target:   &TypeSpec{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 10},
			hasError: false,
		},
		"flba-to-byte-array": {
			source:   SourceTypeInfo{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 10},
			target:   &TypeSpec{Primitive: "BYTE_ARRAY"},
			hasError: false,
		},
		"byte-array-to-byte-array": {
			source:   SourceTypeInfo{Primitive: "BYTE_ARRAY"},
			target:   &TypeSpec{Primitive: "BYTE_ARRAY"},
			hasError: false,
		},
		"byte-array-to-string": {
			source:   SourceTypeInfo{Primitive: "BYTE_ARRAY", Logical: "NONE"},
			target:   &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "STRING"},
			hasError: false,
		},
		"string-to-byte-array": {
			source:   SourceTypeInfo{Primitive: "BYTE_ARRAY", Logical: "STRING"},
			target:   &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "NONE"},
			hasError: false,
		},
		"flba-to-flba": {
			source:   SourceTypeInfo{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 10},
			target:   &TypeSpec{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 16},
			hasError: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			conv, err := buildBinaryConverter(tc.source, tc.target)
			if tc.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, conv)
			}
		})
	}
}

func TestBuildBinaryConverterUTF8Validation(t *testing.T) {
	// Test that BYTE_ARRAY to STRING conversion validates UTF-8
	source := SourceTypeInfo{Primitive: "BYTE_ARRAY", Logical: "NONE"}
	target := &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "STRING"}

	conv, err := buildBinaryConverter(source, target)
	require.NoError(t, err)
	require.NotNil(t, conv)

	// Valid UTF-8 should pass
	result, err := conv("hello world")
	require.NoError(t, err)
	require.Equal(t, "hello world", result)

	// Invalid UTF-8 should fail
	_, err = conv(string([]byte{0xff, 0xfe}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid UTF-8")

	// STRING to BYTE_ARRAY (removing annotation) should not validate
	source2 := SourceTypeInfo{Primitive: "BYTE_ARRAY", Logical: "STRING"}
	target2 := &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "NONE"}

	conv2, err := buildBinaryConverter(source2, target2)
	require.NoError(t, err)
	require.NotNil(t, conv2)

	// Should pass through without validation (already assumed valid from STRING source)
	result2, err := conv2("hello")
	require.NoError(t, err)
	require.Equal(t, "hello", result2)
}

func TestPassThrough(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected any
	}{
		"string":  {input: "hello", expected: "hello"},
		"bytes":   {input: []byte{1, 2, 3}, expected: []byte{1, 2, 3}},
		"nil":     {input: nil, expected: nil},
		"int":     {input: 42, expected: 42},
		"float64": {input: 3.14, expected: 3.14},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := passThrough(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestValidateUTF8(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected any
		errMsg   string
	}{
		"valid-ascii": {
			input:    "hello",
			expected: "hello",
		},
		"valid-utf8": {
			input:    "hello 世界",
			expected: "hello 世界",
		},
		"valid-utf8-bytes": {
			input:    []byte("hello 世界"),
			expected: []byte("hello 世界"),
		},
		"empty": {
			input:    "",
			expected: "",
		},
		"invalid-utf8": {
			input:  string([]byte{0xff, 0xfe, 0x00}),
			errMsg: "invalid UTF-8",
		},
		"invalid-utf8-bytes": {
			input:  []byte{0x80, 0x81, 0x82},
			errMsg: "invalid UTF-8",
		},
		"truncated-utf8": {
			input:  string([]byte{0xe4, 0xb8}), // incomplete 世
			errMsg: "invalid UTF-8",
		},
		"wrong-type": {
			input:  123,
			errMsg: "expected string or []byte",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := validateUTF8(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestByteArrayToFLBA(t *testing.T) {
	tests := map[string]struct {
		input     any
		targetLen int
		expected  string
		errMsg    string
	}{
		"exact-length-string": {
			input:     "hello",
			targetLen: 5,
			expected:  "hello",
		},
		"exact-length-bytes": {
			input:     []byte("hello"),
			targetLen: 5,
			expected:  "hello",
		},
		"needs-padding": {
			input:     "hi",
			targetLen: 5,
			expected:  "hi\x00\x00\x00",
		},
		"too-long": {
			input:     "hello world",
			targetLen: 5,
			errMsg:    "exceeds FIXED_LEN_BYTE_ARRAY length",
		},
		"wrong-type": {
			input:     123,
			targetLen: 5,
			errMsg:    "expected string or []byte",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			conv := byteArrayToFLBA(tc.targetLen)
			result, err := conv(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestFlbaToByteArray(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected string
		errMsg   string
	}{
		"string": {
			input:    "hello",
			expected: "hello",
		},
		"bytes": {
			input:    []byte("world"),
			expected: "world",
		},
		"with-nulls": {
			input:    "hi\x00\x00\x00",
			expected: "hi\x00\x00\x00",
		},
		"wrong-type": {
			input:  123,
			errMsg: "expected string or []byte",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := flbaToByteArray(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestFlbaToFLBA(t *testing.T) {
	tests := map[string]struct {
		input     any
		sourceLen int
		targetLen int
		expected  string
		errMsg    string
	}{
		"same-length": {
			input:     "hello",
			sourceLen: 5,
			targetLen: 5,
			expected:  "hello",
		},
		"expand": {
			input:     "hi",
			sourceLen: 2,
			targetLen: 5,
			expected:  "hi\x00\x00\x00",
		},
		"shrink-zeros-only": {
			input:     "hi\x00\x00\x00",
			sourceLen: 5,
			targetLen: 2,
			expected:  "hi",
		},
		"shrink-loses-data": {
			input:     "hello",
			sourceLen: 5,
			targetLen: 2,
			errMsg:    "truncation would lose non-zero data",
		},
		"wrong-type": {
			input:     123,
			sourceLen: 5,
			targetLen: 5,
			errMsg:    "expected string or []byte",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			conv := flbaToFLBA(tc.sourceLen, tc.targetLen)
			result, err := conv(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestToBytes(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected []byte
		errMsg   string
	}{
		"string": {
			input:    "hello",
			expected: []byte("hello"),
		},
		"bytes": {
			input:    []byte{1, 2, 3},
			expected: []byte{1, 2, 3},
		},
		"wrong-type-int": {
			input:  123,
			errMsg: "expected string or []byte",
		},
		"wrong-type-float": {
			input:  3.14,
			errMsg: "expected string or []byte",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := toBytes(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}
