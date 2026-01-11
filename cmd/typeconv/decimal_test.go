package typeconv

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildDecimalConverter(t *testing.T) {
	tests := map[string]struct {
		source   SourceTypeInfo
		target   *TypeSpec
		input    any
		expected any
		errMsg   string
	}{
		"int32-to-int64-same-scale": {
			source:   SourceTypeInfo{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
			target:   &TypeSpec{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 2},
			input:    int32(12345),
			expected: int64(12345),
		},
		"int32-to-int64-scale-increase": {
			source:   SourceTypeInfo{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
			target:   &TypeSpec{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 4},
			input:    int32(12345),   // represents 123.45
			expected: int64(1234500), // represents 123.4500
		},
		"int64-to-int32-scale-decrease": {
			source:   SourceTypeInfo{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 4},
			target:   &TypeSpec{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
			input:    int64(1234500), // represents 123.4500
			expected: int32(12345),   // represents 123.45
		},
		"int32-to-byte-array": {
			source: SourceTypeInfo{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
			target: &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "DECIMAL", Precision: 20, Scale: 2},
			input:  int32(12345),
		},
		"int32-to-flba": {
			source: SourceTypeInfo{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
			target: &TypeSpec{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 16, Logical: "DECIMAL", Precision: 38, Scale: 2},
			input:  int32(12345),
		},
		"negative-int32-to-int64": {
			source:   SourceTypeInfo{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
			target:   &TypeSpec{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 2},
			input:    int32(-12345),
			expected: int64(-12345),
		},
		"precision-overflow": {
			source: SourceTypeInfo{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 0},
			target: &TypeSpec{Primitive: "INT32", Logical: "DECIMAL", Precision: 5, Scale: 0},
			input:  int64(999999), // exceeds 5 digit precision
			errMsg: "decimal value exceeds precision",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			conv, err := buildDecimalConverter(tc.source, tc.target)
			require.NoError(t, err)
			require.NotNil(t, conv)

			result, err := conv(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				if tc.expected != nil {
					require.Equal(t, tc.expected, result)
				} else {
					require.NotNil(t, result)
				}
			}
		})
	}
}

func TestToBigInt(t *testing.T) {
	tests := map[string]struct {
		value     any
		primitive string
		expected  *big.Int
		errMsg    string
	}{
		"int32-positive": {
			value:     int32(12345),
			primitive: "INT32",
			expected:  big.NewInt(12345),
		},
		"int32-negative": {
			value:     int32(-12345),
			primitive: "INT32",
			expected:  big.NewInt(-12345),
		},
		"int32-zero": {
			value:     int32(0),
			primitive: "INT32",
			expected:  big.NewInt(0),
		},
		"int64-positive": {
			value:     int64(123456789012345),
			primitive: "INT64",
			expected:  big.NewInt(123456789012345),
		},
		"int64-negative": {
			value:     int64(-123456789012345),
			primitive: "INT64",
			expected:  big.NewInt(-123456789012345),
		},
		"byte-array-positive": {
			value:     string([]byte{0x30, 0x39}), // 12345 in big-endian
			primitive: "BYTE_ARRAY",
			expected:  big.NewInt(12345),
		},
		"byte-array-zero": {
			value:     string([]byte{0x00}),
			primitive: "BYTE_ARRAY",
			expected:  big.NewInt(0),
		},
		"byte-array-negative": {
			value:     string([]byte{0xff, 0xcf, 0xc7}), // -12345 in two's complement
			primitive: "BYTE_ARRAY",
			expected:  big.NewInt(-12345),
		},
		"flba-positive": {
			value:     string([]byte{0x00, 0x00, 0x30, 0x39}),
			primitive: "FIXED_LEN_BYTE_ARRAY",
			expected:  big.NewInt(12345),
		},
		"empty-byte-array": {
			value:     string([]byte{}),
			primitive: "BYTE_ARRAY",
			expected:  big.NewInt(0),
		},
		"int32-wrong-type": {
			value:     "not an int",
			primitive: "INT32",
			errMsg:    "expected int32",
		},
		"int64-wrong-type": {
			value:     int32(123),
			primitive: "INT64",
			errMsg:    "expected int64",
		},
		"byte-array-wrong-type": {
			value:     123,
			primitive: "BYTE_ARRAY",
			errMsg:    "expected string or []byte",
		},
		"unsupported-primitive": {
			value:     true,
			primitive: "BOOLEAN",
			errMsg:    "unsupported primitive type",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := toBigInt(tc.value, tc.primitive)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, 0, tc.expected.Cmp(result), "expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestAdjustScale(t *testing.T) {
	tests := map[string]struct {
		unscaled  *big.Int
		scaleDiff int
		expected  *big.Int
	}{
		"no-change": {
			unscaled:  big.NewInt(12345),
			scaleDiff: 0,
			expected:  big.NewInt(12345),
		},
		"increase-by-2": {
			unscaled:  big.NewInt(12345),
			scaleDiff: 2,
			expected:  big.NewInt(1234500),
		},
		"decrease-by-2-exact": {
			unscaled:  big.NewInt(1234500),
			scaleDiff: -2,
			expected:  big.NewInt(12345),
		},
		"decrease-by-2-round-down": {
			unscaled:  big.NewInt(1234549),
			scaleDiff: -2,
			expected:  big.NewInt(12345),
		},
		"decrease-by-2-round-up": {
			unscaled:  big.NewInt(1234550),
			scaleDiff: -2,
			expected:  big.NewInt(12346),
		},
		"negative-exact": {
			unscaled:  big.NewInt(-1234500),
			scaleDiff: -2,
			expected:  big.NewInt(-12345),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := adjustScale(tc.unscaled, tc.scaleDiff)
			require.NoError(t, err)
			require.Equal(t, 0, tc.expected.Cmp(result), "expected %v, got %v", tc.expected, result)
		})
	}
}

func TestDivideRoundHalfUp(t *testing.T) {
	tests := map[string]struct {
		dividend *big.Int
		divisor  *big.Int
		expected *big.Int
	}{
		"exact-division": {
			dividend: big.NewInt(100),
			divisor:  big.NewInt(10),
			expected: big.NewInt(10),
		},
		"round-down": {
			dividend: big.NewInt(104),
			divisor:  big.NewInt(10),
			expected: big.NewInt(10),
		},
		"round-up": {
			dividend: big.NewInt(105),
			divisor:  big.NewInt(10),
			expected: big.NewInt(11),
		},
		"round-up-high": {
			dividend: big.NewInt(109),
			divisor:  big.NewInt(10),
			expected: big.NewInt(11),
		},
		"negative-exact": {
			dividend: big.NewInt(-100),
			divisor:  big.NewInt(10),
			expected: big.NewInt(-10),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := divideRoundHalfUp(tc.dividend, tc.divisor)
			require.Equal(t, 0, tc.expected.Cmp(result), "expected %v, got %v", tc.expected, result)
		})
	}
}

func TestCheckPrecision(t *testing.T) {
	tests := map[string]struct {
		unscaled  *big.Int
		precision int
		errMsg    string
	}{
		"within-precision": {
			unscaled:  big.NewInt(12345),
			precision: 5,
		},
		"exact-precision": {
			unscaled:  big.NewInt(99999),
			precision: 5,
		},
		"exceeds-precision": {
			unscaled:  big.NewInt(100000),
			precision: 5,
			errMsg:    "decimal value exceeds precision",
		},
		"negative-within": {
			unscaled:  big.NewInt(-12345),
			precision: 5,
		},
		"negative-exceeds": {
			unscaled:  big.NewInt(-100000),
			precision: 5,
			errMsg:    "decimal value exceeds precision",
		},
		"zero": {
			unscaled:  big.NewInt(0),
			precision: 5,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := checkPrecision(tc.unscaled, tc.precision)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFromBigInt(t *testing.T) {
	tests := map[string]struct {
		unscaled  *big.Int
		primitive string
		length    int
		expected  any
		errMsg    string
	}{
		"to-int32-positive": {
			unscaled:  big.NewInt(12345),
			primitive: "INT32",
			expected:  int32(12345),
		},
		"to-int32-negative": {
			unscaled:  big.NewInt(-12345),
			primitive: "INT32",
			expected:  int32(-12345),
		},
		"to-int64-positive": {
			unscaled:  big.NewInt(123456789012345),
			primitive: "INT64",
			expected:  int64(123456789012345),
		},
		"to-int32-overflow": {
			unscaled:  big.NewInt(3000000000), // > max int32
			primitive: "INT32",
			errMsg:    "overflows int32",
		},
		"to-byte-array": {
			unscaled:  big.NewInt(12345),
			primitive: "BYTE_ARRAY",
		},
		"to-flba": {
			unscaled:  big.NewInt(12345),
			primitive: "FIXED_LEN_BYTE_ARRAY",
			length:    8,
		},
		"unsupported-primitive": {
			unscaled:  big.NewInt(12345),
			primitive: "BOOLEAN",
			errMsg:    "unsupported primitive type",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := fromBigInt(tc.unscaled, tc.primitive, tc.length)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				if tc.expected != nil {
					require.Equal(t, tc.expected, result)
				} else {
					require.NotNil(t, result)
				}
			}
		})
	}
}

func TestBigIntToBytes(t *testing.T) {
	tests := map[string]struct {
		n         *big.Int
		minLength int
		expected  []byte
	}{
		"zero": {
			n:         big.NewInt(0),
			minLength: 0,
			expected:  []byte{0},
		},
		"zero-with-min-length": {
			n:         big.NewInt(0),
			minLength: 4,
			expected:  []byte{0, 0, 0, 0},
		},
		"positive-small": {
			n:         big.NewInt(127),
			minLength: 0,
			expected:  []byte{0x7f},
		},
		"positive-needs-sign-byte": {
			n:         big.NewInt(128),
			minLength: 0,
			expected:  []byte{0x00, 0x80},
		},
		"positive-with-padding": {
			n:         big.NewInt(127),
			minLength: 4,
			expected:  []byte{0, 0, 0, 0x7f},
		},
		"negative-small": {
			n:         big.NewInt(-1),
			minLength: 0,
			expected:  []byte{0xff},
		},
		"negative-larger": {
			n:         big.NewInt(-128),
			minLength: 0,
			expected:  []byte{0xff, 0x80}, // -128 needs sign extension in two's complement
		},
		"negative-with-padding": {
			n:         big.NewInt(-1),
			minLength: 4,
			expected:  []byte{0xff, 0xff, 0xff, 0xff},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := bigIntToBytes(tc.n, tc.minLength)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestAbs(t *testing.T) {
	tests := map[string]struct {
		input    int
		expected int
	}{
		"positive": {input: 5, expected: 5},
		"negative": {input: -5, expected: 5},
		"zero":     {input: 0, expected: 0},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := abs(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}
