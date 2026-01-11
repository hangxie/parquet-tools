package typeconv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateConversion(t *testing.T) {
	tests := map[string]struct {
		source SourceTypeInfo
		target *TypeSpec
		errMsg string
	}{
		"int32-to-int64": {
			source: SourceTypeInfo{Primitive: "INT32", Logical: "NONE"},
			target: &TypeSpec{Primitive: "INT64", Logical: "NONE"},
		},
		"int64-to-int32": {
			source: SourceTypeInfo{Primitive: "INT64", Logical: "NONE"},
			target: &TypeSpec{Primitive: "INT32", Logical: "NONE"},
		},
		"float-to-double": {
			source: SourceTypeInfo{Primitive: "FLOAT", Logical: "NONE"},
			target: &TypeSpec{Primitive: "DOUBLE", Logical: "NONE"},
		},
		"double-to-float": {
			source: SourceTypeInfo{Primitive: "DOUBLE", Logical: "NONE"},
			target: &TypeSpec{Primitive: "FLOAT", Logical: "NONE"},
		},
		"int32-to-float": {
			source: SourceTypeInfo{Primitive: "INT32", Logical: "NONE"},
			target: &TypeSpec{Primitive: "FLOAT", Logical: "NONE"},
		},
		"int32-to-double": {
			source: SourceTypeInfo{Primitive: "INT32", Logical: "NONE"},
			target: &TypeSpec{Primitive: "DOUBLE", Logical: "NONE"},
		},
		"int64-to-float": {
			source: SourceTypeInfo{Primitive: "INT64", Logical: "NONE"},
			target: &TypeSpec{Primitive: "FLOAT", Logical: "NONE"},
		},
		"int64-to-double": {
			source: SourceTypeInfo{Primitive: "INT64", Logical: "NONE"},
			target: &TypeSpec{Primitive: "DOUBLE", Logical: "NONE"},
		},
		"float-to-int32": {
			source: SourceTypeInfo{Primitive: "FLOAT", Logical: "NONE"},
			target: &TypeSpec{Primitive: "INT32", Logical: "NONE"},
		},
		"float-to-int64": {
			source: SourceTypeInfo{Primitive: "FLOAT", Logical: "NONE"},
			target: &TypeSpec{Primitive: "INT64", Logical: "NONE"},
		},
		"double-to-int32": {
			source: SourceTypeInfo{Primitive: "DOUBLE", Logical: "NONE"},
			target: &TypeSpec{Primitive: "INT32", Logical: "NONE"},
		},
		"double-to-int64": {
			source: SourceTypeInfo{Primitive: "DOUBLE", Logical: "NONE"},
			target: &TypeSpec{Primitive: "INT64", Logical: "NONE"},
		},
		"int96-to-timestamp-nanos": {
			source: SourceTypeInfo{Primitive: "INT96", Logical: "NONE"},
			target: &TypeSpec{Primitive: "INT64", Logical: "TIMESTAMP_NANOS"},
		},
		"int96-to-timestamp-micros": {
			source: SourceTypeInfo{Primitive: "INT96", Logical: "NONE"},
			target: &TypeSpec{Primitive: "INT64", Logical: "TIMESTAMP_MICROS"},
		},
		"int96-to-timestamp-millis": {
			source: SourceTypeInfo{Primitive: "INT96", Logical: "NONE"},
			target: &TypeSpec{Primitive: "INT64", Logical: "TIMESTAMP_MILLIS"},
		},
		"byte-array-to-flba": {
			source: SourceTypeInfo{Primitive: "BYTE_ARRAY", Logical: "NONE"},
			target: &TypeSpec{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 10, Logical: "NONE"},
		},
		"flba-to-byte-array": {
			source: SourceTypeInfo{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 10, Logical: "NONE"},
			target: &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "NONE"},
		},
		"byte-array-to-string": {
			source: SourceTypeInfo{Primitive: "BYTE_ARRAY", Logical: "NONE"},
			target: &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "STRING"},
		},
		"string-to-byte-array": {
			source: SourceTypeInfo{Primitive: "BYTE_ARRAY", Logical: "STRING"},
			target: &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "NONE"},
		},
		"string-to-flba": {
			source: SourceTypeInfo{Primitive: "BYTE_ARRAY", Logical: "STRING"},
			target: &TypeSpec{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 10, Logical: "NONE"},
		},
		"decimal-int32-to-int64": {
			source: SourceTypeInfo{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
			target: &TypeSpec{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 2},
		},
		"decimal-int64-to-flba": {
			source: SourceTypeInfo{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 2},
			target: &TypeSpec{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 16, Logical: "DECIMAL", Precision: 38, Scale: 2},
		},
		"decimal-flba-to-byte-array": {
			source: SourceTypeInfo{Primitive: "FIXED_LEN_BYTE_ARRAY", PrimitiveLen: 16, Logical: "DECIMAL", Precision: 38, Scale: 2},
			target: &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "DECIMAL", Precision: 38, Scale: 2},
		},
		"invalid-string-to-int": {
			source: SourceTypeInfo{Primitive: "BYTE_ARRAY", Logical: "STRING"},
			target: &TypeSpec{Primitive: "INT64", Logical: "NONE"},
			errMsg: "is not supported",
		},
		"invalid-int-to-string": {
			source: SourceTypeInfo{Primitive: "INT32", Logical: "NONE"},
			target: &TypeSpec{Primitive: "BYTE_ARRAY", Logical: "STRING"},
			errMsg: "is not supported",
		},
		"invalid-boolean-to-int": {
			source: SourceTypeInfo{Primitive: "BOOLEAN", Logical: "NONE"},
			target: &TypeSpec{Primitive: "INT32", Logical: "NONE"},
			errMsg: "is not supported",
		},
		"decimal-scale-increase-overflow": {
			source: SourceTypeInfo{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
			target: &TypeSpec{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 6},
			errMsg: "scale increase",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateConversion(tc.source, tc.target)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNormalizeTypeKey(t *testing.T) {
	tests := map[string]struct {
		primitive string
		logical   string
		expected  string
	}{
		"int32-none":             {primitive: "INT32", logical: "NONE", expected: "INT32"},
		"int32-empty":            {primitive: "INT32", logical: "", expected: "INT32"},
		"int64-decimal":          {primitive: "INT64", logical: "DECIMAL", expected: "INT64:DECIMAL"},
		"byte-array-string":      {primitive: "BYTE_ARRAY", logical: "STRING", expected: "BYTE_ARRAY:STRING"},
		"int64-timestamp-nanos":  {primitive: "INT64", logical: "TIMESTAMP_NANOS", expected: "INT64:TIMESTAMP"},
		"int64-timestamp-micros": {primitive: "INT64", logical: "TIMESTAMP_MICROS", expected: "INT64:TIMESTAMP"},
		"int64-timestamp-millis": {primitive: "INT64", logical: "TIMESTAMP_MILLIS", expected: "INT64:TIMESTAMP"},
		"flba-decimal":           {primitive: "FIXED_LEN_BYTE_ARRAY", logical: "DECIMAL", expected: "FIXED_LEN_BYTE_ARRAY:DECIMAL"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := normalizeTypeKey(tc.primitive, tc.logical)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestIsConversionAllowed(t *testing.T) {
	tests := map[string]struct {
		srcKey   string
		tgtKey   string
		expected bool
	}{
		"int32-to-int64":         {srcKey: "INT32", tgtKey: "INT64", expected: true},
		"int64-to-int32":         {srcKey: "INT64", tgtKey: "INT32", expected: true},
		"float-to-double":        {srcKey: "FLOAT", tgtKey: "DOUBLE", expected: true},
		"int96-to-timestamp":     {srcKey: "INT96", tgtKey: "INT64:TIMESTAMP", expected: true},
		"string-to-int":          {srcKey: "BYTE_ARRAY:STRING", tgtKey: "INT64", expected: false},
		"int-to-string":          {srcKey: "INT32", tgtKey: "BYTE_ARRAY:STRING", expected: false},
		"decimal-int32-to-int64": {srcKey: "INT32:DECIMAL", tgtKey: "INT64:DECIMAL", expected: true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := isConversionAllowed(tc.srcKey, tc.tgtKey)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestValidateSpecificConversion(t *testing.T) {
	tests := map[string]struct {
		source SourceTypeInfo
		target *TypeSpec
		errMsg string
	}{
		"decimal-same-scale": {
			source: SourceTypeInfo{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
			target: &TypeSpec{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 2},
		},
		"decimal-scale-increase-ok": {
			source: SourceTypeInfo{Primitive: "INT32", Logical: "DECIMAL", Precision: 5, Scale: 2},
			target: &TypeSpec{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 4},
		},
		"decimal-scale-increase-overflow": {
			source: SourceTypeInfo{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
			target: &TypeSpec{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 6},
			errMsg: "scale increase from 2 to 6 requires precision 13",
		},
		"decimal-scale-decrease": {
			source: SourceTypeInfo{Primitive: "INT64", Logical: "DECIMAL", Precision: 18, Scale: 6},
			target: &TypeSpec{Primitive: "INT32", Logical: "DECIMAL", Precision: 9, Scale: 2},
		},
		"non-decimal": {
			source: SourceTypeInfo{Primitive: "INT32", Logical: "NONE"},
			target: &TypeSpec{Primitive: "INT64", Logical: "NONE"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateSpecificConversion(tc.source, tc.target)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetAllowedTargets(t *testing.T) {
	tests := map[string]struct {
		source          SourceTypeInfo
		expectedTargets []string
	}{
		"int32": {
			source:          SourceTypeInfo{Primitive: "INT32", Logical: "NONE"},
			expectedTargets: []string{"INT64", "FLOAT", "DOUBLE"},
		},
		"int96": {
			source:          SourceTypeInfo{Primitive: "INT96", Logical: "NONE"},
			expectedTargets: []string{"INT64:TIMESTAMP"},
		},
		"byte-array": {
			source:          SourceTypeInfo{Primitive: "BYTE_ARRAY", Logical: "NONE"},
			expectedTargets: []string{"FIXED_LEN_BYTE_ARRAY", "BYTE_ARRAY", "BYTE_ARRAY:STRING"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := GetAllowedTargets(tc.source)
			// Check that all expected targets are in the result
			for _, expected := range tc.expectedTargets {
				found := false
				for _, r := range result {
					if r == expected {
						found = true
						break
					}
				}
				require.True(t, found, "expected target %s not found in result %v", expected, result)
			}
		})
	}
}
