package schema

import (
	"slices"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// EncodingToString converts a slice of parquet encodings to sorted strings.
func EncodingToString(encodings []parquet.Encoding) []string {
	ret := make([]string, len(encodings))
	for i := range encodings {
		ret[i] = encodings[i].String()
	}
	slices.Sort(ret)
	return ret
}

// encodingCompatibilityMap maps Parquet data types to their compatible encodings
// (excluding PLAIN, which is universally compatible).
//
// Per Parquet spec and parquet-go validation rules:
//   - RLE: BOOLEAN, INT32, INT64 only
//   - DELTA_BINARY_PACKED: INT32, INT64 only
//   - DELTA_BYTE_ARRAY, DELTA_LENGTH_BYTE_ARRAY: BYTE_ARRAY only
//   - BYTE_STREAM_SPLIT: FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY
//   - PLAIN_DICTIONARY: All types (v1 data pages only, validated separately)
var encodingCompatibilityMap = map[string][]string{
	"BOOLEAN":              {"BIT_PACKED", "PLAIN_DICTIONARY", "RLE", "RLE_DICTIONARY"},
	"BYTE_ARRAY":           {"DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
	"DOUBLE":               {"BYTE_STREAM_SPLIT", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
	"FIXED_LEN_BYTE_ARRAY": {"BYTE_STREAM_SPLIT", "DELTA_BYTE_ARRAY", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
	"FLOAT":                {"BYTE_STREAM_SPLIT", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
	"INT32":                {"BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
	"INT64":                {"BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
}

// GetAllowedEncodings returns all encodings compatible with the given Parquet data type.
func GetAllowedEncodings(dataType string) []string {
	dataType = strings.ToUpper(dataType)

	allowed, exists := encodingCompatibilityMap[dataType]
	if !exists {
		return []string{"PLAIN"}
	}

	return append([]string{"PLAIN"}, allowed...)
}

// IsEncodingCompatible reports whether the given encoding is compatible with the data type.
func IsEncodingCompatible(encoding, dataType string) bool {
	if dataType == "" {
		return false
	}

	encoding = strings.ToUpper(encoding)
	dataType = strings.ToUpper(dataType)

	if encoding == "PLAIN" {
		return true
	}

	compatibleEncodings, exists := encodingCompatibilityMap[dataType]
	if !exists {
		return false
	}

	return slices.Contains(compatibleEncodings, encoding)
}
