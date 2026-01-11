package typeconv

import (
	"fmt"
	"unicode/utf8"
)

// buildBinaryConverter creates a converter for BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY conversions
func buildBinaryConverter(source SourceTypeInfo, target *TypeSpec) (Converter, error) {
	// Determine conversion type
	srcIsFLBA := source.Primitive == "FIXED_LEN_BYTE_ARRAY"
	tgtIsFLBA := target.Primitive == "FIXED_LEN_BYTE_ARRAY"

	// BYTE_ARRAY -> FIXED_LEN_BYTE_ARRAY (may fail if value too long)
	if !srcIsFLBA && tgtIsFLBA {
		return byteArrayToFLBA(target.PrimitiveLen), nil
	}

	// FIXED_LEN_BYTE_ARRAY -> BYTE_ARRAY (always safe)
	if srcIsFLBA && !tgtIsFLBA {
		return flbaToByteArray, nil
	}

	// BYTE_ARRAY -> BYTE_ARRAY (for STRING annotation changes)
	if !srcIsFLBA && !tgtIsFLBA {
		// If target is STRING, validate UTF-8
		if target.Logical == "STRING" && source.Logical != "STRING" {
			return validateUTF8, nil
		}
		return passThrough, nil
	}

	// FIXED_LEN_BYTE_ARRAY -> FIXED_LEN_BYTE_ARRAY (length change)
	if srcIsFLBA && tgtIsFLBA {
		return flbaToFLBA(source.PrimitiveLen, target.PrimitiveLen), nil
	}

	return nil, fmt.Errorf("no binary converter for %s -> %s", source.Primitive, target.Primitive)
}

// passThrough returns the value unchanged (for adding/removing logical types)
func passThrough(value any) (any, error) {
	return value, nil
}

// validateUTF8 validates that the value is valid UTF-8 (for STRING conversion)
func validateUTF8(value any) (any, error) {
	bytes, err := toBytes(value)
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(bytes) {
		return nil, fmt.Errorf("byte array contains invalid UTF-8 and cannot be converted to STRING")
	}
	return value, nil
}

// byteArrayToFLBA converts BYTE_ARRAY to FIXED_LEN_BYTE_ARRAY
func byteArrayToFLBA(targetLen int) Converter {
	return func(value any) (any, error) {
		bytes, err := toBytes(value)
		if err != nil {
			return nil, err
		}

		if len(bytes) > targetLen {
			return nil, fmt.Errorf("byte array length %d exceeds FIXED_LEN_BYTE_ARRAY length %d", len(bytes), targetLen)
		}

		// Pad with zeros if needed (right-pad)
		if len(bytes) < targetLen {
			padded := make([]byte, targetLen)
			copy(padded, bytes)
			bytes = padded
		}

		return string(bytes), nil
	}
}

// flbaToByteArray converts FIXED_LEN_BYTE_ARRAY to BYTE_ARRAY
func flbaToByteArray(value any) (any, error) {
	bytes, err := toBytes(value)
	if err != nil {
		return nil, err
	}
	return string(bytes), nil
}

// flbaToFLBA converts between FIXED_LEN_BYTE_ARRAY of different lengths
func flbaToFLBA(_, targetLen int) Converter {
	return func(value any) (any, error) {
		bytes, err := toBytes(value)
		if err != nil {
			return nil, err
		}

		if len(bytes) > targetLen {
			// Check if the extra bytes are all zeros
			for i := targetLen; i < len(bytes); i++ {
				if bytes[i] != 0 {
					return nil, fmt.Errorf("FIXED_LEN_BYTE_ARRAY truncation would lose non-zero data")
				}
			}
			bytes = bytes[:targetLen]
		}

		// Pad with zeros if needed (right-pad)
		if len(bytes) < targetLen {
			padded := make([]byte, targetLen)
			copy(padded, bytes)
			bytes = padded
		}

		return string(bytes), nil
	}
}

// toBytes converts value to []byte
func toBytes(value any) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return nil, fmt.Errorf("expected string or []byte, got %T", value)
	}
}
