package typeconv

import (
	"fmt"
)

// Converter is a function that converts a value from one type to another
type Converter func(value any) (any, error)

// BuildConverter creates a converter function for the given source and target types
func BuildConverter(source SourceTypeInfo, target *TypeSpec) (Converter, error) {
	// Validate conversion is allowed
	if err := ValidateConversion(source, target); err != nil {
		return nil, err
	}

	// Build the appropriate converter
	return buildConverterInternal(source, target)
}

func buildConverterInternal(source SourceTypeInfo, target *TypeSpec) (Converter, error) {
	// Handle null passthrough in all converters
	wrapWithNullCheck := func(conv Converter) Converter {
		return func(value any) (any, error) {
			if value == nil {
				return nil, nil
			}
			return conv(value)
		}
	}

	var conv Converter
	var err error

	// INT96 to TIMESTAMP
	if source.Primitive == "INT96" && target.Primitive == "INT64" {
		conv, err = buildInt96ToTimestampConverter(target.Logical)
		if err != nil {
			return nil, err
		}
		return wrapWithNullCheck(conv), nil
	}

	// Numeric conversions (INT32, INT64, FLOAT, DOUBLE)
	if isNumericType(source.Primitive) && isNumericType(target.Primitive) {
		// Check if it's a decimal conversion
		if source.Logical == "DECIMAL" || target.Logical == "DECIMAL" {
			conv, err = buildDecimalConverter(source, target)
		} else {
			conv, err = buildNumericConverter(source.Primitive, target.Primitive)
		}
		if err != nil {
			return nil, err
		}
		return wrapWithNullCheck(conv), nil
	}

	// Binary conversions (BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY)
	if isBinaryType(source.Primitive) && isBinaryType(target.Primitive) {
		// Check if it's a decimal conversion
		if source.Logical == "DECIMAL" || target.Logical == "DECIMAL" {
			conv, err = buildDecimalConverter(source, target)
		} else {
			conv, err = buildBinaryConverter(source, target)
		}
		if err != nil {
			return nil, err
		}
		return wrapWithNullCheck(conv), nil
	}

	// Mixed numeric/binary decimal conversions
	if source.Logical == "DECIMAL" || target.Logical == "DECIMAL" {
		conv, err = buildDecimalConverter(source, target)
		if err != nil {
			return nil, err
		}
		return wrapWithNullCheck(conv), nil
	}

	return nil, fmt.Errorf("no converter available for %s:%s -> %s:%s",
		source.Primitive, source.Logical, target.Primitive, target.Logical)
}

func isNumericType(primitive string) bool {
	switch primitive {
	case "INT32", "INT64", "FLOAT", "DOUBLE":
		return true
	default:
		return false
	}
}

func isBinaryType(primitive string) bool {
	switch primitive {
	case "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY":
		return true
	default:
		return false
	}
}
