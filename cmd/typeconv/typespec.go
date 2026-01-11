package typeconv

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// TypeSpec represents a parsed type specification from --field-type option
type TypeSpec struct {
	Primitive    string // INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96
	PrimitiveLen int    // For FIXED_LEN_BYTE_ARRAY only
	Logical      string // NONE, DECIMAL, TIMESTAMP_NANOS, TIMESTAMP_MICROS, TIMESTAMP_MILLIS, STRING, etc.
	Precision    int    // For DECIMAL
	Scale        int    // For DECIMAL
}

// Valid primitive types
var validPrimitiveTypes = map[string]bool{
	"BOOLEAN":              true,
	"INT32":                true,
	"INT64":                true,
	"INT96":                true,
	"FLOAT":                true,
	"DOUBLE":               true,
	"BYTE_ARRAY":           true,
	"FIXED_LEN_BYTE_ARRAY": true,
}

// Valid logical types
var validLogicalTypes = map[string]bool{
	"NONE":             true,
	"STRING":           true,
	"DECIMAL":          true,
	"TIMESTAMP_NANOS":  true,
	"TIMESTAMP_MICROS": true,
	"TIMESTAMP_MILLIS": true,
	"DATE":             true,
	"TIME_MILLIS":      true,
	"TIME_MICROS":      true,
	"TIME_NANOS":       true,
	"UUID":             true,
	"ENUM":             true,
	"JSON":             true,
	"BSON":             true,
}

// Regex patterns for parsing
var (
	// Matches: FIXED_LEN_BYTE_ARRAY(16)
	flbaPattern = regexp.MustCompile(`^FIXED_LEN_BYTE_ARRAY\((\d+)\)$`)
	// Matches: DECIMAL(18,2)
	decimalPattern = regexp.MustCompile(`^DECIMAL\((\d+),(\d+)\)$`)
)

// ParseTypeSpec parses a type specification string into a TypeSpec struct.
// Format: PRIMITIVE[(params)]:LOGICAL[(params)]
// Examples:
//   - INT64:NONE
//   - INT64:TIMESTAMP_NANOS
//   - INT64:DECIMAL(18,2)
//   - FIXED_LEN_BYTE_ARRAY(16):DECIMAL(38,10)
//   - BYTE_ARRAY:STRING
func ParseTypeSpec(spec string) (*TypeSpec, error) {
	if spec == "" {
		return nil, fmt.Errorf("empty type specification")
	}

	// Split on : to separate primitive and logical
	parts := strings.SplitN(spec, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid type specification [%s]: must be PRIMITIVE:LOGICAL format", spec)
	}

	primitivePart := strings.TrimSpace(parts[0])
	logicalPart := strings.TrimSpace(parts[1])

	if primitivePart == "" {
		return nil, fmt.Errorf("invalid type specification [%s]: missing primitive type", spec)
	}
	if logicalPart == "" {
		return nil, fmt.Errorf("invalid type specification [%s]: missing logical type", spec)
	}

	ts := &TypeSpec{}

	// Parse primitive type
	if err := ts.parsePrimitive(primitivePart); err != nil {
		return nil, fmt.Errorf("invalid type specification [%s]: %w", spec, err)
	}

	// Parse logical type
	if err := ts.parseLogical(logicalPart); err != nil {
		return nil, fmt.Errorf("invalid type specification [%s]: %w", spec, err)
	}

	// Validate combination
	if err := ts.validate(); err != nil {
		return nil, fmt.Errorf("invalid type specification [%s]: %w", spec, err)
	}

	return ts, nil
}

func (ts *TypeSpec) parsePrimitive(part string) error {
	// Check for FIXED_LEN_BYTE_ARRAY(n)
	if matches := flbaPattern.FindStringSubmatch(part); matches != nil {
		length, err := strconv.Atoi(matches[1])
		if err != nil {
			return fmt.Errorf("invalid FIXED_LEN_BYTE_ARRAY length: %w", err)
		}
		if length <= 0 {
			return fmt.Errorf("FIXED_LEN_BYTE_ARRAY length must be positive, got %d", length)
		}
		ts.Primitive = "FIXED_LEN_BYTE_ARRAY"
		ts.PrimitiveLen = length
		return nil
	}

	// Check for simple primitive type
	upperPart := strings.ToUpper(part)
	if !validPrimitiveTypes[upperPart] {
		return fmt.Errorf("unknown primitive type [%s]", part)
	}

	// FIXED_LEN_BYTE_ARRAY requires length
	if upperPart == "FIXED_LEN_BYTE_ARRAY" {
		return fmt.Errorf("FIXED_LEN_BYTE_ARRAY requires length, use FIXED_LEN_BYTE_ARRAY(n)")
	}

	ts.Primitive = upperPart
	return nil
}

func (ts *TypeSpec) parseLogical(part string) error {
	// Normalize to uppercase for matching
	upperPart := strings.ToUpper(part)

	// Check for DECIMAL(precision, scale)
	if matches := decimalPattern.FindStringSubmatch(upperPart); matches != nil {
		precision, err := strconv.Atoi(matches[1])
		if err != nil {
			return fmt.Errorf("invalid DECIMAL precision: %w", err)
		}
		scale, err := strconv.Atoi(matches[2])
		if err != nil {
			return fmt.Errorf("invalid DECIMAL scale: %w", err)
		}
		ts.Logical = "DECIMAL"
		ts.Precision = precision
		ts.Scale = scale
		return nil
	}

	// Check for simple logical type
	if !validLogicalTypes[upperPart] {
		return fmt.Errorf("unknown logical type [%s]", part)
	}

	// DECIMAL requires parameters
	if upperPart == "DECIMAL" {
		return fmt.Errorf("DECIMAL requires precision and scale, use DECIMAL(p,s)")
	}

	ts.Logical = upperPart
	return nil
}

func (ts *TypeSpec) validate() error {
	// Validate TIMESTAMP requires INT64
	if strings.HasPrefix(ts.Logical, "TIMESTAMP_") && ts.Primitive != "INT64" {
		return fmt.Errorf("%s requires INT64 primitive type, got %s", ts.Logical, ts.Primitive)
	}

	// Validate STRING requires BYTE_ARRAY
	if ts.Logical == "STRING" && ts.Primitive != "BYTE_ARRAY" {
		return fmt.Errorf("STRING requires BYTE_ARRAY primitive type, got %s", ts.Primitive)
	}

	// Validate DATE requires INT32
	if ts.Logical == "DATE" && ts.Primitive != "INT32" {
		return fmt.Errorf("DATE requires INT32 primitive type, got %s", ts.Primitive)
	}

	// Validate DECIMAL
	if ts.Logical == "DECIMAL" {
		// First check primitive type is valid for DECIMAL
		validDecimalPrimitive := map[string]bool{
			"INT32":                true,
			"INT64":                true,
			"BYTE_ARRAY":           true,
			"FIXED_LEN_BYTE_ARRAY": true,
		}
		if !validDecimalPrimitive[ts.Primitive] {
			return fmt.Errorf("DECIMAL requires INT32, INT64, BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY primitive type, got %s", ts.Primitive)
		}

		// Then validate precision/scale
		if ts.Precision <= 0 {
			return fmt.Errorf("DECIMAL precision must be positive")
		}
		if ts.Scale < 0 {
			return fmt.Errorf("DECIMAL scale must be non-negative")
		}
		if ts.Scale > ts.Precision {
			return fmt.Errorf("DECIMAL scale (%d) cannot exceed precision (%d)", ts.Scale, ts.Precision)
		}

		// Validate precision fits in primitive type
		maxPrecision := ts.maxDecimalPrecision()
		if ts.Precision > maxPrecision {
			return fmt.Errorf("DECIMAL precision %d exceeds maximum %d for %s", ts.Precision, maxPrecision, ts.Primitive)
		}
	}

	return nil
}

// maxDecimalPrecision returns the maximum decimal precision for the primitive type
func (ts *TypeSpec) maxDecimalPrecision() int {
	switch ts.Primitive {
	case "INT32":
		return 9
	case "INT64":
		return 18
	case "FIXED_LEN_BYTE_ARRAY":
		// floor((n * 8 - 1) * log10(2)) ≈ floor(n * 2.408)
		// Simplified: floor((n * 8 - 1) / 3.32) ≈ n * 2.4
		if ts.PrimitiveLen <= 0 {
			return 0
		}
		// More precise calculation: floor(log10(2^(8*n-1)))
		// For practical purposes: n bytes can hold 2^(8n) values
		// With sign bit: 2^(8n-1) max value
		// Decimal digits: floor((8*n - 1) * log10(2)) = floor((8*n - 1) * 0.30103)
		bits := ts.PrimitiveLen*8 - 1
		return int(float64(bits) * 0.30103)
	case "BYTE_ARRAY":
		return 1000000 // Effectively unlimited
	default:
		return 0
	}
}

// String returns a string representation of the TypeSpec
func (ts *TypeSpec) String() string {
	var primitive string
	if ts.Primitive == "FIXED_LEN_BYTE_ARRAY" {
		primitive = fmt.Sprintf("FIXED_LEN_BYTE_ARRAY(%d)", ts.PrimitiveLen)
	} else {
		primitive = ts.Primitive
	}

	var logical string
	if ts.Logical == "DECIMAL" {
		logical = fmt.Sprintf("DECIMAL(%d,%d)", ts.Precision, ts.Scale)
	} else {
		logical = ts.Logical
	}

	return primitive + ":" + logical
}
