package typeconv

import (
	"fmt"
	"strings"
)

// SourceTypeInfo represents the source field's type information
type SourceTypeInfo struct {
	Primitive    string // INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96
	PrimitiveLen int    // For FIXED_LEN_BYTE_ARRAY only
	Logical      string // NONE, DECIMAL, TIMESTAMP_NANOS, etc.
	Precision    int    // For DECIMAL
	Scale        int    // For DECIMAL
}

// ValidateConversion checks if a conversion from source to target type is allowed
func ValidateConversion(source SourceTypeInfo, target *TypeSpec) error {
	// Build conversion key
	srcKey := normalizeTypeKey(source.Primitive, source.Logical)
	tgtKey := normalizeTypeKey(target.Primitive, target.Logical)

	// Check if conversion is allowed
	if !isConversionAllowed(srcKey, tgtKey) {
		return fmt.Errorf("conversion from %s to %s is not supported", srcKey, tgtKey)
	}

	// Additional validation for specific conversions
	return validateSpecificConversion(source, target)
}

// normalizeTypeKey creates a normalized key for type lookup
func normalizeTypeKey(primitive, logical string) string {
	if logical == "" || logical == "NONE" {
		return primitive
	}
	// For DECIMAL, just use DECIMAL (precision/scale handled separately)
	if logical == "DECIMAL" {
		return primitive + ":DECIMAL"
	}
	// For TIMESTAMP variants, use the specific variant
	if strings.HasPrefix(logical, "TIMESTAMP_") {
		return "INT64:TIMESTAMP"
	}
	return primitive + ":" + logical
}

// Allowed conversions matrix
// Key format: "SOURCE -> TARGET" where SOURCE and TARGET are normalized type keys
var allowedConversions = map[string]bool{
	// Integer widening/narrowing
	"INT32 -> INT64": true,
	"INT64 -> INT32": true,

	// Float widening/narrowing
	"FLOAT -> DOUBLE": true,
	"DOUBLE -> FLOAT": true,

	// Int <-> Float conversions
	"INT32 -> FLOAT":  true,
	"INT32 -> DOUBLE": true,
	"INT64 -> FLOAT":  true,
	"INT64 -> DOUBLE": true,
	"FLOAT -> INT32":  true,
	"FLOAT -> INT64":  true,
	"DOUBLE -> INT32": true,
	"DOUBLE -> INT64": true,

	// INT96 to TIMESTAMP
	"INT96 -> INT64:TIMESTAMP": true,

	// BYTE_ARRAY <-> FIXED_LEN_BYTE_ARRAY
	"BYTE_ARRAY -> FIXED_LEN_BYTE_ARRAY":           true,
	"FIXED_LEN_BYTE_ARRAY -> BYTE_ARRAY":           true,
	"BYTE_ARRAY -> BYTE_ARRAY":                     true, // For adding/removing STRING
	"FIXED_LEN_BYTE_ARRAY -> FIXED_LEN_BYTE_ARRAY": true,

	// BYTE_ARRAY:STRING conversions
	"BYTE_ARRAY -> BYTE_ARRAY:STRING":           true,
	"BYTE_ARRAY:STRING -> BYTE_ARRAY":           true,
	"BYTE_ARRAY:STRING -> BYTE_ARRAY:STRING":    true,
	"BYTE_ARRAY:STRING -> FIXED_LEN_BYTE_ARRAY": true,

	// Decimal primitive type conversions
	"INT32:DECIMAL -> INT32:DECIMAL":                true,
	"INT32:DECIMAL -> INT64:DECIMAL":                true,
	"INT32:DECIMAL -> BYTE_ARRAY:DECIMAL":           true,
	"INT32:DECIMAL -> FIXED_LEN_BYTE_ARRAY:DECIMAL": true,

	"INT64:DECIMAL -> INT32:DECIMAL":                true,
	"INT64:DECIMAL -> INT64:DECIMAL":                true,
	"INT64:DECIMAL -> BYTE_ARRAY:DECIMAL":           true,
	"INT64:DECIMAL -> FIXED_LEN_BYTE_ARRAY:DECIMAL": true,

	"BYTE_ARRAY:DECIMAL -> INT32:DECIMAL":                true,
	"BYTE_ARRAY:DECIMAL -> INT64:DECIMAL":                true,
	"BYTE_ARRAY:DECIMAL -> BYTE_ARRAY:DECIMAL":           true,
	"BYTE_ARRAY:DECIMAL -> FIXED_LEN_BYTE_ARRAY:DECIMAL": true,

	"FIXED_LEN_BYTE_ARRAY:DECIMAL -> INT32:DECIMAL":                true,
	"FIXED_LEN_BYTE_ARRAY:DECIMAL -> INT64:DECIMAL":                true,
	"FIXED_LEN_BYTE_ARRAY:DECIMAL -> BYTE_ARRAY:DECIMAL":           true,
	"FIXED_LEN_BYTE_ARRAY:DECIMAL -> FIXED_LEN_BYTE_ARRAY:DECIMAL": true,
}

func isConversionAllowed(srcKey, tgtKey string) bool {
	key := srcKey + " -> " + tgtKey
	return allowedConversions[key]
}

func validateSpecificConversion(source SourceTypeInfo, target *TypeSpec) error {
	// Validate DECIMAL scale increase doesn't cause overflow
	if source.Logical == "DECIMAL" && target.Logical == "DECIMAL" {
		// Calculate required digits after scale increase
		// If we increase scale, we multiply the stored value, which needs more digits
		scaleDiff := target.Scale - source.Scale
		if scaleDiff > 0 {
			// We need additional digits for the scale increase
			requiredPrecision := source.Precision + scaleDiff
			if requiredPrecision > target.Precision {
				return fmt.Errorf("scale increase from %d to %d requires precision %d, but target precision is %d",
					source.Scale, target.Scale, requiredPrecision, target.Precision)
			}
		}
	}

	return nil
}

// GetAllowedTargets returns a list of allowed target types for a given source type
func GetAllowedTargets(source SourceTypeInfo) []string {
	srcKey := normalizeTypeKey(source.Primitive, source.Logical)
	var targets []string

	for conversion := range allowedConversions {
		parts := strings.Split(conversion, " -> ")
		if len(parts) == 2 && parts[0] == srcKey {
			targets = append(targets, parts[1])
		}
	}

	return targets
}
