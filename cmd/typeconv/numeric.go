package typeconv

import (
	"fmt"
	"math"
)

// buildNumericConverter creates a converter for numeric types (INT32, INT64, FLOAT, DOUBLE)
func buildNumericConverter(srcPrimitive, tgtPrimitive string) (Converter, error) {
	key := srcPrimitive + "->" + tgtPrimitive

	converters := map[string]Converter{
		// Integer widening (always safe)
		"INT32->INT64": int32ToInt64,

		// Integer narrowing (overflow check)
		"INT64->INT32": int64ToInt32,

		// Float widening (always safe)
		"FLOAT->DOUBLE": floatToDouble,

		// Float narrowing (overflow check)
		"DOUBLE->FLOAT": doubleToFloat,

		// Int to Float
		"INT32->FLOAT":  int32ToFloat,
		"INT32->DOUBLE": int32ToDouble,
		"INT64->FLOAT":  int64ToFloat,
		"INT64->DOUBLE": int64ToDouble,

		// Float to Int (truncate toward zero, overflow/NaN/Inf check)
		"FLOAT->INT32":  floatToInt32,
		"FLOAT->INT64":  floatToInt64,
		"DOUBLE->INT32": doubleToInt32,
		"DOUBLE->INT64": doubleToInt64,
	}

	conv, ok := converters[key]
	if !ok {
		return nil, fmt.Errorf("no numeric converter for %s -> %s", srcPrimitive, tgtPrimitive)
	}

	return conv, nil
}

// Integer widening
func int32ToInt64(value any) (any, error) {
	v, ok := value.(int32)
	if !ok {
		return nil, fmt.Errorf("expected int32, got %T", value)
	}
	return int64(v), nil
}

// Integer narrowing with overflow check
func int64ToInt32(value any) (any, error) {
	v, ok := value.(int64)
	if !ok {
		return nil, fmt.Errorf("expected int64, got %T", value)
	}
	if v < math.MinInt32 || v > math.MaxInt32 {
		return nil, fmt.Errorf("int64 value %d overflows int32 range", v)
	}
	return int32(v), nil
}

// Float widening
func floatToDouble(value any) (any, error) {
	v, ok := value.(float32)
	if !ok {
		return nil, fmt.Errorf("expected float32, got %T", value)
	}
	return float64(v), nil
}

// Float narrowing with overflow check
func doubleToFloat(value any) (any, error) {
	v, ok := value.(float64)
	if !ok {
		return nil, fmt.Errorf("expected float64, got %T", value)
	}
	// Check for overflow (value outside float32 range but not infinity)
	if !math.IsInf(v, 0) && !math.IsNaN(v) {
		if v > math.MaxFloat32 || v < -math.MaxFloat32 {
			return nil, fmt.Errorf("float64 value %v overflows float32 range", v)
		}
	}
	return float32(v), nil
}

// Int to Float conversions
func int32ToFloat(value any) (any, error) {
	v, ok := value.(int32)
	if !ok {
		return nil, fmt.Errorf("expected int32, got %T", value)
	}
	return float32(v), nil
}

func int32ToDouble(value any) (any, error) {
	v, ok := value.(int32)
	if !ok {
		return nil, fmt.Errorf("expected int32, got %T", value)
	}
	return float64(v), nil
}

func int64ToFloat(value any) (any, error) {
	v, ok := value.(int64)
	if !ok {
		return nil, fmt.Errorf("expected int64, got %T", value)
	}
	return float32(v), nil
}

func int64ToDouble(value any) (any, error) {
	v, ok := value.(int64)
	if !ok {
		return nil, fmt.Errorf("expected int64, got %T", value)
	}
	return float64(v), nil
}

// Float to Int conversions (truncate toward zero)
func floatToInt32(value any) (any, error) {
	v, ok := value.(float32)
	if !ok {
		return nil, fmt.Errorf("expected float32, got %T", value)
	}
	if math.IsNaN(float64(v)) {
		return nil, fmt.Errorf("cannot convert NaN to int32")
	}
	if math.IsInf(float64(v), 0) {
		return nil, fmt.Errorf("cannot convert Inf to int32")
	}
	// Truncate toward zero
	truncated := math.Trunc(float64(v))
	if truncated < math.MinInt32 || truncated > math.MaxInt32 {
		return nil, fmt.Errorf("float32 value %v overflows int32 range", v)
	}
	return int32(truncated), nil
}

func floatToInt64(value any) (any, error) {
	v, ok := value.(float32)
	if !ok {
		return nil, fmt.Errorf("expected float32, got %T", value)
	}
	if math.IsNaN(float64(v)) {
		return nil, fmt.Errorf("cannot convert NaN to int64")
	}
	if math.IsInf(float64(v), 0) {
		return nil, fmt.Errorf("cannot convert Inf to int64")
	}
	// Truncate toward zero
	truncated := math.Trunc(float64(v))
	return int64(truncated), nil
}

func doubleToInt32(value any) (any, error) {
	v, ok := value.(float64)
	if !ok {
		return nil, fmt.Errorf("expected float64, got %T", value)
	}
	if math.IsNaN(v) {
		return nil, fmt.Errorf("cannot convert NaN to int32")
	}
	if math.IsInf(v, 0) {
		return nil, fmt.Errorf("cannot convert Inf to int32")
	}
	// Truncate toward zero
	truncated := math.Trunc(v)
	if truncated < math.MinInt32 || truncated > math.MaxInt32 {
		return nil, fmt.Errorf("float64 value %v overflows int32 range", v)
	}
	return int32(truncated), nil
}

func doubleToInt64(value any) (any, error) {
	v, ok := value.(float64)
	if !ok {
		return nil, fmt.Errorf("expected float64, got %T", value)
	}
	if math.IsNaN(v) {
		return nil, fmt.Errorf("cannot convert NaN to int64")
	}
	if math.IsInf(v, 0) {
		return nil, fmt.Errorf("cannot convert Inf to int64")
	}
	// Truncate toward zero
	truncated := math.Trunc(v)
	// Check for overflow - float64 can represent values larger than int64
	if truncated < math.MinInt64 || truncated > math.MaxInt64 {
		return nil, fmt.Errorf("float64 value %v overflows int64 range", v)
	}
	return int64(truncated), nil
}
