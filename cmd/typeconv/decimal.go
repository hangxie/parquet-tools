package typeconv

import (
	"fmt"
	"math"
	"math/big"
)

// buildDecimalConverter creates a converter for DECIMAL type conversions
func buildDecimalConverter(source SourceTypeInfo, target *TypeSpec) (Converter, error) {
	// Calculate scale difference
	scaleDiff := target.Scale - source.Scale

	return func(value any) (any, error) {
		// First, convert source value to big.Int (unscaled value)
		unscaled, err := toBigInt(value, source.Primitive)
		if err != nil {
			return nil, fmt.Errorf("failed to read decimal value: %w", err)
		}

		// Adjust for scale difference
		if scaleDiff != 0 {
			unscaled, err = adjustScale(unscaled, scaleDiff)
			if err != nil {
				return nil, err
			}
		}

		// Check if value fits in target precision
		if err := checkPrecision(unscaled, target.Precision); err != nil {
			return nil, err
		}

		// Convert to target primitive type
		return fromBigInt(unscaled, target.Primitive, target.PrimitiveLen)
	}, nil
}

// toBigInt converts a value from its primitive representation to big.Int
func toBigInt(value any, primitive string) (*big.Int, error) {
	switch primitive {
	case "INT32":
		v, ok := value.(int32)
		if !ok {
			return nil, fmt.Errorf("expected int32, got %T", value)
		}
		return big.NewInt(int64(v)), nil

	case "INT64":
		v, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64, got %T", value)
		}
		return big.NewInt(v), nil

	case "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY":
		var bytes []byte
		switch v := value.(type) {
		case string:
			bytes = []byte(v)
		case []byte:
			bytes = v
		default:
			return nil, fmt.Errorf("expected string or []byte, got %T", value)
		}

		if len(bytes) == 0 {
			return big.NewInt(0), nil
		}

		// Big-endian two's complement
		result := new(big.Int)
		result.SetBytes(bytes)

		// Handle sign extension for negative numbers
		// If the high bit is set, it's a negative number
		if bytes[0]&0x80 != 0 {
			// Calculate two's complement
			// For a negative number stored in n bytes:
			// actual_value = stored_value - 2^(n*8)
			complement := new(big.Int).Lsh(big.NewInt(1), uint(len(bytes)*8))
			result.Sub(result, complement)
		}

		return result, nil

	default:
		return nil, fmt.Errorf("unsupported primitive type for decimal: %s", primitive)
	}
}

// adjustScale multiplies or divides the unscaled value to adjust for scale difference
func adjustScale(unscaled *big.Int, scaleDiff int) (*big.Int, error) {
	if scaleDiff == 0 {
		return unscaled, nil
	}

	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs(scaleDiff))), nil)
	result := new(big.Int)

	if scaleDiff > 0 {
		// Increasing scale: multiply
		result.Mul(unscaled, multiplier)
	} else {
		// Decreasing scale: divide with round half up
		result = divideRoundHalfUp(unscaled, multiplier)
	}

	return result, nil
}

// divideRoundHalfUp performs integer division with round half up
func divideRoundHalfUp(dividend, divisor *big.Int) *big.Int {
	// Calculate quotient and remainder
	quotient := new(big.Int)
	remainder := new(big.Int)
	quotient.DivMod(dividend, divisor, remainder)

	// Round half up: if |remainder| >= divisor/2, round away from zero
	halfDivisor := new(big.Int).Div(divisor, big.NewInt(2))
	absRemainder := new(big.Int).Abs(remainder)

	if absRemainder.Cmp(halfDivisor) >= 0 {
		// Round away from zero
		if dividend.Sign() >= 0 {
			quotient.Add(quotient, big.NewInt(1))
		} else {
			quotient.Sub(quotient, big.NewInt(1))
		}
	}

	return quotient
}

// checkPrecision verifies that the unscaled value fits within the specified precision
func checkPrecision(unscaled *big.Int, precision int) error {
	// Calculate the maximum value for the given precision
	// max = 10^precision - 1
	maxValue := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(precision)), nil)
	maxValue.Sub(maxValue, big.NewInt(1))

	absValue := new(big.Int).Abs(unscaled)

	if absValue.Cmp(maxValue) > 0 {
		return fmt.Errorf("decimal value exceeds precision %d", precision)
	}

	return nil
}

// fromBigInt converts a big.Int to the target primitive representation
func fromBigInt(unscaled *big.Int, primitive string, length int) (any, error) {
	switch primitive {
	case "INT32":
		if !unscaled.IsInt64() {
			return nil, fmt.Errorf("decimal value %v overflows int64", unscaled)
		}
		v := unscaled.Int64()
		if v < math.MinInt32 || v > math.MaxInt32 {
			return nil, fmt.Errorf("decimal value %d overflows int32 range", v)
		}
		return int32(v), nil

	case "INT64":
		if !unscaled.IsInt64() {
			return nil, fmt.Errorf("decimal value %v overflows int64", unscaled)
		}
		return unscaled.Int64(), nil

	case "BYTE_ARRAY":
		return bigIntToBytes(unscaled, 0), nil

	case "FIXED_LEN_BYTE_ARRAY":
		bytes := bigIntToBytes(unscaled, length)
		if len(bytes) > length {
			return nil, fmt.Errorf("decimal value requires %d bytes, but FIXED_LEN_BYTE_ARRAY length is %d", len(bytes), length)
		}
		return string(bytes), nil

	default:
		return nil, fmt.Errorf("unsupported primitive type for decimal: %s", primitive)
	}
}

// bigIntToBytes converts a big.Int to big-endian two's complement bytes
func bigIntToBytes(n *big.Int, minLength int) []byte {
	if n.Sign() == 0 {
		result := make([]byte, max(1, minLength))
		return result
	}

	var bytes []byte

	if n.Sign() > 0 {
		bytes = n.Bytes()
		// If high bit is set, prepend a zero byte to indicate positive
		if len(bytes) > 0 && bytes[0]&0x80 != 0 {
			bytes = append([]byte{0}, bytes...)
		}
	} else {
		// For negative numbers, compute two's complement
		// First, get the absolute value
		absVal := new(big.Int).Abs(n)

		// Determine the number of bytes needed
		numBits := absVal.BitLen() + 1 // +1 for sign bit
		numBytes := (numBits + 7) / 8

		// Compute 2^(numBytes*8) - |n|
		twosPower := new(big.Int).Lsh(big.NewInt(1), uint(numBytes*8))
		twosComplement := new(big.Int).Sub(twosPower, absVal)

		bytes = twosComplement.Bytes()

		// Pad to numBytes if needed
		for len(bytes) < numBytes {
			bytes = append([]byte{0xff}, bytes...)
		}
	}

	// Pad to minimum length if specified
	if minLength > 0 && len(bytes) < minLength {
		padding := make([]byte, minLength-len(bytes))
		if n.Sign() < 0 {
			// Pad with 0xff for negative numbers
			for i := range padding {
				padding[i] = 0xff
			}
		}
		bytes = append(padding, bytes...)
	}

	return bytes
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
