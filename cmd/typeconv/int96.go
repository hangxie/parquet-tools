package typeconv

import (
	"encoding/binary"
	"fmt"
	"math"
)

// INT96 timestamp format:
// - 12 bytes total (little-endian)
// - Bytes 0-7: nanoseconds since midnight (int64)
// - Bytes 8-11: Julian day number (int32)
//
// Julian day 2440588 = January 1, 1970 (Unix epoch)
const unixEpochJulianDay = 2440588

// Nanoseconds per day
const nanosPerDay = int64(24 * 60 * 60 * 1e9)

// buildInt96ToTimestampConverter creates a converter from INT96 to TIMESTAMP
func buildInt96ToTimestampConverter(logicalType string) (Converter, error) {
	switch logicalType {
	case "TIMESTAMP_NANOS":
		return int96ToTimestampNanos, nil
	case "TIMESTAMP_MICROS":
		return int96ToTimestampMicros, nil
	case "TIMESTAMP_MILLIS":
		return int96ToTimestampMillis, nil
	default:
		return nil, fmt.Errorf("unsupported timestamp logical type: %s", logicalType)
	}
}

// int96ToTimestampNanos converts INT96 to nanoseconds since Unix epoch
func int96ToTimestampNanos(value any) (any, error) {
	nanos, err := parseINT96(value)
	if err != nil {
		return nil, err
	}
	return nanos, nil
}

// int96ToTimestampMicros converts INT96 to microseconds since Unix epoch (round half up)
func int96ToTimestampMicros(value any) (any, error) {
	nanos, err := parseINT96(value)
	if err != nil {
		return nil, err
	}
	// Round half up: add 500 then divide by 1000
	micros := roundHalfUp(nanos, 1000)
	return micros, nil
}

// int96ToTimestampMillis converts INT96 to milliseconds since Unix epoch (round half up)
func int96ToTimestampMillis(value any) (any, error) {
	nanos, err := parseINT96(value)
	if err != nil {
		return nil, err
	}
	// Round half up: add 500000 then divide by 1000000
	millis := roundHalfUp(nanos, 1000000)
	return millis, nil
}

// parseINT96 parses an INT96 value and returns nanoseconds since Unix epoch
func parseINT96(value any) (int64, error) {
	// INT96 can come in different forms depending on how parquet-go reads it
	var bytes []byte

	switch v := value.(type) {
	case string:
		// parquet-go may return INT96 as a 12-byte string
		bytes = []byte(v)
	case []byte:
		bytes = v
	default:
		return 0, fmt.Errorf("expected string or []byte for INT96, got %T", value)
	}

	if len(bytes) != 12 {
		return 0, fmt.Errorf("INT96 must be 12 bytes, got %d", len(bytes))
	}

	// Little-endian format:
	// - bytes[0:8]: nanoseconds since midnight (int64)
	// - bytes[8:12]: Julian day number (int32)
	nanosOfDay := int64(binary.LittleEndian.Uint64(bytes[0:8]))
	julianDay := int32(binary.LittleEndian.Uint32(bytes[8:12]))

	// Validate nanoseconds of day
	if nanosOfDay < 0 || nanosOfDay >= nanosPerDay {
		return 0, fmt.Errorf("invalid nanoseconds of day: %d", nanosOfDay)
	}

	// Convert Julian day to days since Unix epoch
	daysSinceEpoch := int64(julianDay) - unixEpochJulianDay

	// Calculate total nanoseconds since Unix epoch
	totalNanos := daysSinceEpoch*nanosPerDay + nanosOfDay

	return totalNanos, nil
}

// roundHalfUp performs integer division with round half up
// For positive numbers: (n + divisor/2) / divisor
// For negative numbers: (n - divisor/2) / divisor
func roundHalfUp(n, divisor int64) int64 {
	if n >= 0 {
		return (n + divisor/2) / divisor
	}
	return (n - divisor/2) / divisor
}

// TimestampNanosToINT96 converts nanoseconds since Unix epoch to INT96 bytes
// This is useful for testing
func TimestampNanosToINT96(nanos int64) []byte {
	// Calculate Julian day and nanoseconds of day
	daysSinceEpoch := nanos / nanosPerDay
	nanosOfDay := nanos % nanosPerDay

	// Handle negative timestamps
	if nanosOfDay < 0 {
		daysSinceEpoch--
		nanosOfDay += nanosPerDay
	}

	julianDay := int32(daysSinceEpoch + unixEpochJulianDay)

	// Check for overflow
	if daysSinceEpoch+unixEpochJulianDay > math.MaxInt32 || daysSinceEpoch+unixEpochJulianDay < math.MinInt32 {
		return nil // Invalid date
	}

	// Build the 12-byte INT96 value
	result := make([]byte, 12)
	binary.LittleEndian.PutUint64(result[0:8], uint64(nanosOfDay))
	binary.LittleEndian.PutUint32(result[8:12], uint32(julianDay))

	return result
}
