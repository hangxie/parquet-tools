package typeconv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInt96ToTimestampNanos(t *testing.T) {
	tests := map[string]struct {
		inputNanos int64 // Input as nanoseconds since Unix epoch
		errMsg     string
	}{
		"unix-epoch":     {inputNanos: 0},
		"positive":       {inputNanos: 1609459200000000000}, // 2021-01-01T00:00:00Z
		"negative":       {inputNanos: -86400000000000},     // 1969-12-31T00:00:00Z
		"with-nanos":     {inputNanos: 1609459200123456789},
		"large-positive": {inputNanos: 4102444800000000000}, // 2100-01-01T00:00:00Z
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Convert expected nanos to INT96 bytes
			int96Bytes := TimestampNanosToINT96(tc.inputNanos)
			require.NotNil(t, int96Bytes, "TimestampNanosToINT96 should succeed")

			// Convert INT96 back to nanos
			result, err := int96ToTimestampNanos(string(int96Bytes))
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.inputNanos, result)
			}
		})
	}
}

func TestInt96ToTimestampMicros(t *testing.T) {
	tests := map[string]struct {
		inputNanos     int64 // Input as nanoseconds
		expectedMicros int64 // Expected microseconds
	}{
		"exact":      {inputNanos: 1000000000, expectedMicros: 1000000},
		"round-down": {inputNanos: 1000000499, expectedMicros: 1000000},
		"round-up":   {inputNanos: 1000000500, expectedMicros: 1000001},
		"round-up-2": {inputNanos: 1000000999, expectedMicros: 1000001},
		"negative":   {inputNanos: -1000000000, expectedMicros: -1000000},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			int96Bytes := TimestampNanosToINT96(tc.inputNanos)
			require.NotNil(t, int96Bytes)

			result, err := int96ToTimestampMicros(string(int96Bytes))
			require.NoError(t, err)
			require.Equal(t, tc.expectedMicros, result)
		})
	}
}

func TestInt96ToTimestampMillis(t *testing.T) {
	tests := map[string]struct {
		inputNanos     int64 // Input as nanoseconds
		expectedMillis int64 // Expected milliseconds
	}{
		"exact":      {inputNanos: 1000000000, expectedMillis: 1000},
		"round-down": {inputNanos: 1000499999, expectedMillis: 1000},
		"round-up":   {inputNanos: 1000500000, expectedMillis: 1001},
		"round-up-2": {inputNanos: 1000999999, expectedMillis: 1001},
		"negative":   {inputNanos: -1000000000, expectedMillis: -1000},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			int96Bytes := TimestampNanosToINT96(tc.inputNanos)
			require.NotNil(t, int96Bytes)

			result, err := int96ToTimestampMillis(string(int96Bytes))
			require.NoError(t, err)
			require.Equal(t, tc.expectedMillis, result)
		})
	}
}

func TestParseINT96Errors(t *testing.T) {
	tests := map[string]struct {
		input  any
		errMsg string
	}{
		"wrong-type":     {input: int64(123), errMsg: "expected string or []byte"},
		"wrong-length":   {input: "short", errMsg: "must be 12 bytes"},
		"wrong-length-2": {input: make([]byte, 10), errMsg: "must be 12 bytes"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := parseINT96(tc.input)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestRoundHalfUp(t *testing.T) {
	tests := map[string]struct {
		n        int64
		divisor  int64
		expected int64
	}{
		"exact":         {n: 10, divisor: 2, expected: 5},
		"round-down":    {n: 10, divisor: 3, expected: 3},
		"round-up":      {n: 11, divisor: 3, expected: 4},
		"half-up":       {n: 15, divisor: 10, expected: 2},
		"negative":      {n: -10, divisor: 3, expected: -3},
		"negative-half": {n: -15, divisor: 10, expected: -2},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := roundHalfUp(tc.n, tc.divisor)
			require.Equal(t, tc.expected, result)
		})
	}
}
