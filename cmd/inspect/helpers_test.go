package inspect

import (
	"math"
	"testing"
)

func TestNormalizeNegativeZero(t *testing.T) {
	tests := []any{math.Copysign(0, -1), float32(math.Copysign(0, -1))}
	for _, value := range tests {
		if got := normalizeNegativeZero(value); math.Signbit(toFloat64(got)) {
			t.Fatalf("normalizeNegativeZero(%T) retained negative sign", value)
		}
	}
}

func toFloat64(value any) float64 {
	switch value := value.(type) {
	case float32:
		return float64(value)
	case float64:
		return value
	default:
		return math.NaN()
	}
}
