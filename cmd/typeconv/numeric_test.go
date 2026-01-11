package typeconv

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInt32ToInt64(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected int64
		errMsg   string
	}{
		"zero":       {input: int32(0), expected: 0},
		"positive":   {input: int32(12345), expected: 12345},
		"negative":   {input: int32(-12345), expected: -12345},
		"max":        {input: int32(math.MaxInt32), expected: math.MaxInt32},
		"min":        {input: int32(math.MinInt32), expected: math.MinInt32},
		"wrong-type": {input: int64(123), errMsg: "expected int32"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := int32ToInt64(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestInt64ToInt32(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected int32
		errMsg   string
	}{
		"zero":              {input: int64(0), expected: 0},
		"positive":          {input: int64(12345), expected: 12345},
		"negative":          {input: int64(-12345), expected: -12345},
		"max-int32":         {input: int64(math.MaxInt32), expected: math.MaxInt32},
		"min-int32":         {input: int64(math.MinInt32), expected: math.MinInt32},
		"overflow-positive": {input: int64(math.MaxInt32 + 1), errMsg: "overflows int32"},
		"overflow-negative": {input: int64(math.MinInt32 - 1), errMsg: "overflows int32"},
		"wrong-type":        {input: int32(123), errMsg: "expected int64"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := int64ToInt32(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestFloatToDouble(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected float64
		errMsg   string
	}{
		"zero":       {input: float32(0), expected: 0},
		"positive":   {input: float32(1.5), expected: float64(float32(1.5))},
		"negative":   {input: float32(-1.5), expected: float64(float32(-1.5))},
		"wrong-type": {input: float64(1.5), errMsg: "expected float32"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := floatToDouble(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestDoubleToFloat(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected float32
		errMsg   string
	}{
		"zero":       {input: float64(0), expected: 0},
		"positive":   {input: float64(1.5), expected: 1.5},
		"negative":   {input: float64(-1.5), expected: -1.5},
		"overflow":   {input: float64(math.MaxFloat64), errMsg: "overflows float32"},
		"wrong-type": {input: float32(1.5), errMsg: "expected float64"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := doubleToFloat(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestFloatToInt32(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected int32
		errMsg   string
	}{
		"zero":           {input: float32(0), expected: 0},
		"positive":       {input: float32(3.7), expected: 3},
		"negative":       {input: float32(-3.7), expected: -3},
		"positive-round": {input: float32(3.5), expected: 3},
		"negative-round": {input: float32(-3.5), expected: -3},
		"nan":            {input: float32(math.NaN()), errMsg: "cannot convert NaN"},
		"inf":            {input: float32(math.Inf(1)), errMsg: "cannot convert Inf"},
		"neg-inf":        {input: float32(math.Inf(-1)), errMsg: "cannot convert Inf"},
		"wrong-type":     {input: float64(1.5), errMsg: "expected float32"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := floatToInt32(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestDoubleToInt64(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected int64
		errMsg   string
	}{
		"zero":           {input: float64(0), expected: 0},
		"positive":       {input: float64(3.7), expected: 3},
		"negative":       {input: float64(-3.7), expected: -3},
		"positive-round": {input: float64(3.5), expected: 3},
		"negative-round": {input: float64(-3.5), expected: -3},
		"nan":            {input: float64(math.NaN()), errMsg: "cannot convert NaN"},
		"inf":            {input: float64(math.Inf(1)), errMsg: "cannot convert Inf"},
		"neg-inf":        {input: float64(math.Inf(-1)), errMsg: "cannot convert Inf"},
		"overflow":       {input: float64(1e20), errMsg: "overflows int64"},
		"wrong-type":     {input: float32(1.5), errMsg: "expected float64"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := doubleToInt64(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestBuildNumericConverter(t *testing.T) {
	tests := map[string]struct {
		srcPrimitive string
		tgtPrimitive string
		hasError     bool
	}{
		"int32-to-int64":  {srcPrimitive: "INT32", tgtPrimitive: "INT64"},
		"int64-to-int32":  {srcPrimitive: "INT64", tgtPrimitive: "INT32"},
		"float-to-double": {srcPrimitive: "FLOAT", tgtPrimitive: "DOUBLE"},
		"double-to-float": {srcPrimitive: "DOUBLE", tgtPrimitive: "FLOAT"},
		"int32-to-float":  {srcPrimitive: "INT32", tgtPrimitive: "FLOAT"},
		"int32-to-double": {srcPrimitive: "INT32", tgtPrimitive: "DOUBLE"},
		"int64-to-float":  {srcPrimitive: "INT64", tgtPrimitive: "FLOAT"},
		"int64-to-double": {srcPrimitive: "INT64", tgtPrimitive: "DOUBLE"},
		"float-to-int32":  {srcPrimitive: "FLOAT", tgtPrimitive: "INT32"},
		"float-to-int64":  {srcPrimitive: "FLOAT", tgtPrimitive: "INT64"},
		"double-to-int32": {srcPrimitive: "DOUBLE", tgtPrimitive: "INT32"},
		"double-to-int64": {srcPrimitive: "DOUBLE", tgtPrimitive: "INT64"},
		"invalid":         {srcPrimitive: "BOOLEAN", tgtPrimitive: "INT32", hasError: true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			conv, err := buildNumericConverter(tc.srcPrimitive, tc.tgtPrimitive)
			if tc.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, conv)
			}
		})
	}
}

func TestInt32ToFloat(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected float32
		errMsg   string
	}{
		"zero":       {input: int32(0), expected: 0},
		"positive":   {input: int32(12345), expected: 12345},
		"negative":   {input: int32(-12345), expected: -12345},
		"wrong-type": {input: int64(123), errMsg: "expected int32"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := int32ToFloat(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestInt32ToDouble(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected float64
		errMsg   string
	}{
		"zero":       {input: int32(0), expected: 0},
		"positive":   {input: int32(12345), expected: 12345},
		"negative":   {input: int32(-12345), expected: -12345},
		"wrong-type": {input: int64(123), errMsg: "expected int32"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := int32ToDouble(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestInt64ToFloat(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected float32
		errMsg   string
	}{
		"zero":       {input: int64(0), expected: 0},
		"positive":   {input: int64(12345), expected: 12345},
		"negative":   {input: int64(-12345), expected: -12345},
		"wrong-type": {input: int32(123), errMsg: "expected int64"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := int64ToFloat(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestInt64ToDouble(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected float64
		errMsg   string
	}{
		"zero":       {input: int64(0), expected: 0},
		"positive":   {input: int64(12345), expected: 12345},
		"negative":   {input: int64(-12345), expected: -12345},
		"wrong-type": {input: int32(123), errMsg: "expected int64"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := int64ToDouble(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestFloatToInt64(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected int64
		errMsg   string
	}{
		"zero":           {input: float32(0), expected: 0},
		"positive":       {input: float32(3.7), expected: 3},
		"negative":       {input: float32(-3.7), expected: -3},
		"positive-round": {input: float32(3.5), expected: 3},
		"negative-round": {input: float32(-3.5), expected: -3},
		"nan":            {input: float32(math.NaN()), errMsg: "cannot convert NaN"},
		"inf":            {input: float32(math.Inf(1)), errMsg: "cannot convert Inf"},
		"neg-inf":        {input: float32(math.Inf(-1)), errMsg: "cannot convert Inf"},
		"wrong-type":     {input: float64(1.5), errMsg: "expected float32"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := floatToInt64(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestDoubleToInt32(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected int32
		errMsg   string
	}{
		"zero":           {input: float64(0), expected: 0},
		"positive":       {input: float64(3.7), expected: 3},
		"negative":       {input: float64(-3.7), expected: -3},
		"positive-round": {input: float64(3.5), expected: 3},
		"negative-round": {input: float64(-3.5), expected: -3},
		"nan":            {input: float64(math.NaN()), errMsg: "cannot convert NaN"},
		"inf":            {input: float64(math.Inf(1)), errMsg: "cannot convert Inf"},
		"neg-inf":        {input: float64(math.Inf(-1)), errMsg: "cannot convert Inf"},
		"overflow":       {input: float64(3e9), errMsg: "overflows int32"},
		"wrong-type":     {input: float32(1.5), errMsg: "expected float64"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := doubleToInt32(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}
