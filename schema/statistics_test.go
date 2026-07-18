package schema

import "testing"

func TestDecodeStatisticsNil(t *testing.T) {
	minValue, maxValue := (&SchemaNode{}).DecodeStatistics(nil)
	if minValue != nil || maxValue != nil {
		t.Fatalf("DecodeStatistics(nil) = (%v, %v), want (nil, nil)", minValue, maxValue)
	}
}
