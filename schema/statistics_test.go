package schema

import (
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/stretchr/testify/require"
)

func TestDecodeStatisticsNil(t *testing.T) {
	minValue, maxValue := (&SchemaNode{}).DecodeStatistics(nil)
	if minValue != nil || maxValue != nil {
		t.Fatalf("DecodeStatistics(nil) = (%v, %v), want (nil, nil)", minValue, maxValue)
	}
}

func TestDecodeStatisticsInvalidType(t *testing.T) {
	invalidType := parquet.Type(999)
	minValue, maxValue := (&SchemaNode{
		SchemaElement: parquet.SchemaElement{Type: &invalidType},
	}).DecodeStatistics(&parquet.Statistics{MinValue: []byte{1}, MaxValue: []byte{2}})
	require.Nil(t, minValue)
	require.Nil(t, maxValue)
}
