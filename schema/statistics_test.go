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

// FuzzDecodeStatistics feeds arbitrary min/max payloads through the decoder that
// backs meta and inspect output, where a truncated or oversized bound has to
// come back as nil rather than panic.
func FuzzDecodeStatistics(f *testing.F) {
	f.Add(uint8(1), uint8(0), int32(4), int32(0), []byte{0, 0, 0, 0}, []byte{1})
	f.Add(uint8(4), uint8(1), int32(4), int32(0), []byte{0x7f, 0xc0, 0, 0}, []byte{0xff, 0x80, 0, 0})
	f.Add(uint8(7), uint8(2), int32(16), int32(38), []byte{1, 2, 3}, []byte{})

	physicalTypes := []parquet.Type{
		parquet.Type_BOOLEAN,
		parquet.Type_INT32,
		parquet.Type_INT64,
		parquet.Type_INT96,
		parquet.Type_FLOAT,
		parquet.Type_DOUBLE,
		parquet.Type_BYTE_ARRAY,
		parquet.Type_FIXED_LEN_BYTE_ARRAY,
	}
	convertedTypes := []*parquet.ConvertedType{
		nil,
		parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
		parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
		parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
		parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS),
		parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL),
		parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64),
	}
	logicalTypes := []*parquet.LogicalType{
		nil,
		{FLOAT16: parquet.NewFloat16Type()},
		{UUID: parquet.NewUUIDType()},
		{DECIMAL: &parquet.DecimalType{Scale: 2, Precision: 9}},
	}

	f.Fuzz(func(t *testing.T, typeIndex, annotationIndex uint8, typeLength, precision int32, minValue, maxValue []byte) {
		physicalType := physicalTypes[int(typeIndex)%len(physicalTypes)]
		node := &SchemaNode{
			SchemaElement: parquet.SchemaElement{
				Type:          &physicalType,
				ConvertedType: convertedTypes[int(annotationIndex)%len(convertedTypes)],
				LogicalType:   logicalTypes[int(annotationIndex)%len(logicalTypes)],
				TypeLength:    &typeLength,
				Precision:     &precision,
				Scale:         &precision,
			},
		}
		node.DecodeStatistics(&parquet.Statistics{MinValue: minValue, MaxValue: maxValue})
		node.DecodeStatistics(&parquet.Statistics{Min: minValue, Max: maxValue})
	})
}
