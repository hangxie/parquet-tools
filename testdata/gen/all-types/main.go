package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/types"
	"github.com/hangxie/parquet-go/v2/writer"
	"go.mongodb.org/mongo-driver/bson"
)

type InnerMap struct {
	Map  map[string]int32 `parquet:"name=Map, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	List []string         `parquet:"name=List, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=10"`
}

// there is no TIME_NANOS or TIMESTAMP_NANOS
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#deprecated-time-convertedtype
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#deprecated-timestamp-convertedtype
// Different encodings are used to demonstrate encoding compatibility:
// - BOOLEAN: RLE, BIT_PACKED
// - INT32/INT64: DELTA_BINARY_PACKED, RLE, RLE_DICTIONARY, BYTE_STREAM_SPLIT
// - FLOAT/DOUBLE: BYTE_STREAM_SPLIT, RLE_DICTIONARY
// - BYTE_ARRAY: DELTA_BYTE_ARRAY, DELTA_LENGTH_BYTE_ARRAY, RLE_DICTIONARY
// - FIXED_LEN_BYTE_ARRAY: BYTE_STREAM_SPLIT, RLE_DICTIONARY
type AllTypes struct {
	Bool              bool                `parquet:"name=Bool, type=BOOLEAN, encoding=BIT_PACKED"`
	Int32             int32               `parquet:"name=Int32, type=INT32, encoding=DELTA_BINARY_PACKED"`
	Int64             int64               `parquet:"name=Int64, type=INT64, encoding=DELTA_BINARY_PACKED"`
	Int96             string              `parquet:"name=Int96, type=INT96"`
	Float             float32             `parquet:"name=Float, type=FLOAT, encoding=BYTE_STREAM_SPLIT"`
	Float16Val        string              `parquet:"name=Float16Val, type=FIXED_LEN_BYTE_ARRAY, length=2, logicaltype=FLOAT16, encoding=PLAIN"`
	Double            float64             `parquet:"name=Double, type=DOUBLE, encoding=BYTE_STREAM_SPLIT"`
	ByteArray         string              `parquet:"name=ByteArray, type=BYTE_ARRAY, encoding=DELTA_LENGTH_BYTE_ARRAY"`
	Enum              string              `parquet:"name=Enum, type=BYTE_ARRAY, convertedtype=ENUM, encoding=RLE_DICTIONARY"`
	Uuid              string              `parquet:"name=Uuid, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID, encoding=PLAIN"`
	Json              string              `parquet:"name=Json, type=BYTE_ARRAY, convertedtype=JSON, encoding=DELTA_BYTE_ARRAY"`
	Bson              string              `parquet:"name=Bson, type=BYTE_ARRAY, convertedtype=BSON"`
	Json2             string              `parquet:"name=Json2, type=BYTE_ARRAY, logicaltype=JSON, encoding=PLAIN"`
	Bson2             string              `parquet:"name=Bson2, type=BYTE_ARRAY, logicaltype=BSON, encoding=DELTA_BYTE_ARRAY"`
	Variant           string              `parquet:"name=Variant, type=BYTE_ARRAY, logicaltype=VARIANT, encoding=RLE_DICTIONARY"`
	FixedLenByteArray string              `parquet:"name=FixedLenByteArray, type=FIXED_LEN_BYTE_ARRAY, length=10, encoding=RLE_DICTIONARY"`
	Utf8              string              `parquet:"name=Utf8, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	Utf82             string              `parquet:"name=Utf8_2, type=BYTE_ARRAY, logicaltype=STRING, encoding=PLAIN"`
	ConvertedInt8     int32               `parquet:"name=Int_8, type=INT32, convertedtype=INT32, convertedtype=INT_8, encoding=BIT_PACKED"`
	ConvertedInt16    int32               `parquet:"name=Int_16, type=INT32, convertedtype=INT_16, encoding=PLAIN"`
	ConvertedInt32    int32               `parquet:"name=Int_32, type=INT32, convertedtype=INT_32, encoding=RLE_DICTIONARY"`
	ConvertedInt64    int64               `parquet:"name=Int_64, type=INT64, convertedtype=INT_64, encoding=RLE"`
	ConvertedUint8    int32               `parquet:"name=Uint_8, type=INT32, convertedtype=UINT_8, encoding=BIT_PACKED"`
	ConvertedUint16   int32               `parquet:"name=Uint_16, type=INT32, convertedtype=UINT_16, encoding=DELTA_BINARY_PACKED"`
	ConvertedUint32   int32               `parquet:"name=Uint_32, type=INT32, convertedtype=UINT_32"`
	ConvertedUint64   int64               `parquet:"name=Uint_64, type=INT64, convertedtype=UINT_64, encoding=RLE"`
	Date              int32               `parquet:"name=Date, type=INT32, convertedtype=DATE, encoding=DELTA_BINARY_PACKED"`
	Date2             int32               `parquet:"name=Date2, type=INT32, logicaltype=DATE, encoding=RLE_DICTIONARY"`
	TimeMillis        int32               `parquet:"name=TimeMillis, type=INT32, convertedtype=TIME_MILLIS, encoding=DELTA_BINARY_PACKED"`
	TimeMillis2       int32               `parquet:"name=TimeMillis2, type=INT32, logicaltype=TIME, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS, encoding=RLE"`
	TimeMicros        int64               `parquet:"name=TimeMicros, type=INT64, convertedtype=TIME_MICROS, encoding=DELTA_BINARY_PACKED"`
	TimeMicros2       int64               `parquet:"name=TimeMicros2, type=INT64, logicaltype=TIME, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS, encoding=RLE"`
	TimeNanos2        int64               `parquet:"name=TimeNanos2, type=INT64, logicaltype=TIME, logicaltype.isadjustedtoutc=false, logicaltype.unit=NANOS, encoding=DELTA_BINARY_PACKED"`
	TimestampMillis   int64               `parquet:"name=TimestampMillis, type=INT64, convertedtype=TIMESTAMP_MILLIS, encoding=DELTA_BINARY_PACKED"`
	TimestampMillis2  int64               `parquet:"name=TimestampMillis2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS, encoding=RLE"`
	TimestampMicros   int64               `parquet:"name=TimestampMicros, type=INT64, convertedtype=TIMESTAMP_MICROS, encoding=DELTA_BINARY_PACKED"`
	TimestampMicros2  int64               `parquet:"name=TimestampMicros2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS, encoding=RLE_DICTIONARY"`
	TimestampNanos2   int64               `parquet:"name=TimestampNanos2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=NANOS, encoding=RLE"`
	Interval          string              `parquet:"name=Interval, type=FIXED_LEN_BYTE_ARRAY, convertedtype=INTERVAL, length=12, encoding=PLAIN"`
	Decimal1          int32               `parquet:"name=Decimal1, type=INT32, convertedtype=DECIMAL, scale=2, precision=9, encoding=DELTA_BINARY_PACKED"`
	Decimal2          int64               `parquet:"name=Decimal2, type=INT64, convertedtype=DECIMAL, scale=2, precision=18, encoding=DELTA_BINARY_PACKED"`
	Decimal3          string              `parquet:"name=Decimal3, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12, encoding=PLAIN"`
	Decimal4          string              `parquet:"name=Decimal4, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=20, encoding=DELTA_LENGTH_BYTE_ARRAY"`
	Decimal5          int32               `parquet:"name=decimal5, type=INT32, scale=2, precision=9, logicaltype=DECIMAL, logicaltype.precision=9, logicaltype.scale=2, encoding=RLE_DICTIONARY"`
	DecimalPointer    *string             `parquet:"name=DecimalPointer, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12, repetitiontype=OPTIONAL, encoding=RLE_DICTIONARY"`
	Map               map[string]int32    `parquet:"name=Map, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	List              []string            `parquet:"name=List, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	Repeated          []int32             `parquet:"name=Repeated, type=INT32, repetitiontype=REPEATED, encoding=DELTA_BINARY_PACKED"`
	NestedMap         map[string]InnerMap `parquet:"name=NestedMap, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=STRUCT"`
	NestedList        []InnerMap          `parquet:"name=NestedList, type=LIST, valuetype=STRUCT"`
}

func main() {
	fw, err := local.NewLocalFileWriter("all-types.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(AllTypes), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.PageSize = 8 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	decimals := []int32{0, 1, 22, 333, 4444, 0, -1, -22, -333, -4444}
	interval := make([]byte, 4)
	for i := range 10 {
		ts, _ := time.Parse("2006-01-02T15:04:05.000000Z", fmt.Sprintf("2022-01-01T%02d:%02d:%02d.%03d%03dZ", i, i, i, i, i))
		strI := fmt.Sprintf("%d", i)
		binary.LittleEndian.PutUint32(interval, uint32(i))
		jsonObj := map[string]int{
			strI: i,
		}
		jsonStr, _ := json.Marshal(jsonObj)
		bsonStr, _ := bson.Marshal(jsonObj)
		value := AllTypes{
			Bool:              i%2 == 0,
			Int32:             int32(i),
			Int64:             int64(i),
			Int96:             types.TimeToINT96(ts),
			Float:             float32(i) * 0.5,
			Float16Val:        float32ToFloat16(float32(i) + 0.5),
			Double:            float64(i) * 0.5,
			ByteArray:         fmt.Sprintf("ByteArray-%d", i),
			Enum:              fmt.Sprintf("Enum-%d", i),
			Uuid:              string(bytes.Repeat([]byte{byte(i)}, 16)),
			Json:              string(jsonStr),
			Bson:              string(bsonStr),
			Json2:             string(jsonStr),
			Bson2:             string(bsonStr),
			Variant:           string(jsonStr),
			FixedLenByteArray: fmt.Sprintf("Fixed-%04d", i),
			Utf8:              fmt.Sprintf("UTF8-%d", i),
			Utf82:             fmt.Sprintf("UTF8_2-%d", i),
			ConvertedInt8:     int32(i),
			ConvertedInt16:    int32(i),
			ConvertedInt32:    int32(i),
			ConvertedInt64:    int64(i),
			ConvertedUint8:    int32(i),
			ConvertedUint16:   int32(i),
			ConvertedUint32:   int32(i),
			ConvertedUint64:   int64(i),
			Date:              int32(i * -1000),
			Date2:             int32(i * 1000),
			TimeMillis:        int32(i) * 1_001,
			TimeMillis2:       int32(i) * 1_001,
			TimeMicros:        int64(i) * 1_000_001,
			TimeMicros2:       int64(i) * 1_000_001,
			TimeNanos2:        int64(i) * 1_000_000_001,
			TimestampMillis:   int64(i) + 1_640_995_200_000,
			TimestampMillis2:  int64(i) + 1_640_995_200_000,
			TimestampMicros:   int64(i) + 1_640_995_200_000_000,
			TimestampMicros2:  int64(i) + 1_640_995_200_000_000,
			TimestampNanos2:   int64(i) + 1_640_995_200_000_000_000,
			Interval:          string(bytes.Repeat(interval, 3)),
			Decimal1:          decimals[i],
			Decimal2:          int64(decimals[i]),
			Decimal3:          types.StrIntToBinary(fmt.Sprintf("%0.2f", float32(decimals[i]/100.0)), "BigEndian", 12, true),
			Decimal4:          types.StrIntToBinary(fmt.Sprintf("%0.2f", float32(decimals[i]/100.0)), "BigEndian", 0, true),
			Decimal5:          decimals[i],
			DecimalPointer:    nil,
			Map:               map[string]int32{},
			List:              []string{},
			Repeated:          []int32{},
			NestedMap:         map[string]InnerMap{},
			NestedList:        []InnerMap{},
		}
		if i%2 == 0 {
			value.DecimalPointer = nil
		} else {
			value.DecimalPointer = &value.Decimal3
		}
		for j := range i {
			key := fmt.Sprintf("Composite-%d", j)
			value.Map[key] = int32(j)
			value.List = append(value.List, key)
			value.Repeated = append(value.Repeated, int32(j))
			nested := InnerMap{
				Map:  map[string]int32{},
				List: []string{},
			}
			for k := range j {
				key := fmt.Sprintf("Embedded-%d", k)
				nested.Map[key] = int32(k)
				nested.List = append(nested.List, types.StrIntToBinary(fmt.Sprintf("%0.2f", float32(decimals[k]/100.0)), "BigEndian", 12, true))
			}
			value.NestedMap[key] = nested
			value.NestedList = append(value.NestedList, nested)
		}

		if err = pw.Write(value); err != nil {
			fmt.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}

// float32ToFloat16 converts a float32 into IEEE 754 binary16 (big-endian 2 bytes)
func float32ToFloat16(f float32) string {
	// Handle NaN/Inf explicitly
	if f != f { // NaN
		return string([]byte{0x7e, 0x00})
	}
	if f > 65504 { // max half
		return string([]byte{0x7c, 0x00})
	}
	if f < -65504 {
		return string([]byte{0xfc, 0x00})
	}
	bits := math.Float32bits(f)
	sign := uint16((bits >> 31) & 0x1)
	exp := int32((bits>>23)&0xff) - 127 + 15 // re-bias
	mant := uint32(bits & 0x7fffff)

	var half uint16
	if exp <= 0 {
		// subnormal or zero in half
		if exp < -10 {
			half = sign << 15 // zero
		} else {
			// add implicit leading 1 to mantissa
			mant = (mant | 0x800000) >> uint32(1-exp)
			// round to nearest
			mant = (mant + 0x1000) >> 13
			half = (sign << 15) | uint16(mant)
		}
	} else if exp >= 31 {
		// overflow -> inf
		half = (sign << 15) | (0x1f << 10)
	} else {
		// normalized
		// round mantissa
		mant = mant + 0x1000
		if mant&0x800000 != 0 {
			// mant overflow -> adjust exp
			mant = 0
			exp++
			if exp >= 31 {
				half = (sign << 15) | (0x1f << 10)
				return string([]byte{byte(half >> 8), byte(half)})
			}
		}
		half = (sign << 15) | (uint16(exp) << 10) | uint16(mant>>13)
	}
	return string([]byte{byte(half >> 8), byte(half)})
}
