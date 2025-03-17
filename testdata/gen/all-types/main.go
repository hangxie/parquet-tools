package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/types"
	"github.com/xitongsys/parquet-go/writer"
)

type InnerMap struct {
	Map  map[string]int32 `parquet:"name=Map, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	List []string         `parquet:"name=List, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=10"`
}

// there is no TIME_NANOS or TIMESTAMP_NANOS
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#deprecated-time-convertedtype
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#deprecated-timestamp-convertedtype
type AllTypes struct {
	Bool              bool                `parquet:"name=Bool, type=BOOLEAN"`
	Int32             int32               `parquet:"name=Int32, type=INT32"`
	Int64             int64               `parquet:"name=Int64, type=INT64"`
	Int96             string              `parquet:"name=Int96, type=INT96"`
	Float             float32             `parquet:"name=Float, type=FLOAT"`
	Double            float64             `parquet:"name=Double, type=DOUBLE"`
	ByteArray         string              `parquet:"name=ByteArray, type=BYTE_ARRAY"`
	Enum              string              `parquet:"name=Enum, type=BYTE_ARRAY, convertedtype=ENUM"`
	Uuid              string              `parquet:"name=Uuid, type=BYTE_ARRAY, convertedtype=UUID"`
	Json              string              `parquet:"name=Json, type=BYTE_ARRAY, convertedtype=JSON"`
	FixedLenByteArray string              `parquet:"name=FixedLenByteArray, type=FIXED_LEN_BYTE_ARRAY, length=10"`
	Utf8              string              `parquet:"name=Utf8, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ConvertedInt8     int32               `parquet:"name=Int_8, type=INT32, convertedtype=INT32, convertedtype=INT_8"`
	ConvertedInt16    int32               `parquet:"name=Int_16, type=INT32, convertedtype=INT_16"`
	ConvertedInt32    int32               `parquet:"name=Int_32, type=INT32, convertedtype=INT_32"`
	ConvertedInt64    int64               `parquet:"name=Int_64, type=INT64, convertedtype=INT_64"`
	ConvertedUint8    int32               `parquet:"name=Uint_8, type=INT32, convertedtype=UINT_8"`
	ConvertedUint16   int32               `parquet:"name=Uint_16, type=INT32, convertedtype=UINT_16"`
	ConvertedUint32   int32               `parquet:"name=Uint_32, type=INT32, convertedtype=UINT_32"`
	ConvertedUint64   int64               `parquet:"name=Uint_64, type=INT64, convertedtype=UINT_64"`
	Date              int32               `parquet:"name=Date, type=INT32, convertedtype=DATE"`
	Date2             int32               `parquet:"name=Date2, type=INT32, convertedtype=DATE, logicaltype=DATE"`
	TimeMillis        int32               `parquet:"name=TimeMillis, type=INT32, convertedtype=TIME_MILLIS"`
	TimeMillis2       int32               `parquet:"name=TimeMillis2, type=INT32, logicaltype=TIME, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	TimeMicros        int64               `parquet:"name=TimeMicros, type=INT64, convertedtype=TIME_MICROS"`
	TimeMicros2       int64               `parquet:"name=TimeMicros2, type=INT64, logicaltype=TIME, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS"`
	TimeNanos2        int64               `parquet:"name=TimeNanos2, type=INT64, logicaltype=TIME, logicaltype.isadjustedtoutc=false, logicaltype.unit=NANOS"`
	TimestampMillis   int64               `parquet:"name=TimestampMillis, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	TimestampMillis2  int64               `parquet:"name=TimestampMillis2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	TimestampMicros   int64               `parquet:"name=TimestampMicros, type=INT64, convertedtype=TIMESTAMP_MICROS"`
	TimestampMicros2  int64               `parquet:"name=TimestampMicros2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS"`
	TimestampNanos2   int64               `parquet:"name=TimestampNanos2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=NANOS"`
	Interval          string              `parquet:"name=Interval, type=FIXED_LEN_BYTE_ARRAY, convertedtype=INTERVAL, length=12"`
	Decimal1          int32               `parquet:"name=Decimal1, type=INT32, convertedtype=DECIMAL, scale=2, precision=9"`
	Decimal2          int64               `parquet:"name=Decimal2, type=INT64, convertedtype=DECIMAL, scale=2, precision=18"`
	Decimal3          string              `parquet:"name=Decimal3, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12"`
	Decimal4          string              `parquet:"name=Decimal4, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=20"`
	Decimal5          int32               `parquet:"name=decimal5, type=INT32, scale=2, precision=9, logicaltype=DECIMAL, logicaltype.precision=9, logicaltype.scale=2"`
	DecimalPointer    *string             `parquet:"name=DecimalPointer, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12, repetitiontype=OPTIONAL"`
	Map               map[string]int32    `parquet:"name=Map, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	List              []string            `parquet:"name=List, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	Repeated          []int32             `parquet:"name=Repeated, type=INT32, repetitiontype=REPEATED"`
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
	for i := 0; i < 10; i++ {
		ts, _ := time.Parse("2006-01-02T15:04:05.000000Z", fmt.Sprintf("2022-01-01T%02d:%02d:%02d.%03d%03dZ", i, i, i, i, i))
		strI := fmt.Sprintf("%d", i)
		value := AllTypes{
			Bool:              i%2 == 0,
			Int32:             int32(i),
			Int64:             int64(i),
			Int96:             types.TimeToINT96(ts),
			Float:             float32(i) * 0.5,
			Double:            float64(i) * 0.5,
			ByteArray:         fmt.Sprintf("ByteArray-%d", i),
			Enum:              fmt.Sprintf("Enum-%d", i),
			Uuid:              "12345678-ABCD-4444-" + strings.Repeat(strI, 4) + "-567890ABCDEF",
			Json:              `{"` + strI + `":` + strI + `}`,
			FixedLenByteArray: fmt.Sprintf("Fixed-%04d", i),
			Utf8:              fmt.Sprintf("UTF8-%d", i),
			ConvertedInt8:     int32(i),
			ConvertedInt16:    int32(i),
			ConvertedInt32:    int32(i),
			ConvertedInt64:    int64(i),
			ConvertedUint8:    int32(i),
			ConvertedUint16:   int32(i),
			ConvertedUint32:   int32(i),
			ConvertedUint64:   int64(i),
			Date:              int32(1640995200 + i),
			Date2:             int32(1640995200 + i),
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
			Interval:          types.StrIntToBinary(strings.Repeat(strI, 5), "LittleEndian", 12, false),
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
		for j := 0; j < i; j++ {
			key := fmt.Sprintf("Composite-%d", j)
			value.Map[key] = int32(j)
			value.List = append(value.List, key)
			value.Repeated = append(value.Repeated, int32(j))
			nested := InnerMap{
				Map:  map[string]int32{},
				List: []string{},
			}
			for k := 0; k < j; k++ {
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
	fw.Close()
}
