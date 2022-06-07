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
	Map  map[string]int32 `parquet:"name=Map, type=MAP, repetitiontype=REQUIRED, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	List []string         `parquet:"name=List, type=LIST, repetitiontype=REQUIRED, valuetype=BYTE_ARRAY, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=10"`
}

type AllTypes struct {
	Bool              bool                `parquet:"name=bool, type=BOOLEAN"`
	Int32             int32               `parquet:"name=int32, type=INT32"`
	Int64             int64               `parquet:"name=int64, type=INT64"`
	Int96             string              `parquet:"name=int96, type=INT96"`
	Float             float32             `parquet:"name=float, type=FLOAT"`
	Double            float64             `parquet:"name=double, type=DOUBLE"`
	ByteArray         string              `parquet:"name=bytearray, type=BYTE_ARRAY"`
	Enum              string              `parquet:"name=enum, type=BYTE_ARRAY, convertedtype=ENUM"`
	Uuid              string              `parquet:"name=uuid, type=BYTE_ARRAY, convertedtype=UUID"`
	Json              string              `parquet:"name=json, type=BYTE_ARRAY, convertedtype=JSON"`
	FixedLenByteArray string              `parquet:"name=FixedLenByteArray, type=FIXED_LEN_BYTE_ARRAY, length=10, repetitiontype=REQUIRED"`
	Utf8              string              `parquet:"name=utf8, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Int_8             int32               `parquet:"name=int_8, type=INT32, convertedtype=INT32, convertedtype=INT_8"`
	Int_16            int32               `parquet:"name=Int_16, type=INT32, convertedtype=INT_16, repetitiontype=REQUIRED"`
	Int_32            int32               `parquet:"name=Int_32, type=INT32, convertedtype=INT_32, repetitiontype=REQUIRED"`
	Int_64            int64               `parquet:"name=Int_64, type=INT64, convertedtype=INT_64, repetitiontype=REQUIRED"`
	Uint_8            int32               `parquet:"name=Uint_8, type=INT32, convertedtype=UINT_8, repetitiontype=REQUIRED"`
	Uint_16           int32               `parquet:"name=Uint_16, type=INT32, convertedtype=UINT_16, repetitiontype=REQUIRED"`
	Uint_32           int32               `parquet:"name=Uint_32, type=INT32, convertedtype=UINT_32, repetitiontype=REQUIRED"`
	Uint_64           int64               `parquet:"name=Uint_64, type=INT64, convertedtype=UINT_64, repetitiontype=REQUIRED"`
	Date              int32               `parquet:"name=date, type=INT32, convertedtype=DATE"`
	Date2             int32               `parquet:"name=date2, type=INT32, convertedtype=DATE, logicaltype=DATE"`
	TimeMillis        int32               `parquet:"name=timemillis, type=INT32, convertedtype=TIME_MILLIS"`
	TimeMillis2       int32               `parquet:"name=timemillis2, type=INT32, logicaltype=TIME, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	TimeMicros        int64               `parquet:"name=timemicros, type=INT64, convertedtype=TIME_MICROS"`
	TimeMicros2       int64               `parquet:"name=timemicros2, type=INT64, logicaltype=TIME, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS"`
	TimestampMillis   int64               `parquet:"name=timestampmillis, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	TimestampMillis2  int64               `parquet:"name=timestampmillis2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	TimestampMicros   int64               `parquet:"name=timestampmicros, type=INT64, convertedtype=TIMESTAMP_MICROS"`
	TimestampMicros2  int64               `parquet:"name=timestampmicros2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS"`
	Interval          string              `parquet:"name=interval, type=FIXED_LEN_BYTE_ARRAY, convertedtype=INTERVAL, length=12"`
	Decimal1          int32               `parquet:"name=Decimal1, type=INT32, convertedtype=DECIMAL, scale=2, precision=9, repetitiontype=REQUIRED"`
	Decimal2          int64               `parquet:"name=Decimal2, type=INT64, convertedtype=DECIMAL, scale=2, precision=18, repetitiontype=REQUIRED"`
	Decimal3          string              `parquet:"name=Decimal3, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12, repetitiontype=REQUIRED"`
	Decimal4          string              `parquet:"name=Decimal4, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=20, repetitiontype=REQUIRED"`
	Decimal5          int32               `parquet:"name=decimal5, type=INT32, scale=2, precision=9, logicaltype=DECIMAL, logicaltype.precision=9, logicaltype.scale=2"`
	Decimal_pointer   *string             `parquet:"name=Decimal_pointer, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12, repetitiontype=OPTIONAL"`
	Map               map[string]int32    `parquet:"name=Map, type=MAP, repetitiontype=REQUIRED, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	List              []string            `parquet:"name=List, type=LIST, repetitiontype=REQUIRED, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	Repeated          []int32             `parquet:"name=Repeated, type=INT32, repetitiontype=REPEATED"`
	NestedMap         map[string]InnerMap `parquet:"name=NestedMap, type=MAP, repetitiontype=REQUIRED, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=STRUCT"`
	NestedList        []InnerMap          `parquet:"name=NestedList, type=LIST, repetitiontype=REQUIRED, valuetype=STRUCT"`
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
			Int_8:             int32(i),
			Int_16:            int32(i),
			Int_32:            int32(i),
			Int_64:            int64(i),
			Uint_8:            int32(i),
			Uint_16:           int32(i),
			Uint_32:           int32(i),
			Uint_64:           int64(i),
			Date:              int32(1640995200 + i),
			Date2:             int32(1640995200 + i),
			TimeMillis:        int32(i),
			TimeMillis2:       int32(i),
			TimeMicros:        int64(i),
			TimeMicros2:       int64(i),
			TimestampMillis:   int64(1640995200000 + i),
			TimestampMillis2:  int64(1640995200000 + i),
			TimestampMicros:   int64(1640995200000000 + i),
			TimestampMicros2:  int64(1640995200000000 + i),
			Interval:          types.StrIntToBinary(strings.Repeat(strI, 5), "LittleEndian", 12, false),
			Decimal1:          decimals[i],
			Decimal2:          int64(decimals[i]),
			Decimal3:          types.StrIntToBinary(fmt.Sprintf("%0.2f", float32(decimals[i]/100.0)), "BigEndian", 12, true),
			Decimal4:          types.StrIntToBinary(fmt.Sprintf("%0.2f", float32(decimals[i]/100.0)), "BigEndian", 0, true),
			Decimal5:          decimals[i],
			Decimal_pointer:   nil,
			Map:               map[string]int32{},
			List:              []string{},
			Repeated:          []int32{},
			NestedMap:         map[string]InnerMap{},
			NestedList:        []InnerMap{},
		}
		if i%2 == 0 {
			value.Decimal_pointer = nil
		} else {
			value.Decimal_pointer = &value.Decimal3
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
