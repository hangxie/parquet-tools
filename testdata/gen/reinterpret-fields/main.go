package main

import (
	"fmt"
	"os"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/types"
	"github.com/xitongsys/parquet-go/writer"
)

type Scalar struct {
	V1 int32  `parquet:"name=v1, type=INT32, convertedtype=DECIMAL, scale=2, precision=9"`
	V2 int64  `parquet:"name=v2, type=INT64, convertedtype=DECIMAL, scale=2, precision=9"`
	V3 string `parquet:"name=v3, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12"`
	V4 string `parquet:"name=v4, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10"`
	V5 string `parquet:"name=v5, type=FIXED_LEN_BYTE_ARRAY, convertedtype=INTERVAL, length=12"`
	V6 string `parquet:"name=v6, type=INT96"`
}

type Pointer struct {
	V1 *int32  `parquet:"name=v1, type=INT32, convertedtype=DECIMAL, scale=2, precision=9"`
	V2 *int64  `parquet:"name=v2, type=INT64, convertedtype=DECIMAL, scale=2, precision=9"`
	V3 *string `parquet:"name=v3, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12"`
	V4 *string `parquet:"name=v4, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10"`
	V5 *string `parquet:"name=v5, type=FIXED_LEN_BYTE_ARRAY, convertedtype=INTERVAL, length=12"`
	V6 *string `parquet:"name=v6, type=INT96"`
}

type List struct {
	V1 []int32  `parquet:"name=v1, type=LIST, convertedtype=LIST, valuetype=INT32, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=9"`
	V2 []int64  `parquet:"name=v2, type=LIST, convertedtype=LIST, valuetype=INT64, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=9"`
	V3 []string `parquet:"name=v3, type=LIST, convertedtype=LIST, valuetype=FIXED_LEN_BYTE_ARRAY, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=10, valuelength=12"`
	V4 []string `parquet:"name=v4, type=LIST, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=10"`
	V5 []string `parquet:"name=v5, type=LIST, convertedtype=LIST, valuetype=FIXED_LEN_BYTE_ARRAY, valueconvertedtype=INTERVAL, valuelength=12"`
	V6 []string `parquet:"name=v6, type=LIST, convertedtype=LIST, valuetype=INT96"`
}

type MapKey struct {
	V1 map[int32]string  `parquet:"name=v1, type=MAP, convertedtype=MAP, keytype=INT32, keyconvertedtype=DECIMAL, keyscale=2, keyprecision=9, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	V2 map[int64]string  `parquet:"name=v2, type=MAP, convertedtype=MAP, keytype=INT64, keyconvertedtype=DECIMAL, keyscale=2, keyprecision=9, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	V3 map[string]string `parquet:"name=v3, type=MAP, convertedtype=MAP, keytype=FIXED_LEN_BYTE_ARRAY, keyconvertedtype=DECIMAL, keyscale=2, keyprecision=10, keylength=12, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	V4 map[string]string `parquet:"name=v4, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=DECIMAL, keyscale=2, keyprecision=10, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	V5 map[string]string `parquet:"name=v5, type=MAP, convertedtype=MAP, keytype=FIXED_LEN_BYTE_ARRAY, keyconvertedtype=INTERVAL, keylength=12, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	V6 map[string]string `parquet:"name=v6, type=MAP, convertedtype=MAP, keytype=INT96, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

type MapValue struct {
	V1 map[string]int32  `parquet:"name=v1, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=9"`
	V2 map[string]int64  `parquet:"name=v2, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT64, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=9"`
	V3 map[string]string `parquet:"name=v3, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=FIXED_LEN_BYTE_ARRAY, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=10, valuelength=12"`
	V4 map[string]string `parquet:"name=v4, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=10"`
	V5 map[string]string `parquet:"name=v5, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=FIXED_LEN_BYTE_ARRAY, valueconvertedtype=INTERVAL, valuelength=12"`
	V6 map[string]string `parquet:"name=v6, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT96"`
}

type Struct struct {
	EmbeddedMap  map[string]int32 `parquet:"name=embeddedMap, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=DECIMAL, keyscale=2, keyprecision=10, valuetype=INT32, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=9"`
	EmbeddedList []*string        `parquet:"name=embeddedList, type=LIST, convertedtype=LIST, valuetype=INT96"`
}

type Composite struct {
	Map map[int32][]Struct `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=INT32, keyconvertedtype=DECIMAL, keyscale=2, keyprecision=9, valuetype=STRUCT"`
}

func main() {
	genScalar()
	genPointer()
	genList()
	genMapKey()
	genMapValue()
	genComposite()
}

func genScalar() {
	fw, err := local.NewLocalFileWriter("reinterpret-scalar.parquet")
	if err != nil {
		fmt.Println("Can't create file", err)
		os.Exit(1)
	}
	pw, err := writer.NewParquetWriter(fw, new(Scalar), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		os.Exit(1)
	}
	timeValue, _ := time.Parse("2006-01-02", "2022-01-01")
	timeStep, _ := time.ParseDuration("1h1m1s1ms1us")
	for i := -125; i <= 125; i += 25 {
		strValue := fmt.Sprintf("%04d", i)
		timeValue = timeValue.Add(timeStep)

		value := Scalar{
			V1: int32(i),
			V2: int64(i),
			V3: types.StrIntToBinary(strValue, "BigEndian", 12, true),
			V4: types.StrIntToBinary(strValue, "BigEndian", 0, true),
			V5: types.StrIntToBinary(strValue, "LittleEndian", 12, false),
			V6: types.TimeToINT96(timeValue),
		}
		if err = pw.Write(value); err != nil {
			fmt.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
	}
	fw.Close()
}

func genPointer() {
	fw, err := local.NewLocalFileWriter("reinterpret-pointer.parquet")
	if err != nil {
		fmt.Println("Can't create file", err)
		os.Exit(1)
	}
	pw, err := writer.NewParquetWriter(fw, new(Pointer), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		os.Exit(1)
	}
	timeValue, _ := time.Parse("2006-01-02", "2022-01-01")
	timeStep, _ := time.ParseDuration("1h1m1s1ms1us")
	for i := -125; i <= 125; i += 25 {
		strValue := fmt.Sprintf("%04d", i)
		timeValue = timeValue.Add(timeStep)

		value := Pointer{
			V1: new(int32),
			V2: new(int64),
			V3: new(string),
			V4: new(string),
			V5: new(string),
			V6: new(string),
		}
		*value.V1 = int32(i)
		*value.V2 = int64(i)
		*value.V3 = types.StrIntToBinary(strValue, "BigEndian", 12, true)
		*value.V4 = types.StrIntToBinary(strValue, "BigEndian", 0, true)
		*value.V5 = types.StrIntToBinary(strValue, "LittleEndian", 12, false)
		*value.V6 = types.TimeToINT96(timeValue)
		if err = pw.Write(value); err != nil {
			fmt.Println("Write error", err)
		}
	}
	if err = pw.Write(Pointer{}); err != nil {
		fmt.Println("Write error", err)
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
	}
	fw.Close()
}

func genList() {
	fw, err := local.NewLocalFileWriter("reinterpret-list.parquet")
	if err != nil {
		fmt.Println("Can't create file", err)
		os.Exit(1)
	}
	pw, err := writer.NewParquetWriter(fw, new(List), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		os.Exit(1)
	}
	timeValue, _ := time.Parse("2006-01-02", "2022-01-01")
	timeStep, _ := time.ParseDuration("1h1m1s1ms1us")
	for i := -125; i <= 125; i += 25 {
		strValue := fmt.Sprintf("%04d", i)
		timeValue = timeValue.Add(timeStep)
		length := i / 25
		if length < 0 {
			length = -length
		}

		value := List{
			V1: make([]int32, length),
			V2: make([]int64, length),
			V3: make([]string, length),
			V4: make([]string, length),
			V5: make([]string, length),
			V6: make([]string, length),
		}
		for j := 0; j < length; j++ {
			value.V1[j] = int32(i)
			value.V2[j] = int64(i)
			value.V3[j] = types.StrIntToBinary(strValue, "BigEndian", 12, true)
			value.V4[j] = types.StrIntToBinary(strValue, "BigEndian", 0, true)
			value.V5[j] = types.StrIntToBinary(strValue, "LittleEndian", 12, false)
			value.V6[j] = types.TimeToINT96(timeValue)
		}
		if err = pw.Write(value); err != nil {
			fmt.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
	}
	fw.Close()
}

func genMapKey() {
	fw, err := local.NewLocalFileWriter("reinterpret-map-key.parquet")
	if err != nil {
		fmt.Println("Can't create file", err)
		os.Exit(1)
	}
	pw, err := writer.NewParquetWriter(fw, new(MapKey), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		os.Exit(1)
	}
	timeValue, _ := time.Parse("2006-01-02", "2022-01-01")
	timeStep, _ := time.ParseDuration("1h1m1s1ms1us")
	value := MapKey{
		V1: make(map[int32]string),
		V2: make(map[int64]string),
		V3: make(map[string]string),
		V4: make(map[string]string),
		V5: make(map[string]string),
		V6: make(map[string]string),
	}
	for i := -125; i <= 125; i += 25 {
		strValue := fmt.Sprintf("%04d", i)
		timeValue = timeValue.Add(timeStep)

		value.V1[int32(i)] = fmt.Sprintf("INT32-[%0.2f]", float64(i)/100)
		value.V2[int64(i)] = fmt.Sprintf("INT64-[%0.2f]", float64(i)/100)
		value.V3[types.StrIntToBinary(strValue, "BigEndian", 12, true)] = fmt.Sprintf("FIXED_LEN_BYTE_ARRAY-[%0.2f]", float64(i)/100)
		value.V4[types.StrIntToBinary(strValue, "BigEndian", 0, true)] = fmt.Sprintf("BYTE_ARRAY-[%0.2f]", float64(i)/100)
		value.V5[types.StrIntToBinary(strValue, "LittleEndian", 12, false)] = fmt.Sprintf("INTERVAL-[%d]", i)
		value.V6[types.TimeToINT96(timeValue)] = fmt.Sprintf("INT96-[%s]", timeValue.Format(time.RFC3339Nano))
	}
	if err = pw.Write(value); err != nil {
		fmt.Println("Write error", err)
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
	}
	fw.Close()
}

func genMapValue() {
	fw, err := local.NewLocalFileWriter("reinterpret-map-value.parquet")
	if err != nil {
		fmt.Println("Can't create file", err)
		os.Exit(1)
	}
	pw, err := writer.NewParquetWriter(fw, new(MapValue), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		os.Exit(1)
	}
	timeValue, _ := time.Parse("2006-01-02", "2022-01-01")
	timeStep, _ := time.ParseDuration("1h1m1s1ms1us")
	value := MapValue{
		V1: make(map[string]int32),
		V2: make(map[string]int64),
		V3: make(map[string]string),
		V4: make(map[string]string),
		V5: make(map[string]string),
		V6: make(map[string]string),
	}
	for i := -125; i <= 125; i += 25 {
		strValue := fmt.Sprintf("%04d", i)
		timeValue = timeValue.Add(timeStep)
		key := fmt.Sprintf("value-%d", (i+125)/25)

		value.V1[key] = int32(i)
		value.V2[key] = int64(i)
		value.V3[key] = types.StrIntToBinary(strValue, "BigEndian", 12, true)
		value.V4[key] = types.StrIntToBinary(strValue, "BigEndian", 0, true)
		value.V5[key] = types.StrIntToBinary(strValue, "LittleEndian", 12, false)
		value.V6[key] = types.TimeToINT96(timeValue)
	}
	if err = pw.Write(value); err != nil {
		fmt.Println("Write error", err)
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
	}
	fw.Close()
}

func genComposite() {
	fw, err := local.NewLocalFileWriter("reinterpret-composite.parquet")
	if err != nil {
		fmt.Println("Can't create file", err)
		os.Exit(1)
	}
	pw, err := writer.NewParquetWriter(fw, new(Composite), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		os.Exit(1)
	}

	timeValue, _ := time.Parse("2006-01-02", "2022-01-01")
	timeStep, _ := time.ParseDuration("1h1m1s1ms1us")
	value := Composite{
		Map: make(map[int32][]Struct),
	}
	for i := -125; i <= 125; i += 25 {
		length := i / 25
		if length < 0 {
			length = -length
		}

		structList := make([]Struct, length)
		for j := 0; j < length; j++ {
			structList[j] = Struct{
				EmbeddedMap:  make(map[string]int32),
				EmbeddedList: make([]*string, j+1),
			}
			for k := 0; k < j+1; k++ {
				key := types.StrIntToBinary(fmt.Sprintf("%04d%03d", j, k), "BigEndian", 0, true)
				structList[j].EmbeddedMap[key] = int32(j)

				timeValue = timeValue.Add(timeStep)
				structList[j].EmbeddedList[k] = new(string)
				*structList[j].EmbeddedList[k] = types.TimeToINT96(timeValue)
			}
		}
		value.Map[int32(i)] = structList
	}
	if err = pw.Write(value); err != nil {
		fmt.Println("Write error", err)
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
	}
	fw.Close()
}
