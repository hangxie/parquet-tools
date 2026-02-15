package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/types"
	"github.com/hangxie/parquet-go/v2/writer"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// DeepNestedBson is nested inside MapValueStruct to test deep conversion
type DeepNestedBson struct {
	DeepBson string `parquet:"name=DeepBson, type=BYTE_ARRAY, convertedtype=BSON, encoding=DELTA_BYTE_ARRAY, compression=UNCOMPRESSED"`
}

// MapValueStruct is used as the value type in a map
type MapValueStruct struct {
	ValueBson   string         `parquet:"name=ValueBson, type=BYTE_ARRAY, convertedtype=BSON, encoding=PLAIN, compression=SNAPPY"`
	ValueInt96  string         `parquet:"name=ValueInt96, type=INT96, compression=LZ4_RAW"`
	ValueNested DeepNestedBson `parquet:"name=ValueNested"`
}

// ListElementStruct is used as elements in a list
type ListElementStruct struct {
	ElemBson  string `parquet:"name=ElemBson, type=BYTE_ARRAY, logicaltype=BSON, encoding=DELTA_BYTE_ARRAY, compression=LZ4_RAW"`
	ElemInt96 string `parquet:"name=ElemInt96, type=INT96, compression=LZ4_RAW"`
	ElemName  string `parquet:"name=ElemName, type=BYTE_ARRAY, convertedtype=UTF8, encoding=DELTA_LENGTH_BYTE_ARRAY, compression=ZSTD"`
}

// NestedContainer has a map containing structs with BSON
type NestedContainer struct {
	ContainerBson string `parquet:"name=ContainerBson, type=BYTE_ARRAY, convertedtype=BSON, encoding=DELTA_LENGTH_BYTE_ARRAY, compression=GZIP"`
}

// RetypeTest is the struct for testing retype command with deep nesting
// It tests INT96 and BSON conversion at various nesting levels:
// - Top-level fields
// - In a struct
// - In a list of structs
// - In a map with struct values
// - In a struct inside a map value (3 levels deep)
type RetypeTest struct {
	// Regular fields for data integrity verification
	Id   int32  `parquet:"name=Id, type=INT32"`
	Name string `parquet:"name=Name, type=BYTE_ARRAY, convertedtype=UTF8"`

	// Top-level INT96 fields
	Int96Field  string  `parquet:"name=Int96Field, type=INT96"`
	Int96Field2 *string `parquet:"name=Int96Field2, type=INT96, repetitiontype=OPTIONAL"`

	// Top-level BSON fields (both convertedtype and logicaltype variants)
	BsonField  string `parquet:"name=BsonField, type=BYTE_ARRAY, convertedtype=BSON"`
	BsonField2 string `parquet:"name=BsonField2, type=BYTE_ARRAY, logicaltype=BSON"`

	// Top-level JSON fields (both convertedtype and logicaltype variants)
	JsonField  string `parquet:"name=JsonField, type=BYTE_ARRAY, convertedtype=JSON"`
	JsonField2 string `parquet:"name=JsonField2, type=BYTE_ARRAY, logicaltype=JSON"`

	// Top-level FLOAT16 fields
	Float16Field string `parquet:"name=Float16Field, type=FIXED_LEN_BYTE_ARRAY, length=2, logicaltype=FLOAT16"`

	// Nested struct containing BSON (1 level deep)
	Nested NestedContainer `parquet:"name=Nested"`

	// List of structs containing BSON and INT96 (tests list element conversion)
	ListOfStructs []ListElementStruct `parquet:"name=ListOfStructs, type=LIST"`

	// Map with struct values containing BSON and INT96 (tests map value conversion)
	MapOfStructs map[string]MapValueStruct `parquet:"name=MapOfStructs, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8"`

	// List of BSON values directly (tests list of BSON conversion)
	BsonList []string `parquet:"name=BsonList, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=BSON"`

	// Map with BSON values directly (tests map with BSON values)
	BsonMap map[string]string `parquet:"name=BsonMap, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=BSON"`

	// Legacy repeated primitive (non-standard LIST)
	LegacyRepeated []int32 `parquet:"name=LegacyRepeated, type=INT32, repetitiontype=REPEATED"`
}

func main() {
	fw, err := local.NewLocalFileWriter("retype.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(RetypeTest), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.PageSize = 8 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	pw.DataPageVersion = 2

	for i := range 3 {
		ts, _ := time.Parse("2006-01-02T15:04:05.000000Z", fmt.Sprintf("2022-01-01T%02d:%02d:%02d.%03d%03dZ", i, i, i, i, i))
		int96Str := types.TimeToINT96(ts)

		// Create JSON/BSON data
		jsonObj := map[string]any{
			"id":    i,
			"value": fmt.Sprintf("item-%d", i),
		}
		jsonStr, _ := json.Marshal(jsonObj)
		bsonStr, _ := bson.Marshal(jsonObj)

		nestedBsonObj := map[string]any{
			"nested_id": i * 10,
			"nested":    true,
		}
		nestedBsonStr, _ := bson.Marshal(nestedBsonObj)

		deepBsonObj := map[string]any{
			"deep": i * 100,
		}
		deepBsonStr, _ := bson.Marshal(deepBsonObj)

		listElemBsonObj := map[string]any{
			"elem_index": i,
		}
		listElemBsonStr, _ := bson.Marshal(listElemBsonObj)

		mapValueBsonObj := map[string]any{
			"map_key": fmt.Sprintf("key-%d", i),
		}
		mapValueBsonStr, _ := bson.Marshal(mapValueBsonObj)

		// Float16 values (little endian)
		// 0: 0.0 (0x0000) -> \x00\x00
		// 1: 1.0 (0x3c00) -> \x00\x3c
		// 2: 2.0 (0x4000) -> \x00\x40
		// 3: 3.0 (0x4200) -> \x00\x42
		// 4: -2.0 (0xc000) -> \x00\xc0
		float16Vals := []string{
			"\x00\x00",
			"\x00\x3c",
			"\x00\x40",
			"\x00\x42",
			"\x00\xc0",
		}
		float16Val := float16Vals[i%len(float16Vals)]

		// Build the record
		value := RetypeTest{
			Id:           int32(i),
			Name:         fmt.Sprintf("record-%d", i),
			Int96Field:   int96Str,
			BsonField:    string(bsonStr),
			BsonField2:   string(bsonStr),
			JsonField:    string(jsonStr),
			JsonField2:   string(jsonStr),
			Float16Field: float16Val,
			Nested: NestedContainer{
				ContainerBson: string(nestedBsonStr),
			},
			ListOfStructs:  []ListElementStruct{},
			MapOfStructs:   map[string]MapValueStruct{},
			BsonList:       []string{},
			BsonMap:        map[string]string{},
			LegacyRepeated: []int32{},
		}

		// Set optional INT96 field for even rows
		if i%2 == 0 {
			value.Int96Field2 = &int96Str
		}

		// Populate LegacyRepeated
		for k := 0; k < i+1; k++ {
			value.LegacyRepeated = append(value.LegacyRepeated, int32(k*10+i))
		}

		// Add elements to list of structs (variable number based on i)
		for j := range i {
			elemTs, _ := time.Parse("2006-01-02T15:04:05.000000Z", fmt.Sprintf("2022-02-%02dT%02d:%02d:%02d.000000Z", j+1, j, j, j))
			elemInt96 := types.TimeToINT96(elemTs)
			elemBsonObj := map[string]any{"elem": j, "parent": i}
			elemBsonStr, _ := bson.Marshal(elemBsonObj)

			value.ListOfStructs = append(value.ListOfStructs, ListElementStruct{
				ElemBson:  string(elemBsonStr),
				ElemInt96: elemInt96,
				ElemName:  fmt.Sprintf("elem-%d-%d", i, j),
			})
		}

		// Add entries to map of structs
		for j := range i {
			mapKey := fmt.Sprintf("key-%d", j)
			mapTs, _ := time.Parse("2006-01-02T15:04:05.000000Z", fmt.Sprintf("2022-03-%02dT%02d:%02d:%02d.000000Z", j+1, j, j, j))
			mapInt96 := types.TimeToINT96(mapTs)
			valueBsonObj := map[string]any{"map_value": j, "parent": i}
			valueBsonStr, _ := bson.Marshal(valueBsonObj)

			value.MapOfStructs[mapKey] = MapValueStruct{
				ValueBson:  string(valueBsonStr),
				ValueInt96: mapInt96,
				ValueNested: DeepNestedBson{
					DeepBson: string(deepBsonStr),
				},
			}
		}

		// Add BSON values directly to list
		for j := range i {
			listBsonObj := map[string]any{"list_item": j}
			listBsonStr, _ := bson.Marshal(listBsonObj)
			value.BsonList = append(value.BsonList, string(listBsonStr))
		}

		// Add BSON values directly to map
		for j := range i {
			mapBsonObj := map[string]any{"map_item": j}
			mapBsonStr, _ := bson.Marshal(mapBsonObj)
			value.BsonMap[fmt.Sprintf("bson-key-%d", j)] = string(mapBsonStr)
		}

		// Use the prepared BSON strings for consistency
		_ = listElemBsonStr
		_ = mapValueBsonStr

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
