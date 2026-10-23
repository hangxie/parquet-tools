package main

import (
	"context"
	"fmt"
	"os"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// LIST and MAP layouts that are valid per the spec but differ from what
// parquet-go writes: alternate wrapper names, logical-type-only annotations,
// and a repeated group that is the element itself.

type Bag struct {
	ArrayElement *string `parquet:"name=array_element, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
}

// Athena's three-level LIST naming.
type AthenaList struct {
	Bag []Bag `parquet:"name=bag, repetitiontype=REPEATED"`
}

type ElementGroup struct {
	Element []int32 `parquet:"name=element, type=INT32, repetitiontype=REPEATED"`
}

// The repeated group's only field is repeated, so the group is the element.
type LegacyList struct {
	List []ElementGroup `parquet:"name=list, repetitiontype=REPEATED"`
}

type Entry struct {
	Key   string `parquet:"name=key, type=BYTE_ARRAY, convertedtype=UTF8"`
	Value int32  `parquet:"name=value, type=INT32"`
}

// MAP whose repeated group is named entries instead of key_value.
type EntriesMap struct {
	Entries []Entry `parquet:"name=entries, repetitiontype=REPEATED"`
}

type Row struct {
	Athena         AthenaList       `parquet:"name=athena"`
	AthenaLogical  AthenaList       `parquet:"name=athena_logical"`
	LogicalList    []int32          `parquet:"name=logical_list, type=LIST, valuetype=INT32"`
	LogicalMap     map[string]int32 `parquet:"name=logical_map, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	Legacy         LegacyList       `parquet:"name=legacy"`
	EntriesMap     EntriesMap       `parquet:"name=entries_map"`
	EntriesLogical EntriesMap       `parquet:"name=entries_logical"`
}

func str(s string) *string { return &s }

var rows = []Row{
	{
		Athena:         AthenaList{Bag: []Bag{{str("a")}, {nil}, {str("b")}}},
		AthenaLogical:  AthenaList{Bag: []Bag{{str("c")}}},
		LogicalList:    []int32{1, 2, 3},
		LogicalMap:     map[string]int32{"x": 1},
		Legacy:         LegacyList{List: []ElementGroup{{Element: []int32{1, 2}}, {Element: []int32{3}}}},
		EntriesMap:     EntriesMap{Entries: []Entry{{"x", 42}}},
		EntriesLogical: EntriesMap{Entries: []Entry{{"x", 42}}},
	},
	{
		Athena:         AthenaList{},
		AthenaLogical:  AthenaList{Bag: []Bag{{str("d")}, {str("e")}}},
		LogicalList:    []int32{},
		LogicalMap:     map[string]int32{"y": 2, "z": 3},
		Legacy:         LegacyList{List: []ElementGroup{{}}},
		EntriesMap:     EntriesMap{},
		EntriesLogical: EntriesMap{Entries: []Entry{{"y", 1}, {"z", 2}}},
	},
}

func main() {
	if err := write("list-map-layout.parquet"); err != nil {
		fmt.Println("write error:", err)
		os.Exit(1)
	}
}

func write(name string) error {
	fw, err := local.NewLocalFileWriter(name)
	if err != nil {
		return err
	}
	defer func() { _ = fw.Close() }()
	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(Row),
		writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
	)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := pw.WriteWithContext(context.Background(), row); err != nil {
			return err
		}
	}
	// Levels already match the target layouts, so only the annotations change.
	listType, mapType := parquet.ConvertedType_LIST, parquet.ConvertedType_MAP
	for i, se := range pw.Footer.Schema {
		element := *se
		switch pw.SchemaHandler.Infos[i].ExName {
		case "athena", "legacy":
			element.ConvertedType = &listType
			element.LogicalType = &parquet.LogicalType{LIST: parquet.NewListType()}
		case "athena_logical":
			element.LogicalType = &parquet.LogicalType{LIST: parquet.NewListType()}
		case "logical_list":
			element.ConvertedType = nil
			element.LogicalType = &parquet.LogicalType{LIST: parquet.NewListType()}
		case "entries_map":
			element.ConvertedType = &mapType
			element.LogicalType = &parquet.LogicalType{MAP: parquet.NewMapType()}
		case "logical_map", "entries_logical":
			element.ConvertedType = nil
			element.LogicalType = &parquet.LogicalType{MAP: parquet.NewMapType()}
		default:
			continue
		}
		pw.Footer.Schema[i] = &element
	}
	return pw.WriteStopWithContext(context.Background())
}
