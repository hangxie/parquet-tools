package main

import (
	"fmt"
	"os"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/types"
	"github.com/hangxie/parquet-go/v2/writer"
)

// this is for cat test (csv/tsv)
type Shoe struct {
	ShoeBrand string `parquet:"name=_shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8"`
	ShoeName  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8"`
}

// this is for meta/schema test
type InnerMap struct {
	Map  map[string]int32 `parquet:"name=_Map, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	List []string         `parquet:"name=_List, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=10"`
}

type PargoPrefix struct {
	NestedMap  map[string]InnerMap `parquet:"name=_NestedMap, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=STRUCT"`
	NestedList []InnerMap          `parquet:"name=_NestedList, type=LIST, valuetype=STRUCT"`
}

func main() {
	// "nested" parquet file for schema/meta test
	fw, err := local.NewLocalFileWriter("pargo-prefix-nested.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		os.Exit(1)
	}

	pw, err := writer.NewParquetWriter(fw, new(PargoPrefix), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		os.Exit(1)
	}

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.PageSize = 8 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	decimals := []int32{0, 1, 22, 333, 4444, 0, -1, -22, -333, -4444}
	for i := range 10 {
		value := PargoPrefix{
			NestedMap:  map[string]InnerMap{},
			NestedList: []InnerMap{},
		}
		for j := range i {
			key := fmt.Sprintf("Composite-%d", j)
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
		os.Exit(1)
	}
	_ = fw.Close()

	// "flat" parquet file for cat command's CSV/TSV tests
	fw, err = local.NewLocalFileWriter("pargo-prefix-flat.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		os.Exit(1)
	}

	pw, err = writer.NewParquetWriter(fw, new(Shoe), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		os.Exit(1)
	}

	pw.CompressionType = parquet.CompressionCodec_GZIP
	_ = pw.Write(Shoe{"nike", "air_griffey"})
	_ = pw.Write(Shoe{"fila", "grant_hill_2"})
	_ = pw.Write(Shoe{"steph_curry", "curry7"})
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		os.Exit(1)
	}
	_ = fw.Close()
}
