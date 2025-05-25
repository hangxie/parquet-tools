package main

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
)

type NestedStruct struct {
	Str string `parquet:"name=str, type=BYTE_ARRAY, convertedtype=UTF8"`
	Int int32  `parquet:"name=int, type=INT32"`
}

type ListTypes struct {
	ListOfStruct []NestedStruct `parquet:"name=list_of_struct, type=LIST"`
	ListOfString []string       `parquet:"name=list_of_string, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

func main() {
	fw, err := local.NewLocalFileWriter("gostruct-list.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(ListTypes), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.PageSize = 8 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	for i := range 3 {
		value := ListTypes{
			ListOfStruct: slices.Repeat([]NestedStruct{{
				Str: strconv.FormatInt(int64(i*10), 10),
				Int: int32(i * 10),
			}}, i),
			ListOfString: slices.Repeat([]string{strconv.FormatInt(int64(i), 10)}, i),
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
