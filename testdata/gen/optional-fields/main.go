package main

import (
	"fmt"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
)

type SubStruct struct {
	Foo string `parquet:"name=foo, type=BYTE_ARRAY, convertedType=UTF8"`
}

type Row struct {
	Field1 *[]string          `parquet:"name=field1, type=LIST, repetitionType=OPTIONAL, valueType=BYTE_ARRAY, valueConvertedType=UTF8"`
	Field2 *map[string]string `parquet:"name=field2, type=MAP, repetitionType=OPTIONAL, keyType=BYTE_ARRAY, keyConvertedType=UTF8, valueType=BYTE_ARRAY, valueConvertedType=UTF8"`
	Field3 *SubStruct         `parquet:"name=field3, type=STRUCT, repetitionType=OPTIONAL"`
}

func main() {
	fw, err := local.NewLocalFileWriter("optional-fields.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(Row), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	pw.CompressionType = parquet.CompressionCodec_GZIP
	_ = pw.Write(Row{nil, nil, nil})
	_ = pw.Write(Row{
		Field1: toPtr([]string{"val1", "val2"}),
		Field2: toPtr(map[string]string{"val1": "val2"}),
		Field3: toPtr(SubStruct{Foo: "bar"}),
	})
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}

func toPtr[T any](val T) *T {
	return &val
}
