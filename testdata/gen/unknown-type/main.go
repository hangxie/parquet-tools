package main

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

type UnknownType struct {
	ID         *int32  `parquet:"name=id, type=INT32, repetitiontype=OPTIONAL"`
	UnknownCol *int32  `parquet:"name=unknown_col, type=INT32, logicaltype=UNKNOWN, repetitiontype=OPTIONAL"`
	Name       *string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
}

func main() {
	fw, err := local.NewLocalFileWriter("unknown-type.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(
		fw, new(UnknownType),
		writer.WithRowGroupSize(128*1024*1024),
		writer.WithPageSize(8*1024),
		writer.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	names := []string{"alice", "bob", "charlie", "david", "eve"}
	for i, name := range names {
		id := int32(i + 1)
		n := name
		row := UnknownType{ID: &id, UnknownCol: nil, Name: &n}
		if err := pw.Write(row); err != nil {
			fmt.Println("Write error:", err)
			return
		}
	}

	if err := pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error:", err)
		return
	}
	if err := fw.Close(); err != nil {
		fmt.Println("Close error:", err)
		return
	}
	fmt.Println("Generated unknown-type.parquet")
}
