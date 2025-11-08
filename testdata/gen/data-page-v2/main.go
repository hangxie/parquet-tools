package main

import (
	"fmt"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
)

type Shoe struct {
	ShoeBrand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8"`
	ShoeName  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func main() {
	fw, err := local.NewLocalFileWriter("data-page-v2.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(Shoe), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	pw.CompressionType = parquet.CompressionCodec_GZIP
	pw.DataPageVersion = 2 // Use DATA_PAGE_V2 format
	_ = pw.Write(Shoe{"nike", "air_griffey"})
	_ = pw.Write(Shoe{"fila", "grant_hill_2"})
	_ = pw.Write(Shoe{"steph_curry", "curry7"})
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}
