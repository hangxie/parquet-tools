package main

import (
	"fmt"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type Shoe struct {
	ShoeBrand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8"`
	ShoeName  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func main() {
	fw, err := local.NewLocalFileWriter("good.parquet")
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
	_ = pw.Write(Shoe{"nike", "air_griffey"})
	_ = pw.Write(Shoe{"fila", "grant_hill_2"})
	_ = pw.Write(Shoe{"steph_curry", "curry7"})
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}
