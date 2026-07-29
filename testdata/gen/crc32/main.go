package main

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

type Shoe struct {
	ShoeBrand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8"`
	ShoeName  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func main() {
	fw, err := local.NewLocalFileWriter("crc32.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(Shoe),
		writer.WithNP(4),
		writer.WithCompressionCodec(parquet.CompressionCodec_GZIP),
		writer.WithDataPageVersion(2),
		writer.WithWriteCRC(true),
	)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	_ = pw.WriteWithContext(context.Background(), Shoe{"nike", "air_griffey"})
	_ = pw.WriteWithContext(context.Background(), Shoe{"fila", "grant_hill_2"})
	_ = pw.WriteWithContext(context.Background(), Shoe{"steph_curry", "curry7"})
	if err = pw.WriteStopWithContext(context.Background()); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}
