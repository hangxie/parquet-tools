package main

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

type BloomFilterData struct {
	ID       int64   `parquet:"name=ID, type=INT64, bloomfilter=true"`
	Name     string  `parquet:"name=Name, type=BYTE_ARRAY, convertedtype=UTF8, bloomfilter=true, bloomfiltersize=4096"`
	Age      int32   `parquet:"name=Age, type=INT32"`
	Score    float64 `parquet:"name=Score, type=DOUBLE, bloomfilter=true"`
	Category string  `parquet:"name=Category, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func main() {
	fw, err := local.NewLocalFileWriter("bloom-filter.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(BloomFilterData),
		writer.WithNP(4),
		writer.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}
	for i := range 10 {
		value := BloomFilterData{
			ID:       int64(i),
			Name:     fmt.Sprintf("name-%d", i),
			Age:      int32(20 + i),
			Score:    float64(i) * 1.5,
			Category: fmt.Sprintf("cat-%d", i%3),
		}
		if err = pw.WriteWithContext(context.Background(), value); err != nil {
			fmt.Println("Write error", err)
			return
		}
	}
	if err = pw.WriteStopWithContext(context.Background()); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}
