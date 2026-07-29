package main

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// Record uses PLAIN encoding (no dictionary) with a single repeated string value
// to achieve extreme compression ratios.
type Record struct {
	Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
}

func main() {
	path := "high-compression.parquet"

	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		fmt.Printf("Can't create file: %v", err)
		return
	}

	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(Record),
		writer.WithNP(1),
		writer.WithCompressionCodec(parquet.CompressionCodec_ZSTD),
		writer.WithRowGroupSize(512*1024*1024), // 512MB row groups
		writer.WithPageSize(512*1024*1024),     // 512MB pages to maximize compression ratio
	)
	if err != nil {
		fmt.Printf("Can't create writer: %v", err)
		return
	}

	// Write 2M identical records — compresses extremely well with PLAIN encoding
	const numRecords = 2_000_000
	repeatedValue := "this is a repeated value that appears in every single row of the parquet file"
	for range numRecords {
		if err = pw.WriteWithContext(context.Background(), Record{Value: repeatedValue}); err != nil {
			fmt.Printf("Write error: %v", err)
			return
		}
	}
	if err = pw.WriteStopWithContext(context.Background()); err != nil {
		fmt.Printf("WriteStop error: %v", err)
		return
	}
	_ = fw.Close()
}
