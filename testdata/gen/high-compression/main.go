package main

import (
	"fmt"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
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

	pw, err := writer.NewParquetWriter(fw, new(Record), 1)
	if err != nil {
		fmt.Printf("Can't create writer: %v", err)
		return
	}
	pw.CompressionType = parquet.CompressionCodec_ZSTD
	pw.RowGroupSize = 512 * 1024 * 1024 // 512MB row groups
	pw.PageSize = 512 * 1024 * 1024     // 512MB pages to maximize compression ratio

	// Write 2M identical records — compresses extremely well with PLAIN encoding
	const numRecords = 2_000_000
	repeatedValue := "this is a repeated value that appears in every single row of the parquet file"
	for range numRecords {
		if err = pw.Write(Record{Value: repeatedValue}); err != nil {
			fmt.Printf("Write error: %v", err)
			return
		}
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Printf("WriteStop error: %v", err)
		return
	}
	_ = fw.Close()
}
