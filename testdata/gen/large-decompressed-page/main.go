package main

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// One ZSTD page of repeated PLAIN values decompresses past the reader's
// 256 MB default while the file itself stays tiny.
type Record struct {
	Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
}

func main() {
	path := "large-decompressed-page.parquet"

	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		fmt.Printf("Can't create file: %v", err)
		return
	}

	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(Record),
		writer.WithNP(1),
		writer.WithCompressionCodec(parquet.CompressionCodec_ZSTD),
		writer.WithRowGroupSize(512*1024*1024),
		writer.WithPageSize(512*1024*1024),
	)
	if err != nil {
		fmt.Printf("Can't create writer: %v", err)
		return
	}

	// 3.5M values of 79 bytes plus a 4-byte length each come to about 290 MB.
	const numRecords = 3_500_000
	value := "this value repeats in every row so the page compresses to almost nothing at all"
	for range numRecords {
		if err = pw.WriteWithContext(context.Background(), Record{Value: value}); err != nil {
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
