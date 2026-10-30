package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// 3,000 values in one page as runs of 700 identical 1,000-byte values, so runs of
// shared values cross the reader's 1,024-value batches.
type Record struct {
	Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=DELTA_BYTE_ARRAY, omitstats=true"`
}

func main() {
	path := "delta-byte-array-runs.parquet"

	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		fmt.Printf("Can't create file: %v", err)
		return
	}

	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(Record),
		writer.WithNP(1),
		writer.WithDataPageVersion(2),
		writer.WithCompressionCodec(parquet.CompressionCodec_ZSTD),
		writer.WithPageSize(64*1024*1024),
	)
	if err != nil {
		fmt.Printf("Can't create writer: %v", err)
		return
	}

	for i := range 3000 {
		value := strings.Repeat(string(rune('a'+i/700)), 1000)
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
