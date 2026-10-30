package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// Every value repeats the previous one in full, so DELTA_BYTE_ARRAY stores it as
// a prefix length alone; the reader shares one copy, so 300 MB of values fit the batch budget.
type Record struct {
	Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=DELTA_BYTE_ARRAY, omitstats=true"`
}

func main() {
	path := "delta-byte-array-repeat.parquet"

	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		fmt.Printf("Can't create file: %v", err)
		return
	}

	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(Record),
		writer.WithNP(1),
		writer.WithDataPageVersion(2),
		writer.WithCompressionCodec(parquet.CompressionCodec_ZSTD),
		writer.WithPageSize(512*1024*1024),
	)
	if err != nil {
		fmt.Printf("Can't create writer: %v", err)
		return
	}

	value := strings.Repeat("x", 300*1024)
	for range 1024 {
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
