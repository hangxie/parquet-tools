package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// Every value extends the previous one by 512 bytes, so DELTA_BYTE_ARRAY stores
// only the suffix and one small page decodes to over 256 MB of distinct values.
type Record struct {
	Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=DELTA_BYTE_ARRAY, omitstats=true"`
}

func main() {
	path := "delta-byte-array-bomb.parquet"

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

	for i := range 1024 {
		if err = pw.WriteWithContext(context.Background(), Record{Value: strings.Repeat("x", (i+1)*512)}); err != nil {
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
