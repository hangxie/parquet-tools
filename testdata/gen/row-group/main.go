package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

type Something struct {
	Brand string `parquet:"name=brand, type=BYTE_ARRAY, convertedtype=UTF8"`
	Name  string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func main() {
	fw, err := local.NewLocalFileWriter("row-group.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(Something),
		writer.WithNP(4),
		writer.WithRowGroupSize(256),
		writer.WithPageSize(32),
		writer.WithCompressionCodec(parquet.CompressionCodec_GZIP),
	)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	for i := range 20 {
		_ = pw.WriteWithContext(context.Background(), Something{"the brand is: " + strconv.Itoa(i), "the name is: " + strconv.Itoa(i)})
	}
	if err = pw.WriteStopWithContext(context.Background()); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}
