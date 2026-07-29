package main

import (
	"context"
	"log"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

type Dummy struct {
	Dummy int32 `parquet:"name=dummy, type=INT32"`
}

func main() {
	fw, err := local.NewLocalFileWriter("empty.parquet")
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(Dummy),
		writer.WithNP(4),
		writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
	)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	if err = pw.WriteStopWithContext(context.Background()); err != nil {
		log.Println("WriteStop error", err)
	}
	_ = fw.Close()
}
