package main

import (
	"log"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
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
	pw, err := writer.NewParquetWriter(fw, new(Dummy), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	_ = fw.Close()
}
