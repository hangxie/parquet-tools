package main

import (
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
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
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	fw.Close()
}
