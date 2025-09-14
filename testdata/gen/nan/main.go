package main

import (
	"log"
	"math"

	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
)

type Data struct {
	Value float64 `parquet:"name=value, type=DOUBLE"`
}

func main() {
	file, err := local.NewLocalFileWriter("nan.parquet")
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}

	pw, err := writer.NewParquetWriter(file, new(Data), 1)
	if err != nil {
		log.Fatalf("Failed to create parquet writer: %v", err)
	}

	data := Data{
		Value: math.NaN(),
	}

	if err := pw.Write(data); err != nil {
		log.Fatalf("Failed to write data: %v", err)
	}

	if err := pw.WriteStop(); err != nil {
		log.Fatalf("Failed to close parquet writer: %v", err)
	}

	if err := file.Close(); err != nil {
		log.Fatalf("Failed to close file: %v", err)
	}
}
