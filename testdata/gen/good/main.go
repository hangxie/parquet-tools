package main

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

type Shoe struct {
	ShoeBrand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8"`
	ShoeName  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8"`
}

var shoes = []Shoe{
	{"nike", "air_griffey"},
	{"fila", "grant_hill_2"},
	{"steph_curry", "curry7"},
}

func writeFile(filename string, codec parquet.CompressionCodec) error {
	fw, err := local.NewLocalFileWriter(filename)
	if err != nil {
		return fmt.Errorf("can't create local file %s: %w", filename, err)
	}

	pw, err := writer.NewParquetWriterWithContext(
		context.Background(),
		fw,
		new(Shoe),
		writer.WithNP(4),
		writer.WithCompressionCodec(codec),
	)
	if err != nil {
		_ = fw.Close()
		return fmt.Errorf("can't create parquet writer for %s: %w", filename, err)
	}

	for _, s := range shoes {
		_ = pw.WriteWithContext(context.Background(), s)
	}
	if err = pw.WriteStopWithContext(context.Background()); err != nil {
		return fmt.Errorf("WriteStop error for %s: %w", filename, err)
	}
	_ = fw.Close()
	return nil
}

func main() {
	files := []struct {
		name  string
		codec parquet.CompressionCodec
	}{
		{"good.parquet", parquet.CompressionCodec_GZIP},
		{"good-snappy.parquet", parquet.CompressionCodec_SNAPPY},
	}

	for _, f := range files {
		if err := writeFile(f.name, f.codec); err != nil {
			fmt.Println(err)
			return
		}
	}
}
