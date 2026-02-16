package main

import (
	"fmt"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
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

	pw, err := writer.NewParquetWriter(fw, new(Shoe), 4)
	if err != nil {
		_ = fw.Close()
		return fmt.Errorf("can't create parquet writer for %s: %w", filename, err)
	}

	pw.CompressionType = codec
	for _, s := range shoes {
		_ = pw.Write(s)
	}
	if err = pw.WriteStop(); err != nil {
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
