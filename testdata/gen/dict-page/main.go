package main

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

type Shoe struct {
	ShoeBrand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ShoeName  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func main() {
	fw, err := local.NewLocalFileWriter("dict-page.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(Shoe),
		writer.WithNP(4),
		writer.WithCompressionCodec(parquet.CompressionCodec_GZIP),
	)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	// Create records with repeating brand values to benefit from dictionary encoding
	brands := []string{"nike", "adidas", "reebok"}
	shoes := []string{"air_jordan", "ultra_boost", "classic_leather", "suede_classic", "990v5"}
	for i, shoe := range shoes {
		shoe := Shoe{
			ShoeBrand: brands[i%len(brands)],
			ShoeName:  shoe,
		}
		if err = pw.WriteWithContext(context.Background(), shoe); err != nil {
			fmt.Println("Write error", err)
			return
		}
	}

	if err = pw.WriteStopWithContext(context.Background()); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}
