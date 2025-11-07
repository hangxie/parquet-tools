package main

import (
	"fmt"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
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

	pw, err := writer.NewParquetWriter(fw, new(Shoe), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	pw.CompressionType = parquet.CompressionCodec_GZIP

	// Create records with repeating brand values to benefit from dictionary encoding
	brands := []string{"nike", "adidas", "reebok"}
	shoes := []string{"air_jordan", "ultra_boost", "classic_leather", "suede_classic", "990v5"}
	for i, shoe := range shoes {
		shoe := Shoe{
			ShoeBrand: brands[i%len(brands)],
			ShoeName:  shoe,
		}
		if err = pw.Write(shoe); err != nil {
			fmt.Println("Write error", err)
			return
		}
	}

	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}
