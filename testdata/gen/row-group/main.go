package main

import (
	"fmt"
	"strconv"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
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

	pw, err := writer.NewParquetWriter(fw, new(Something), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}
	pw.RowGroupSize = 256
	pw.PageSize = 32
	pw.CompressionType = parquet.CompressionCodec_GZIP

	for i := range 20 {
		_ = pw.Write(Something{"the brand is: " + strconv.Itoa(i), "the name is: " + strconv.Itoa(i)})
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}
