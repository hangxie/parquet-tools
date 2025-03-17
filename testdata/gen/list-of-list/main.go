package main

import (
	"fmt"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type RecordType struct {
	Lol [][]string
}

var jsonSchema = `
{
  "Tag": "name=parquet_go_root, repetitiontype=REQUIRED",
  "Fields": [
    {
      "Tag": "name=lol, inname=Lol, type=LIST, repetitiontype=REQUIRED",
      "Fields": [
	    {
		  "Tag": "name=element, type=LIST, repetitiontype=REQUIRED",
		  "Fields": [
		    {
			  "Tag": "name=element, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"
			}
		  ]
		}
	  ]
    }
  ]
}
`

func main() {
	var err error
	fw, err := local.NewLocalFileWriter("list-of-list.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		os.Exit(1)
	}

	// write
	pw, err := writer.NewParquetWriter(fw, jsonSchema, 1)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		os.Exit(1)
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_LZ4
	for i := 0; i < 5; i++ {
		rec := RecordType{
			Lol: make([][]string, i+1),
		}
		for j := 0; j <= i; j++ {
			rec.Lol[j] = make([]string, j+1)
			for k := 0; k <= j; k++ {
				rec.Lol[j][k] = fmt.Sprintf("%d-%d-%d", i+1, j+1, k+1)
			}
		}
		if err = pw.Write(rec); err != nil {
			fmt.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		os.Exit(1)
	}
	_ = fw.Close()
}
