package main

import (
	"fmt"
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type RecordType struct {
	Lol [][]string
}

var jsonSchema string = `
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
		log.Println("Can't create local file", err)
		return
	}

	// write
	pw, err := writer.NewParquetWriter(fw, jsonSchema, 1)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
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
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	fw.Close()
}
