package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

type Student struct {
	Name   string
	Scores map[string]map[string]float32
}

var jsonSchema = `
{
  "Tag": "name=parquet_go_root, repetitiontype=REQUIRED",
  "Fields": [
    {
      "Tag": "name=name, inname=Name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"
    },
    {
      "Tag": "name=scores, inname=Scores, type=MAP, repetitiontype=REQUIRED",
      "Fields": [
        {
          "Tag": "name=key, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"
        },
        {
          "Tag": "name=value, type=MAP, repetitiontype=REQUIRED",
          "Fields": [
            {
              "Tag": "name=key, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"
            },
            {
              "Tag": "name=value, type=FLOAT, repetitiontype=REQUIRED"
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
	fw, err := local.NewLocalFileWriter("map-value-map.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		os.Exit(1)
	}

	// write
	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, jsonSchema,
		writer.WithNP(4),
		writer.WithRowGroupSize(128*1024*1024),
		writer.WithCompressionCodec(parquet.CompressionCodec_LZ4_RAW),
	)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		os.Exit(1)
	}

	for i := range 10 {
		stu := Student{
			Name: "StudentName" + strconv.Itoa(i+1),
			Scores: map[string]map[string]float32{
				"Math": {
					"mid-term": 80.0 + float32(i),
					"final":    70.0 + float32(i),
				},
				"Physics": {
					"mid-term": 90.0 + float32(i),
					"final":    75.0 + float32(i),
				},
			},
		}
		if err = pw.WriteWithContext(context.Background(), stu); err != nil {
			fmt.Println("Write error", err)
		}
	}
	if err = pw.WriteStopWithContext(context.Background()); err != nil {
		fmt.Println("WriteStop error", err)
		os.Exit(1)
	}
	_ = fw.Close()
}
