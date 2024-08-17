package main

import (
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type Student struct {
	Name   string
	Scores map[string]map[string]float32
}

var jsonSchema string = `
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
		log.Println("Can't create local file", err)
		return
	}

	// write
	pw, err := writer.NewParquetWriter(fw, jsonSchema, 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_LZ4_RAW
	for i := 0; i < 10; i++ {
		stu := Student{
			Name: "StudentName",
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
		if err = pw.Write(stu); err != nil {
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
