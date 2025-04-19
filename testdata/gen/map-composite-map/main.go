package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/hangxie/parquet-go/parquet"
	"github.com/hangxie/parquet-go/source/local"
	"github.com/hangxie/parquet-go/writer"
)

type Personal struct {
	Name string
	Id   int64
}
type Student struct {
	Name     string
	Age      int32
	Id       int64
	Weight   float32
	Sex      bool
	Classes  []string
	Scores   map[string][]float32
	Friends  []Personal
	Teachers []Personal
}

var jsonSchema = `
{
  "Tag": "name=Parquet_go_root",
  "Fields": [
    {
      "Tag": "name=Name, type=BYTE_ARRAY, convertedtype=UTF8"
    },
    {
      "Tag": "name=Age, type=INT32"
    },
    {
      "Tag": "name=Id, type=INT64"
    },
    {
      "Tag": "name=Weight, type=FLOAT"
    },
    {
      "Tag": "name=Sex, type=BOOLEAN"
    },
    {
      "Tag": "name=Classes, type=LIST",
      "Fields": [
        {
          "Tag": "name=Element, type=BYTE_ARRAY, convertedtype=UTF8"
        }
      ]
    },
    {
      "Tag": "name=Scores, type=MAP",
      "Fields": [
        {
          "Tag": "name=Key, type=BYTE_ARRAY, convertedtype=UTF8"
        },
        {
          "Tag": "name=Value, type=LIST",
          "Fields": [
            {
              "Tag": "name=Element, type=FLOAT"
            }
          ]
        }
      ]
    },
    {
      "Tag": "name=Friends, type=LIST",
      "Fields": [
        {
          "Tag": "name=Element",
          "Fields": [
            {
              "Tag": "name=Name, type=BYTE_ARRAY, convertedtype=UTF8"
            },
            {
              "Tag": "name=Id, type=INT64"
            }
          ]
        }
      ]
    },
    {
      "Tag": "name=Teachers, repetitiontype=REPEATED",
      "Fields": [
        {
          "Tag": "name=Name, type=BYTE_ARRAY, convertedtype=UTF8"
        },
        {
          "Tag": "name=Id, type=INT64"
        }
      ]
    }
  ]
}
`

func main() {
	var err error
	fw, err := local.NewLocalFileWriter("map-composite-map.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		os.Exit(1)
	}

	// write
	pw, err := writer.NewParquetWriter(fw, jsonSchema, 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		os.Exit(1)
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_LZ4_RAW
	for i := 0; i < 10; i++ {
		stu := Student{
			Name:     "StudentName" + strconv.Itoa(i),
			Age:      int32(i + 20),
			Id:       int64(i * i),
			Weight:   float32(100 + i),
			Sex:      i%2 == 0,
			Classes:  []string{},
			Scores:   map[string][]float32{},
			Friends:  []Personal{},
			Teachers: []Personal{},
		}
		for j := 0; i < i%5; j++ {
			stu.Classes = append(stu.Classes, "class"+strconv.Itoa(j))
		}
		for j := i - 1; j > 0 && j-i < 5; j++ {
			stu.Friends = append(stu.Friends, Personal{
				"StudentName" + strconv.Itoa(j),
				int64(j * j),
			})
		}

		if err = pw.Write(stu); err != nil {
			fmt.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		os.Exit(1)
	}
	_ = fw.Close()
}
