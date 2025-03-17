package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type Student struct {
	Name string
	Id   int64
}

const jsonSchema1 string = `
{
  "Tag": "name=top_level_tag1",
  "Fields": [
    {
      "Tag": "name=Name, type=BYTE_ARRAY, convertedtype=UTF8"
    },
    {
      "Tag": "name=Id, type=INT64"
    }
  ]
}
`

const jsonSchema2 string = `
{
  "Tag": "name=top_level_tag2",
  "Fields": [
    {
      "Tag": "name=Name, type=BYTE_ARRAY, convertedtype=UTF8"
    },
    {
      "Tag": "name=Id, type=INT64"
    }
  ]
}
`

func main() {
	if err := genParquet("top-level-tag1.parquet", jsonSchema1); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := genParquet("top-level-tag2.parquet", jsonSchema2); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func genParquet(name, jsonSchema string) error {
	var err error
	fw, err := local.NewLocalFileWriter(name)
	if err != nil {
		return fmt.Errorf("cannot create local file: %w", err)
	}

	// write
	pw, err := writer.NewParquetWriter(fw, jsonSchema, 4)
	if err != nil {
		return fmt.Errorf("cannot create parquet writer: %w", err)
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_LZ4_RAW
	for i := 0; i < 3; i++ {
		stu := Student{
			Name: "StudentName" + strconv.Itoa(i),
			Id:   int64(i * i),
		}

		if err = pw.Write(stu); err != nil {
			return fmt.Errorf("error from Write(): %w", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		return fmt.Errorf("error from WriteStop(): %w", err)
	}
	_ = fw.Close()

	return nil
}
