package main

import (
	"fmt"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type Student1 struct {
	Name string `parquet:"name=Name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Id   int64  `parquet:"name=Id, type=INT64"`
}

type Student2 struct {
	Name string `parquet:"name=NaMe, type=BYTE_ARRAY, convertedtype=UTF8"`
	Id   int64  `parquet:"name=ID, type=INT64"`
}

func main() {
	if err := genParquet("case-sensitive1.parquet", Student1{"student1", 111}); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := genParquet("case-sensitive2.parquet", Student2{"student2", 222}); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func genParquet[T any](name string, student T) error {
	var err error
	fw, err := local.NewLocalFileWriter(name)
	if err != nil {
		return fmt.Errorf("Can't create local file: %w", err)
	}

	// write
	pw, err := writer.NewParquetWriter(fw, new(T), 4)
	if err != nil {
		return fmt.Errorf("Can't create parquet writer: %w", err)
		os.Exit(1)
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_LZ4_RAW
	if err = pw.Write(student); err != nil {
		return fmt.Errorf("Write error: %w", err)
	}
	if err = pw.WriteStop(); err != nil {
		return fmt.Errorf("WriteStop error; %w", err)
	}
	fw.Close()

	return nil
}
