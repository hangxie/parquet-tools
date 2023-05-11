package main

import (
	"fmt"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type AllTypes struct {
	Utf8  string  `parquet:"name=Utf8, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Int96 *string `parquet:"name=Int96, type=INT96"`
}

func main() {
	fw, err := local.NewLocalFileWriter("int96-nil-min-max.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(AllTypes), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	for i := 0; i < 10; i++ {
		value := AllTypes{
			Int96: nil,
			Utf8:  fmt.Sprintf("UTF8-%d", i),
		}
		if err = pw.Write(value); err != nil {
			fmt.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	fw.Close()
}
