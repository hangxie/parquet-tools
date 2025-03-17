package main

import (
	"fmt"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func main() {
	good()
	optional()
	repeated()
	nested()
}

func good() {
	type Student struct {
		Id          int64   `parquet:"name=Id, type=INT64"`
		Name        string  `parquet:"name=Name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age         int32   `parquet:"name=Age, type=INT32"`
		Temperature float32 `parquet:"name=Temperature, type=FLOAT"`
		Vaccinated  bool    `parquet:"name=Vaccinated, type=BOOLEAN"`
	}

	fw, err := local.NewLocalFileWriter("csv-good.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	pw.CompressionType = parquet.CompressionCodec_GZIP
	_ = pw.Write(Student{123, "John Doe", 30, 98.2, false})
	_ = pw.Write(Student{123, "Jane Doe", 25, 98.7, true})
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}

func optional() {
	type Student struct {
		Id          int64    `parquet:"name=Id, type=INT64"`
		Name        string   `parquet:"name=Name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age         int32    `parquet:"name=Age, type=INT32"`
		Temperature *float32 `parquet:"name=Temperature, type=FLOAT, repetitiontype=OPTIONAL"`
		Vaccinated  bool     `parquet:"name=Vaccinated, type=BOOLEAN"`
	}

	fw, err := local.NewLocalFileWriter("csv-optional.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	pw.CompressionType = parquet.CompressionCodec_GZIP
	_ = pw.Write(Student{123, "John Doe", 30, nil, false})
	_ = pw.Write(Student{123, "Jane Doe", 25, nil, true})
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}

func repeated() {
	type Student struct {
		Id          int64     `parquet:"name=Id, type=INT64"`
		Name        string    `parquet:"name=Name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age         int32     `parquet:"name=Age, type=INT32"`
		Temperature []float32 `parquet:"name=Temperature, type=FLOAT, repetitiontype=REPEATED"`
		Vaccinated  bool      `parquet:"name=Vaccinated, type=BOOLEAN"`
	}

	fw, err := local.NewLocalFileWriter("csv-repeated.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	pw.CompressionType = parquet.CompressionCodec_GZIP
	_ = pw.Write(Student{123, "John Doe", 30, []float32{}, false})
	_ = pw.Write(Student{123, "Jane Doe", 25, []float32{98.1, 99.2}, true})
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}

func nested() {
	type Student struct {
		Id          int64     `parquet:"name=Id, type=INT64"`
		Name        string    `parquet:"name=Name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age         int32     `parquet:"name=Age, type=INT32"`
		Temperature []float32 `parquet:"name=Temperature, type=LIST, valuetype=FLOAT"`
		Vaccinated  bool      `parquet:"name=Vaccinated, type=BOOLEAN"`
	}

	fw, err := local.NewLocalFileWriter("csv-nested.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	pw.CompressionType = parquet.CompressionCodec_GZIP
	_ = pw.Write(Student{123, "John Doe", 30, []float32{}, false})
	_ = pw.Write(Student{123, "Jane Doe", 25, []float32{98.4, 99.3}, true})
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}
