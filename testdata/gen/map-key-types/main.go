package main

import (
	"context"
	"log"

	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// The spec lets a MAP key be any required type; readers disagree on everything but strings.

type Point struct {
	X int32 `parquet:"name=x, type=INT32"`
	Y int32 `parquet:"name=y, type=INT32"`
}

type MapKeyTypes struct {
	StringKey map[string]int32 `parquet:"name=string_key, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	BinaryKey map[string]int32 `parquet:"name=binary_key, type=MAP, keytype=BYTE_ARRAY, valuetype=INT32"`
	IntKey    map[int32]string `parquet:"name=int_key, type=MAP, keytype=INT32, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	StructKey map[Point]string `parquet:"name=struct_key, type=MAP, keytype=STRUCT, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

func main() {
	file, err := local.NewLocalFileWriter("map-key-types.parquet")
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}

	pw, err := writer.NewParquetWriterWithContext(context.Background(), file, new(MapKeyTypes), writer.WithNP(1))
	if err != nil {
		log.Fatalf("Failed to create parquet writer: %v", err)
	}

	// one entry per map keeps the output independent of Go's map iteration order
	rows := []MapKeyTypes{
		{
			StringKey: map[string]int32{"a": 1},
			BinaryKey: map[string]int32{"\x00\xff": 1},
			IntKey:    map[int32]string{1: "one"},
			StructKey: map[Point]string{{X: 1, Y: 2}: "p12"},
		},
		{
			StringKey: map[string]int32{"b": 2},
			BinaryKey: map[string]int32{"\x01": 2},
			IntKey:    map[int32]string{2: "two"},
			StructKey: map[Point]string{{X: 3, Y: 4}: "p34"},
		},
	}
	for _, row := range rows {
		if err := pw.WriteWithContext(context.Background(), row); err != nil {
			log.Fatalf("Failed to write data: %v", err)
		}
	}

	if err := pw.WriteStopWithContext(context.Background()); err != nil {
		log.Fatalf("Failed to close parquet writer: %v", err)
	}

	if err := file.Close(); err != nil {
		log.Fatalf("Failed to close file: %v", err)
	}
}
