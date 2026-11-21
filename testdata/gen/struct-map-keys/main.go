package main

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// Struct MAP keys that nest a struct or hold byte-backed logical types, whose JSON form
// differs between interpreted and raw value modes.

type Inner struct {
	X   int32  `parquet:"name=x, type=INT32"`
	Doc string `parquet:"name=doc, type=BYTE_ARRAY, convertedtype=BSON"`
}

type NestedKey struct {
	ID    int32 `parquet:"name=id, type=INT32"`
	Inner Inner `parquet:"name=inner"`
}

type BytesKey struct {
	Raw     string `parquet:"name=raw, type=BYTE_ARRAY"`
	Text    string `parquet:"name=text, type=BYTE_ARRAY, convertedtype=UTF8"`
	Doc     string `parquet:"name=doc, type=BYTE_ARRAY, logicaltype=BSON"`
	Json    string `parquet:"name=json, type=BYTE_ARRAY, convertedtype=JSON"`
	Amount  string `parquet:"name=amount, type=BYTE_ARRAY, convertedtype=DECIMAL, precision=10, scale=2"`
	Fixed   string `parquet:"name=fixed, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, precision=10, scale=2, length=8"`
	Uuid    string `parquet:"name=uuid, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID"`
	Float16 string `parquet:"name=float16, type=FIXED_LEN_BYTE_ARRAY, length=2, logicaltype=FLOAT16"`
}

type StructMapKeys struct {
	NestedKey map[NestedKey]string `parquet:"name=nested_key, type=MAP, keytype=STRUCT, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	BytesKey  map[BytesKey]string  `parquet:"name=bytes_key, type=MAP, keytype=STRUCT, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

func mustBSON(doc bson.D) string {
	data, err := bson.Marshal(doc)
	if err != nil {
		log.Fatalf("Failed to marshal BSON: %v", err)
	}
	return string(data)
}

func main() {
	file, err := local.NewLocalFileWriter("struct-map-keys.parquet")
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}

	pw, err := writer.NewParquetWriterWithContext(context.Background(), file, new(StructMapKeys), writer.WithNP(1))
	if err != nil {
		log.Fatalf("Failed to create parquet writer: %v", err)
	}

	// one entry per map keeps the output independent of Go's map iteration order
	rows := []StructMapKeys{
		{
			NestedKey: map[NestedKey]string{{ID: 1, Inner: Inner{X: 2, Doc: mustBSON(bson.D{{Key: "a", Value: int32(1)}})}}: "n1"},
			BytesKey: map[BytesKey]string{{
				Raw:     "\x00\xff",
				Text:    `{"x":1}`,
				Doc:     mustBSON(bson.D{{Key: "b", Value: "two"}}),
				Json:    `{"k":[1,2]}`,
				Amount:  "\x30\x39",                         // 123.45
				Fixed:   "\xff\xff\xff\xff\xff\xff\xff\x85", // -1.23
				Uuid:    "\x12\x34\x56\x78\x9a\xbc\xde\xf0\x12\x34\x56\x78\x9a\xbc\xde\xf0",
				Float16: "\x00\x3c", // 1.0
			}: "b1"},
		},
		{
			NestedKey: map[NestedKey]string{{ID: 1, Inner: Inner{X: 3, Doc: mustBSON(bson.D{{Key: "a", Value: int32(2)}})}}: "n2"},
			BytesKey: map[BytesKey]string{{
				Raw:     "\x01",
				Text:    "plain",
				Doc:     mustBSON(bson.D{}),
				Json:    `"s"`,
				Amount:  "\xff", // -0.01
				Fixed:   "\x00\x00\x00\x00\x00\x00\x00\x00",
				Uuid:    "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01",
				Float16: "\x00\xc0", // -2.0
			}: "b2"},
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
