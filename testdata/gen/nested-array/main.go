package main

import (
	"context"
	"fmt"
	"os"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// Repeated group and leaf both named array: without a LIST annotation this is a
// plain nested list, with one it is a parquet-mr two-level list.

type Inner struct {
	Array []int32 `parquet:"name=array, type=INT32, repetitiontype=REPEATED"`
}

type Row struct {
	Array []Inner `parquet:"name=array, repetitiontype=REPEATED"`
}

var rows = []Row{
	{Array: []Inner{{Array: []int32{1, 2}}, {Array: []int32{3, 4}}}},
	{Array: []Inner{{Array: []int32{5}}, {}, {Array: []int32{6, 7, 8}}}},
}

func main() {
	fixtures := []struct {
		name     string
		annotate func(*parquet.SchemaElement)
	}{
		{"nested-array.parquet", func(*parquet.SchemaElement) {}},
		// Logical type only, no converted type, as some newer writers emit.
		{"nested-array-logical-list.parquet", func(e *parquet.SchemaElement) {
			e.LogicalType = &parquet.LogicalType{LIST: parquet.NewListType()}
		}},
	}
	for _, f := range fixtures {
		if err := write(f.name, f.annotate); err != nil {
			fmt.Println("write error:", err)
			os.Exit(1)
		}
	}
}

func write(name string, annotate func(*parquet.SchemaElement)) error {
	fw, err := local.NewLocalFileWriter(name)
	if err != nil {
		return err
	}
	defer func() { _ = fw.Close() }()
	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(Row),
		writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
	)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := pw.WriteWithContext(context.Background(), row); err != nil {
			return err
		}
	}
	// Two-level levels match the unannotated nesting, so only the footer differs.
	group := *pw.Footer.Schema[1]
	annotate(&group)
	pw.Footer.Schema[1] = &group
	return pw.WriteStopWithContext(context.Background())
}
