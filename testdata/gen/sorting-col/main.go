package main

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

type student struct {
	Name   string  `parquet:"name=Name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age    int32   `parquet:"name=Age, type=INT32"`
	ID     int64   `parquet:"name=Id, type=INT64"`
	Weight float32 `parquet:"name=Weight, type=FLOAT"`
	Sex    bool    `parquet:"name=Sex, type=BOOLEAN"`
}

func students() []student {
	rows := make([]student, 0, 20)
	for i := range 10 {
		row := student{
			Age:    int32(20 + i%5),
			ID:     int64(i),
			Weight: 50 + float32(i)/10,
			Sex:    i%2 == 0,
		}
		row.Name = fmt.Sprintf("Student Name_%d", i)
		rows = append(rows, row)
		row.Name = "Student Name"
		rows = append(rows, row)
	}
	return rows
}

func writeSortingColumns(filename string) error {
	ctx := context.Background()
	fw, err := local.NewLocalFileWriter(filename)
	if err != nil {
		return fmt.Errorf("create %s: %w", filename, err)
	}

	pw, err := writer.NewParquetWriterWithContext(ctx, fw, new(student),
		writer.WithNP(1),
		writer.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		writer.WithSortingColumns(
			&parquet.SortingColumn{ColumnIdx: 0, Descending: true},
			&parquet.SortingColumn{ColumnIdx: 1},
		),
	)
	if err != nil {
		_ = fw.Close()
		return fmt.Errorf("create parquet writer: %w", err)
	}

	for _, row := range students() {
		if err := pw.WriteWithContext(ctx, row); err != nil {
			_ = fw.Close()
			return fmt.Errorf("write row: %w", err)
		}
	}
	if err := pw.WriteStopWithContext(ctx); err != nil {
		_ = fw.Close()
		return fmt.Errorf("finish parquet writer: %w", err)
	}
	if err := fw.Close(); err != nil {
		return fmt.Errorf("close %s: %w", filename, err)
	}
	return nil
}

func main() {
	if err := writeSortingColumns("sorting-col.parquet"); err != nil {
		panic(err)
	}
}
