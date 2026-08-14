package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// Lists of varying length make the column's value count differ from its row count.
type Something struct {
	Id    int32    `parquet:"name=id, type=INT32"`
	Tags  []string `parquet:"name=tags, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	Brand string   `parquet:"name=brand, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func main() {
	if err := run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run() error {
	fw, err := local.NewLocalFileWriter("repeated-row-group.parquet")
	if err != nil {
		return fmt.Errorf("create local file: %w", err)
	}

	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(Something),
		writer.WithNP(1),
		writer.WithPageSize(64),
		writer.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	if err != nil {
		return fmt.Errorf("create parquet writer: %w", err)
	}

	for i := range 12 {
		tags := make([]string, i%3+1)
		for j := range tags {
			// Long enough that a row group holds more than one page of tags.
			tags[j] = "tag-" + strconv.Itoa(i) + "-" + strconv.Itoa(j) + strings.Repeat("-pad", 8)
		}
		value := Something{
			Id:    int32(i),
			Tags:  tags,
			Brand: "the brand is: " + strconv.Itoa(i),
		}
		if err := pw.WriteWithContext(context.Background(), value); err != nil {
			return fmt.Errorf("write row %d: %w", i, err)
		}
		// Explicit boundaries keep the fixture's shape independent of writer sizing.
		if i == 4 || i == 8 {
			if err := pw.FlushWithContext(context.Background(), true); err != nil {
				return fmt.Errorf("flush row group at row %d: %w", i, err)
			}
		}
	}
	if err := pw.WriteStopWithContext(context.Background()); err != nil {
		return fmt.Errorf("stop writer: %w", err)
	}
	if err := fw.Close(); err != nil {
		return fmt.Errorf("close local file: %w", err)
	}
	return nil
}
