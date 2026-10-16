package main

import (
	"context"
	"fmt"
	"os"

	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// Mimics pa.table({}) with rows: the schema root has no children but num_rows is set.
func main() {
	fw, err := local.NewLocalFileWriter("zero-column.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		os.Exit(1)
	}
	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(struct{}))
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		os.Exit(1)
	}
	for range 3 {
		if err = pw.WriteWithContext(context.Background(), struct{}{}); err != nil {
			fmt.Println("Write error", err)
			os.Exit(1)
		}
	}
	if err = pw.WriteStopWithContext(context.Background()); err != nil {
		fmt.Println("WriteStop error", err)
		os.Exit(1)
	}
	_ = fw.Close()
}
