package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/hangxie/parquet-go/v3/writer"
)

func main() {
	const columns = 2048
	ctx := context.Background()
	file, err := os.Create("ultra-wide.parquet")
	if err != nil {
		log.Fatal(err)
	}

	fields := make([]string, columns)
	for i := range fields {
		fields[i] = fmt.Sprintf("name=c%d, type=INT32", i)
	}
	w, err := writer.NewCSVWriterFromWriterWithContext(ctx, fields, file, writer.WithNP(1))
	if err != nil {
		log.Fatal(err)
	}
	for r := range 2 {
		row := make([]any, columns)
		for i := range row {
			row[i] = int32(r*columns + i)
		}
		if err := w.WriteWithContext(ctx, row); err != nil {
			log.Fatal(err)
		}
	}
	if err := w.WriteStopWithContext(ctx); err != nil {
		log.Fatal(err)
	}
	if err := file.Close(); err != nil {
		log.Fatal(err)
	}
}
