package io

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/writer"
)

// PipelineWriter reads rows from writerChan and writes them to fileWriter.
func PipelineWriter(ctx context.Context, fileWriter *writer.ParquetWriter, writerChan chan any, target string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case row, more := <-writerChan:
			if !more {
				return nil
			}
			if err := fileWriter.Write(row); err != nil {
				return fmt.Errorf("failed to write data to [%s]: %w", target, err)
			}
		}
	}
}

// PipelineReader reads rows from fileReader in batches, optionally transforms each row,
// and sends them to writerChan. Pass nil for transform to skip transformation.
func PipelineReader(ctx context.Context, fileReader *reader.ParquetReader, writerChan chan any, source string, pageSize int, transform func(any) (any, error)) error {
	for {
		rows, err := fileReader.ReadByNumber(pageSize)
		if err != nil {
			return fmt.Errorf("failed to read from [%s]: %w", source, err)
		}
		if len(rows) == 0 {
			return nil
		}
		for _, row := range rows {
			if transform != nil {
				row, err = transform(row)
				if err != nil {
					return fmt.Errorf("failed to convert row: %w", err)
				}
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case writerChan <- row:
			}
		}
	}
}
