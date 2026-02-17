package io

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/writer"
	"golang.org/x/sync/errgroup"
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

// RunPipeline runs a reader and writer in parallel using errgroup. The reader sends rows
// through an internal channel to the writer. If either side fails, the shared context is
// cancelled so the other side exits promptly.
func RunPipeline(fileReader *reader.ParquetReader, fileWriter *writer.ParquetWriter, source, target string, pageSize int, transform func(any) (any, error)) error {
	g, gctx := errgroup.WithContext(context.Background())
	writerChan := make(chan any)

	g.Go(func() error {
		return PipelineWriter(gctx, fileWriter, writerChan, target)
	})

	g.Go(func() error {
		defer close(writerChan)
		return PipelineReader(gctx, fileReader, writerChan, source, pageSize, transform)
	})

	return g.Wait()
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
