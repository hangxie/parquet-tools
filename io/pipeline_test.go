package io

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
	"github.com/stretchr/testify/require"
)

func newTestReader(t *testing.T, path string) *reader.ParquetReader {
	t.Helper()
	fr, err := local.NewLocalFileReader(path)
	require.NoError(t, err)
	pr, err := reader.NewParquetReader(fr, nil, int64(runtime.NumCPU()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = pr.PFile.Close() })
	return pr
}

func newTestWriter(t *testing.T, path, schema string) *writer.ParquetWriter {
	t.Helper()
	fw, err := local.NewLocalFileWriter(path)
	require.NoError(t, err)
	pw, err := writer.NewParquetWriter(fw, schema, int64(runtime.NumCPU()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pw.WriteStop()
		_ = pw.PFile.Close()
	})
	return pw
}

func TestPipelineWriter(t *testing.T) {
	schema := `{"Tag":"name=root","Fields":[{"Tag":"name=id, type=INT64"}]}`

	t.Run("success", func(t *testing.T) {
		tempDir := t.TempDir()
		pw := newTestWriter(t, filepath.Join(tempDir, "out.parquet"), schema)
		writerChan := make(chan any, 2)

		type row struct {
			Id int64 `parquet:"name=id, type=INT64"`
		}
		writerChan <- &row{Id: 1}
		writerChan <- &row{Id: 2}
		close(writerChan)

		err := PipelineWriter(context.Background(), pw, writerChan, "test-target")
		require.NoError(t, err)
	})

	t.Run("context-cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		writerChan := make(chan any)
		err := PipelineWriter(ctx, nil, writerChan, "test-target")
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("write-error", func(t *testing.T) {
		tempDir := t.TempDir()
		pw := newTestWriter(t, filepath.Join(tempDir, "out.parquet"), schema)

		// Stop the writer first so writes will fail
		_ = pw.WriteStop()

		writerChan := make(chan any, 1)
		type row struct {
			Id int64 `parquet:"name=id, type=INT64"`
		}
		writerChan <- &row{Id: 1}
		close(writerChan)

		err := PipelineWriter(context.Background(), pw, writerChan, "test-target")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to write data to [test-target]")
	})
}

func TestPipelineReader(t *testing.T) {
	goodParquet := filepath.Join("..", "testdata", "good.parquet")

	t.Run("success-no-transform", func(t *testing.T) {
		pr := newTestReader(t, goodParquet)
		writerChan := make(chan any, 100)

		err := PipelineReader(context.Background(), pr, writerChan, "good.parquet", 10, nil)
		require.NoError(t, err)
		close(writerChan)

		var rows []any
		for row := range writerChan {
			rows = append(rows, row)
		}
		require.Equal(t, 3, len(rows))
	})

	t.Run("success-with-transform", func(t *testing.T) {
		pr := newTestReader(t, goodParquet)
		writerChan := make(chan any, 100)

		callCount := 0
		transform := func(row any) (any, error) {
			callCount++
			return row, nil
		}

		err := PipelineReader(context.Background(), pr, writerChan, "good.parquet", 10, transform)
		require.NoError(t, err)
		close(writerChan)

		var rows []any
		for row := range writerChan {
			rows = append(rows, row)
		}
		require.Equal(t, 3, len(rows))
		require.Equal(t, 3, callCount)
	})

	t.Run("transform-error", func(t *testing.T) {
		pr := newTestReader(t, goodParquet)
		writerChan := make(chan any, 100)

		transform := func(row any) (any, error) {
			return nil, fmt.Errorf("transform failed")
		}

		err := PipelineReader(context.Background(), pr, writerChan, "good.parquet", 10, transform)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to convert row")
	})

	t.Run("context-cancelled", func(t *testing.T) {
		pr := newTestReader(t, goodParquet)
		// Unbuffered channel so the send blocks
		writerChan := make(chan any)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := PipelineReader(ctx, pr, writerChan, "good.parquet", 10, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestRunPipeline(t *testing.T) {
	goodParquet := filepath.Join("..", "testdata", "good.parquet")
	schema := `{"Tag":"name=root","Fields":[{"Tag":"name=id, type=INT64"}]}`

	t.Run("writer-failure", func(t *testing.T) {
		pr := newTestReader(t, goodParquet)
		tempDir := t.TempDir()
		pw := newTestWriter(t, filepath.Join(tempDir, "out.parquet"), schema)
		// Stop the writer so writes fail immediately.
		_ = pw.WriteStop()

		err := RunPipeline(pr, pw, "good.parquet", "test-target", 1, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to write data to [test-target]")
	})

	t.Run("reader-failure", func(t *testing.T) {
		pr := newTestReader(t, goodParquet)
		tempDir := t.TempDir()
		pw := newTestWriter(t, filepath.Join(tempDir, "out.parquet"), schema)

		err := RunPipeline(pr, pw, "good.parquet", "test-target", 1, func(any) (any, error) {
			return nil, fmt.Errorf("injected reader failure")
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "injected reader failure")
	})
}
