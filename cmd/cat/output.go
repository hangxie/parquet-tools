package cat

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

// maxOutputBytes caps, across all encoders, the buffer capacity queued for the
// printer and, separately, the idle capacity kept for reuse.
const maxOutputBytes = 64 << 20

type outputBudget struct {
	sem      *semaphore.Weighted
	size     int64
	retained atomic.Int64
}

func newOutputBudget(size int64) *outputBudget {
	return &outputBudget{sem: semaphore.NewWeighted(size), size: size}
}

// acquire caps n at the budget, so a row larger than the budget can still go out alone.
func (b *outputBudget) acquire(ctx context.Context, n int) (int64, error) {
	weight := min(int64(n), b.size)
	return weight, b.sem.Acquire(ctx, weight)
}

func (b *outputBudget) release(weight int64) {
	b.sem.Release(weight)
}

// retain reports whether an idle buffer of capacity n fits what is already kept.
func (b *outputBudget) retain(n int) bool {
	if b.retained.Add(int64(n)) <= b.size {
		return true
	}
	b.retained.Add(-int64(n))
	return false
}

// reclaim returns a kept buffer's share when its formatter leaves the pool.
func (b *outputBudget) reclaim(n int) {
	b.retained.Add(-int64(n))
}

type encodedRow struct {
	buffer  *bytes.Buffer
	release func()
}

func (c Cmd) printer(ctx context.Context, outputChan chan encodedRow, writer io.Writer, header string) (err error) {
	if err := writeString(writer, delimiter[c.Format].begin+header); err != nil {
		return err
	}
	defer func() {
		endErr := writeString(writer, delimiter[c.Format].end+"\n")
		if err == nil {
			err = endErr
		}
	}()
	first := true
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case row, more := <-outputChan:
			if !more {
				return nil
			}
			if !first {
				if err := writeString(writer, delimiter[c.Format].lineDelimiter); err != nil {
					return err
				}
			}
			first = false
			if _, err := row.buffer.WriteTo(writer); err != nil {
				return err
			}
			row.release()
		}
	}
}

func (c Cmd) bufferedPrinter(ctx context.Context, rows chan encodedRow, output io.Writer, header string) error {
	writer := bufio.NewWriter(output)
	err := c.printer(ctx, rows, writer, header)
	if flushErr := writer.Flush(); err == nil {
		err = flushErr
	}
	return err
}

// writeString reports short writes, which io.Writer permits without an error.
func writeString(writer io.Writer, s string) error {
	if s == "" {
		return nil
	}
	n, err := io.WriteString(writer, s)
	if err == nil && n < len(s) {
		err = io.ErrShortWrite
	}
	return err
}
