package cat

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrinter(t *testing.T) {
	for _, tc := range []struct{ format, header, want string }{
		{"json", "", "[one,two]\n"},
		{"jsonl", "", "one\ntwo\n"},
		{"csv", "h1,h2\n", "h1,h2\none\ntwo\n"},
		{"tsv", "h1\th2\n", "h1\th2\none\ntwo\n"},
		{"csv", "", "one\ntwo\n"},
	} {
		t.Run(tc.format+"/"+tc.header, func(t *testing.T) {
			rows := make(chan encodedRow, 2)
			released := 0
			release := func() { released++ }
			rows <- encodedRow{bytes.NewBufferString("one"), release}
			rows <- encodedRow{bytes.NewBufferString("two"), release}
			close(rows)
			var output bytes.Buffer
			require.NoError(t, (Cmd{Format: tc.format}).printer(context.Background(), rows, &output, tc.header))
			require.Equal(t, tc.want, output.String())
			require.Equal(t, 2, released)
		})
	}
}

type failingOutput struct {
	calls, failAt int
	err           error
}

func (w *failingOutput) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		return 0, w.err
	}
	return len(p), nil
}

func TestPrinterWriteErrors(t *testing.T) {
	writeErr := errors.New("write failed")
	for _, tc := range []struct {
		name   string
		format string
		header string
		failAt int
	}{
		{"begin", "json", "", 1},
		{"row", "json", "", 2},
		{"delimiter", "json", "", 3},
		{"second-row", "json", "", 4},
		{"end", "json", "", 5},
		{"csv-header", "csv", "h\n", 1},
	} {
		// A nil error makes failingOutput report a short write.
		for _, want := range []error{writeErr, io.ErrShortWrite} {
			t.Run(tc.name+"/"+want.Error(), func(t *testing.T) {
				rows := make(chan encodedRow, 2)
				for range 2 {
					rows <- encodedRow{bytes.NewBufferString("row"), func() {}}
				}
				close(rows)
				writer := &failingOutput{failAt: tc.failAt}
				if want != io.ErrShortWrite {
					writer.err = want
				}
				require.ErrorIs(t, (Cmd{Format: tc.format}).printer(context.Background(), rows, writer, tc.header), want)
			})
		}
	}
}

func TestPrinterCancellation(t *testing.T) {
	for _, duringWrite := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-row", true: "during-write"}[duringWrite], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// Never closed, so the printer can only leave its wait through cancellation.
			rows := make(chan encodedRow, 1)
			if duringWrite {
				rows <- encodedRow{bytes.NewBufferString("row"), func() {}}
			} else {
				cancel()
			}
			writer := &cancelOutput{cancel: cancel}
			require.ErrorIs(t, (Cmd{Format: "json"}).printer(ctx, rows, writer, ""), context.Canceled)
		})
	}
}

type cancelOutput struct{ cancel context.CancelFunc }

func (w *cancelOutput) Write(p []byte) (int, error) {
	if string(p) == "row" {
		w.cancel()
	}
	return len(p), nil
}

func TestBufferedPrinterFlushError(t *testing.T) {
	rows := make(chan encodedRow)
	close(rows)
	want := errors.New("flush failed")
	writer := &failingOutput{failAt: 1, err: want}
	require.ErrorIs(t, (Cmd{Format: "json"}).bufferedPrinter(context.Background(), rows, writer, ""), want)
}

func TestBufferedPrinterPreservesWriteError(t *testing.T) {
	rows := make(chan encodedRow, 1)
	rows <- encodedRow{bytes.NewBufferString(strings.Repeat("x", 8192)), func() {}}
	close(rows)
	want := errors.New("write failed")
	writer := &failingOutput{failAt: 1, err: want}
	require.ErrorIs(t, (Cmd{Format: "json"}).bufferedPrinter(context.Background(), rows, writer, ""), want)
}

func TestOutputBudget(t *testing.T) {
	budget := newOutputBudget(10)
	ctx := context.Background()
	weight, err := budget.acquire(ctx, 4)
	require.NoError(t, err)
	require.EqualValues(t, 4, weight)
	// A request over the budget is capped so it can still proceed alone.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = budget.acquire(cancelled, 100)
	require.ErrorIs(t, err, context.Canceled)
	budget.release(weight)
	weight, err = budget.acquire(ctx, 100)
	require.NoError(t, err)
	require.EqualValues(t, 10, weight)
	budget.release(weight)
}
