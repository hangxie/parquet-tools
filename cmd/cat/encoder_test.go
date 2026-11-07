package cat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
)

func TestEncoderValueMode(t *testing.T) {
	handler, err := schema.NewSchemaHandlerFromJSON(`{"Tag":"name=root","Fields":[{"Tag":"name=Amount, type=INT64, convertedtype=DECIMAL, precision=18, scale=2"},{"Tag":"name=Unknown, type=INT32, repetitiontype=OPTIONAL, logicaltype=UNKNOWN"}]}`)
	require.NoError(t, err)
	for _, tc := range []struct{ mode, geo, want string }{
		{"", "geojson", `{"Amount":1.23,"Unknown":null}`},
		{"interpreted", "geojson", `{"Amount":1.23,"Unknown":null}`},
		{"raw", "geojson", `{"Amount":123,"Unknown":7}`},
		{"raw", "hybrid", `{"Amount":123,"Unknown":7}`},
	} {
		t.Run(tc.mode+tc.geo, func(t *testing.T) {
			input := make(chan any, 1)
			input <- struct {
				Amount  int64
				Unknown int32
			}{123, 7}
			close(input)
			output := make(chan encodedRow, 1)
			cmd := Cmd{Format: "json", ValueMode: tc.mode, GeoFormat: tc.geo}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- cmd.encoder(ctx, input, output, newOutputBudget(maxOutputBytes), handler, nil, map[string]struct{}{"Unknown": {}})
			}()
			select {
			case row := <-output:
				require.JSONEq(t, tc.want, row.buffer.String())
				row.release()
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.NoError(t, <-done)
		})
	}
}

func TestRawValueModeLegacyList(t *testing.T) {
	for _, fixture := range []string{"old-style-list.parquet", "nested-array.parquet", "nested-array-logical-list.parquet", "list-map-layout.parquet"} {
		t.Run(fixture, func(t *testing.T) {
			interpreted := Cmd{URI: "../../testdata/" + fixture, Format: "json", ReadPageSize: 10, SampleRatio: 1}
			expected := testutils.CommandStdout(t, interpreted)
			raw := interpreted
			raw.ValueMode = "raw"
			require.Equal(t, expected, testutils.CommandStdout(t, raw))
		})
	}
}

func TestRawValueModeFixtures(t *testing.T) {
	for _, fixture := range []string{"all-types.parquet", "geospatial.parquet", "retype.parquet"} {
		t.Run(fixture, func(t *testing.T) {
			cmd := Cmd{URI: "../../testdata/" + fixture, Format: "json", ReadPageSize: 2, SampleRatio: 1, ValueMode: "raw", Concurrent: true}
			var rows []map[string]any
			require.NoError(t, json.Unmarshal([]byte(testutils.CommandStdout(t, cmd)), &rows))
			require.NotEmpty(t, rows)
			if fixture == "geospatial.parquet" {
				for _, row := range rows {
					for _, column := range []string{"Geography", "Geometry"} {
						value, ok := row[column].(string)
						require.True(t, ok)
						data, err := base64.StdEncoding.DecodeString(value)
						require.NoError(t, err)
						require.NotEmpty(t, data)
					}
				}
			}
		})
	}
}

func TestInvalidValueMode(t *testing.T) {
	require.ErrorContains(t, (Cmd{ValueMode: "auto"}).Run(context.Background()), "invalid value mode")
}

func TestEncoderCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := (Cmd{Format: "json"}).encoder(ctx, make(chan any), make(chan encodedRow), newOutputBudget(maxOutputBytes), nil, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRowFormatter(t *testing.T) {
	for _, format := range []string{"json", "jsonl", "csv", "tsv"} {
		t.Run(format, func(t *testing.T) {
			f := newRowFormatter(format)
			for _, value := range []string{strings.Repeat("x", 1<<20), "small", "quoted \" & < >\nline"} {
				row := map[string]any{"value": value}
				require.NoError(t, f.encode(row, []string{"value"}))
				if format == "json" || format == "jsonl" {
					want, err := json.Marshal(row)
					require.NoError(t, err)
					require.Equal(t, want, f.buffer.Bytes())
				} else {
					builder := new(strings.Builder)
					writer := csv.NewWriter(builder)
					writer.Comma = delimiter[format].fieldDelimiter
					require.NoError(t, writer.Write([]string{value}))
					writer.Flush()
					require.Equal(t, strings.TrimRight(builder.String(), "\n"), f.buffer.String())
				}
			}
		})
	}
}

func TestRowFormatterErrors(t *testing.T) {
	require.Error(t, newRowFormatter("json").encode(make(chan int), nil))
	require.ErrorContains(t, newRowFormatter("unsupported").encode(nil, nil), "unsupported format")
	f := newRowFormatter("csv")
	f.csv.Comma = '\n'
	require.ErrorContains(t, f.encode(map[string]any{}, nil), "invalid field or comment delimiter")
	require.ErrorContains(t, f.encodeRecord([]string{"a"}), "invalid field or comment delimiter")
}

func TestRowFormatterEncodeRecord(t *testing.T) {
	f := newRowFormatter("tsv")
	require.NoError(t, f.encodeRecord([]string{"a", "b c"}))
	require.Equal(t, "a\tb c\n", f.buffer.String())
}

func BenchmarkRowFormatterLargeValue(b *testing.B) {
	row := map[string]any{"value": strings.Repeat("x", 1<<20)}
	f := newRowFormatter("jsonl")
	b.ReportAllocs()
	b.SetBytes(1 << 20)
	for b.Loop() {
		if err := f.encode(row, nil); err != nil {
			b.Fatal(err)
		}
		if _, err := io.Discard.Write(f.buffer.Bytes()); err != nil {
			b.Fatal(err)
		}
	}
}

func TestEncoderCancelledWhileWaitingForPrinter(t *testing.T) {
	handler, err := schema.NewSchemaHandlerFromJSON(`{"Tag":"name=root","Fields":[{"Tag":"name=Value, type=INT64"}]}`)
	require.NoError(t, err)
	input := make(chan any, formatterPoolSize+1)
	for i := range formatterPoolSize + 1 {
		input <- struct{ Value int64 }{int64(i)}
	}
	close(input)
	output := make(chan encodedRow)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (Cmd{Format: "json"}).encoder(ctx, input, output, newOutputBudget(maxOutputBytes), handler, nil, nil)
	}()
	// Hold every pooled buffer so the encoder has nothing to encode the last row into.
	for i := range formatterPoolSize {
		select {
		case row := <-output:
			require.Equal(t, fmt.Sprintf(`{"Value":%d}`, i), row.buffer.String())
		case <-time.After(5 * time.Second):
			t.Fatal("encoder did not send its output")
		}
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("encoder did not cancel while waiting for printer")
	}
}

func TestEncoderReusesReleasedBuffers(t *testing.T) {
	handler, err := schema.NewSchemaHandlerFromJSON(`{"Tag":"name=root","Fields":[{"Tag":"name=Value, type=INT64"}]}`)
	require.NoError(t, err)
	const rows = formatterPoolSize * 3
	input := make(chan any, rows)
	for i := range rows {
		input <- struct{ Value int64 }{int64(i)}
	}
	close(input)
	output := make(chan encodedRow)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (Cmd{Format: "json"}).encoder(ctx, input, output, newOutputBudget(maxOutputBytes), handler, nil, nil)
	}()
	buffers := map[*bytes.Buffer]struct{}{}
	for i := range rows {
		select {
		case row := <-output:
			require.Equal(t, fmt.Sprintf(`{"Value":%d}`, i), row.buffer.String())
			buffers[row.buffer] = struct{}{}
			row.release()
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	require.NoError(t, <-done)
	require.LessOrEqual(t, len(buffers), formatterPoolSize)
}

func TestEncoderWaitsForOutputBudget(t *testing.T) {
	handler, err := schema.NewSchemaHandlerFromJSON(`{"Tag":"name=root","Fields":[{"Tag":"name=Value, type=INT64"}]}`)
	require.NoError(t, err)
	input := make(chan any, 2)
	input <- struct{ Value int64 }{1}
	input <- struct{ Value int64 }{2}
	close(input)
	output := make(chan encodedRow, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Each row is 11 bytes, so the budget fits one at a time.
	done := make(chan error, 1)
	go func() {
		done <- (Cmd{Format: "json"}).encoder(ctx, input, output, newOutputBudget(15), handler, nil, nil)
	}()
	first := <-output
	require.Equal(t, `{"Value":1}`, first.buffer.String())
	select {
	case <-output:
		t.Fatal("second row sent while the first still holds the budget")
	case <-time.After(100 * time.Millisecond):
	}
	first.release()
	select {
	case second := <-output:
		require.Equal(t, `{"Value":2}`, second.buffer.String())
		second.release()
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, <-done)
}

func TestEncoderSendsRowLargerThanBudget(t *testing.T) {
	handler, err := schema.NewSchemaHandlerFromJSON(`{"Tag":"name=root","Fields":[{"Tag":"name=Value, type=INT64"}]}`)
	require.NoError(t, err)
	input := make(chan any, 1)
	input <- struct{ Value int64 }{1}
	close(input)
	output := make(chan encodedRow, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, (Cmd{Format: "json"}).encoder(ctx, input, output, newOutputBudget(1), handler, nil, nil))
	row := <-output
	require.Equal(t, `{"Value":1}`, row.buffer.String())
	row.release()
}

func TestRowFormatterRecycle(t *testing.T) {
	budget := newOutputBudget(100)
	first, second := newRowFormatter("jsonl"), newRowFormatter("jsonl")
	for _, f := range []*rowFormatter{first, second} {
		require.NoError(t, f.encode("small", nil))
		require.LessOrEqual(t, f.buffer.Cap(), 64)
	}
	first.recycle(budget)
	require.Positive(t, first.buffer.Cap(), "fits the retain budget")
	second.recycle(budget)
	require.Zero(t, second.buffer.Cap(), "over the retain budget")

	// Taking the kept buffer out of the pool frees its share of the budget.
	budget.reclaim(first.buffer.Cap())
	require.NoError(t, second.encode("small", nil))
	second.recycle(budget)
	require.Positive(t, second.buffer.Cap())

	oversized := newRowFormatter("jsonl")
	require.NoError(t, oversized.encode(strings.Repeat("x", 200), nil))
	oversized.recycle(newOutputBudget(100))
	require.Zero(t, oversized.buffer.Cap())
	// The formatter stays usable after its buffer is dropped.
	require.NoError(t, oversized.encode("y", nil))
	require.Equal(t, `"y"`, oversized.buffer.String())
}

func TestEncoderChargesBufferCapacity(t *testing.T) {
	handler, err := schema.NewSchemaHandlerFromJSON(`{"Tag":"name=root","Fields":[{"Tag":"name=Value, type=INT64"}]}`)
	require.NoError(t, err)
	input := make(chan any, 2)
	input <- struct{ Value int64 }{1}
	input <- struct{ Value int64 }{2}
	close(input)
	output := make(chan encodedRow, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Two 11-byte rows fit 100 bytes by length, but each holds a 64-byte buffer.
	done := make(chan error, 1)
	go func() {
		done <- (Cmd{Format: "json"}).encoder(ctx, input, output, newOutputBudget(100), handler, nil, nil)
	}()
	first := <-output
	require.Equal(t, 64, first.buffer.Cap())
	select {
	case <-output:
		t.Fatal("second row sent although both buffers exceed the budget")
	case <-time.After(100 * time.Millisecond):
	}
	first.release()
	second := <-output
	second.release()
	require.NoError(t, <-done)
}
