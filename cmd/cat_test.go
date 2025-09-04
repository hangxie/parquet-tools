package cmd

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

func Test_CatCmd_Run_error(t *testing.T) {
	rOpt := pio.ReadOption{}
	fileName := "../testdata/all-types.parquet"
	testCases := map[string]struct {
		cmd    CatCmd
		errMsg string
	}{
		"non-existent-file":      {CatCmd{rOpt, 0, 10, 10, 10, 1.0, "json", false, "file/does/not/exist", false, false}, "no such file or directory"},
		"invalid-read-page-size": {CatCmd{rOpt, 0, 10, 10, 0, 0.5, "json", false, "does/not/matter", false, false}, "invalid read page size"},
		"invalid-skip-size":      {CatCmd{rOpt, -10, 10, 10, 10, 0.5, "json", false, "does/not/matter", false, false}, "invalid skip -10"},
		"sampling-too-high":      {CatCmd{rOpt, 10, 10, 10, 10, 2.0, "json", false, "does/not/matter", false, false}, "invalid sampling"},
		"sampling-too-low":       {CatCmd{rOpt, 10, 10, 10, 10, -0.5, "json", false, "does/not/matter", false, false}, "invalid sampling"},
		"invalid-format":         {CatCmd{rOpt, 10, 10, 10, 10, 0.5, "foobar", false, "does/not/matter", false, false}, "unknown format: foobar"},
		"fail-on-int96":          {CatCmd{rOpt, 10, 10, 10, 10, 0.5, "json", true, fileName, true, false}, "type INT96 which is not supported"},
		"nested-schema-csv":      {CatCmd{rOpt, 10, 10, 10, 10, 0.5, "csv", true, fileName, false, false}, "cannot output in csv format"},
		"nested-schema-tsv":      {CatCmd{rOpt, 10, 10, 10, 10, 0.5, "tsv", true, fileName, false, false}, "cannot output in tsv format"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			if tc.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_CatCmd_Run_good(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    CatCmd
		golden string
	}{
		"default":       {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "json", false, "good.parquet", false, false}, "cat-good-json.json"},
		"limit-0":       {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "json", false, "good.parquet", false, false}, "cat-good-json.json"},
		"limit-2":       {CatCmd{rOpt, 0, 10, 2, 10, 1.0, "json", false, "good.parquet", false, false}, "cat-good-json-limit-2.json"},
		"skip-2":        {CatCmd{rOpt, 2, 10, 0, 10, 1.0, "json", false, "good.parquet", false, false}, "cat-good-json-skip-2.json"},
		"skip-all":      {CatCmd{rOpt, 20, 10, 0, 10, 1.0, "json", false, "good.parquet", false, false}, "empty-json.txt"},
		"sampling-0":    {CatCmd{rOpt, 0, 10, 0, 10, 0.0, "json", false, "good.parquet", false, false}, "empty-json.txt"},
		"empty":         {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "json", false, "empty.parquet", false, false}, "empty-json.txt"},
		"RI-scalar":     {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "reinterpret-scalar.parquet", false, false}, "cat-reinterpret-scalar.jsonl"},
		"RI-pointer":    {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "reinterpret-pointer.parquet", false, false}, "cat-reinterpret-pointer.jsonl"},
		"RI-list":       {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "reinterpret-list.parquet", false, false}, "cat-reinterpret-list.jsonl"},
		"RI-map-key":    {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "reinterpret-map-key.parquet", false, false}, "cat-reinterpret-map-key.jsonl"},
		"RI-map-value":  {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "reinterpret-map-value.parquet", false, false}, "cat-reinterpret-map-value.jsonl"},
		"RI-composite":  {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "reinterpret-composite.parquet", false, false}, "cat-reinterpret-composite.jsonl"},
		"jsonl":         {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "good.parquet", false, false}, "cat-good-jsonl.jsonl"},
		"csv":           {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "csv", false, "good.parquet", false, false}, "cat-good-csv.txt"},
		"csv-no-header": {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "csv", true, "good.parquet", false, false}, "cat-good-csv-no-header.txt"},
		"tsv":           {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "tsv", false, "good.parquet", false, false}, "cat-good-tsv.txt"},
		"tsv-no-header": {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "tsv", true, "good.parquet", false, false}, "cat-good-tsv-no-header.txt"},
		"all-types":     {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", true, "all-types.parquet", false, false}, "cat-all-types.jsonl"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tc.cmd.URI = "file://../testdata/" + tc.cmd.URI
			stdout, stderr := captureStdoutStderr(func() {
				require.Nil(t, tc.cmd.Run())
			})
			require.Equal(t, loadExpected(t, "../testdata/golden/"+tc.golden), stdout)
			require.Equal(t, "", stderr)
		})
	}
}

func Benchmark_CatCmd_Run(b *testing.B) {
	savedStdout, savedStderr := os.Stdout, os.Stderr
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0o666)
	if err != nil {
		panic(err)
	}
	os.Stdout = devNull
	defer func() {
		os.Stdout, os.Stderr = savedStdout, savedStderr
		_ = devNull.Close()
	}()

	cmd := CatCmd{
		ReadOption:   pio.ReadOption{},
		ReadPageSize: 1000,
		SampleRatio:  1.0,
		Format:       "jsonl",
		URI:          "../build/benchmark.parquet",
	}
	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})

	cmd.Concurrent = true
	b.Run("concurrent", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})

	cmd.Format = "csv"
	cmd.URI = "../build/flat.parquet"
	b.Run("csv", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}

func Test_CatCmd_encoder_context_cancel(t *testing.T) {
	cmd := CatCmd{Format: "json"}

	// Create a context that will be canceled
	ctx, cancel := context.WithCancel(context.Background())

	// Create channels - don't buffer them to ensure blocking behavior
	rowChan := make(chan any)
	outputChan := make(chan any)

	// Create a minimal schema handler
	schemaHandler := &schema.SchemaHandler{}

	// Start encoder in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- cmd.encoder(ctx, rowChan, outputChan, schemaHandler)
	}()

	// Cancel the context immediately - encoder should be blocked on reading from rowChan
	cancel()

	// Wait for the encoder to return with context.Canceled error
	select {
	case err := <-errChan:
		require.Equal(t, context.Canceled, err)
	case <-time.After(1 * time.Second):
		t.Fatal("encoder did not return within expected time")
	}
}

func Test_CatCmd_printer_context_cancel(t *testing.T) {
	cmd := CatCmd{Format: "json"}

	// Create a context that will be canceled
	ctx, cancel := context.WithCancel(context.Background())

	// Create unbuffered output channel so printer blocks on reading
	outputChan := make(chan any)

	// Start printer in a goroutine, capture its output
	errChan := make(chan error, 1)
	go func() {
		errChan <- cmd.printer(ctx, outputChan, nil)
	}()

	// Cancel the context immediately - printer should be blocked on reading from outputChan
	cancel()

	// Wait for the printer to return with context.Canceled error
	select {
	case err := <-errChan:
		require.Equal(t, context.Canceled, err)
	case <-time.After(1 * time.Second):
		t.Fatal("printer did not return within expected time")
	}
}
