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
		"non-existent-file":      {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "file/does/not/exist", FailOnInt96: false, Concurrent: false}, "no such file or directory"},
		"invalid-read-page-size": {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 10, ReadPageSize: 0, SampleRatio: 0.5, Format: "json", NoHeader: false, URI: "does/not/matter", FailOnInt96: false, Concurrent: false}, "invalid read page size"},
		"invalid-skip-size":      {CatCmd{ReadOption: rOpt, Skip: -10, SkipPageSize: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "json", NoHeader: false, URI: "does/not/matter", FailOnInt96: false, Concurrent: false}, "invalid skip -10"},
		"sampling-too-high":      {CatCmd{ReadOption: rOpt, Skip: 10, SkipPageSize: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 2.0, Format: "json", NoHeader: false, URI: "does/not/matter", FailOnInt96: false, Concurrent: false}, "invalid sampling"},
		"sampling-too-low":       {CatCmd{ReadOption: rOpt, Skip: 10, SkipPageSize: 10, Limit: 10, ReadPageSize: 10, SampleRatio: -0.5, Format: "json", NoHeader: false, URI: "does/not/matter", FailOnInt96: false, Concurrent: false}, "invalid sampling"},
		"invalid-format":         {CatCmd{ReadOption: rOpt, Skip: 10, SkipPageSize: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "foobar", NoHeader: false, URI: "does/not/matter", FailOnInt96: false, Concurrent: false}, "unknown format: foobar"},
		"fail-on-int96":          {CatCmd{ReadOption: rOpt, Skip: 10, SkipPageSize: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "json", NoHeader: true, URI: fileName, FailOnInt96: true, Concurrent: false}, "type INT96 which is not supported"},
		"nested-schema-csv":      {CatCmd{ReadOption: rOpt, Skip: 10, SkipPageSize: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "csv", NoHeader: true, URI: fileName, FailOnInt96: false, Concurrent: false}, "cannot output in csv format"},
		"nested-schema-tsv":      {CatCmd{ReadOption: rOpt, Skip: 10, SkipPageSize: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "tsv", NoHeader: true, URI: fileName, FailOnInt96: false, Concurrent: false}, "cannot output in tsv format"},
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
		"default":       {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-json.json"},
		"limit-0":       {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-json.json"},
		"limit-2":       {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 2, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-json-limit-2.json"},
		"skip-2":        {CatCmd{ReadOption: rOpt, Skip: 2, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-json-skip-2.json"},
		"skip-all":      {CatCmd{ReadOption: rOpt, Skip: 20, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "empty-json.txt"},
		"sampling-0":    {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 0.0, Format: "json", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "empty-json.txt"},
		"empty":         {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "empty.parquet", FailOnInt96: false, Concurrent: false}, "empty-json.txt"},
		"RI-scalar":     {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: false, URI: "reinterpret-scalar.parquet", FailOnInt96: false, Concurrent: false}, "cat-reinterpret-scalar.jsonl"},
		"RI-pointer":    {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: false, URI: "reinterpret-pointer.parquet", FailOnInt96: false, Concurrent: false}, "cat-reinterpret-pointer.jsonl"},
		"RI-list":       {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: false, URI: "reinterpret-list.parquet", FailOnInt96: false, Concurrent: false}, "cat-reinterpret-list.jsonl"},
		"RI-map-key":    {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: false, URI: "reinterpret-map-key.parquet", FailOnInt96: false, Concurrent: false}, "cat-reinterpret-map-key.jsonl"},
		"RI-map-value":  {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: false, URI: "reinterpret-map-value.parquet", FailOnInt96: false, Concurrent: false}, "cat-reinterpret-map-value.jsonl"},
		"RI-composite":  {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: false, URI: "reinterpret-composite.parquet", FailOnInt96: false, Concurrent: false}, "cat-reinterpret-composite.jsonl"},
		"jsonl":         {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-jsonl.jsonl"},
		"csv":           {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "csv", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-csv.txt"},
		"csv-no-header": {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "csv", NoHeader: true, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-csv-no-header.txt"},
		"tsv":           {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "tsv", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-tsv.txt"},
		"tsv-no-header": {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "tsv", NoHeader: true, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-tsv-no-header.txt"},
		"all-types":     {CatCmd{ReadOption: rOpt, Skip: 0, SkipPageSize: 10, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "all-types.parquet", FailOnInt96: false, Concurrent: false}, "cat-all-types.jsonl"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Create a separate CatCmd instance for each parallel test to avoid race conditions
			cmd := tc.cmd
			cmd.URI = "file://../testdata/" + tc.cmd.URI
			stdout, stderr := captureStdoutStderr(func() {
				require.Nil(t, cmd.Run())
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
