package cmd

import (
	"context"
	"os"
	"testing"

	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
)

func TestCatCmd(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		fileName := "../testdata/all-types.parquet"
		testCases := map[string]struct {
			cmd    CatCmd
			errMsg string
		}{
			"non-existent-file":      {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "file/does/not/exist", FailOnInt96: false, Concurrent: false}, "no such file or directory"},
			"parquet-1481":           {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "../testdata/PARQUET-1481.parquet", FailOnInt96: false, Concurrent: false}, "unknown parquet type: <UNSET>"},
			"arrow-rs-gh-6229":       {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "../testdata/ARROW-RS-GH-6229-LEVELS.parquet", FailOnInt96: false, Concurrent: false}, "expected 21 values but got 1 from RLE/bit-packed hybrid decoder"},
			"invalid-read-page-size": {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 0, SampleRatio: 0.5, Format: "json", NoHeader: false, URI: "does/not/matter", FailOnInt96: false, Concurrent: true}, "invalid read page size"},
			"invalid-skip-size":      {CatCmd{ReadOption: rOpt, Skip: -10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "json", NoHeader: false, URI: "does/not/matter", FailOnInt96: false, Concurrent: false}, "invalid skip -10"},
			"sampling-too-high":      {CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 2.0, Format: "json", NoHeader: false, URI: "does/not/matter", FailOnInt96: false, Concurrent: true}, "invalid sampling"},
			"sampling-too-low":       {CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: -0.5, Format: "json", NoHeader: false, URI: "does/not/matter", FailOnInt96: false, Concurrent: false}, "invalid sampling"},
			"invalid-format":         {CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "foobar", NoHeader: false, URI: "does/not/matter", FailOnInt96: false, Concurrent: false}, "unknown format: foobar"},
			"fail-on-int96":          {CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "json", NoHeader: true, URI: fileName, FailOnInt96: true, Concurrent: false}, "type INT96 which is not supported"},
			"nested-schema-csv":      {CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "csv", NoHeader: true, URI: fileName, FailOnInt96: false, Concurrent: false}, "field [Map] is not scalar type"},
			"nested-schema-tsv":      {CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "tsv", NoHeader: true, URI: fileName, FailOnInt96: false, Concurrent: false}, "field [Map] is not scalar type"},
			"geospatial-csv":         {CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "csv", NoHeader: true, URI: "../testdata/geospatial.parquet", FailOnInt96: false, Concurrent: false}, "field [Geometry] is not scalar type"},
			"geospatial-tsv":         {CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "tsv", NoHeader: true, URI: "../testdata/geospatial.parquet", FailOnInt96: false, Concurrent: false}, "field [Geometry] is not scalar type"},
			"invalid-geo-format":     {CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "tsv", GeoFormat: "foobar", NoHeader: true, URI: fileName, FailOnInt96: false, Concurrent: false}, "unknown geo format:"},
			"nan-json-error":         {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "../testdata/nan.parquet", FailOnInt96: false, Concurrent: false}, "json: unsupported value: NaN"},
			"arrow-gh-41321":         {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "../testdata/ARROW-GH-41321.parquet", FailOnInt96: false, Concurrent: false}, "invalid count"},
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
	})

	t.Run("good", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		testCases := map[string]struct {
			cmd    CatCmd
			golden string
		}{
			"default":            {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-json.json"},
			"limit-0":            {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-json.json"},
			"limit-2":            {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 2, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-json-limit-2.json"},
			"skip-2":             {CatCmd{ReadOption: rOpt, Skip: 2, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: true}, "cat-good-json-skip-2.json"},
			"skip-all":           {CatCmd{ReadOption: rOpt, Skip: 20, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "empty-json.txt"},
			"sampling-0":         {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 0.0, Format: "json", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "empty-json.txt"},
			"empty":              {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "empty.parquet", FailOnInt96: false, Concurrent: false}, "empty-json.txt"},
			"jsonl":              {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-jsonl.jsonl"},
			"csv":                {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "csv", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-csv.txt"},
			"csv-no-header":      {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "csv", NoHeader: true, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-csv-no-header.txt"},
			"tsv":                {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "tsv", NoHeader: false, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-tsv.txt"},
			"tsv-no-header":      {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "tsv", NoHeader: true, URI: "good.parquet", FailOnInt96: false, Concurrent: false}, "cat-good-tsv-no-header.txt"},
			"all-types":          {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "all-types.parquet", FailOnInt96: false, Concurrent: false}, "cat-all-types.jsonl"},
			"geospatial-hex":     {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", GeoFormat: "hex", NoHeader: true, URI: "geospatial.parquet", FailOnInt96: false, Concurrent: false}, "cat-geospatial-hex.jsonl"},
			"geospatial-base64":  {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", GeoFormat: "base64", NoHeader: true, URI: "geospatial.parquet", FailOnInt96: false, Concurrent: false}, "cat-geospatial-base64.jsonl"},
			"geospatial-geojson": {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "geospatial.parquet", FailOnInt96: false, Concurrent: false}, "cat-geospatial-geojson.jsonl"},
			"old-style-list":     {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "old-style-list.parquet", FailOnInt96: false, Concurrent: false}, "cat-old-style-list.jsonl"},
			"multi-row-groups":   {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "row-group.parquet", FailOnInt96: false, Concurrent: false}, "cat-row-group.jsonl"},
			"dict-page":          {CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "dict-page.parquet", FailOnInt96: false, Concurrent: false}, "cat-dict-page.jsonl"},
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
	})
}

func TestCatCmdEncoder(t *testing.T) {
	rOpt := pio.ReadOption{}

	testCases := map[string]struct {
		setup       func(t *testing.T) (context.Context, context.CancelFunc, chan any, chan string, *reader.ParquetReader, []string)
		wantErr     bool
		errContains string
	}{
		"context-cancelled-in-main-loop": {
			setup: func(t *testing.T) (context.Context, context.CancelFunc, chan any, chan string, *reader.ParquetReader, []string) {
				// Create a context that we can cancel
				ctx, cancel := context.WithCancel(context.Background())

				// Create channels
				rowChan := make(chan any, 10)
				outputChan := make(chan string, 10)

				// Open a test parquet file
				fileReader, err := pio.NewParquetFileReader("file://../testdata/good.parquet", rOpt)
				require.NoError(t, err)

				// Populate rowChan with some data, then cancel context
				rows, err := fileReader.ReadByNumber(5)
				require.NoError(t, err)

				// Send one row and then cancel
				rowChan <- rows[0]
				cancel() // Cancel the context immediately

				return ctx, cancel, rowChan, outputChan, fileReader, nil
			},
			wantErr:     true,
			errContains: "context canceled",
		},
		"context-cancelled-before-send": {
			setup: func(t *testing.T) (context.Context, context.CancelFunc, chan any, chan string, *reader.ParquetReader, []string) {
				// Create a context that we can cancel
				ctx, cancel := context.WithCancel(context.Background())

				// Create channels
				rowChan := make(chan any, 10)
				outputChan := make(chan string, 1) // Small buffer to test the send path

				// Open a test parquet file
				fileReader, err := pio.NewParquetFileReader("file://../testdata/good.parquet", rOpt)
				require.NoError(t, err)

				// Populate rowChan with data
				rows, err := fileReader.ReadByNumber(5)
				require.NoError(t, err)

				// Fill output channel to block sending
				outputChan <- "blocking"

				// Send row to process
				rowChan <- rows[0]

				// Cancel before the encoder can send to outputChan
				cancel()

				return ctx, cancel, rowChan, outputChan, fileReader, nil
			},
			wantErr:     true,
			errContains: "context canceled",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel, rowChan, outputChan, fileReader, fieldList := tc.setup(t)
			defer cancel()
			defer func() { _ = fileReader.PFile.Close() }()

			cmd := CatCmd{
				Format: "json",
			}

			err := cmd.encoder(ctx, rowChan, outputChan, fileReader.SchemaHandler, fieldList)

			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCatCmdEncoderInvalidFormat(t *testing.T) {
	// Test the "unsupported format" error path in encoder function (line 188)
	// This tests when cmd.Format has an invalid value that passes initial validation
	// but fails in the encoder switch statement

	rOpt := pio.ReadOption{}

	// Open a test parquet file
	fileReader, err := pio.NewParquetFileReader("file://../testdata/good.parquet", rOpt)
	require.NoError(t, err)
	defer func() { _ = fileReader.PFile.Close() }()

	// Read some data
	rows, err := fileReader.ReadByNumber(1)
	require.NoError(t, err)
	require.NotEmpty(t, rows)

	// Create channels
	rowChan := make(chan any, 10)
	outputChan := make(chan string, 10)

	// Send a row to process
	rowChan <- rows[0]
	close(rowChan) // Close to signal completion

	// Create context
	ctx := context.Background()

	// Create a CatCmd with an invalid format
	// We need to bypass the delimiter map check by using a format
	// that exists in the delimiter map but will fail in encoder
	cmd := CatCmd{
		Format: "xml", // This format doesn't exist in the delimiter map
	}

	// Manually set up delimiter for this invalid format to bypass Run() validation
	// This simulates the scenario where Format is corrupted after validation
	delimiter["xml"] = struct {
		begin          string
		lineDelimiter  string
		fieldDelimiter rune
		end            string
	}{"", "", ' ', ""}
	defer delete(delimiter, "xml")

	err = cmd.encoder(ctx, rowChan, outputChan, fileReader.SchemaHandler, nil)

	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported format: xml")
}

func BenchmarkCatCmd(b *testing.B) {
	// savedStdout, savedStderr := os.Stdout, os.Stderr
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0o666)
	if err != nil {
		panic(err)
	}
	os.Stdout = devNull
	defer func() {
		// os.Stdout, os.Stderr = savedStdout, savedStderr
		// _ = devNull.Close()
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
	cmd.Concurrent = true
	b.Run("csv", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}
