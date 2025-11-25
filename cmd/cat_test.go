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
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    CatCmd
		golden string
		errMsg string
	}{
		// error cases
		"non-existent-file":      {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "file/does/not/exist"}, errMsg: "no such file or directory"},
		"parquet-1481":           {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "../testdata/PARQUET-1481.parquet"}, errMsg: "unknown parquet type: <UNSET>"},
		"arrow-rs-gh-6229":       {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "../testdata/ARROW-RS-GH-6229-LEVELS.parquet"}, errMsg: "expected 21 values but got 1 from RLE/bit-packed hybrid decoder"},
		"invalid-read-page-size": {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 0, SampleRatio: 0.5, Format: "json", NoHeader: false, URI: "does/not/matter", Concurrent: true}, errMsg: "invalid read page size"},
		"invalid-skip-size":      {cmd: CatCmd{ReadOption: rOpt, Skip: -10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "json", NoHeader: false, URI: "does/not/matter"}, errMsg: "invalid skip -10"},
		"sampling-too-high":      {cmd: CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 2.0, Format: "json", NoHeader: false, URI: "does/not/matter", Concurrent: true}, errMsg: "invalid sampling"},
		"sampling-too-low":       {cmd: CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: -0.5, Format: "json", NoHeader: false, URI: "does/not/matter"}, errMsg: "invalid sampling"},
		"invalid-format":         {cmd: CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "foobar", NoHeader: false, URI: "does/not/matter"}, errMsg: "unknown format: foobar"},
		"fail-on-int96":          {cmd: CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "json", NoHeader: true, URI: "../testdata/all-types.parquet", FailOnInt96: true}, errMsg: "type INT96 which is not supported"},
		"nested-schema-csv":      {cmd: CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "csv", NoHeader: true, URI: "../testdata/all-types.parquet"}, errMsg: "field [Map] is not scalar type"},
		"nested-schema-tsv":      {cmd: CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "tsv", NoHeader: true, URI: "../testdata/all-types.parquet"}, errMsg: "field [Map] is not scalar type"},
		"geospatial-csv":         {cmd: CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "csv", NoHeader: true, URI: "../testdata/geospatial.parquet"}, errMsg: "field [Geometry] is not scalar type"},
		"geospatial-tsv":         {cmd: CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "tsv", NoHeader: true, URI: "../testdata/geospatial.parquet"}, errMsg: "field [Geometry] is not scalar type"},
		"invalid-geo-format":     {cmd: CatCmd{ReadOption: rOpt, Skip: 10, Limit: 10, ReadPageSize: 10, SampleRatio: 0.5, Format: "tsv", GeoFormat: "foobar", NoHeader: true, URI: "../testdata/all-types.parquet"}, errMsg: "unknown geo format:"},
		"nan-json-error":         {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "../testdata/nan.parquet"}, errMsg: "json: unsupported value: NaN"},
		"arrow-gh-41321":         {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 10, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "../testdata/ARROW-GH-41321.parquet"}, errMsg: "invalid count"},
		// good cases - URI will be prefixed with "file://../testdata/"
		"default":            {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet"}, golden: "cat-good-json.json"},
		"limit-0":            {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet"}, golden: "cat-good-json.json"},
		"limit-2":            {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 2, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet"}, golden: "cat-good-json-limit-2.json"},
		"skip-2":             {cmd: CatCmd{ReadOption: rOpt, Skip: 2, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet", Concurrent: true}, golden: "cat-good-json-skip-2.json"},
		"skip-all":           {cmd: CatCmd{ReadOption: rOpt, Skip: 20, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "good.parquet"}, golden: "empty-json.txt"},
		"sampling-0":         {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 0.0, Format: "json", NoHeader: false, URI: "good.parquet"}, golden: "empty-json.txt"},
		"empty":              {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "json", NoHeader: false, URI: "empty.parquet"}, golden: "empty-json.txt"},
		"jsonl":              {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: false, URI: "good.parquet"}, golden: "cat-good-jsonl.jsonl"},
		"csv":                {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "csv", NoHeader: false, URI: "good.parquet"}, golden: "cat-good-csv.txt"},
		"csv-no-header":      {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "csv", NoHeader: true, URI: "good.parquet"}, golden: "cat-good-csv-no-header.txt"},
		"tsv":                {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "tsv", NoHeader: false, URI: "good.parquet"}, golden: "cat-good-tsv.txt"},
		"tsv-no-header":      {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "tsv", NoHeader: true, URI: "good.parquet"}, golden: "cat-good-tsv-no-header.txt"},
		"all-types":          {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "all-types.parquet"}, golden: "cat-all-types.jsonl"},
		"geospatial-hex":     {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", GeoFormat: "hex", NoHeader: true, URI: "geospatial.parquet"}, golden: "cat-geospatial-hex.jsonl"},
		"geospatial-base64":  {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", GeoFormat: "base64", NoHeader: true, URI: "geospatial.parquet"}, golden: "cat-geospatial-base64.jsonl"},
		"geospatial-geojson": {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "geospatial.parquet"}, golden: "cat-geospatial-geojson.jsonl"},
		"old-style-list":     {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "old-style-list.parquet"}, golden: "cat-old-style-list.jsonl"},
		"multi-row-groups":   {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "row-group.parquet"}, golden: "cat-row-group.jsonl"},
		"dict-page":          {cmd: CatCmd{ReadOption: rOpt, Skip: 0, Limit: 0, ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", NoHeader: true, URI: "dict-page.parquet"}, golden: "cat-dict-page.jsonl"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if tc.errMsg == "" {
				t.Parallel()
			}
			cmd := tc.cmd
			if tc.golden != "" {
				cmd.URI = "file://../testdata/" + tc.cmd.URI
			}
			if tc.errMsg != "" {
				err := cmd.Run()
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				stdout, stderr := captureStdoutStderr(func() {
					require.NoError(t, cmd.Run())
				})
				require.Equal(t, loadExpected(t, "../testdata/golden/"+tc.golden), stdout)
				require.Equal(t, "", stderr)
			}
		})
	}
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
