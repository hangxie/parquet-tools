package schema_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
	"github.com/hangxie/parquet-tools/cmd/schema"
	pio "github.com/hangxie/parquet-tools/io"
)

func TestCmd(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    schema.Cmd
		golden string
		errMsg string
	}{
		// error cases
		"invalid-uri":       {cmd: schema.Cmd{ReadOption: rOpt, Format: "foobar", URI: "dummy://location"}, errMsg: "unknown location scheme"},
		"invalid-format":    {cmd: schema.Cmd{ReadOption: rOpt, Format: "foobar", URI: "../../testdata/good.parquet"}, errMsg: "unknown schema format"},
		"go-map-value":      {cmd: schema.Cmd{ReadOption: rOpt, Format: "go", URI: "../../testdata/map-composite-value.parquet"}, errMsg: "go struct does not support LIST as MAP value in [Parquet_go_root.Scores]"},
		"go-list-value":     {cmd: schema.Cmd{ReadOption: rOpt, Format: "go", URI: "../../testdata/list-of-list.parquet"}, errMsg: "go struct does not support LIST of LIST in [Parquet_go_root.Lol]"},
		"go-old-style-list": {cmd: schema.Cmd{ReadOption: rOpt, Format: "go", URI: "../../testdata/old-style-list.parquet"}, errMsg: "go struct does not support LIST of LIST in [My_record.First.Second.A]"},
		"csv-nested":        {cmd: schema.Cmd{ReadOption: rOpt, Format: "csv", URI: "../../testdata/csv-nested.parquet"}, errMsg: "CSV supports flat schema only"},
		"csv-optional":      {cmd: schema.Cmd{ReadOption: rOpt, Format: "csv", URI: "../../testdata/csv-optional.parquet"}, errMsg: "CSV does not support optional column"},
		"csv-repeated":      {cmd: schema.Cmd{ReadOption: rOpt, Format: "csv", URI: "../../testdata/csv-repeated.parquet"}, errMsg: "CSV does not support column in LIST type"},
		// good cases - URI will be prefixed with "../../testdata/"
		"raw":                    {cmd: schema.Cmd{ReadOption: rOpt, Format: "raw", URI: "all-types.parquet"}, golden: "schema-all-types-raw.json"},
		"json":                   {cmd: schema.Cmd{ReadOption: rOpt, Format: "json", URI: "all-types.parquet"}, golden: "schema-all-types-json.json"},
		"go":                     {cmd: schema.Cmd{ReadOption: rOpt, Format: "go", URI: "all-types.parquet"}, golden: "schema-all-types-go.txt"},
		"csv":                    {cmd: schema.Cmd{ReadOption: rOpt, Format: "csv", URI: "csv-good.parquet"}, golden: "schema-csv-good.txt"},
		"raw-map-value-list":     {cmd: schema.Cmd{ReadOption: rOpt, Format: "raw", URI: "map-composite-value.parquet"}, golden: "schema-map-composite-value-raw.json"},
		"json-map-value-list":    {cmd: schema.Cmd{ReadOption: rOpt, Format: "json", URI: "map-composite-value.parquet"}, golden: "schema-map-composite-value-json.json"},
		"json-map-value-map":     {cmd: schema.Cmd{ReadOption: rOpt, Format: "json", URI: "map-value-map.parquet"}, golden: "schema-map-value-map-json.json"},
		"pargo-prefix-flat":      {cmd: schema.Cmd{ReadOption: rOpt, Format: "go", URI: "pargo-prefix-flat.parquet"}, golden: "schema-pargo-prefix-flat-go.txt"},
		"pargo-prefix-nested":    {cmd: schema.Cmd{ReadOption: rOpt, Format: "go", URI: "pargo-prefix-nested.parquet"}, golden: "schema-pargo-prefix-nested-go.txt"},
		"geospatial-go":          {cmd: schema.Cmd{ReadOption: rOpt, Format: "go", URI: "geospatial.parquet"}, golden: "schema-geospatial-go.txt"},
		"geospatial-json":        {cmd: schema.Cmd{ReadOption: rOpt, Format: "json", URI: "geospatial.parquet"}, golden: "schema-geospatial-json.json"},
		"geospatial-raw":         {cmd: schema.Cmd{ReadOption: rOpt, Format: "raw", URI: "geospatial.parquet"}, golden: "schema-geospatial-raw.json"},
		"camel-case":             {cmd: schema.Cmd{ReadOption: rOpt, Format: "go", CamelCase: true, URI: "good.parquet"}, golden: "schema-good-go-camel-case.txt"},
		"skip-page-encoding":     {cmd: schema.Cmd{ReadOption: rOpt, Format: "json", SkipPageEncoding: true, URI: "good.parquet"}, golden: "schema-good-skip-page-encoding.json"},
		"skip-page-encoding-raw": {cmd: schema.Cmd{ReadOption: rOpt, Format: "raw", SkipPageEncoding: true, URI: "good.parquet"}, golden: "schema-good-skip-page-encoding-raw.json"},
		"skip-page-encoding-go":  {cmd: schema.Cmd{ReadOption: rOpt, Format: "go", SkipPageEncoding: true, URI: "good.parquet"}, golden: "schema-good-skip-page-encoding-go.txt"},
		"unknown-type-raw":       {cmd: schema.Cmd{ReadOption: rOpt, Format: "raw", URI: "unknown-type.parquet"}, golden: "schema-unknown-type-raw.json"},
		"unknown-type-json":      {cmd: schema.Cmd{ReadOption: rOpt, Format: "json", URI: "unknown-type.parquet"}, golden: "schema-unknown-type-json.json"},
		"unknown-type-go":        {cmd: schema.Cmd{ReadOption: rOpt, Format: "go", URI: "unknown-type.parquet"}, golden: "schema-unknown-type-go.txt"},
		"bloom-filter-raw":       {cmd: schema.Cmd{ReadOption: rOpt, Format: "raw", URI: "bloom-filter.parquet"}, golden: "schema-bloom-filter-raw.json"},
		"bloom-filter-json":      {cmd: schema.Cmd{ReadOption: rOpt, Format: "json", URI: "bloom-filter.parquet"}, golden: "schema-bloom-filter-json.json"},
		"bloom-filter-go":        {cmd: schema.Cmd{ReadOption: rOpt, Format: "go", URI: "bloom-filter.parquet"}, golden: "schema-bloom-filter-go.txt"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if tc.errMsg == "" {
				t.Parallel()
			}
			cmd := tc.cmd
			if tc.golden != "" {
				cmd.URI = "../../testdata/" + tc.cmd.URI
			}
			if tc.errMsg != "" {
				err := cmd.Run()
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				stdout, stderr := testutils.CaptureStdoutStderr(func() {
					require.NoError(t, cmd.Run())
				})
				require.Equal(t, testutils.LoadExpected(t, "../../testdata/golden/"+tc.golden), stdout)
				require.Equal(t, "", stderr)
			}
		})
	}
}

func BenchmarkSchemaCmd(b *testing.B) {
	savedStdout, savedStderr := os.Stdout, os.Stderr
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0o666)
	if err != nil {
		b.Fatal(err)
	}
	os.Stdout = devNull
	defer func() {
		os.Stdout, os.Stderr = savedStdout, savedStderr
		_ = devNull.Close()
	}()

	cmd := schema.Cmd{
		ReadOption: pio.ReadOption{},
		Format:     "json",
		URI:        "../../build/benchmark.parquet",
	}

	// Warm up the Go runtime before actual benchmark
	for range 10 {
		_ = cmd.Run()
	}

	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}
