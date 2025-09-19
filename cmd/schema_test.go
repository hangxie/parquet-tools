package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

func Test_SchemaCmd_Run_error(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    SchemaCmd
		errMsg string
	}{
		"invalid-uri":    {SchemaCmd{ReadOption: rOpt, Format: "foobar", URI: "dummy://location"}, "unknown location scheme"},
		"invalid-format": {SchemaCmd{ReadOption: rOpt, Format: "foobar", URI: "../testdata/good.parquet"}, "unknown schema format"},
		"go-map-value":   {SchemaCmd{ReadOption: rOpt, Format: "go", URI: "../testdata/map-composite-value.parquet"}, "go struct does not support composite type as map value in field [Parquet_go_root.Scores]"},
		"go-list-item":   {SchemaCmd{ReadOption: rOpt, Format: "go", URI: "../testdata/list-of-list.parquet"}, "go struct does not support composite type as list element in field [Parquet_go_root.Lol]"},
		"csv-nested":     {SchemaCmd{ReadOption: rOpt, Format: "csv", URI: "../testdata/csv-nested.parquet"}, "CSV supports flat schema only"},
		"csv-optional":   {SchemaCmd{ReadOption: rOpt, Format: "csv", URI: "../testdata/csv-optional.parquet"}, "CSV does not support optional column"},
		"csv-repeated":   {SchemaCmd{ReadOption: rOpt, Format: "csv", URI: "../testdata/csv-repeated.parquet"}, "CSV does not support column in LIST type"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func Test_SchemaCmd_Run_good(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    SchemaCmd
		golden string
	}{
		"raw":                 {SchemaCmd{ReadOption: rOpt, Format: "raw", URI: "all-types.parquet"}, "schema-all-types-raw.json"},
		"json":                {SchemaCmd{ReadOption: rOpt, Format: "json", URI: "all-types.parquet"}, "schema-all-types-json.json"},
		"go":                  {SchemaCmd{ReadOption: rOpt, Format: "go", URI: "all-types.parquet"}, "schema-all-types-go.txt"},
		"csv":                 {SchemaCmd{ReadOption: rOpt, Format: "csv", URI: "csv-good.parquet"}, "schema-csv-good.txt"},
		"raw-map-value-list":  {SchemaCmd{ReadOption: rOpt, Format: "raw", URI: "map-composite-value.parquet"}, "schema-map-composite-value-raw.json"},
		"json-map-value-list": {SchemaCmd{ReadOption: rOpt, Format: "json", URI: "map-composite-value.parquet"}, "schema-map-composite-value-json.json"},
		"json-map-value-map":  {SchemaCmd{ReadOption: rOpt, Format: "json", URI: "map-value-map.parquet"}, "schema-map-value-map-json.json"},
		"pargo-prefix-flat":   {SchemaCmd{ReadOption: rOpt, Format: "go", URI: "pargo-prefix-flat.parquet"}, "schema-pargo-prefix-flat-go.txt"},
		"pargo-prefix-nested": {SchemaCmd{ReadOption: rOpt, Format: "go", URI: "pargo-prefix-nested.parquet"}, "schema-pargo-prefix-nested-go.txt"},
		"geospatial-go":       {SchemaCmd{ReadOption: rOpt, Format: "go", URI: "geospatial.parquet"}, "schema-geospatial-go.txt"},
		"geospatial-json":     {SchemaCmd{ReadOption: rOpt, Format: "json", URI: "geospatial.parquet"}, "schema-geospatial-json.json"},
		"geospatial-raw":      {SchemaCmd{ReadOption: rOpt, Format: "raw", URI: "geospatial.parquet"}, "schema-geospatial-raw.json"},
		"camel-case":          {SchemaCmd{ReadOption: rOpt, Format: "go", CamelCase: true, URI: "good.parquet"}, "schema-good-go-camel-case.txt"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tc.cmd.URI = "../testdata/" + tc.cmd.URI
			tc.golden = "../testdata/golden/" + tc.golden
			stdout, stderr := captureStdoutStderr(func() {
				require.Nil(t, tc.cmd.Run())
			})
			expected := loadExpected(t, tc.golden)
			require.Equal(t, expected, stdout)
			require.Equal(t, "", stderr)
		})
	}
}

func Benchmark_SchemaCmd_Run(b *testing.B) {
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

	cmd := SchemaCmd{
		ReadOption: pio.ReadOption{},
		Format:     "json",
		URI:        "../build/benchmark.parquet",
	}
	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}
