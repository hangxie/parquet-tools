package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
)

func TestSchemaCmd(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    SchemaCmd
		golden string
		errMsg string
	}{
		// error cases
		"invalid-uri":       {cmd: SchemaCmd{ReadOption: rOpt, Format: "foobar", URI: "dummy://location"}, errMsg: "unknown location scheme"},
		"invalid-format":    {cmd: SchemaCmd{ReadOption: rOpt, Format: "foobar", URI: "../testdata/good.parquet"}, errMsg: "unknown schema format"},
		"go-map-value":      {cmd: SchemaCmd{ReadOption: rOpt, Format: "go", URI: "../testdata/map-composite-value.parquet"}, errMsg: "go struct does not support LIST as MAP value in Parquet_go_root.Scores"},
		"go-list-value":     {cmd: SchemaCmd{ReadOption: rOpt, Format: "go", URI: "../testdata/list-of-list.parquet"}, errMsg: "go struct does not support LIST of LIST in Parquet_go_root.Lol"},
		"go-old-style-list": {cmd: SchemaCmd{ReadOption: rOpt, Format: "go", URI: "../testdata/old-style-list.parquet"}, errMsg: "go struct does not support LIST of LIST in My_record.First.Second.A"},
		"csv-nested":        {cmd: SchemaCmd{ReadOption: rOpt, Format: "csv", URI: "../testdata/csv-nested.parquet"}, errMsg: "CSV supports flat schema only"},
		"csv-optional":      {cmd: SchemaCmd{ReadOption: rOpt, Format: "csv", URI: "../testdata/csv-optional.parquet"}, errMsg: "CSV does not support optional column"},
		"csv-repeated":      {cmd: SchemaCmd{ReadOption: rOpt, Format: "csv", URI: "../testdata/csv-repeated.parquet"}, errMsg: "CSV does not support column in LIST type"},
		// good cases - URI will be prefixed with "../testdata/"
		"raw":                 {cmd: SchemaCmd{ReadOption: rOpt, Format: "raw", URI: "all-types.parquet"}, golden: "schema-all-types-raw.json"},
		"json":                {cmd: SchemaCmd{ReadOption: rOpt, Format: "json", URI: "all-types.parquet"}, golden: "schema-all-types-json.json"},
		"go":                  {cmd: SchemaCmd{ReadOption: rOpt, Format: "go", URI: "all-types.parquet"}, golden: "schema-all-types-go.txt"},
		"csv":                 {cmd: SchemaCmd{ReadOption: rOpt, Format: "csv", URI: "csv-good.parquet"}, golden: "schema-csv-good.txt"},
		"raw-map-value-list":  {cmd: SchemaCmd{ReadOption: rOpt, Format: "raw", URI: "map-composite-value.parquet"}, golden: "schema-map-composite-value-raw.json"},
		"json-map-value-list": {cmd: SchemaCmd{ReadOption: rOpt, Format: "json", URI: "map-composite-value.parquet"}, golden: "schema-map-composite-value-json.json"},
		"json-map-value-map":  {cmd: SchemaCmd{ReadOption: rOpt, Format: "json", URI: "map-value-map.parquet"}, golden: "schema-map-value-map-json.json"},
		"pargo-prefix-flat":   {cmd: SchemaCmd{ReadOption: rOpt, Format: "go", URI: "pargo-prefix-flat.parquet"}, golden: "schema-pargo-prefix-flat-go.txt"},
		"pargo-prefix-nested": {cmd: SchemaCmd{ReadOption: rOpt, Format: "go", URI: "pargo-prefix-nested.parquet"}, golden: "schema-pargo-prefix-nested-go.txt"},
		"geospatial-go":       {cmd: SchemaCmd{ReadOption: rOpt, Format: "go", URI: "geospatial.parquet"}, golden: "schema-geospatial-go.txt"},
		"geospatial-json":     {cmd: SchemaCmd{ReadOption: rOpt, Format: "json", URI: "geospatial.parquet"}, golden: "schema-geospatial-json.json"},
		"geospatial-raw":      {cmd: SchemaCmd{ReadOption: rOpt, Format: "raw", URI: "geospatial.parquet"}, golden: "schema-geospatial-raw.json"},
		"camel-case":          {cmd: SchemaCmd{ReadOption: rOpt, Format: "go", CamelCase: true, URI: "good.parquet"}, golden: "schema-good-go-camel-case.txt"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if tc.errMsg == "" {
				t.Parallel()
			}
			cmd := tc.cmd
			if tc.golden != "" {
				cmd.URI = "../testdata/" + tc.cmd.URI
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

func BenchmarkSchemaCmd(b *testing.B) {
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
