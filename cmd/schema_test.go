package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/internal"
)

func Test_SchemaCmd_Run_error(t *testing.T) {
	rOpt := internal.ReadOption{}
	testCases := map[string]struct {
		cmd    SchemaCmd
		errMsg string
	}{
		"invalid-uri":    {SchemaCmd{rOpt, "foobar", "dummy://location", ""}, "unknown location scheme"},
		"invalid-format": {SchemaCmd{rOpt, "foobar", "../testdata/good.parquet", ""}, "unknown schema format"},
		"go-map-value":   {SchemaCmd{rOpt, "go", "../testdata/map-composite-value.parquet", "go"}, "go struct does not support composite type as map value in field [Parquet_go_root.Scores]"},
		"go-list-item":   {SchemaCmd{rOpt, "go", "../testdata/list-of-list.parquet", "go"}, "go struct does not support composite type as list element in field [Parquet_go_root.Lol]"},
		"csv-nested":     {SchemaCmd{rOpt, "csv", "../testdata/csv-nested.parquet", "csv"}, "CSV supports flat schema only"},
		"csv-optional":   {SchemaCmd{rOpt, "csv", "../testdata/csv-optional.parquet", "csv"}, "CSV does not support optional column"},
		"csv-repeated":   {SchemaCmd{rOpt, "csv", "../testdata/csv-repeated.parquet", "csv"}, "CSV does not support column in LIST type"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			require.NotNil(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func Test_SchemaCmd_Run_good(t *testing.T) {
	rOpt := internal.ReadOption{}
	testCases := map[string]struct {
		cmd    SchemaCmd
		golden string
	}{
		"raw":                 {SchemaCmd{rOpt, "raw", "all-types.parquet", ""}, "schema-all-types-raw.json"},
		"json":                {SchemaCmd{rOpt, "json", "all-types.parquet", ""}, "schema-all-types-json.json"},
		"go":                  {SchemaCmd{rOpt, "go", "all-types.parquet", ""}, "schema-all-types-go.txt"},
		"csv":                 {SchemaCmd{rOpt, "csv", "csv-good.parquet", ""}, "schema-csv-good.txt"},
		"raw-map-value-list":  {SchemaCmd{rOpt, "raw", "map-composite-value.parquet", ""}, "schema-map-composite-value-raw.json"},
		"json-map-value-list": {SchemaCmd{rOpt, "json", "map-composite-value.parquet", ""}, "schema-map-composite-value-json.json"},
		"json-map-value-map":  {SchemaCmd{rOpt, "json", "map-value-map.parquet", ""}, "schema-map-value-map-json.json"},
		"pargo-prefix-keep":   {SchemaCmd{rOpt, "json", "pargo-prefix-nested.parquet", ""}, "schema-pargo-prefix-nested-keep.json"},
		"pargo-prefix-remove": {SchemaCmd{rOpt, "json", "pargo-prefix-nested.parquet", "PARGO_PREFIX_"}, "schema-pargo-prefix-nested-remove.json"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
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
