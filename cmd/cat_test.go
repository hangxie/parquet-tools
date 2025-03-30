package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/internal"
)

func Test_CatCmd_Run_error(t *testing.T) {
	rOpt := internal.ReadOption{}
	fileName := "../testdata/all-types.parquet"
	testCases := map[string]struct {
		cmd    CatCmd
		errMsg string
	}{
		"non-existent-file":      {CatCmd{rOpt, 0, 10, 10, 10, 1.0, "json", false, "file/does/not/exist", false, ""}, "failed to open local"},
		"invalid-read-page-size": {CatCmd{rOpt, 0, 10, 10, 0, 0.5, "json", false, "does/not/matter", false, ""}, "invalid read page size"},
		"invalid-skip-size":      {CatCmd{rOpt, -10, 10, 10, 10, 0.5, "json", false, "does/not/matter", false, ""}, "invalid skip -10"},
		"invalid-skip-page-size": {CatCmd{rOpt, 10, 0, 10, 10, 0.5, "json", false, "does/not/matter", false, ""}, "invalid skip page size"},
		"sampling-too-high":      {CatCmd{rOpt, 10, 10, 10, 10, 2.0, "json", false, "does/not/matter", false, ""}, "invalid sampling"},
		"sampling-too-low":       {CatCmd{rOpt, 10, 10, 10, 10, -0.5, "json", false, "does/not/matter", false, ""}, "invalid sampling"},
		"invalid-format":         {CatCmd{rOpt, 10, 10, 10, 10, 0.5, "foobar", false, "does/not/matter", false, ""}, "unknown format: foobar"},
		"fail-on-int96":          {CatCmd{rOpt, 10, 10, 10, 10, 0.5, "json", true, fileName, true, ""}, "type INT96 which is not supported"},
		"nested-schema-csv":      {CatCmd{rOpt, 10, 10, 10, 10, 0.5, "csv", true, fileName, false, ""}, "cannot output in csv format"},
		"nested-schema-tsv":      {CatCmd{rOpt, 10, 10, 10, 10, 0.5, "tsv", true, fileName, false, ""}, "cannot output in tsv format"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.cmd.Run()
			if tc.errMsg == "" {
				require.Nil(t, err)
			} else {
				require.NotNil(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_CatCmd_Run_good(t *testing.T) {
	rOpt := internal.ReadOption{}
	testCases := map[string]struct {
		cmd    CatCmd
		golden string
	}{
		"default":           {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "json", false, "good.parquet", false, ""}, "cat-good-json.json"},
		"limit-0":           {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "json", false, "good.parquet", false, ""}, "cat-good-json.json"},
		"limit-2":           {CatCmd{rOpt, 0, 10, 2, 10, 1.0, "json", false, "good.parquet", false, ""}, "cat-good-json-limit-2.json"},
		"skip-2":            {CatCmd{rOpt, 2, 10, 0, 10, 1.0, "json", false, "good.parquet", false, ""}, "cat-good-json-skip-2.json"},
		"skip-all":          {CatCmd{rOpt, 20, 10, 0, 10, 1.0, "json", false, "good.parquet", false, ""}, "empty-json.txt"},
		"sampling-0":        {CatCmd{rOpt, 0, 10, 0, 10, 0.0, "json", false, "good.parquet", false, ""}, "empty-json.txt"},
		"empty":             {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "json", false, "empty.parquet", false, ""}, "empty-json.txt"},
		"RI-scalar":         {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "reinterpret-scalar.parquet", false, ""}, "cat-reinterpret-scalar.jsonl"},
		"RI-pointer":        {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "reinterpret-pointer.parquet", false, ""}, "cat-reinterpret-pointer.jsonl"},
		"RI-list":           {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "reinterpret-list.parquet", false, ""}, "cat-reinterpret-list.jsonl"},
		"RI-map-key":        {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "reinterpret-map-key.parquet", false, ""}, "cat-reinterpret-map-key.jsonl"},
		"RI-map-value":      {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "reinterpret-map-value.parquet", false, ""}, "cat-reinterpret-map-value.jsonl"},
		"RI-composite":      {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "reinterpret-composite.parquet", false, ""}, "cat-reinterpret-composite.jsonl"},
		"jsonl":             {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "jsonl", false, "good.parquet", false, ""}, "cat-good-jsonl.jsonl"},
		"csv":               {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "csv", false, "good.parquet", false, ""}, "cat-good-csv.txt"},
		"csv-no-header":     {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "csv", true, "good.parquet", false, ""}, "cat-good-csv-no-header.txt"},
		"tsv":               {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "tsv", false, "good.parquet", false, ""}, "cat-good-tsv.txt"},
		"tsv-no-header":     {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "tsv", true, "good.parquet", false, ""}, "cat-good-tsv-no-header.txt"},
		"pargo-keep-csv":    {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "csv", false, "pargo-prefix-flat.parquet", false, ""}, "cat-pargo-prefix-flat-keep.csv"},
		"pargo-remove-csv":  {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "csv", false, "pargo-prefix-flat.parquet", false, "PARGO_PREFIX_"}, "cat-pargo-prefix-flat-remove.csv"},
		"pargo-keep-json":   {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "json", false, "pargo-prefix-nested.parquet", false, ""}, "cat-pargo-prefix-nested-keep.json"},
		"pargo-remove-json": {CatCmd{rOpt, 0, 10, 0, 10, 1.0, "json", false, "pargo-prefix-nested.parquet", false, "PARGO_PREFIX_"}, "cat-pargo-prefix-nested-remove.json"},
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
