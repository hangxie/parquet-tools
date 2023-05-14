package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_CatCmd_Run_non_existent_file(t *testing.T) {
	cmd := &CatCmd{}
	cmd.Limit = 10
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "file/does/not/exist"
	cmd.Format = "json"

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open local")
}

func Test_CatCmd_Run_default_limit(t *testing.T) {
	cmd := &CatCmd{}
	cmd.Limit = 0
	cmd.PageSize = 10
	cmd.SampleRatio = 0.5
	cmd.URI = "../testdata/all-types.parquet"
	cmd.Format = "json"

	stdout, stderr := captureStdoutStderr(func() {
		err := cmd.Run(&Context{})
		require.Nil(t, err)
		require.Equal(t, cmd.Limit, ^uint64(0))
	})
	require.NotEqual(t, "", stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_invalid_page_size(t *testing.T) {
	cmd := &CatCmd{}
	cmd.Limit = 10
	cmd.PageSize = 0
	cmd.SampleRatio = 0.5
	cmd.URI = "../testdata/all-types.parquet"
	cmd.Format = "json"

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "invalid page size")
}

func Test_CatCmd_Run_invalid_sampling_too_big(t *testing.T) {
	cmd := &CatCmd{}
	cmd.Limit = 10
	cmd.PageSize = 10
	cmd.SampleRatio = 2.0
	cmd.URI = "../testdata/all-types.parquet"
	cmd.Format = "json"

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "invalid sampling")
}

func Test_CatCmd_Run_invalid_sampling_too_small(t *testing.T) {
	cmd := &CatCmd{}
	cmd.Limit = 10
	cmd.PageSize = 10
	cmd.SampleRatio = -0.5
	cmd.URI = "../testdata/all-types.parquet"
	cmd.Format = "json"

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "invalid sampling")
}

func Test_CatCmd_Run_good_default(t *testing.T) {
	cmd := &CatCmd{}
	cmd.Limit = 10
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/good.parquet"
	cmd.Format = "json"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "../testdata/golden/cat-good-json.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_stream(t *testing.T) {
	cmd := &CatCmd{}
	cmd.Limit = 10
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/good.parquet"
	cmd.Format = "jsonl"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "../testdata/golden/cat-good-jsonl.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_bad_format(t *testing.T) {
	cmd := &CatCmd{}
	cmd.Limit = 10
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/good.parquet"
	cmd.Format = "random-dude"

	stdout, stderr := captureStdoutStderr(func() {
		err := cmd.Run(&Context{})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "unknown format: random-dude")
	})
	require.Equal(t, "", stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_skip(t *testing.T) {
	cmd := &CatCmd{}
	cmd.Skip = 2
	cmd.Limit = 10
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/good.parquet"
	cmd.Format = "json"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "../testdata/golden/cat-good-json-skip-2.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_all_skip(t *testing.T) {
	cmd := &CatCmd{}
	cmd.Skip = 12
	cmd.Limit = 10
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/good.parquet"
	cmd.Format = "json"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, "[]\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_limit(t *testing.T) {
	cmd := &CatCmd{}
	cmd.Limit = 2
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/good.parquet"
	cmd.Format = "json"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "../testdata/golden/cat-good-json-limit-2.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_sampling(t *testing.T) {
	cmd := &CatCmd{}
	cmd.Limit = 2
	cmd.PageSize = 10
	cmd.SampleRatio = 0.0
	cmd.URI = "../testdata/good.parquet"
	cmd.Format = "json"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, "[]\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_empty(t *testing.T) {
	cmd := &CatCmd{}
	cmd.Limit = 2
	cmd.PageSize = 10
	cmd.SampleRatio = 0.0
	cmd.URI = "../testdata/empty.parquet"
	cmd.Format = "json"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, "[]\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_scalar(t *testing.T) {
	cmd := &CatCmd{}
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/reinterpret-scalar.parquet"
	cmd.Format = "jsonl"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "../testdata/golden/cat-reinterpret-scalar.jsonl")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_decimal_pointer(t *testing.T) {
	cmd := &CatCmd{}
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/reinterpret-pointer.parquet"
	cmd.Format = "jsonl"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "../testdata/golden/cat-reinterpret-pointer.jsonl")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_list(t *testing.T) {
	cmd := &CatCmd{}
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/reinterpret-list.parquet"
	cmd.Format = "jsonl"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "../testdata/golden/cat-reinterpret-list.jsonl")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_map_key(t *testing.T) {
	cmd := &CatCmd{}
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/reinterpret-map-key.parquet"
	cmd.Format = "jsonl"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "../testdata/golden/cat-reinterpret-map-key.jsonl")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_map_value(t *testing.T) {
	cmd := &CatCmd{}
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/reinterpret-map-value.parquet"
	cmd.Format = "jsonl"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "../testdata/golden/cat-reinterpret-map-value.jsonl")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_composite(t *testing.T) {
	cmd := &CatCmd{}
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/reinterpret-composite.parquet"
	cmd.Format = "jsonl"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})

	expected := loadExpected(t, "../testdata/golden/cat-reinterpret-composite.jsonl")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_csv(t *testing.T) {
	cmd := &CatCmd{}
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/good.parquet"
	cmd.Format = "csv"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})

	expected := loadExpected(t, "../testdata/golden/cat-good-csv.txt")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)

	cmd.NoHeader = true
	stdout, stderr = captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})

	expected = loadExpected(t, "../testdata/golden/cat-good-csv-no-header.txt")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_tsv(t *testing.T) {
	cmd := &CatCmd{}
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/good.parquet"
	cmd.Format = "tsv"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})

	expected := loadExpected(t, "../testdata/golden/cat-good-tsv.txt")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)

	cmd.NoHeader = true
	stdout, stderr = captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})

	expected = loadExpected(t, "../testdata/golden/cat-good-tsv-no-header.txt")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_CatCmd_Run_nested_csv(t *testing.T) {
	cmd := &CatCmd{}
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/all-types.parquet"
	cmd.Format = "csv"

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "field [Map] is not scalar type, cannot output in csv format")
}

func Test_CatCmd_Run_nested_tsv(t *testing.T) {
	cmd := &CatCmd{}
	cmd.PageSize = 10
	cmd.SampleRatio = 1.0
	cmd.URI = "../testdata/all-types.parquet"
	cmd.Format = "tsv"

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "field [Map] is not scalar type, cannot output in tsv format")
}
