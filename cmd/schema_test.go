package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_SchemaCmd_Run_invalid_format(t *testing.T) {
	cmd := &SchemaCmd{}
	cmd.URI = "../testdata/all-types.parquet"
	cmd.Format = "invalid"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unknown schema format")
}

func Test_SchemaCmd_Run_good_raw(t *testing.T) {
	cmd := &SchemaCmd{}
	cmd.URI = "../testdata/all-types.parquet"
	cmd.Format = "raw"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/schema-all-types-raw.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_good_json(t *testing.T) {
	cmd := &SchemaCmd{}
	cmd.URI = "../testdata/all-types.parquet"
	cmd.Format = "json"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/schema-all-types-json.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_good_go(t *testing.T) {
	cmd := &SchemaCmd{}
	cmd.URI = "../testdata/all-types.parquet"
	cmd.Format = "go"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/schema-all-types-go.txt")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_map_composite_value_raw(t *testing.T) {
	cmd := &SchemaCmd{}
	cmd.URI = "../testdata/map-composite-value.parquet"
	cmd.Format = "raw"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/schema-map-composite-value-raw.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_map_composite_value_json(t *testing.T) {
	cmd := &SchemaCmd{}
	cmd.URI = "../testdata/map-composite-value.parquet"
	cmd.Format = "json"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/schema-map-composite-value-json.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_map_composite_value_go(t *testing.T) {
	cmd := &SchemaCmd{}
	cmd.URI = "../testdata/map-composite-value.parquet"
	cmd.Format = "go"

	stdout, stderr := captureStdoutStderr(func() {
		require.NotNil(t, cmd.Run())
	})
	require.Equal(t, "", stdout)
	require.Contains(t, "go struct does not support composite type as map value in field [Parquet_go_root.Scores]", stderr)
}

func Test_SchemaCmd_Run_map_value_map(t *testing.T) {
	cmd := &SchemaCmd{}
	cmd.URI = "../testdata/map-value-map.parquet"
	cmd.Format = "json"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/schema-map-value-map-json.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_list_of_list_go(t *testing.T) {
	cmd := &SchemaCmd{}
	cmd.URI = "../testdata/list-of-list.parquet"
	cmd.Format = "go"

	stdout, stderr := captureStdoutStderr(func() {
		require.NotNil(t, cmd.Run())
	})
	require.Equal(t, "", stdout)
	require.Contains(t, "go struct does not support composite type as list element in field [Parquet_go_root.Lol]", stderr)
}
