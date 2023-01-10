package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go/parquet"
)

func Test_SchemaCmd_typeStr_both_nil(t *testing.T) {
	require.Equal(t, "STRUCT", typeStr(parquet.SchemaElement{Type: nil, ConvertedType: nil}))
}

func Test_SchemaCmd_typeStr_converted_type_nil(t *testing.T) {
	pType := parquet.Type_FIXED_LEN_BYTE_ARRAY
	require.Equal(t, "FIXED_LEN_BYTE_ARRAY", typeStr(parquet.SchemaElement{Type: &pType, ConvertedType: nil}))
}

func Test_SchemaCmd_typeStr_type_nil(t *testing.T) {
	cType := parquet.ConvertedType_LIST
	require.Equal(t, "LIST", typeStr(parquet.SchemaElement{Type: nil, ConvertedType: &cType}))
}

func Test_SchemaCmd_typeStr_both_non_nil(t *testing.T) {
	pType := parquet.Type_FIXED_LEN_BYTE_ARRAY
	cType := parquet.ConvertedType_LIST
	require.Equal(t, "FIXED_LEN_BYTE_ARRAY", typeStr(parquet.SchemaElement{Type: &pType, ConvertedType: &cType}))
}

func Test_SchemaCmd_repetitionTyeStr_good(t *testing.T) {
	require.Equal(t, "REQUIRED", repetitionTyeStr(parquet.SchemaElement{RepetitionType: nil}))

	rType := parquet.FieldRepetitionType_OPTIONAL
	require.Equal(t, "OPTIONAL", repetitionTyeStr(parquet.SchemaElement{RepetitionType: &rType}))

	rType = parquet.FieldRepetitionType_REQUIRED
	require.Equal(t, "REQUIRED", repetitionTyeStr(parquet.SchemaElement{RepetitionType: &rType}))

	rType = parquet.FieldRepetitionType_REPEATED
	require.Equal(t, "REPEATED", repetitionTyeStr(parquet.SchemaElement{RepetitionType: &rType}))
}

func Test_SchemaCmd_goTypeStr_good(t *testing.T) {
	require.Equal(t, "", goTypeStr(parquet.SchemaElement{Type: nil}))

	pType := parquet.Type_BOOLEAN
	require.Equal(t, "bool", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_INT32
	require.Equal(t, "int32", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_INT64
	require.Equal(t, "int64", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_INT96
	require.Equal(t, "string", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_FLOAT
	require.Equal(t, "float32", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_DOUBLE
	require.Equal(t, "float64", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_BYTE_ARRAY
	require.Equal(t, "string", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_FIXED_LEN_BYTE_ARRAY
	require.Equal(t, "string", goTypeStr(parquet.SchemaElement{Type: &pType}))
}

func Test_SchemaCmd_Run_non_existent(t *testing.T) {
	cmd := &SchemaCmd{
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "file/does/not/exist",
			},
		},
	}

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open local file")
}

func Test_SchemaCmd_Run_invalid_format(t *testing.T) {
	cmd := &SchemaCmd{
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
		},
		Format: "invalid",
	}

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unknown schema format")
}

func Test_SchemaCmd_Run_good_raw(t *testing.T) {
	cmd := &SchemaCmd{
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
		},
		Format: "raw",
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/schema-all-types-raw.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_good_json(t *testing.T) {
	cmd := &SchemaCmd{
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/schema-all-types-json.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_good_go(t *testing.T) {
	cmd := &SchemaCmd{
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "file://./testdata/all-types.parquet",
			},
		},
		Format: "go",
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/schema-all-types-go.txt")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_map_composite_value_raw(t *testing.T) {
	cmd := &SchemaCmd{
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/map-composite-value.parquet",
			},
		},
		Format: "raw",
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/schema-map-composite-value-raw.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_map_composite_value_json(t *testing.T) {
	cmd := &SchemaCmd{
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/map-composite-value.parquet",
			},
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/schema-map-composite-value-json.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_map_composite_value_go(t *testing.T) {
	cmd := &SchemaCmd{
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "file://./testdata/map-composite-value.parquet",
			},
		},
		Format: "go",
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.NotNil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, "", stdout)
	require.Contains(t, "go struct does not support composite type as map value in field [Parquet_go_root.Scores]", stderr)
}
