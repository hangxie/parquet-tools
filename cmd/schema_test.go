package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go/parquet"
)

func Test_SchemaCmd_typeStr_both_nil(t *testing.T) {
	assert.Equal(t, "STRUCT", typeStr(parquet.SchemaElement{Type: nil, ConvertedType: nil}))
}

func Test_SchemaCmd_typeStr_converted_type_nil(t *testing.T) {
	pType := parquet.Type_FIXED_LEN_BYTE_ARRAY
	assert.Equal(t, "FIXED_LEN_BYTE_ARRAY", typeStr(parquet.SchemaElement{Type: &pType, ConvertedType: nil}))
}

func Test_SchemaCmd_typeStr_type_nil(t *testing.T) {
	cType := parquet.ConvertedType_LIST
	assert.Equal(t, "LIST", typeStr(parquet.SchemaElement{Type: nil, ConvertedType: &cType}))
}

func Test_SchemaCmd_typeStr_both_non_nil(t *testing.T) {
	pType := parquet.Type_FIXED_LEN_BYTE_ARRAY
	cType := parquet.ConvertedType_LIST
	assert.Equal(t, "FIXED_LEN_BYTE_ARRAY", typeStr(parquet.SchemaElement{Type: &pType, ConvertedType: &cType}))
}

func Test_SchemaCmd_repetitionTyeStr_good(t *testing.T) {
	assert.Equal(t, "REQUIRED", repetitionTyeStr(parquet.SchemaElement{RepetitionType: nil}))

	rType := parquet.FieldRepetitionType_OPTIONAL
	assert.Equal(t, "OPTIONAL", repetitionTyeStr(parquet.SchemaElement{RepetitionType: &rType}))

	rType = parquet.FieldRepetitionType_REQUIRED
	assert.Equal(t, "REQUIRED", repetitionTyeStr(parquet.SchemaElement{RepetitionType: &rType}))

	rType = parquet.FieldRepetitionType_REPEATED
	assert.Equal(t, "REPEATED", repetitionTyeStr(parquet.SchemaElement{RepetitionType: &rType}))
}

func Test_SchemaCmd_goTypeStr_good(t *testing.T) {
	assert.Equal(t, "", goTypeStr(parquet.SchemaElement{Type: nil}))

	pType := parquet.Type_BOOLEAN
	assert.Equal(t, "bool", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_INT32
	assert.Equal(t, "int32", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_INT64
	assert.Equal(t, "int64", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_INT96
	assert.Equal(t, "string", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_FLOAT
	assert.Equal(t, "float32", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_DOUBLE
	assert.Equal(t, "float64", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_BYTE_ARRAY
	assert.Equal(t, "string", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_FIXED_LEN_BYTE_ARRAY
	assert.Equal(t, "string", goTypeStr(parquet.SchemaElement{Type: &pType}))
}

func Test_SchemaCmd_Run_non_existent(t *testing.T) {
	cmd := &SchemaCmd{
		CommonOption: CommonOption{
			URI: "file/does/not/exist",
		},
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open local file")
}

func Test_SchemaCmd_Run_invalid_format(t *testing.T) {
	cmd := &SchemaCmd{
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
		Format: "invalid",
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unknown schema format")
}

func Test_SchemaCmd_Run_good_raw(t *testing.T) {
	cmd := &SchemaCmd{
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
		Format: "raw",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/schema-all-types-raw.json")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_good_json(t *testing.T) {
	cmd := &SchemaCmd{
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/schema-all-types-json.json")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_good_go(t *testing.T) {
	cmd := &SchemaCmd{
		CommonOption: CommonOption{
			URI: "file://./testdata/all-types.parquet",
		},
		Format: "go",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/schema-all-types-go.txt")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}
