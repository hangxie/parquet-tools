package internal

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/types"
)

func Test_NewSchemaTree(t *testing.T) {
	option := ReadOption{}
	option.URI = "../testdata/all-types.parquet"
	pr, err := NewParquetFileReader(option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	actual, _ := json.MarshalIndent(schemaRoot, "", "  ")
	expected, _ := os.ReadFile("../testdata/golden/schema-all-types-raw.json")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), string(actual))
}

func Test_SchemaNode_GetReinterpretFields(t *testing.T) {
	option := ReadOption{}
	option.URI = "../testdata/all-types.parquet"
	pr, err := NewParquetFileReader(option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	expected := []string{
		".Decimal1",
		".Decimal2",
		".Decimal3",
		".Decimal4",
		".DecimalPointer",
		".Int96",
		".Interval",
		".NestedList.Element.List.Element",
		".NestedMap.Value.List.Element",
	}

	fields := schemaRoot.GetReinterpretFields("", true)
	require.Equal(t, len(expected), len(fields))
	for _, field := range expected {
		require.Contains(t, fields, field)
	}
}

func Test_DecimalToFloat_nil(t *testing.T) {
	f64, err := DecimalToFloat(ReinterpretField{}, nil)
	require.Nil(t, err)
	require.Nil(t, f64)
}

func Test_typeStr(t *testing.T) {
	// all nil
	se := parquet.SchemaElement{}
	require.Equal(t, "STRUCT", typeStr(se))

	// Type only
	se.Type = new(parquet.Type)
	*se.Type = parquet.Type_FLOAT
	require.Equal(t, "FLOAT", typeStr(se))

	// both Type and ConvertedType
	se.ConvertedType = new(parquet.ConvertedType)
	*se.ConvertedType = parquet.ConvertedType_DECIMAL
	require.Equal(t, "FLOAT", typeStr(se))

	// ConvertedType only
	se.Type = nil
	require.Equal(t, "DECIMAL", typeStr(se))
}

func Test_repetitionTyeStr(t *testing.T) {
	require.Equal(t, "REQUIRED", repetitionTyeStr(parquet.SchemaElement{RepetitionType: nil}))

	rType := parquet.FieldRepetitionType_OPTIONAL
	require.Equal(t, "OPTIONAL", repetitionTyeStr(parquet.SchemaElement{RepetitionType: &rType}))

	rType = parquet.FieldRepetitionType_REQUIRED
	require.Equal(t, "REQUIRED", repetitionTyeStr(parquet.SchemaElement{RepetitionType: &rType}))

	rType = parquet.FieldRepetitionType_REPEATED
	require.Equal(t, "REPEATED", repetitionTyeStr(parquet.SchemaElement{RepetitionType: &rType}))
}

func Test_DecimalToFloat_int32(t *testing.T) {
	fieldAttr := ReinterpretField{
		Scale: 2,
	}
	f64, err := DecimalToFloat(fieldAttr, int32(0))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 0.0, *f64)

	f64, err = DecimalToFloat(fieldAttr, int32(11))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 0.11, *f64)

	f64, err = DecimalToFloat(fieldAttr, int32(222))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 2.22, *f64)

	f64, err = DecimalToFloat(fieldAttr, int32(-11))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, -0.11, *f64)

	f64, err = DecimalToFloat(fieldAttr, int32(-222))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, -2.22, *f64)
}

func Test_DecimalToFloat_int64(t *testing.T) {
	fieldAttr := ReinterpretField{
		Scale: 2,
	}
	f64, err := DecimalToFloat(fieldAttr, int64(0))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 0.0, *f64)

	f64, err = DecimalToFloat(fieldAttr, int64(11))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 0.11, *f64)

	f64, err = DecimalToFloat(fieldAttr, int64(222))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 2.22, *f64)

	f64, err = DecimalToFloat(fieldAttr, int64(-11))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, -0.11, *f64)

	f64, err = DecimalToFloat(fieldAttr, int64(-222))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, -2.22, *f64)
}

func Test_DecimalToFloat_string(t *testing.T) {
	fieldAttr := ReinterpretField{
		Scale:     2,
		Precision: 10,
	}

	f64, err := DecimalToFloat(fieldAttr, types.StrIntToBinary("000", "BigEndian", 0, true))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 0.0, *f64)

	f64, err = DecimalToFloat(fieldAttr, types.StrIntToBinary("011", "BigEndian", 0, true))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 0.11, *f64)

	f64, err = DecimalToFloat(fieldAttr, types.StrIntToBinary("222", "BigEndian", 0, true))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 2.22, *f64)

	f64, err = DecimalToFloat(fieldAttr, types.StrIntToBinary("-011", "BigEndian", 0, true))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, -0.11, *f64)

	f64, err = DecimalToFloat(fieldAttr, types.StrIntToBinary("-222", "BigEndian", 0, true))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, -2.22, *f64)
}

func Test_DecimalToFloat_invalid_type(t *testing.T) {
	fieldAttr := ReinterpretField{}

	f64, err := DecimalToFloat(fieldAttr, int(0))
	require.NotNil(t, err)
	require.Equal(t, "unknown type: int", err.Error())
	require.Nil(t, f64)

	f64, err = DecimalToFloat(fieldAttr, float32(0.0))
	require.NotNil(t, err)
	require.Equal(t, "unknown type: float32", err.Error())
	require.Nil(t, f64)

	f64, err = DecimalToFloat(fieldAttr, float64(0.0))
	require.NotNil(t, err)
	require.Equal(t, "unknown type: float64", err.Error())
	require.Nil(t, f64)
}

func Test_StringToBytes(t *testing.T) {
	var fieldAttr ReinterpretField
	require.Equal(t, []byte("123"), StringToBytes(fieldAttr, "123"))

	fieldAttr.ConvertedType = parquet.ConvertedType_INTERVAL
	require.Equal(t, []byte("321"), StringToBytes(fieldAttr, "123"))
}

func Test_TimeUnitToTag(t *testing.T) {
	require.Equal(t, "", TimeUnitToTag(nil))

	unit := parquet.TimeUnit{}
	require.Equal(t, "UNKNOWN_UNIT", TimeUnitToTag(&unit))

	unit = parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}}
	require.Equal(t, "NANOS", TimeUnitToTag(&unit))

	unit = parquet.TimeUnit{MICROS: &parquet.MicroSeconds{}}
	require.Equal(t, "MICROS", TimeUnitToTag(&unit))

	unit = parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}}
	require.Equal(t, "MILLIS", TimeUnitToTag(&unit))
}

func Test_JSON_schema_list_variant(t *testing.T) {
	buf, err := os.ReadFile("../testdata/golden/schema-list-variants-raw.json")
	require.Nil(t, err)

	se := SchemaNode{}
	require.Nil(t, json.Unmarshal(buf, &se))

	schemaRoot := jsonSchemaNode{se}
	schema := schemaRoot.Schema()
	actual, err := json.MarshalIndent(schema, "", "  ")
	require.Nil(t, err)

	expected, err := os.ReadFile("../testdata/golden/schema-list-variants-json.json")
	require.Nil(t, err)

	require.Equal(t, string(expected), string(actual)+"\n")
}

func Test_Json_schema_go_struct_good(t *testing.T) {
	option := ReadOption{}
	option.URI = "../testdata/all-types.parquet"
	pr, err := NewParquetFileReader(option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	actual, err := schemaRoot.GoStruct()
	require.Nil(t, err)
	expected, _ := os.ReadFile("../testdata/golden/schema-all-types-go.txt")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), string(actual))
}

func Test_Json_schema_json_schema_good(t *testing.T) {
	option := ReadOption{}
	option.URI = "../testdata/all-types.parquet"
	pr, err := NewParquetFileReader(option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	actual := schemaRoot.JSONSchema()

	raw, _ := os.ReadFile("../testdata/golden/schema-all-types-json.json")
	temp := JSONSchema{}
	_ = json.Unmarshal([]byte(raw), &temp)
	expected, _ := json.Marshal(temp)
	require.Equal(t, strings.TrimRight(string(expected), "\n"), actual)
}

func Test_Json_schema_csv_schema_good(t *testing.T) {
	option := ReadOption{}
	option.URI = "../testdata/csv-good.parquet"
	pr, err := NewParquetFileReader(option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	actual, err := schemaRoot.CSVSchema()
	require.Nil(t, err)
	expected, _ := os.ReadFile("../testdata/golden/schema-csv-good.txt")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), string(actual))
}

func Test_Json_schema_csv_schema_nested(t *testing.T) {
	option := ReadOption{}
	option.URI = "../testdata/csv-nested.parquet"
	pr, err := NewParquetFileReader(option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	_, err = schemaRoot.CSVSchema()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "CSV supports flat schema only")
}

func Test_Json_schema_csv_schema_optional(t *testing.T) {
	option := ReadOption{}
	option.URI = "../testdata/csv-optional.parquet"
	pr, err := NewParquetFileReader(option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	_, err = schemaRoot.CSVSchema()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "CSV does not support optional column")
}

func Test_Json_schema_csv_schema_repeated(t *testing.T) {
	option := ReadOption{}
	option.URI = "../testdata/csv-repeated.parquet"
	pr, err := NewParquetFileReader(option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	_, err = schemaRoot.CSVSchema()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "CSV does not support column in LIST typ")
}
