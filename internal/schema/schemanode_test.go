package schema

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/hangxie/parquet-go/common"
	"github.com/hangxie/parquet-go/parquet"
	"github.com/hangxie/parquet-go/types"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

func Test_NewSchemaTree_fail_on_int96(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/all-types.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	_, err = NewSchemaTree(pr, SchemaOption{FailOnInt96: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "type INT96 which is not supporte")
}

func Test_NewSchemaTree_good(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/all-types.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.Nil(t, err)
	require.NotNil(t, schemaRoot)

	actual, _ := json.MarshalIndent(schemaRoot, "", "  ")
	expected, _ := os.ReadFile("../../testdata/golden/schema-all-types-raw.json")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), string(actual))
}

func Test_SchemaNode_GetReinterpretFields(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/all-types.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.Nil(t, err)
	require.NotNil(t, schemaRoot)

	expected := []string{
		strings.Join([]string{"Decimal1"}, common.PAR_GO_PATH_DELIMITER),
		strings.Join([]string{"Decimal2"}, common.PAR_GO_PATH_DELIMITER),
		strings.Join([]string{"Decimal3"}, common.PAR_GO_PATH_DELIMITER),
		strings.Join([]string{"Decimal4"}, common.PAR_GO_PATH_DELIMITER),
		strings.Join([]string{"DecimalPointer"}, common.PAR_GO_PATH_DELIMITER),
		strings.Join([]string{"Int96"}, common.PAR_GO_PATH_DELIMITER),
		strings.Join([]string{"Interval"}, common.PAR_GO_PATH_DELIMITER),
		strings.Join([]string{"NestedList", "element", "List", "element"}, common.PAR_GO_PATH_DELIMITER),
		strings.Join([]string{"NestedMap", "value", "List", "element"}, common.PAR_GO_PATH_DELIMITER),
	}

	fieldMap := map[string]ReinterpretField{}
	for _, field := range schemaRoot.GetReinterpretFields(true) {
		fieldMap[field.ExPath] = field
	}

	require.Equal(t, len(expected), len(fieldMap))
	for _, field := range expected {
		_, found := fieldMap[field]
		require.True(t, found)
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

	testCases := map[string]parquet.FieldRepetitionType{
		"OPTIONAL": parquet.FieldRepetitionType_OPTIONAL,
		"REQUIRED": parquet.FieldRepetitionType_REQUIRED,
		"REPEATED": parquet.FieldRepetitionType_REPEATED,
	}

	for expected, repetitionType := range testCases {
		t.Run(expected, func(t *testing.T) {
			require.Equal(t, expected, repetitionTyeStr(parquet.SchemaElement{RepetitionType: &repetitionType}))
		})
	}
}

func Test_DecimalToFloat_int32(t *testing.T) {
	fieldAttr := ReinterpretField{
		Scale: 2,
	}

	testCases := map[string]struct {
		intValue     int
		decimalValue float64
	}{
		"zero":                   {0, 0.0},
		"fraction-only":          {11, 0.11},
		"decimal":                {222, 2.22},
		"whole-only":             {300, 3.00},
		"negative-fraction-only": {-11, -0.11},
		"negative-decimal":       {-222, -2.22},
		"negative-whole-only":    {-300, -3.00},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			f64, err := DecimalToFloat(fieldAttr, int32(tc.intValue))
			require.Nil(t, err)
			require.NotNil(t, f64)
			require.Equal(t, tc.decimalValue, *f64)

			f64, err = DecimalToFloat(fieldAttr, int64(tc.intValue))
			require.Nil(t, err)
			require.NotNil(t, f64)
			require.Equal(t, tc.decimalValue, *f64)
		})
	}
}

func Test_DecimalToFloat_string(t *testing.T) {
	fieldAttr := ReinterpretField{
		Scale:     2,
		Precision: 10,
	}

	testCases := map[string]struct {
		strValue     string
		decimalValue float64
	}{
		"zero":                   {"000", 0.0},
		"fraction-only":          {"011", 0.11},
		"decimal":                {"222", 2.22},
		"whole-only":             {"300", 3.00},
		"negative-fraction-only": {"-011", -0.11},
		"negative-decimal":       {"-222", -2.22},
		"negative-whole-only":    {"-300", -3.00},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			f64, err := DecimalToFloat(fieldAttr, types.StrIntToBinary(tc.strValue, "BigEndian", 0, true))
			require.Nil(t, err)
			require.NotNil(t, f64)
			require.Equal(t, tc.decimalValue, *f64)
		})
	}
}

func Test_DecimalToFloat_invalid_type(t *testing.T) {
	fieldAttr := ReinterpretField{}
	testCases := []struct {
		value  any
		errMsg string
	}{
		{0, "unknown type: int"},
		{float32(0.0), "unknown type: float32"},
		{0.0, "unknown type: float64"},
	}

	for _, tc := range testCases {
		t.Run(tc.errMsg, func(t *testing.T) {
			f64, err := DecimalToFloat(fieldAttr, tc.value)
			require.Error(t, err)
			require.Equal(t, tc.errMsg, err.Error())
			require.Nil(t, f64)
		})
	}
}

func Test_StringToBytes(t *testing.T) {
	var fieldAttr ReinterpretField
	require.Equal(t, []byte("123"), StringToBytes(fieldAttr, "123"))

	fieldAttr.ConvertedType = parquet.ConvertedType_INTERVAL
	require.Equal(t, []byte("321"), StringToBytes(fieldAttr, "123"))
}

func Test_TimeUnitToTag(t *testing.T) {
	require.Equal(t, "", TimeUnitToTag(nil))

	testCases := map[string]struct {
		unit    parquet.TimeUnit
		unitTag string
	}{
		"empty-unit": {parquet.TimeUnit{}, "UNKNOWN_UNIT"},
		"nanos":      {parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}}, "NANOS"},
		"micros":     {parquet.TimeUnit{MICROS: &parquet.MicroSeconds{}}, "MICROS"},
		"millis":     {parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}}, "MILLIS"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.unitTag, TimeUnitToTag(&tc.unit))
		})
	}
}

func Test_JSON_schema_list_variant(t *testing.T) {
	buf, err := os.ReadFile("../../testdata/golden/schema-list-variants-raw.json")
	require.Nil(t, err)

	se := SchemaNode{}
	require.Nil(t, json.Unmarshal(buf, &se))

	schemaRoot := jsonSchemaNode{se}
	schema := schemaRoot.Schema()
	actual, err := json.MarshalIndent(schema, "", "  ")
	require.Nil(t, err)

	expected, err := os.ReadFile("../../testdata/golden/schema-list-variants-json.json")
	require.Nil(t, err)

	require.Equal(t, string(expected), string(actual)+"\n")
}

func Test_Json_schema_go_struct_good(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/all-types.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.Nil(t, err)
	require.NotNil(t, schemaRoot)

	actual, err := schemaRoot.GoStruct()
	require.Nil(t, err)
	expected, _ := os.ReadFile("../../testdata/golden/schema-all-types-go.txt")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), actual)
}

func Test_Json_schema_json_schema_good(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/all-types.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.Nil(t, err)
	require.NotNil(t, schemaRoot)

	actual := schemaRoot.JSONSchema()

	raw, _ := os.ReadFile("../../testdata/golden/schema-all-types-json.json")
	temp := JSONSchema{}
	_ = json.Unmarshal(raw, &temp)
	expected, _ := json.Marshal(temp)
	require.Equal(t, strings.TrimRight(string(expected), "\n"), actual)
}

func Test_Json_schema_csv_schema_good(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/csv-good.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.Nil(t, err)
	require.NotNil(t, schemaRoot)

	actual, err := schemaRoot.CSVSchema()
	require.Nil(t, err)
	expected, _ := os.ReadFile("../../testdata/golden/schema-csv-good.txt")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), actual)
}

func Test_Json_schema_csv_schema_nested(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/csv-nested.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.Nil(t, err)
	require.NotNil(t, schemaRoot)

	_, err = schemaRoot.CSVSchema()
	require.Error(t, err)
	require.Contains(t, err.Error(), "CSV supports flat schema only")
}

func Test_Json_schema_csv_schema_optional(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/csv-optional.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.Nil(t, err)
	require.NotNil(t, schemaRoot)

	_, err = schemaRoot.CSVSchema()
	require.Error(t, err)
	require.Contains(t, err.Error(), "CSV does not support optional column")
}

func Test_Json_schema_csv_schema_repeated(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/csv-repeated.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.Nil(t, err)
	require.NotNil(t, schemaRoot)

	_, err = schemaRoot.CSVSchema()
	require.Error(t, err)
	require.Contains(t, err.Error(), "CSV does not support column in LIST typ")
}
