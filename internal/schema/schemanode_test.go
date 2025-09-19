package schema

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

func Test_NewSchemaTree_fail_on_int96(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/all-types.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	_, err = NewSchemaTree(pr, SchemaOption{FailOnInt96: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "type INT96 which is not supported")
}

func Test_NewSchemaTree_good(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/all-types.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	actual, _ := json.MarshalIndent(schemaRoot, "", "  ")
	expected, _ := os.ReadFile("../../testdata/golden/schema-all-types-raw.json")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), string(actual))
}

func Test_SchemaNode_GetPathMap(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/all-types.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	pathMap := schemaRoot.GetPathMap()
	require.NotNil(t, pathMap)

	// Test that root node is included with empty path
	rootNode, found := pathMap[""]
	require.True(t, found)
	require.Equal(t, schemaRoot, rootNode)

	// Test some expected paths exist (based on actual schema)
	expectedPaths := []string{
		"Bool",
		"Int32",
		"Int64",
		"Float",
		"Double",
		"ByteArray",
		"FixedLenByteArray",
		"Decimal1",
		"Decimal2",
		"Decimal3",
		"Decimal4",
		"DecimalPointer",
		"Int96",
		"Interval",
		"NestedList",
		"NestedMap",
		"List",
		"Map",
	}

	for _, path := range expectedPaths {
		node, found := pathMap[path]
		require.True(t, found, "Path %s should be found in path map", path)
		require.NotNil(t, node, "Node for path %s should not be nil", path)
		require.Equal(t, path, strings.Join(node.InNamePath[1:], common.PAR_GO_PATH_DELIMITER), "Path should match node's InNamePath")
	}

	// Test some known nested paths from the debug output
	knownNestedPaths := []string{
		"ListListElement",
		"MapKey_value",
		"MapKey_valueKey",
		"MapKey_valueValue",
		"NestedListList",
		"NestedListListElement",
		"NestedMapKey_value",
		"NestedMapKey_valueKey",
		"NestedMapKey_valueValue",
	}

	for _, path := range knownNestedPaths {
		node, found := pathMap[path]
		if found { // Only test if it exists (some may not exist in this particular test file)
			require.NotNil(t, node, "Node for nested path %s should not be nil", path)
			require.Equal(t, path, strings.Join(node.InNamePath[1:], common.PAR_GO_PATH_DELIMITER), "Nested path should match node's InNamePath")
		}
	}

	// Test that we have a reasonable number of paths (schema should be complex)
	require.Greater(t, len(pathMap), 20, "Should have many paths in a complex schema")

	// Test that all nodes in the map have valid InNamePath
	for path, node := range pathMap {
		require.NotNil(t, node, "Node should not be nil for path %s", path)
		expectedPath := strings.Join(node.InNamePath[1:], common.PAR_GO_PATH_DELIMITER)
		require.Equal(t, path, expectedPath, "Path key should match node's InNamePath for %s", path)

		// Ensure InNamePath is properly set
		require.NotEmpty(t, node.InNamePath, "InNamePath should not be empty for path %s", path)
		require.NotNil(t, node.InNamePath, "InNamePath should not be nil for path %s", path)
	}
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

func Test_timeUnitToTag(t *testing.T) {
	require.Equal(t, "", timeUnitToTag(nil))

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
			require.Equal(t, tc.unitTag, timeUnitToTag(&tc.unit))
		})
	}
}

func Test_JSON_schema_list_variant(t *testing.T) {
	buf, err := os.ReadFile("../../testdata/golden/schema-list-variants-raw.json")
	require.NoError(t, err)

	se := SchemaNode{}
	require.Nil(t, json.Unmarshal(buf, &se))

	schemaRoot := jsonSchemaNode{se}
	schema := schemaRoot.Schema()
	actual, err := json.MarshalIndent(schema, "", "  ")
	require.NoError(t, err)

	expected, err := os.ReadFile("../../testdata/golden/schema-list-variants-json.json")
	require.NoError(t, err)

	require.Equal(t, string(expected), string(actual)+"\n")
}

func Test_Json_schema_go_struct_good(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/all-types.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	actual, err := schemaRoot.GoStruct(false)
	require.NoError(t, err)
	expected, _ := os.ReadFile("../../testdata/golden/schema-all-types-go.txt")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), actual)
}

func Test_Json_schema_go_struct_good_camel_case(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/good.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	actual, err := schemaRoot.GoStruct(true)
	require.NoError(t, err)
	expected, _ := os.ReadFile("../../testdata/golden/schema-good-go-camel-case.txt")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), actual)
}

func Test_Json_schema_json_schema_good(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/all-types.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
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
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	actual, err := schemaRoot.CSVSchema()
	require.NoError(t, err)
	expected, _ := os.ReadFile("../../testdata/golden/schema-csv-good.txt")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), actual)
}

func Test_Json_schema_csv_schema_nested(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/csv-nested.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	_, err = schemaRoot.CSVSchema()
	require.Error(t, err)
	require.Contains(t, err.Error(), "CSV supports flat schema only")
}

func Test_Json_schema_csv_schema_optional(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/csv-optional.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	_, err = schemaRoot.CSVSchema()
	require.Error(t, err)
	require.Contains(t, err.Error(), "CSV does not support optional column")
}

func Test_Json_schema_csv_schema_repeated(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/csv-repeated.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	_, err = schemaRoot.CSVSchema()
	require.Error(t, err)
	require.Contains(t, err.Error(), "CSV does not support column in LIST typ")
}
