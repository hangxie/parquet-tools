package schema

import (
	"encoding/json"
	"go/format"
	"os"
	"strings"
	"testing"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
)

func Test_NewSchemaTree_fail_on_int96(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../testdata/all-types.parquet"
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
	uri := "../testdata/all-types.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	actual, _ := json.MarshalIndent(schemaRoot, "", "  ")
	expected, _ := os.ReadFile("../testdata/golden/schema-all-types-raw.json")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), string(actual))
}

func Test_buildEncodingMap_empty_row_groups(t *testing.T) {
	result := buildEncodingMap([]*parquet.RowGroup{})
	require.NotNil(t, result)
	require.Empty(t, result)
}

func Test_buildEncodingMap_no_encodings(t *testing.T) {
	rowGroups := []*parquet.RowGroup{
		{
			Columns: []*parquet.ColumnChunk{
				{
					MetaData: &parquet.ColumnMetaData{
						PathInSchema: []string{"col1"},
						Encodings:    []parquet.Encoding{},
					},
				},
			},
		},
	}
	result := buildEncodingMap(rowGroups)
	require.NotNil(t, result)
	require.Empty(t, result)
}

func Test_buildEncodingMap_single_row_group(t *testing.T) {
	rowGroups := []*parquet.RowGroup{
		{
			Columns: []*parquet.ColumnChunk{
				{
					MetaData: &parquet.ColumnMetaData{
						PathInSchema: []string{"col1"},
						Encodings:    []parquet.Encoding{parquet.Encoding_PLAIN, parquet.Encoding_RLE},
					},
				},
				{
					MetaData: &parquet.ColumnMetaData{
						PathInSchema: []string{"col2"},
						Encodings:    []parquet.Encoding{parquet.Encoding_RLE_DICTIONARY},
					},
				},
			},
		},
	}
	result := buildEncodingMap(rowGroups)
	require.NotNil(t, result)
	require.Len(t, result, 2)
	require.Equal(t, "PLAIN", result["col1"])
	require.Equal(t, "RLE_DICTIONARY", result["col2"])
}

func Test_buildEncodingMap_multiple_row_groups(t *testing.T) {
	// Test that only the first row group is used
	rowGroups := []*parquet.RowGroup{
		{
			Columns: []*parquet.ColumnChunk{
				{
					MetaData: &parquet.ColumnMetaData{
						PathInSchema: []string{"col1"},
						Encodings:    []parquet.Encoding{parquet.Encoding_PLAIN},
					},
				},
			},
		},
		{
			Columns: []*parquet.ColumnChunk{
				{
					MetaData: &parquet.ColumnMetaData{
						PathInSchema: []string{"col1"},
						Encodings:    []parquet.Encoding{parquet.Encoding_RLE},
					},
				},
			},
		},
	}
	result := buildEncodingMap(rowGroups)
	require.NotNil(t, result)
	require.Len(t, result, 1)
	// Should use encoding from first row group
	require.Equal(t, "PLAIN", result["col1"])
}

func Test_buildEncodingMap_nested_paths(t *testing.T) {
	rowGroups := []*parquet.RowGroup{
		{
			Columns: []*parquet.ColumnChunk{
				{
					MetaData: &parquet.ColumnMetaData{
						PathInSchema: []string{"parent", "child", "field"},
						Encodings:    []parquet.Encoding{parquet.Encoding_DELTA_BINARY_PACKED},
					},
				},
			},
		},
	}
	result := buildEncodingMap(rowGroups)
	require.NotNil(t, result)
	require.Len(t, result, 1)
	require.Equal(t, "DELTA_BINARY_PACKED", result["parent"+common.PAR_GO_PATH_DELIMITER+"child"+common.PAR_GO_PATH_DELIMITER+"field"])
}

func Test_buildEncodingMap_from_real_file(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../testdata/good.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	result := buildEncodingMap(pr.Footer.RowGroups)
	require.NotNil(t, result)
	require.NotEmpty(t, result)
	// Verify we get encodings for the columns in the file (internal names are capitalized)
	require.Contains(t, result, "Shoe_brand")
	require.Contains(t, result, "Shoe_name")
	// Encodings should be valid encoding strings
	require.NotEmpty(t, result["Shoe_brand"])
	require.NotEmpty(t, result["Shoe_name"])
}

func Test_NewSchemaTree_with_encodings(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../testdata/good.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	// Verify encoding is populated in the schema nodes
	require.NotNil(t, schemaRoot.Children)
	require.Len(t, schemaRoot.Children, 2)

	// Check that leaf nodes have encoding populated
	for _, child := range schemaRoot.Children {
		if child.Type != nil {
			require.NotEmpty(t, child.Encoding, "Encoding should be set for leaf node %s", child.Name)
		}
	}
}

func Test_SchemaNode_GetPathMap(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../testdata/all-types.parquet"
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

func Test_OrderedTags(t *testing.T) {
	// Expected tags in order
	expected := []string{
		"name",
		"type",
		"keytype",
		"keyconvertedtype",
		"keyscale",
		"keyprecision",
		"valuetype",
		"valueconvertedtype",
		"valuescale",
		"valueprecision",
		"convertedtype",
		"scale",
		"precision",
		"length",
		"logicaltype",
		"logicaltype.precision",
		"logicaltype.scale",
		"logicaltype.isadjustedtoutc",
		"logicaltype.unit",
		"repetitiontype",
		"encoding",
		"omitstats",
	}

	// Get the ordered tags
	actual := OrderedTags()

	// Verify the tags are in the expected order
	require.Equal(t, expected, actual)

	// Verify that modifying the returned slice doesn't affect the internal state
	// (i.e., the function returns a copy)
	actual[0] = "modified"
	secondCall := OrderedTags()
	require.Equal(t, "name", secondCall[0], "Modifying returned slice should not affect internal orderedTags")
	require.Equal(t, expected, secondCall)
}

func Test_updateTagFromConvertedType(t *testing.T) {
	// Test with nan.parquet which has no converted type annotation
	option := pio.ReadOption{}
	uri := "../testdata/nan.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	// Find the "value" field which should not have converted type
	pathMap := schemaRoot.GetPathMap()
	valueNode, found := pathMap["Value"]
	require.True(t, found, "Value field should be found")
	require.NotNil(t, valueNode)

	// Verify that ConvertedType is nil
	require.Nil(t, valueNode.ConvertedType, "nan.parquet should not have converted type")

	// Get the tag map
	tagMap := valueNode.GetTagMap()

	// Verify that convertedtype tag is not set
	_, hasConvertedType := tagMap["convertedtype"]
	require.False(t, hasConvertedType, "convertedtype tag should not be present when ConvertedType is nil")

	// Verify expected tags are present
	require.Equal(t, "value", tagMap["name"])
	require.Equal(t, "DOUBLE", tagMap["type"])
}

func Test_updateTagFromLogicalType(t *testing.T) {
	// Test with nan.parquet which has no logical type annotation
	option := pio.ReadOption{}
	uri := "../testdata/nan.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	// Find the "value" field which should not have logical type
	pathMap := schemaRoot.GetPathMap()
	valueNode, found := pathMap["Value"]
	require.True(t, found, "Value field should be found")
	require.NotNil(t, valueNode)

	// Verify that LogicalType is nil
	require.Nil(t, valueNode.LogicalType, "nan.parquet should not have logical type")

	// Get the tag map
	tagMap := valueNode.GetTagMap()

	// Verify that logicaltype tag is not set
	_, hasLogicalType := tagMap["logicaltype"]
	require.False(t, hasLogicalType, "logicaltype tag should not be present when LogicalType is nil")

	// Verify that logicaltype.* tags are not set
	_, hasPrecision := tagMap["logicaltype.precision"]
	require.False(t, hasPrecision, "logicaltype.precision tag should not be present when LogicalType is nil")

	_, hasScale := tagMap["logicaltype.scale"]
	require.False(t, hasScale, "logicaltype.scale tag should not be present when LogicalType is nil")

	_, hasIsAdjusted := tagMap["logicaltype.isadjustedtoutc"]
	require.False(t, hasIsAdjusted, "logicaltype.isadjustedtoutc tag should not be present when LogicalType is nil")

	_, hasUnit := tagMap["logicaltype.unit"]
	require.False(t, hasUnit, "logicaltype.unit tag should not be present when LogicalType is nil")

	// Verify expected tags are present
	require.Equal(t, "value", tagMap["name"])
	require.Equal(t, "DOUBLE", tagMap["type"])

	// Test with geospatial.parquet which has GEOMETRY and GEOGRAPHY logical types
	uri = "../testdata/geospatial.parquet"
	pr, err = pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err = NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	pathMap = schemaRoot.GetPathMap()

	// Test GEOMETRY logical type
	geometryNode, found := pathMap["Geometry"]
	require.True(t, found, "Geometry field should be found")
	require.NotNil(t, geometryNode)
	require.NotNil(t, geometryNode.LogicalType, "Geometry field should have logical type")
	require.True(t, geometryNode.LogicalType.IsSetGEOMETRY(), "Geometry field should be GEOMETRY type")

	geometryTagMap := geometryNode.GetTagMap()
	require.Equal(t, "GEOMETRY", geometryTagMap["logicaltype"], "logicaltype tag should be GEOMETRY")
	require.Equal(t, "Geometry", geometryTagMap["name"])
	require.Equal(t, "BYTE_ARRAY", geometryTagMap["type"])

	// Test GEOGRAPHY logical type
	geographyNode, found := pathMap["Geography"]
	require.True(t, found, "Geography field should be found")
	require.NotNil(t, geographyNode)
	require.NotNil(t, geographyNode.LogicalType, "Geography field should have logical type")
	require.True(t, geographyNode.LogicalType.IsSetGEOGRAPHY(), "Geography field should be GEOGRAPHY type")

	geographyTagMap := geographyNode.GetTagMap()
	require.Equal(t, "GEOGRAPHY", geographyTagMap["logicaltype"], "logicaltype tag should be GEOGRAPHY")
	require.Equal(t, "Geography", geographyTagMap["name"])
	require.Equal(t, "BYTE_ARRAY", geographyTagMap["type"])
}

func Test_JSON_schema_list_variant(t *testing.T) {
	buf, err := os.ReadFile("../testdata/golden/schema-list-variants-raw.json")
	require.NoError(t, err)

	se := SchemaNode{}
	require.Nil(t, json.Unmarshal(buf, &se))

	schemaRoot := jsonSchemaNode{se}
	schema := schemaRoot.Schema()
	actual, err := json.MarshalIndent(schema, "", "  ")
	require.NoError(t, err)

	expected, err := os.ReadFile("../testdata/golden/schema-list-variants-json.json")
	require.NoError(t, err)

	require.Equal(t, string(expected), string(actual)+"\n")
}

func Test_Json_schema_go_struct(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../testdata/all-types.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	actual, err := schemaRoot.GoStruct(false)
	require.NoError(t, err)
	formatted, err := format.Source([]byte(actual))
	require.NoError(t, err)
	actual = string(formatted)

	expected, _ := os.ReadFile("../testdata/golden/schema-all-types-go.txt")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), actual)
	_ = pr.PFile.Close()

	uri = "../testdata/good.parquet"
	pr, err = pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)

	schemaRoot, err = NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	actual, err = schemaRoot.GoStruct(true)
	require.NoError(t, err)
	formatted, err = format.Source([]byte(actual))
	require.NoError(t, err)
	actual = string(formatted)

	expected, _ = os.ReadFile("../testdata/golden/schema-good-go-camel-case.txt")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), actual)
	_ = pr.PFile.Close()

	uri = "../testdata/list-of-list.parquet"
	pr, err = pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)

	schemaRoot, err = NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	_, err = schemaRoot.GoStruct(false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "go struct does not support LIST of LIST")
	_ = pr.PFile.Close()
}

func Test_Json_schema_json_schema_good(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../testdata/all-types.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	actual := schemaRoot.JSONSchema()

	raw, _ := os.ReadFile("../testdata/golden/schema-all-types-json.json")
	temp := JSONSchema{}
	_ = json.Unmarshal(raw, &temp)
	expected, _ := json.Marshal(temp)
	require.Equal(t, strings.TrimRight(string(expected), "\n"), actual)
}

func Test_Json_schema_csv_schema_good(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../testdata/csv-good.parquet"
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

	expected, _ := os.ReadFile("../testdata/golden/schema-csv-good.txt")
	require.Equal(t, strings.TrimRight(string(expected), "\n"), actual)
}

func Test_Json_schema_csv_schema_nested(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../testdata/csv-nested.parquet"
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
	uri := "../testdata/csv-optional.parquet"
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
	uri := "../testdata/csv-repeated.parquet"
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
