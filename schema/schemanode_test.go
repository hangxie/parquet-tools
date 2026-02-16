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

func TestNewSchemaTree(t *testing.T) {
	verifySchemaNode := func(t *testing.T, schemaRoot *SchemaNode, checkEncodings, checkNoEncodings bool) {
		t.Helper()
		for _, child := range schemaRoot.Children {
			if child.Type != nil && checkEncodings {
				require.NotEmpty(t, child.Encoding)
			}
		}
		for _, node := range schemaRoot.GetPathMap() {
			if node.Type != nil && checkNoEncodings {
				require.Empty(t, node.Encoding)
			}
		}
	}

	testCases := []struct {
		name             string
		uri              string
		option           SchemaOption
		goldenFile       string
		errMsg           string
		checkEncodings   bool
		checkNoEncodings bool
		expectedChildren int
	}{
		{
			name:   "fail on int96",
			uri:    "../testdata/all-types.parquet",
			option: SchemaOption{FailOnInt96: true},
			errMsg: "type INT96 which is not supported",
		},
		{
			name:       "good with golden file",
			uri:        "../testdata/all-types.parquet",
			option:     SchemaOption{},
			goldenFile: "../testdata/golden/schema-all-types-raw.json",
		},
		{
			name:             "with encodings populated",
			uri:              "../testdata/good.parquet",
			option:           SchemaOption{},
			checkEncodings:   true,
			expectedChildren: 2,
		},
		{
			name:             "skip-page-encoding",
			uri:              "../testdata/good.parquet",
			option:           SchemaOption{SkipPageEncoding: true},
			checkNoEncodings: true,
			expectedChildren: 2,
		},
		{
			name:       "unknown type with golden file",
			uri:        "../testdata/unknown-type.parquet",
			option:     SchemaOption{},
			goldenFile: "../testdata/golden/schema-unknown-type-raw.json",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			option := pio.ReadOption{}
			pr, err := pio.NewParquetFileReader(tc.uri, option)
			require.NoError(t, err)
			defer func() {
				_ = pr.PFile.Close()
			}()

			schemaRoot, err := NewSchemaTree(pr, tc.option)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, schemaRoot)

			if tc.goldenFile != "" {
				actual, _ := json.MarshalIndent(schemaRoot, "", "  ")
				expected, _ := os.ReadFile(tc.goldenFile)
				require.Equal(t, strings.TrimRight(string(expected), "\n"), string(actual))
			}

			if tc.expectedChildren > 0 {
				require.NotNil(t, schemaRoot.Children)
				require.Len(t, schemaRoot.Children, tc.expectedChildren)
			}

			verifySchemaNode(t, schemaRoot, tc.checkEncodings, tc.checkNoEncodings)
		})
	}
}

func TestBuildEncodingMap(t *testing.T) {
	testCases := []struct {
		name         string
		uri          string
		expectEmpty  bool
		expectedKeys []string // keys that should exist in the result
	}{
		{
			name:        "empty row groups",
			uri:         "../testdata/empty.parquet",
			expectEmpty: true,
		},
		{
			name:         "real file with data",
			uri:          "../testdata/good.parquet",
			expectEmpty:  false,
			expectedKeys: []string{"Shoe_brand", "Shoe_name"},
		},
		{
			name:        "data page v2 format",
			uri:         "../testdata/data-page-v2.parquet",
			expectEmpty: false,
		},
		{
			name:        "dictionary encoded file",
			uri:         "../testdata/dict-page.parquet",
			expectEmpty: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			option := pio.ReadOption{}
			pr, err := pio.NewParquetFileReader(tc.uri, option)
			require.NoError(t, err)
			defer func() {
				_ = pr.PFile.Close()
			}()

			result := buildEncodingMap(pr)
			require.NotNil(t, result)

			if tc.expectEmpty {
				require.Empty(t, result)
			} else {
				require.NotEmpty(t, result)
				for _, encoding := range result {
					require.NotEmpty(t, encoding)
				}
			}

			for _, key := range tc.expectedKeys {
				require.Contains(t, result, key)
				require.NotEmpty(t, result[key])
			}
		})
	}
}

func TestSchemaNodeGetPathMap(t *testing.T) {
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
		require.True(t, found)
		require.NotNil(t, node)
		require.Equal(t, path, strings.Join(node.InNamePath[1:], common.PAR_GO_PATH_DELIMITER))
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
			require.NotNil(t, node)
			require.Equal(t, path, strings.Join(node.InNamePath[1:], common.PAR_GO_PATH_DELIMITER))
		}
	}

	// Test that we have a reasonable number of paths (schema should be complex)
	require.Greater(t, len(pathMap), 20)

	// Test that all nodes in the map have valid InNamePath
	for path, node := range pathMap {
		require.NotNil(t, node)
		expectedPath := strings.Join(node.InNamePath[1:], common.PAR_GO_PATH_DELIMITER)
		require.Equal(t, path, expectedPath)

		// Ensure InNamePath is properly set
		require.NotEmpty(t, node.InNamePath)
		require.NotNil(t, node.InNamePath)
	}
}

func TestSchemaNodeGetInExNameMap(t *testing.T) {
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

	inExNameMap := schemaRoot.GetInExNameMap()
	require.NotNil(t, inExNameMap)

	// Root node should map to empty external path
	rootExPath, found := inExNameMap[""]
	require.True(t, found)
	require.Empty(t, rootExPath)

	// Test some expected top-level fields
	expectedFields := []string{
		"Bool",
		"Int32",
		"Int64",
		"Float",
		"Double",
		"ByteArray",
		"FixedLenByteArray",
	}

	for _, field := range expectedFields {
		exPath, found := inExNameMap[field]
		require.True(t, found, "expected field %q in inExNameMap", field)
		require.NotEmpty(t, exPath, "expected non-empty external path for %q", field)
	}

	// Verify consistency with GetPathMap: every key in pathMap should also be in inExNameMap
	pathMap := schemaRoot.GetPathMap()
	for path := range pathMap {
		_, found := inExNameMap[path]
		require.True(t, found, "path %q in GetPathMap but not in GetInExNameMap", path)
	}

	// Verify reasonable size
	require.Greater(t, len(inExNameMap), 20)
}

func TestTypeStr(t *testing.T) {
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

func TestRepetitionTypeStr(t *testing.T) {
	require.Equal(t, "REQUIRED", repetitionTypeStr(parquet.SchemaElement{RepetitionType: nil}))

	testCases := map[string]parquet.FieldRepetitionType{
		"OPTIONAL": parquet.FieldRepetitionType_OPTIONAL,
		"REQUIRED": parquet.FieldRepetitionType_REQUIRED,
		"REPEATED": parquet.FieldRepetitionType_REPEATED,
	}

	for expected, repetitionType := range testCases {
		t.Run(expected, func(t *testing.T) {
			require.Equal(t, expected, repetitionTypeStr(parquet.SchemaElement{RepetitionType: &repetitionType}))
		})
	}
}

func TestTimeUnitToTag(t *testing.T) {
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

func TestOrderedTags(t *testing.T) {
	// Expected tags in order
	expected := []string{
		"name",
		"inname",
		"type",
		"keytype",
		"keyconvertedtype",
		"keyscale",
		"keyprecision",
		"keyencoding",
		"keycompression",
		"valuetype",
		"valueconvertedtype",
		"valuescale",
		"valueprecision",
		"valueencoding",
		"valuecompression",
		"convertedtype",
		"scale",
		"precision",
		"length",
		"logicaltype",
		"logicaltype.precision",
		"logicaltype.scale",
		"logicaltype.isadjustedtoutc",
		"logicaltype.unit",
		"logicaltype.bitwidth",
		"logicaltype.issigned",
		"repetitiontype",
		"encoding",
		"compression",
		"omitstats",
		"bloomfilter",
		"bloomfiltersize",
	}

	// Get the ordered tags
	actual := OrderedTags()

	// Verify the tags are in the expected order
	require.Equal(t, expected, actual)

	// Verify that modifying the returned slice doesn't affect the internal state
	// (i.e., the function returns a copy)
	actual[0] = "modified"
	secondCall := OrderedTags()
	require.Equal(t, "name", secondCall[0])
	require.Equal(t, expected, secondCall)
}

func TestUpdateTagFromConvertedType(t *testing.T) {
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
	require.True(t, found)
	require.NotNil(t, valueNode)

	// Verify that ConvertedType is nil
	require.Nil(t, valueNode.ConvertedType)

	// Get the tag map
	tagMap := valueNode.GetTagMap()

	// Verify that convertedtype tag is not set
	_, hasConvertedType := tagMap["convertedtype"]
	require.False(t, hasConvertedType)

	// Verify expected tags are present
	require.Equal(t, "value", tagMap["name"])
	require.Equal(t, "DOUBLE", tagMap["type"])
}

func TestUpdateTagFromLogicalType(t *testing.T) {
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
	require.True(t, found)
	require.NotNil(t, valueNode)

	// Verify that LogicalType is nil
	require.Nil(t, valueNode.LogicalType)

	// Get the tag map
	tagMap := valueNode.GetTagMap()

	// Verify that logicaltype tag is not set
	_, hasLogicalType := tagMap["logicaltype"]
	require.False(t, hasLogicalType)

	// Verify that logicaltype.* tags are not set
	_, hasPrecision := tagMap["logicaltype.precision"]
	require.False(t, hasPrecision)

	_, hasScale := tagMap["logicaltype.scale"]
	require.False(t, hasScale)

	_, hasIsAdjusted := tagMap["logicaltype.isadjustedtoutc"]
	require.False(t, hasIsAdjusted)

	_, hasUnit := tagMap["logicaltype.unit"]
	require.False(t, hasUnit)

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
	require.True(t, found)
	require.NotNil(t, geometryNode)
	require.NotNil(t, geometryNode.LogicalType)
	require.True(t, geometryNode.LogicalType.IsSetGEOMETRY())

	geometryTagMap := geometryNode.GetTagMap()
	require.Equal(t, "GEOMETRY", geometryTagMap["logicaltype"])
	require.Equal(t, "Geometry", geometryTagMap["name"])
	require.Equal(t, "BYTE_ARRAY", geometryTagMap["type"])

	// Test GEOGRAPHY logical type
	geographyNode, found := pathMap["Geography"]
	require.True(t, found)
	require.NotNil(t, geographyNode)
	require.NotNil(t, geographyNode.LogicalType)
	require.True(t, geographyNode.LogicalType.IsSetGEOGRAPHY())

	geographyTagMap := geographyNode.GetTagMap()
	require.Equal(t, "GEOGRAPHY", geographyTagMap["logicaltype"])
	require.Equal(t, "Geography", geographyTagMap["name"])
	require.Equal(t, "BYTE_ARRAY", geographyTagMap["type"])
}

func TestJSONSchemaListVariant(t *testing.T) {
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

func TestGoStruct(t *testing.T) {
	testCases := []struct {
		name       string
		uri        string
		camelCase  bool
		goldenFile string
		errMsg     string
	}{
		{
			name:       "all types",
			uri:        "../testdata/all-types.parquet",
			camelCase:  false,
			goldenFile: "../testdata/golden/schema-all-types-go.txt",
		},
		{
			name:       "good with camel case",
			uri:        "../testdata/good.parquet",
			camelCase:  true,
			goldenFile: "../testdata/golden/schema-good-go-camel-case.txt",
		},
		{
			name:      "list of list not supported",
			uri:       "../testdata/list-of-list.parquet",
			camelCase: false,
			errMsg:    "go struct does not support LIST of LIST",
		},
		{
			name:       "unknown type",
			uri:        "../testdata/unknown-type.parquet",
			camelCase:  false,
			goldenFile: "../testdata/golden/schema-unknown-type-go.txt",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			option := pio.ReadOption{}
			pr, err := pio.NewParquetFileReader(tc.uri, option)
			require.NoError(t, err)
			defer func() {
				_ = pr.PFile.Close()
			}()

			schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
			require.NoError(t, err)
			require.NotNil(t, schemaRoot)

			actual, err := schemaRoot.GoStruct(tc.camelCase)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}

			require.NoError(t, err)
			formatted, err := format.Source([]byte(actual))
			require.NoError(t, err)

			expected, _ := os.ReadFile(tc.goldenFile)
			require.Equal(t, strings.TrimRight(string(expected), "\n"), string(formatted))
		})
	}
}

func TestJSONSchema(t *testing.T) {
	testCases := []struct {
		name       string
		uri        string
		goldenFile string
	}{
		{
			name:       "all types",
			uri:        "../testdata/all-types.parquet",
			goldenFile: "../testdata/golden/schema-all-types-json.json",
		},
		{
			name:       "unknown type",
			uri:        "../testdata/unknown-type.parquet",
			goldenFile: "../testdata/golden/schema-unknown-type-json.json",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			option := pio.ReadOption{}
			pr, err := pio.NewParquetFileReader(tc.uri, option)
			require.NoError(t, err)
			defer func() {
				_ = pr.PFile.Close()
			}()

			schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
			require.NoError(t, err)
			require.NotNil(t, schemaRoot)

			actual := schemaRoot.JSONSchema()

			raw, _ := os.ReadFile(tc.goldenFile)
			temp := JSONSchema{}
			_ = json.Unmarshal(raw, &temp)
			expected, _ := json.Marshal(temp)
			require.Equal(t, strings.TrimRight(string(expected), "\n"), actual)
		})
	}
}

func TestCSVSchema(t *testing.T) {
	testCases := []struct {
		name       string
		uri        string
		goldenFile string
		errMsg     string
	}{
		{
			name:       "good flat schema",
			uri:        "../testdata/csv-good.parquet",
			goldenFile: "../testdata/golden/schema-csv-good.txt",
		},
		{
			name:   "nested schema not supported",
			uri:    "../testdata/csv-nested.parquet",
			errMsg: "CSV supports flat schema only",
		},
		{
			name:   "optional column not supported",
			uri:    "../testdata/csv-optional.parquet",
			errMsg: "CSV does not support optional column",
		},
		{
			name:   "repeated column not supported",
			uri:    "../testdata/csv-repeated.parquet",
			errMsg: "CSV does not support column in LIST typ",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			option := pio.ReadOption{}
			pr, err := pio.NewParquetFileReader(tc.uri, option)
			require.NoError(t, err)
			defer func() {
				_ = pr.PFile.Close()
			}()

			schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
			require.NoError(t, err)
			require.NotNil(t, schemaRoot)

			actual, err := schemaRoot.CSVSchema()
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}

			require.NoError(t, err)
			expected, _ := os.ReadFile(tc.goldenFile)
			require.Equal(t, strings.TrimRight(string(expected), "\n"), actual)
		})
	}
}

func TestBuildCompressionCodecMap(t *testing.T) {
	testCases := []struct {
		name         string
		uri          string
		expectEmpty  bool
		expectedKeys []string
	}{
		{
			name:        "empty row groups",
			uri:         "../testdata/empty.parquet",
			expectEmpty: true,
		},
		{
			name:         "real file with data",
			uri:          "../testdata/good.parquet",
			expectEmpty:  false,
			expectedKeys: []string{"Shoe_brand", "Shoe_name"},
		},
		{
			name:        "all types file",
			uri:         "../testdata/all-types.parquet",
			expectEmpty: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			option := pio.ReadOption{}
			pr, err := pio.NewParquetFileReader(tc.uri, option)
			require.NoError(t, err)
			defer func() {
				_ = pr.PFile.Close()
			}()

			result := buildCompressionCodecMap(pr)
			require.NotNil(t, result)

			if tc.expectEmpty {
				require.Empty(t, result)
			} else {
				require.NotEmpty(t, result)
				for _, codec := range result {
					require.NotEmpty(t, codec)
				}
			}

			for _, key := range tc.expectedKeys {
				require.Contains(t, result, key)
				require.NotEmpty(t, result[key])
			}
		})
	}
}

func TestEncodingToString(t *testing.T) {
	testCases := []struct {
		name      string
		encodings []parquet.Encoding
		expected  []string
	}{
		{
			name:      "nil",
			encodings: nil,
			expected:  []string{},
		},
		{
			name:      "empty",
			encodings: []parquet.Encoding{},
			expected:  []string{},
		},
		{
			name:      "single",
			encodings: []parquet.Encoding{parquet.Encoding_PLAIN},
			expected:  []string{"PLAIN"},
		},
		{
			name:      "multiple-unsorted",
			encodings: []parquet.Encoding{parquet.Encoding_RLE, parquet.Encoding_PLAIN},
			expected:  []string{"PLAIN", "RLE"},
		},
		{
			name:      "multiple-sorted",
			encodings: []parquet.Encoding{parquet.Encoding_PLAIN, parquet.Encoding_RLE},
			expected:  []string{"PLAIN", "RLE"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := EncodingToString(tc.encodings)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestIsEncodingCompatible(t *testing.T) {
	testCases := []struct {
		encoding string
		dataType string
		expected bool
	}{
		// PLAIN works with all types
		{"PLAIN", "INT32", true},
		{"PLAIN", "INT64", true},
		{"PLAIN", "FLOAT", true},
		{"PLAIN", "DOUBLE", true},
		{"PLAIN", "BOOLEAN", true},
		{"PLAIN", "BYTE_ARRAY", true},
		{"PLAIN", "FIXED_LEN_BYTE_ARRAY", true},

		// Empty data type (struct/group) should return false
		{"PLAIN", "", false},
		{"RLE", "", false},

		// Integer encodings
		{"DELTA_BINARY_PACKED", "INT32", true},
		{"DELTA_BINARY_PACKED", "INT64", true},
		{"DELTA_BINARY_PACKED", "FLOAT", false},

		// Byte array encodings
		{"DELTA_LENGTH_BYTE_ARRAY", "BYTE_ARRAY", true},
		{"DELTA_LENGTH_BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY", false},
		{"DELTA_BYTE_ARRAY", "BYTE_ARRAY", true},
		{"DELTA_BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY", true},

		// Boolean encodings
		{"RLE", "BOOLEAN", true},
		{"BIT_PACKED", "BOOLEAN", true},

		// Float/Double encodings
		{"BYTE_STREAM_SPLIT", "FLOAT", true},
		{"BYTE_STREAM_SPLIT", "DOUBLE", true},
		{"BYTE_STREAM_SPLIT", "INT32", true},
		{"BYTE_STREAM_SPLIT", "INT64", true},
		{"BYTE_STREAM_SPLIT", "FIXED_LEN_BYTE_ARRAY", true},
		{"BYTE_STREAM_SPLIT", "BYTE_ARRAY", false},
		{"BYTE_STREAM_SPLIT", "BOOLEAN", false},

		// Dictionary encodings
		{"PLAIN_DICTIONARY", "INT32", true},
		{"PLAIN_DICTIONARY", "BOOLEAN", true},
		{"RLE_DICTIONARY", "FLOAT", true},
		{"RLE_DICTIONARY", "BYTE_ARRAY", true},

		// Case insensitivity
		{"plain", "int32", true},
		{"rle", "boolean", true},

		// Unknown
		{"UNKNOWN_ENCODING", "INT32", false},
		{"PLAIN", "UNKNOWN_TYPE", true},
		{"RLE", "INT96", false},
	}

	for _, tc := range testCases {
		t.Run(tc.encoding+"-"+tc.dataType, func(t *testing.T) {
			result := IsEncodingCompatible(tc.encoding, tc.dataType)
			require.Equal(t, tc.expected, result, "encoding=%s, type=%s", tc.encoding, tc.dataType)
		})
	}
}

func TestGetAllowedEncodings(t *testing.T) {
	testCases := []struct {
		name              string
		dataType          string
		expectedEncodings []string
	}{
		{
			name:              "BOOLEAN",
			dataType:          "BOOLEAN",
			expectedEncodings: []string{"PLAIN", "BIT_PACKED", "PLAIN_DICTIONARY", "RLE", "RLE_DICTIONARY"},
		},
		{
			name:              "BYTE_ARRAY",
			dataType:          "BYTE_ARRAY",
			expectedEncodings: []string{"PLAIN", "DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		},
		{
			name:              "INT32",
			dataType:          "INT32",
			expectedEncodings: []string{"PLAIN", "BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		},
		{
			name:              "INT64",
			dataType:          "INT64",
			expectedEncodings: []string{"PLAIN", "BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		},
		{
			name:              "FLOAT",
			dataType:          "FLOAT",
			expectedEncodings: []string{"PLAIN", "BYTE_STREAM_SPLIT", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		},
		{
			name:              "DOUBLE",
			dataType:          "DOUBLE",
			expectedEncodings: []string{"PLAIN", "BYTE_STREAM_SPLIT", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		},
		{
			name:              "FIXED_LEN_BYTE_ARRAY",
			dataType:          "FIXED_LEN_BYTE_ARRAY",
			expectedEncodings: []string{"PLAIN", "BYTE_STREAM_SPLIT", "DELTA_BYTE_ARRAY", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		},
		{
			name:              "unknown type",
			dataType:          "UNKNOWN_TYPE",
			expectedEncodings: []string{"PLAIN"},
		},
		{
			name:              "lowercase input",
			dataType:          "byte_array",
			expectedEncodings: []string{"PLAIN", "DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetAllowedEncodings(tc.dataType)
			require.Equal(t, tc.expectedEncodings, result)
		})
	}
}

func TestIsCompatible(t *testing.T) {
	int32Type := common.ToPtr(parquet.Type_INT32)
	fixed := common.ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
	ba := common.ToPtr(parquet.Type_BYTE_ARRAY)
	decimal := common.ToPtr(parquet.ConvertedType_DECIMAL)
	stringLT := &parquet.LogicalType{STRING: &parquet.StringType{}}

	tests := []struct {
		name   string
		a, b   *SchemaNode
		option CompareOption
		expect bool
	}{
		// nil handling
		{"both nil", nil, nil, CompareOption{}, true},
		{"a nil b non-nil", nil, &SchemaNode{SchemaElement: parquet.SchemaElement{Name: "root"}}, CompareOption{}, false},
		{"a non-nil b nil", &SchemaNode{SchemaElement: parquet.SchemaElement{Name: "root"}}, nil, CompareOption{}, false},

		// schema element fields
		{
			"identical nodes",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type}},
			CompareOption{},
			true,
		},
		{
			"different Type",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: common.ToPtr(parquet.Type_FLOAT)}},
			CompareOption{},
			false,
		},
		{
			"nil vs non-nil Type",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f"}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type}},
			CompareOption{},
			false,
		},
		{
			"different TypeLength for FIXED_LEN_BYTE_ARRAY",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: fixed, TypeLength: common.ToPtr(int32(12))}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: fixed, TypeLength: common.ToPtr(int32(16))}},
			CompareOption{},
			false,
		},
		{
			"TypeLength ignored for non-FIXED_LEN_BYTE_ARRAY",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: ba, TypeLength: common.ToPtr(int32(0))}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: ba}},
			CompareOption{},
			true,
		},
		{
			"different RepetitionType",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type, RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL)}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type, RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)}},
			CompareOption{},
			false,
		},
		{
			"different ConvertedType",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", ConvertedType: common.ToPtr(parquet.ConvertedType_LIST)}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", ConvertedType: common.ToPtr(parquet.ConvertedType_DECIMAL)}},
			CompareOption{},
			false,
		},
		{
			"different Scale for DECIMAL",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type, ConvertedType: decimal, Scale: common.ToPtr(int32(5))}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type, ConvertedType: decimal, Scale: common.ToPtr(int32(10))}},
			CompareOption{},
			false,
		},
		{
			"different Precision for DECIMAL",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type, ConvertedType: decimal, Precision: common.ToPtr(int32(10))}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type, ConvertedType: decimal, Precision: common.ToPtr(int32(20))}},
			CompareOption{},
			false,
		},
		{
			"Scale ignored for non-DECIMAL",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type, Scale: common.ToPtr(int32(0))}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type}},
			CompareOption{},
			true,
		},
		{
			"different LogicalType",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: ba, LogicalType: stringLT}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: ba, LogicalType: &parquet.LogicalType{DATE: &parquet.DateType{}}}},
			CompareOption{},
			false,
		},
		{
			"nil vs non-nil LogicalType",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: ba}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: ba, LogicalType: stringLT}},
			CompareOption{},
			false,
		},

		// root name handling
		{
			"different Name with CompareRootName",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "alpha", Type: int32Type}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "beta", Type: int32Type}},
			CompareOption{CompareRootName: true},
			false,
		},
		{
			"same Name with CompareRootName",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "alpha", Type: int32Type}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "alpha", Type: int32Type}},
			CompareOption{CompareRootName: true},
			true,
		},
		{
			"different root Name ignored by default",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "root1"}, Children: []*SchemaNode{{SchemaElement: parquet.SchemaElement{Name: "field", Type: int32Type}}}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "root2"}, Children: []*SchemaNode{{SchemaElement: parquet.SchemaElement{Name: "field", Type: int32Type}}}},
			CompareOption{},
			true,
		},
		{
			"different root Name fails with CompareRootName",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "root1"}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "root2"}},
			CompareOption{CompareRootName: true},
			false,
		},
		{
			"child name difference detected even without CompareRootName",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "root1"}, Children: []*SchemaNode{{SchemaElement: parquet.SchemaElement{Name: "field", Type: int32Type}}}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "root2"}, Children: []*SchemaNode{{SchemaElement: parquet.SchemaElement{Name: "other", Type: int32Type}}}},
			CompareOption{},
			false,
		},

		// children
		{
			"different child count",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "root"}, Children: []*SchemaNode{{SchemaElement: parquet.SchemaElement{Name: "a", Type: int32Type}}}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "root"}, Children: []*SchemaNode{{SchemaElement: parquet.SchemaElement{Name: "a", Type: int32Type}}, {SchemaElement: parquet.SchemaElement{Name: "b", Type: int32Type}}}},
			CompareOption{},
			false,
		},
		{
			"different child at leaf",
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "root"}, Children: []*SchemaNode{{SchemaElement: parquet.SchemaElement{Name: "a", Type: int32Type}}}},
			&SchemaNode{SchemaElement: parquet.SchemaElement{Name: "root"}, Children: []*SchemaNode{{SchemaElement: parquet.SchemaElement{Name: "a", Type: common.ToPtr(parquet.Type_INT64)}}}},
			CompareOption{},
			false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expect, tc.a.IsCompatible(tc.b, tc.option))
		})
	}

	// Writer directive tests need field mutation after construction
	t.Run("different writer directives ignored by default", func(t *testing.T) {
		a := &SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type}}
		a.Encoding = "PLAIN"
		a.CompressionCodec = "SNAPPY"
		a.BloomFilter = "true"
		a.BloomFilterSize = "1024"
		a.OmitStats = "true"

		b := &SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type}}
		b.Encoding = "RLE"
		b.CompressionCodec = "GZIP"
		b.BloomFilter = "false"
		b.OmitStats = "false"

		require.True(t, a.IsCompatible(b, CompareOption{}))
	})

	directiveTests := []struct {
		name   string
		fieldA func(*SchemaNode)
		fieldB func(*SchemaNode)
		option CompareOption
		expect bool
	}{
		{"CompareEncoding same", func(n *SchemaNode) { n.Encoding = "PLAIN" }, func(n *SchemaNode) { n.Encoding = "PLAIN" }, CompareOption{CompareEncoding: true}, true},
		{"CompareEncoding different", func(n *SchemaNode) { n.Encoding = "PLAIN" }, func(n *SchemaNode) { n.Encoding = "RLE" }, CompareOption{CompareEncoding: true}, false},
		{"CompareCompression same", func(n *SchemaNode) { n.CompressionCodec = "SNAPPY" }, func(n *SchemaNode) { n.CompressionCodec = "SNAPPY" }, CompareOption{CompareCompression: true}, true},
		{"CompareCompression different", func(n *SchemaNode) { n.CompressionCodec = "SNAPPY" }, func(n *SchemaNode) { n.CompressionCodec = "GZIP" }, CompareOption{CompareCompression: true}, false},
		{
			"CompareBloomFilter same",
			func(n *SchemaNode) { n.BloomFilter = "true"; n.BloomFilterSize = "1024" },
			func(n *SchemaNode) { n.BloomFilter = "true"; n.BloomFilterSize = "1024" },
			CompareOption{CompareBloomFilter: true},
			true,
		},
		{
			"CompareBloomFilter different",
			func(n *SchemaNode) { n.BloomFilter = "true"; n.BloomFilterSize = "1024" },
			func(n *SchemaNode) { n.BloomFilter = "false" },
			CompareOption{CompareBloomFilter: true},
			false,
		},
		{"CompareOmitStats same", func(n *SchemaNode) { n.OmitStats = "true" }, func(n *SchemaNode) { n.OmitStats = "true" }, CompareOption{CompareOmitStats: true}, true},
		{"CompareOmitStats different", func(n *SchemaNode) { n.OmitStats = "true" }, func(n *SchemaNode) { n.OmitStats = "false" }, CompareOption{CompareOmitStats: true}, false},
	}

	for _, tc := range directiveTests {
		t.Run(tc.name, func(t *testing.T) {
			a := &SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type}}
			tc.fieldA(a)
			b := &SchemaNode{SchemaElement: parquet.SchemaElement{Name: "f", Type: int32Type}}
			tc.fieldB(b)
			require.Equal(t, tc.expect, a.IsCompatible(b, tc.option))
		})
	}

	// Integration tests with real files
	integrationTests := []struct {
		name   string
		file1  string
		file2  string
		option CompareOption
		expect bool
	}{
		{"same file", "../testdata/good.parquet", "../testdata/good.parquet", CompareOption{}, true},
		{"same file with encoding+compression", "../testdata/good.parquet", "../testdata/good.parquet", CompareOption{CompareEncoding: true, CompareCompression: true}, true},
		{"different schemas", "../testdata/good.parquet", "../testdata/all-types.parquet", CompareOption{}, false},
	}
	for _, tc := range integrationTests {
		t.Run(tc.name, func(t *testing.T) {
			pr1, err := pio.NewParquetFileReader(tc.file1, pio.ReadOption{})
			require.NoError(t, err)
			defer func() { _ = pr1.PFile.Close() }()

			pr2, err := pio.NewParquetFileReader(tc.file2, pio.ReadOption{})
			require.NoError(t, err)
			defer func() { _ = pr2.PFile.Close() }()

			tree1, err := NewSchemaTree(pr1, SchemaOption{})
			require.NoError(t, err)
			tree2, err := NewSchemaTree(pr2, SchemaOption{})
			require.NoError(t, err)

			require.Equal(t, tc.expect, tree1.IsCompatible(tree2, tc.option))
		})
	}
}

func TestGetTagMapWithCompression(t *testing.T) {
	option := pio.ReadOption{}
	pr, err := pio.NewParquetFileReader("../testdata/good.parquet", option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	// Find a leaf node and verify compression tag is in the tag map
	for _, child := range schemaRoot.Children {
		if child.Type != nil {
			tagMap := child.GetTagMap()
			_, hasCompression := tagMap["compression"]
			require.True(t, hasCompression)
			require.NotEmpty(t, tagMap["compression"])
			break
		}
	}
}
