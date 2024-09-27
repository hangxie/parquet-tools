package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go/parquet"
)

func Test_GoStructNode_String_good(t *testing.T) {
	option := ReadOption{}
	uri := "../testdata/all-types.parquet"
	pr, err := NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	typeStr, err := goStructNode{*schemaRoot}.String()
	require.Nil(t, err)

	expected, _ := os.ReadFile("../testdata/golden/schema-all-types-go.txt")
	// golden file has prefix of "type <root node name>"
	prefix := fmt.Sprintf("type %s ", schemaRoot.Name)
	require.Equal(t, string(expected), prefix+typeStr+"\n")
}

func Test_GoStructNode_String_composite_map_key(t *testing.T) {
	option := ReadOption{}
	uri := "../testdata/map-value-map.parquet"
	pr, err := NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	mapType := parquet.ConvertedType_MAP
	// A bit explanation:
	// 2nd field is "Scores", its
	// 1st field is "Key_value", its
	// 1st field is map's key
	schemaRoot.Children[1].Children[0].Children[0].ConvertedType = &mapType
	_, err = goStructNode{*schemaRoot}.String()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "go struct does not support composite type as map key")
}

func Test_GoStructNode_String_composite_map_value(t *testing.T) {
	option := ReadOption{}
	uri := "../testdata/map-composite-value.parquet"
	pr, err := NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	_, err = goStructNode{*schemaRoot}.String()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "go struct does not support composite type as map value")
}

func Test_GoStructNode_String_invalid_scalar(t *testing.T) {
	option := ReadOption{}
	uri := "../testdata/good.parquet"
	pr, err := NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	// A bit explanation:
	// 1st field is "Shoe_brand"
	schemaRoot.Children[0].Type = nil
	_, err = goStructNode{*schemaRoot}.String()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "type not set")
}

func Test_GoStructNode_String_invalid_list(t *testing.T) {
	option := ReadOption{}
	uri := "../testdata/reinterpret-list.parquet"
	pr, err := NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	invalidType := parquet.Type(999)
	// A bit explanation:
	// 2nd field is "V1", its
	// 1st field is "List", its
	// 1st field is "Element"
	schemaRoot.Children[0].Children[0].Children[0].Type = &invalidType
	_, err = goStructNode{*schemaRoot}.String()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unknown type: 999")
}

func Test_GoStructNode_String_invalid_map_key(t *testing.T) {
	option := ReadOption{}
	uri := "../testdata/reinterpret-map-key.parquet"
	pr, err := NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	invalidType := parquet.Type(999)
	// A bit explanation:
	// 2nd field is "V1", its
	// 1st field is "Key_value", its
	// 1st field is map's key
	schemaRoot.Children[1].Children[0].Children[0].Type = &invalidType
	_, err = goStructNode{*schemaRoot}.String()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unknown type: 999")
}

func Test_GoStructNode_String_invalid_map_value(t *testing.T) {
	option := ReadOption{}
	uri := "../testdata/reinterpret-map-key.parquet"
	pr, err := NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	// A bit explanation:
	// 2nd field is "V1", its
	// 1st field is "Key_value", its
	// 3nd field is map's value
	schemaRoot.Children[1].Children[0].Children[1].Type = nil
	_, err = goStructNode{*schemaRoot}.String()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "type not set")
}

func Test_GoStructNode_String_invalid_list_element(t *testing.T) {
	option := ReadOption{}
	uri := "../testdata/list-of-list.parquet"
	pr, err := NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	_, err = goStructNode{*schemaRoot}.String()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "go struct does not support composite type as list element in field [Parquet_go_root.Lol]")
}

func Test_go_struct_list_variant(t *testing.T) {
	buf, err := os.ReadFile("../testdata/golden/schema-list-variants-raw.json")
	require.Nil(t, err)

	se := SchemaNode{}
	require.Nil(t, json.Unmarshal(buf, &se))

	schemaRoot := goStructNode{se}
	actual, err := schemaRoot.String()
	require.Nil(t, err)

	buf, err = os.ReadFile("../testdata/golden/schema-list-variants-go.txt")
	require.Nil(t, err)
	// un-gofmt ...
	expected := strings.ReplaceAll(string(buf), "\t", "")
	re := regexp.MustCompile(" +")
	expected = re.ReplaceAllString(expected, " ")
	expected = strings.TrimRight(expected, "\n")

	require.Equal(t, expected, actual)
}
