package schema

import (
	"fmt"
	"os"
	"testing"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

func Test_GoStructNode_String_good(t *testing.T) {
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

	typeStr, err := goStructNode{SchemaNode: *schemaRoot}.String()
	require.NoError(t, err)

	expected, _ := os.ReadFile("../../testdata/golden/schema-all-types-go.txt")
	// golden file has prefix of "type <root node name>"
	prefix := fmt.Sprintf("type %s ", schemaRoot.InNamePath[0])
	require.Equal(t, string(expected), prefix+typeStr+"\n")
}

func Test_GoStructNode_String_composite_map_key(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/map-value-map.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	mapType := parquet.ConvertedType_MAP
	// 2nd field is "Scores", whose 1st field is "Key_value", whose 1st field is map's key
	schemaRoot.Children[1].Children[0].Children[0].ConvertedType = &mapType
	_, err = goStructNode{SchemaNode: *schemaRoot}.String()
	require.Error(t, err)
	require.Contains(t, err.Error(), "go struct does not support MAP as MAP value in Parquet_go_root.Scores")
}

func Test_GoStructNode_String_composite_map_value(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/map-composite-value.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	_, err = goStructNode{SchemaNode: *schemaRoot}.String()
	require.Error(t, err)
	require.Contains(t, err.Error(), "go struct does not support LIST as MAP value in Parquet_go_root.Scores")
}

func Test_GoStructNode_String_invalid_scalar(t *testing.T) {
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

	// 1st field is "Shoe_brand"
	schemaRoot.Children[0].Type = nil
	_, err = goStructNode{SchemaNode: *schemaRoot}.String()
	require.Error(t, err)
	require.Contains(t, err.Error(), "type not set")
}

func Test_GoStructNode_String_invalid_list(t *testing.T) {
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

	invalidType := parquet.Type(999)
	// 45th field is "List", whose 1st field is "List", whose 1st field is "Element"
	schemaRoot.Children[45].Children[0].Children[0].Type = &invalidType
	_, err = goStructNode{SchemaNode: *schemaRoot}.String()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown type: 999")
}

func Test_GoStructNode_String_invalid_map_key(t *testing.T) {
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

	invalidType := parquet.Type(999)
	// 45th field is "Map", whose 1st field is "Key_value", whose 1st field is map's key
	schemaRoot.Children[45].Children[0].Children[0].Type = &invalidType
	_, err = goStructNode{SchemaNode: *schemaRoot}.String()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown type: 999")
}

func Test_GoStructNode_String_invalid_map_value(t *testing.T) {
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

	// 45th field is "Map", whose 1st field is "Key_value", whose 3rd field is map's value
	schemaRoot.Children[45].Children[0].Children[1].Type = nil
	schemaRoot.Children[45].Children[0].Children[1].ConvertedType = common.ToPtr(parquet.ConvertedType_BSON)
	_, err = goStructNode{SchemaNode: *schemaRoot}.String()
	require.Error(t, err)
	require.Contains(t, err.Error(), "type not set")
}

func Test_GoStructNode_String_invalid_list_element(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/list-of-list.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, schemaRoot)

	_, err = goStructNode{SchemaNode: *schemaRoot}.String()
	require.Error(t, err)
	require.Contains(t, err.Error(), "go struct does not support LIST of LIST in Parquet_go_root.Lol")
}

func Test_GoStructNode_asList(t *testing.T) {
	option := pio.ReadOption{}
	uri := "../../testdata/gostruct-list.parquet"
	pr, err := pio.NewParquetFileReader(uri, option)
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	root, err := NewSchemaTree(pr, SchemaOption{})
	require.NoError(t, err)
	require.NotNil(t, root)

	// remove interim layer from schema tree, ie
	// from "ListName -> list -> element" to "ListName -> element"
	root.Children[0].Children[0] = root.Children[0].Children[0].Children[0]
	root.Children[1].Children[0] = root.Children[1].Children[0].Children[0]
	typeStr, err := goStructNode{SchemaNode: *root}.String()
	require.NoError(t, err)

	expected, _ := os.ReadFile("../../testdata/golden/schema-gostruct-list-go.txt")
	// golden file has prefix of "type <root node name>"
	prefix := fmt.Sprintf("type %s ", root.InNamePath[0])
	require.Equal(t, string(expected), prefix+typeStr+"\n")
}
