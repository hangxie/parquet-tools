package schema

import (
	"context"
	"fmt"
	"go/format"
	"os"
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
)

func TestGoStructNode(t *testing.T) {
	t.Run("good", func(t *testing.T) {
		option := pio.ReadOption{}
		uri := "../testdata/all-types.parquet"
		pr, err := pio.NewParquetFileReader(context.Background(), uri, option)
		require.NoError(t, err)
		defer func() {
			_ = pr.PFile.Close()
		}()

		schemaRoot, err := NewSchemaTree(context.Background(), pr, SchemaOption{})
		require.NoError(t, err)
		require.NotNil(t, schemaRoot)

		typeStr, err := goStructNode{SchemaNode: *schemaRoot}.String()
		require.NoError(t, err)
		formatted, err := format.Source([]byte(typeStr))
		require.NoError(t, err)
		typeStr = string(formatted)

		expected, _ := os.ReadFile("../testdata/golden/schema-all-types-go.txt")
		// golden file has prefix of "type <root node name>"
		prefix := fmt.Sprintf("type %s ", schemaRoot.InNamePath[0])
		require.Equal(t, string(expected), prefix+typeStr+"\n")
	})

	t.Run("composite-map-key", func(t *testing.T) {
		option := pio.ReadOption{}
		uri := "../testdata/map-value-map.parquet"
		pr, err := pio.NewParquetFileReader(context.Background(), uri, option)
		require.NoError(t, err)
		defer func() {
			_ = pr.PFile.Close()
		}()

		schemaRoot, err := NewSchemaTree(context.Background(), pr, SchemaOption{})
		require.NoError(t, err)
		require.NotNil(t, schemaRoot)

		mapType := parquet.ConvertedType_MAP
		// 2nd field is "Scores", whose 1st field is "Key_value", whose 1st field is map's key
		schemaRoot.Children[1].Children[0].Children[0].ConvertedType = &mapType
		_, err = goStructNode{SchemaNode: *schemaRoot}.String()
		require.Error(t, err)
		require.Contains(t, err.Error(), "go struct does not support MAP as MAP value in [Parquet_go_root.Scores]")
	})

	t.Run("composite-map-value", func(t *testing.T) {
		option := pio.ReadOption{}
		uri := "../testdata/map-composite-value.parquet"
		pr, err := pio.NewParquetFileReader(context.Background(), uri, option)
		require.NoError(t, err)
		defer func() {
			_ = pr.PFile.Close()
		}()

		schemaRoot, err := NewSchemaTree(context.Background(), pr, SchemaOption{})
		require.NoError(t, err)
		require.NotNil(t, schemaRoot)

		_, err = goStructNode{SchemaNode: *schemaRoot}.String()
		require.Error(t, err)
		require.Contains(t, err.Error(), "go struct does not support LIST as MAP value in [Parquet_go_root.Scores]")
	})

	t.Run("invalid-scalar", func(t *testing.T) {
		option := pio.ReadOption{}
		uri := "../testdata/good.parquet"
		pr, err := pio.NewParquetFileReader(context.Background(), uri, option)
		require.NoError(t, err)
		defer func() {
			_ = pr.PFile.Close()
		}()

		schemaRoot, err := NewSchemaTree(context.Background(), pr, SchemaOption{})
		require.NoError(t, err)
		require.NotNil(t, schemaRoot)

		// 1st field is "Shoe_brand"
		schemaRoot.Children[0].Type = nil
		_, err = goStructNode{SchemaNode: *schemaRoot}.String()
		require.Error(t, err)
		require.Contains(t, err.Error(), "type not set")
	})

	t.Run("invalid-list", func(t *testing.T) {
		option := pio.ReadOption{}
		uri := "../testdata/all-types.parquet"
		pr, err := pio.NewParquetFileReader(context.Background(), uri, option)
		require.NoError(t, err)
		defer func() {
			_ = pr.PFile.Close()
		}()

		schemaRoot, err := NewSchemaTree(context.Background(), pr, SchemaOption{})
		require.NoError(t, err)
		require.NotNil(t, schemaRoot)

		invalidType := parquet.Type(999)
		// 45th field is "List", whose 1st field is "List", whose 1st field is "Element"
		schemaRoot.Children[45].Children[0].Children[0].Type = &invalidType
		_, err = goStructNode{SchemaNode: *schemaRoot}.String()
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown type: 999")
	})

	t.Run("invalid-map-key", func(t *testing.T) {
		option := pio.ReadOption{}
		uri := "../testdata/all-types.parquet"
		pr, err := pio.NewParquetFileReader(context.Background(), uri, option)
		require.NoError(t, err)
		defer func() {
			_ = pr.PFile.Close()
		}()

		schemaRoot, err := NewSchemaTree(context.Background(), pr, SchemaOption{})
		require.NoError(t, err)
		require.NotNil(t, schemaRoot)

		invalidType := parquet.Type(999)
		// 45th field is "Map", whose 1st field is "Key_value", whose 1st field is map's key
		schemaRoot.Children[45].Children[0].Children[0].Type = &invalidType
		_, err = goStructNode{SchemaNode: *schemaRoot}.String()
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown type: 999")
	})

	t.Run("invalid-map-value", func(t *testing.T) {
		option := pio.ReadOption{}
		uri := "../testdata/all-types.parquet"
		pr, err := pio.NewParquetFileReader(context.Background(), uri, option)
		require.NoError(t, err)
		defer func() {
			_ = pr.PFile.Close()
		}()

		schemaRoot, err := NewSchemaTree(context.Background(), pr, SchemaOption{})
		require.NoError(t, err)
		require.NotNil(t, schemaRoot)

		// 45th field is "Map", whose 1st field is "Key_value", whose 3rd field is map's value
		schemaRoot.Children[45].Children[0].Children[1].Type = nil
		schemaRoot.Children[45].Children[0].Children[1].ConvertedType = new(parquet.ConvertedType_BSON)
		_, err = goStructNode{SchemaNode: *schemaRoot}.String()
		require.Error(t, err)
		require.Contains(t, err.Error(), "type not set")
	})

	t.Run("invalid-list-element", func(t *testing.T) {
		option := pio.ReadOption{}
		uri := "../testdata/list-of-list.parquet"
		pr, err := pio.NewParquetFileReader(context.Background(), uri, option)
		require.NoError(t, err)
		defer func() {
			_ = pr.PFile.Close()
		}()

		schemaRoot, err := NewSchemaTree(context.Background(), pr, SchemaOption{})
		require.NoError(t, err)
		require.NotNil(t, schemaRoot)

		_, err = goStructNode{SchemaNode: *schemaRoot}.String()
		require.Error(t, err)
		require.Contains(t, err.Error(), "go struct does not support LIST of LIST in [Parquet_go_root.Lol]")
	})

	t.Run("malformed-list", func(t *testing.T) {
		testCases := map[string]func(*SchemaNode){
			"no-child": func(list *SchemaNode) {
				list.Children = nil
			},
			"nil-child": func(list *SchemaNode) {
				list.Children = []*SchemaNode{nil}
			},
			"element-without-type": func(list *SchemaNode) {
				list.Children[0].Children = nil
				list.Children[0].LogicalType = nil
				list.Children[0].Type = nil
			},
			"multiple-children": func(list *SchemaNode) {
				list.Children = append(list.Children, list.Children[0])
			},
			"logical-type-element-not-repeated": func(list *SchemaNode) {
				element := list.Children[0].Children[0].Children[0].Children[0]
				element.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
				list.Children[0] = element
			},
			"struct-element-not-repeated": func(list *SchemaNode) {
				element := list.Children[0]
				element.Children = append(element.Children, element.Children[0])
				element.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)
			},
			"interim-layer-not-repeated": func(list *SchemaNode) {
				list.Children[0].RepetitionType = nil
			},
			"scalar-element-not-repeated": func(list *SchemaNode) {
				list.Children[0].Children = nil
				list.Children[0].LogicalType = nil
				list.Children[0].Type = parquet.TypePtr(parquet.Type_INT32)
				list.Children[0].RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
			},
		}

		for name, corrupt := range testCases {
			t.Run(name, func(t *testing.T) {
				option := pio.ReadOption{}
				uri := "../testdata/list-of-list.parquet"
				pr, err := pio.NewParquetFileReader(context.Background(), uri, option)
				require.NoError(t, err)
				defer func() {
					_ = pr.PFile.Close()
				}()

				schemaRoot, err := NewSchemaTree(context.Background(), pr, SchemaOption{})
				require.NoError(t, err)
				require.NotNil(t, schemaRoot)

				corrupt(schemaRoot.Children[0])
				_, err = goStructNode{SchemaNode: *schemaRoot}.String()
				require.Error(t, err)
				require.Contains(t, err.Error(), "invalid LIST structure in [lol]")
			})
		}
	})

	t.Run("two-level-list", func(t *testing.T) {
		option := pio.ReadOption{}
		uri := "../testdata/list-of-list.parquet"
		pr, err := pio.NewParquetFileReader(context.Background(), uri, option)
		require.NoError(t, err)
		defer func() {
			_ = pr.PFile.Close()
		}()

		schemaRoot, err := NewSchemaTree(context.Background(), pr, SchemaOption{})
		require.NoError(t, err)
		require.NotNil(t, schemaRoot)

		// legacy 2-level LIST: the LIST group holds a repeated scalar element
		// without any logical type, ie "lol -> element" instead of
		// "lol -> list -> element"
		element := schemaRoot.Children[0].Children[0].Children[0]
		element.Type = parquet.TypePtr(parquet.Type_INT32)
		element.ConvertedType = nil
		element.LogicalType = nil
		element.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REPEATED)
		element.Children = nil
		schemaRoot.Children[0].Children[0] = element

		fieldStr, err := goStructNode{SchemaNode: *schemaRoot.Children[0]}.stringWithName()
		require.NoError(t, err)
		require.Equal(t, "Lol []int32 `parquet:\"name=lol, type=LIST, valuetype=INT32, convertedtype=LIST\"`", fieldStr)

		element.Type = parquet.TypePtr(parquet.Type(999))
		_, err = goStructNode{SchemaNode: *schemaRoot}.String()
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown type: 999")
	})

	t.Run("as-list", func(t *testing.T) {
		option := pio.ReadOption{}
		uri := "../testdata/gostruct-list.parquet"
		pr, err := pio.NewParquetFileReader(context.Background(), uri, option)
		require.NoError(t, err)
		defer func() {
			_ = pr.PFile.Close()
		}()

		root, err := NewSchemaTree(context.Background(), pr, SchemaOption{})
		require.NoError(t, err)
		require.NotNil(t, root)

		// remove interim layer from schema tree, ie from
		// "ListName -> list -> element" to the 2-level "ListName -> element",
		// whose element is repeated
		for _, list := range root.Children {
			element := list.Children[0].Children[0]
			element.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REPEATED)
			list.Children[0] = element
		}
		typeStr, err := goStructNode{SchemaNode: *root}.String()
		require.NoError(t, err)
		formatted, err := format.Source([]byte(typeStr))
		require.NoError(t, err)
		typeStr = string(formatted)

		expected, _ := os.ReadFile("../testdata/golden/schema-gostruct-list-go.txt")
		// golden file has prefix of "type <root node name>"
		prefix := fmt.Sprintf("type %s ", root.InNamePath[0])
		require.Equal(t, string(expected), prefix+typeStr+"\n")
	})
}
