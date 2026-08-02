package schema

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
)

func TestJSONSchemaNode(t *testing.T) {
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

	schema := jsonSchemaNode{*schemaRoot}.Schema()
	require.NoError(t, err)

	actual, _ := json.MarshalIndent(schema, "", "  ")
	expected, _ := os.ReadFile("../testdata/golden/schema-all-types-json.json")
	require.Equal(t, string(expected), string(actual)+"\n")
}

func TestNormalizeMapForRenderingRejectsInvalidLayer(t *testing.T) {
	mapType := parquet.ConvertedType_MAP
	listType := parquet.ConvertedType_LIST
	node := jsonSchemaNode{SchemaNode{SchemaElement: parquet.SchemaElement{ConvertedType: &mapType}, Children: []*SchemaNode{
		{
			SchemaElement: parquet.SchemaElement{ConvertedType: &listType},
			Children:      []*SchemaNode{{}, {}},
		},
	}}}
	originalChild := node.Children[0]

	node.normalizeMapForRendering()

	require.Same(t, originalChild, node.Children[0])
}
