package internal

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_JSONSchemaNode_Schema_good(t *testing.T) {
	option := ReadOption{}
	uri := "../testdata/all-types.parquet"
	pr, err := NewParquetFileReader(uri, option)
	require.Nil(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	schemaRoot, err := NewSchemaTree(pr, SchemaOption{})
	require.Nil(t, err)
	require.NotNil(t, schemaRoot)

	schema := jsonSchemaNode{*schemaRoot}.Schema()
	require.Nil(t, err)

	actual, _ := json.MarshalIndent(schema, "", "  ")
	expected, _ := os.ReadFile("../testdata/golden/schema-all-types-json.json")
	require.Equal(t, string(expected), string(actual)+"\n")
}
