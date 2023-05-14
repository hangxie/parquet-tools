package internal

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_JSONSchemaNode_Schema_good(t *testing.T) {
	option := ReadOption{}
	option.URI = "../testdata/all-types.parquet"
	pr, err := NewParquetFileReader(option)
	require.Nil(t, err)
	defer pr.PFile.Close()

	schemaRoot := NewSchemaTree(pr)
	require.NotNil(t, schemaRoot)

	schema := NewJSONSchemaNode(*schemaRoot).Schema()
	require.Nil(t, err)

	actual, _ := json.MarshalIndent(schema, "", "  ")
	expected, _ := os.ReadFile("../testdata/golden/schema-all-types-json.json")
	require.Equal(t, string(expected), string(actual)+"\n")
}
