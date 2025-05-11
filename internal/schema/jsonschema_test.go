package schema

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

func Test_JSONSchemaNode_Schema_good(t *testing.T) {
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

	schema := jsonSchemaNode{*schemaRoot}.Schema()
	require.NoError(t, err)

	actual, _ := json.MarshalIndent(schema, "", "  ")
	expected, _ := os.ReadFile("../../testdata/golden/schema-all-types-json.json")
	require.Equal(t, string(expected), string(actual)+"\n")
}
