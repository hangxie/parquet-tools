package importcmd

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

func TestValueModeRoundTrip(t *testing.T) {
	for _, tc := range []struct{ fixture, mode string }{
		{"all-types.parquet", "raw"},
		{"geospatial.parquet", "raw"},
		{"retype.parquet", "raw"},
		{"map-key-types.parquet", "raw"},
		{"map-key-types.parquet", "interpreted"},
		{"struct-map-keys.parquet", "raw"},
		{"struct-map-keys.parquet", "interpreted"},
	} {
		t.Run(tc.fixture+"/"+tc.mode, func(t *testing.T) {
			path := filepath.Join("../../testdata", tc.fixture)
			reader, err := pio.NewParquetFileReader(context.Background(), path, pio.ReadOption{})
			require.NoError(t, err)
			tree, err := pschema.NewSchemaTree(context.Background(), reader, pschema.SchemaOption{})
			require.NoError(t, err)
			require.NoError(t, reader.PFile.Close())
			for _, format := range []string{"json", "jsonl"} {
				t.Run(format, func(t *testing.T) {
					dir := t.TempDir()
					catCmd := importTestCatCmd(path, pio.ReadOption{})
					catCmd.ValueMode = tc.mode
					catCmd.Format = format
					original := testutils.CommandStdout(t, catCmd)
					source, schema := filepath.Join(dir, "input"), filepath.Join(dir, "schema")
					require.NoError(t, os.WriteFile(source, []byte(original), 0o600))
					require.NoError(t, os.WriteFile(schema, []byte(tree.JSONSchema()), 0o600))
					output := filepath.Join(dir, "output.parquet")
					importCmd := Cmd{Source: source, Schema: schema, Format: format, ValueMode: tc.mode, URI: output}
					require.NoError(t, importCmd.Run(context.Background()))
					catCmd.URI = output
					require.Equal(t, original, testutils.CommandStdout(t, catCmd))
					require.True(t, testutils.HasSameSchema(path, output))
				})
			}
		})
	}
}

func TestRawCSVValueMode(t *testing.T) {
	const sourceData = "999999999999999999,\"AQEAAAAAAAAAAAAAAAAAAAAAAAAA\",\"AQEAAAAAAAAAAAAAAAAAAAAAAAAA\",\"DAAAABBpAAEAAAAA\",SGVsbG8=\n"
	const schemaData = "name=Amount, type=INT64, convertedtype=DECIMAL, precision=18, scale=2\nname=Geography, type=BYTE_ARRAY, logicaltype=GEOGRAPHY\nname=Geometry, type=BYTE_ARRAY, logicaltype=GEOMETRY\nname=Document, type=BYTE_ARRAY, convertedtype=BSON\nname=Text, type=BYTE_ARRAY, convertedtype=UTF8\n"
	dir := t.TempDir()
	source, schema := filepath.Join(dir, "input.csv"), filepath.Join(dir, "schema")
	require.NoError(t, os.WriteFile(source, []byte(sourceData), 0o600))
	require.NoError(t, os.WriteFile(schema, []byte(schemaData), 0o600))
	cmd := Cmd{Source: source, Schema: schema, Format: "csv", ValueMode: "raw", URI: filepath.Join(dir, "output.parquet")}
	require.NoError(t, cmd.Run(context.Background()))
	catCmd := importTestCatCmd(cmd.URI, pio.ReadOption{})
	catCmd.ValueMode = "raw"
	catCmd.Format = "csv"
	catCmd.NoHeader = true
	expected, err := csv.NewReader(strings.NewReader(sourceData)).ReadAll()
	require.NoError(t, err)
	actual, err := csv.NewReader(strings.NewReader(testutils.CommandStdout(t, catCmd))).ReadAll()
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestDuplicateStructMapKey(t *testing.T) {
	const pointA, pointB = `{"x":1,"y":2}`, `{"y":2,"x":1}`
	const nestedRaw = `{"id":1,"inner":{"x":2,"doc":"DAAAABBhAAEAAAAA"}}`
	const nestedInterpreted = `{"id":1,"inner":{"x":2,"doc":"{\"a\":1}"}}`
	const bytesTail = `"doc":"{}","fixed":0.00,"float16":1,"json":"1","raw":"AQ==","text":"t"`
	// Each pair spells one key two ways: reordered fields, or another spelling of a value.
	for _, tc := range []struct {
		name, fixture, column, mode, a, b string
		empty                             []string
	}{
		{"struct/raw", "map-key-types.parquet", "struct_key", "raw", pointA, pointB, []string{"string_key", "binary_key", "int_key"}},
		{"struct/interpreted", "map-key-types.parquet", "struct_key", "interpreted", pointA, pointB, []string{"string_key", "binary_key", "int_key"}},
		{"nested/raw", "struct-map-keys.parquet", "nested_key", "raw", nestedRaw, `{"inner":{"doc":"DAAAABBhAAEAAAAA","x":2},"id":1}`, []string{"bytes_key"}},
		{"nested/interpreted", "struct-map-keys.parquet", "nested_key", "interpreted", nestedInterpreted, `{"inner":{"doc":"{\"a\":1}","x":2},"id":1}`, []string{"bytes_key"}},
		{
			"byte-backed/interpreted", "struct-map-keys.parquet", "bytes_key", "interpreted",
			`{"amount":123.45,` + bytesTail + `,"uuid":"12345678-9abc-def0-1234-56789abcdef0"}`,
			`{"amount":123.450,` + bytesTail + `,"uuid":"12345678-9ABC-DEF0-1234-56789ABCDEF0"}`,
			[]string{"nested_key"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join("../../testdata", tc.fixture)
			reader, err := pio.NewParquetFileReader(context.Background(), path, pio.ReadOption{})
			require.NoError(t, err)
			tree, err := pschema.NewSchemaTree(context.Background(), reader, pschema.SchemaOption{})
			require.NoError(t, err)
			require.NoError(t, reader.PFile.Close())
			row := map[string]any{tc.column: map[string]string{tc.a: "a", tc.b: "b"}}
			for _, column := range tc.empty {
				row[column] = map[string]string{}
			}
			data, err := json.Marshal(row)
			require.NoError(t, err)
			dir := t.TempDir()
			source, schema := filepath.Join(dir, "input.jsonl"), filepath.Join(dir, "schema")
			require.NoError(t, os.WriteFile(source, append(data, '\n'), 0o600))
			require.NoError(t, os.WriteFile(schema, []byte(tree.JSONSchema()), 0o600))
			cmd := Cmd{Source: source, Schema: schema, Format: "jsonl", ValueMode: tc.mode, URI: filepath.Join(dir, "output.parquet")}
			require.ErrorContains(t, cmd.Run(context.Background()), fmt.Sprintf("duplicate MAP key: %q and %q", min(tc.a, tc.b), max(tc.a, tc.b)))
		})
	}
}

func TestInvalidValueMode(t *testing.T) {
	require.ErrorContains(t, (Cmd{ValueMode: "auto"}).Run(context.Background()), "invalid value mode")
}
