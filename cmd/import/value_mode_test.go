package importcmd

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

func TestRawValueModeRoundTrip(t *testing.T) {
	for _, fixture := range []string{"all-types.parquet", "geospatial.parquet", "retype.parquet"} {
		t.Run(fixture, func(t *testing.T) {
			path := filepath.Join("../../testdata", fixture)
			reader, err := pio.NewParquetFileReader(context.Background(), path, pio.ReadOption{})
			require.NoError(t, err)
			tree, err := pschema.NewSchemaTree(context.Background(), reader, pschema.SchemaOption{})
			require.NoError(t, err)
			require.NoError(t, reader.PFile.Close())
			for _, format := range []string{"json", "jsonl"} {
				t.Run(format, func(t *testing.T) {
					dir := t.TempDir()
					catCmd := importTestCatCmd(path, pio.ReadOption{})
					catCmd.ValueMode = "raw"
					catCmd.Format = format
					original := testutils.CommandStdout(t, catCmd)
					source, schema := filepath.Join(dir, "input"), filepath.Join(dir, "schema")
					require.NoError(t, os.WriteFile(source, []byte(original), 0o600))
					require.NoError(t, os.WriteFile(schema, []byte(tree.JSONSchema()), 0o600))
					output := filepath.Join(dir, "output.parquet")
					importCmd := Cmd{Source: source, Schema: schema, Format: format, ValueMode: "raw", URI: output}
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

func TestInvalidValueMode(t *testing.T) {
	require.ErrorContains(t, (Cmd{ValueMode: "auto"}).Run(context.Background()), "invalid value mode")
}
