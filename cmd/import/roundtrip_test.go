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

	"github.com/hangxie/parquet-tools/cmd/cat"
	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
	"github.com/hangxie/parquet-tools/cmd/merge"
	"github.com/hangxie/parquet-tools/cmd/split"
	"github.com/hangxie/parquet-tools/cmd/transcode"
	pio "github.com/hangxie/parquet-tools/io"
)

const roundTripJSON = `[
 {"Amount":9999999999999999.99,"Blob":"AP8=","Enabled":true,"Id":9007199254740993,"Text":"雪, \"hello\"\nworld","Value":1.5},
 {"Amount":-9999999999999999.99,"Blob":"SGVsbG8=","Enabled":false,"Id":-9007199254740993,"Text":"","Value":-2.5},
 {"Amount":0.00,"Blob":"","Enabled":true,"Id":0,"Text":"🌍","Value":0}
]`

var roundTripFields = []string{
	"name=Amount, type=INT64, convertedtype=DECIMAL, precision=18, scale=2",
	"name=Blob, type=BYTE_ARRAY",
	"name=Enabled, type=BOOLEAN",
	"name=Id, type=INT64",
	"name=Text, type=BYTE_ARRAY, convertedtype=UTF8",
	"name=Value, type=DOUBLE",
}

func roundTripRows(t *testing.T, data string) []map[string]any {
	t.Helper()
	var rows []map[string]any
	decoder := json.NewDecoder(strings.NewReader(data))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&rows))
	return rows
}

func roundTripCat(t *testing.T, path string) string {
	t.Helper()
	cmd := importTestCatCmd(path, pio.ReadOption{})
	return testutils.CommandStdout(t, cmd)
}

func roundTripInput(t *testing.T, dir, format string) (string, string) {
	t.Helper()
	source, schema := filepath.Join(dir, "input."+format), filepath.Join(dir, "schema")
	input := roundTripJSON
	var schemaData []byte
	if format == "csv" {
		schemaData = []byte(strings.Join(roundTripFields, "\n"))
		var output strings.Builder
		writer := csv.NewWriter(&output)
		for _, row := range roundTripRows(t, input) {
			require.NoError(t, writer.Write([]string{
				fmt.Sprint(row["Amount"]), fmt.Sprint(row["Blob"]), fmt.Sprint(row["Enabled"]),
				fmt.Sprint(row["Id"]), fmt.Sprint(row["Text"]), fmt.Sprint(row["Value"]),
			}))
		}
		writer.Flush()
		require.NoError(t, writer.Error())
		input = output.String()
	} else {
		fields := make([]map[string]string, len(roundTripFields))
		for i, tag := range roundTripFields {
			fields[i] = map[string]string{"Tag": tag}
		}
		var err error
		schemaData, err = json.Marshal(map[string]any{"Tag": "name=root", "Fields": fields})
		require.NoError(t, err)
		if format == "jsonl" {
			var rows []json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(input), &rows))
			lines := make([]string, len(rows))
			for i, row := range rows {
				lines[i] = string(row)
			}
			input = strings.Join(lines, "\n") + "\n"
		}
	}
	require.NoError(t, os.WriteFile(source, []byte(input), 0o600))
	require.NoError(t, os.WriteFile(schema, schemaData, 0o600))
	return source, schema
}

func TestCmdDataRoundTrip(t *testing.T) {
	for _, format := range []string{"json", "jsonl", "csv"} {
		t.Run(format, func(t *testing.T) {
			dir := t.TempDir()
			source, schema := roundTripInput(t, dir, format)
			imported := filepath.Join(dir, "imported.parquet")
			cmd := Cmd{Format: format, Source: source, Schema: schema, URI: imported}
			require.NoError(t, cmd.Run(context.Background()))
			expected := roundTripRows(t, roundTripJSON)
			require.Equal(t, expected, roundTripRows(t, roundTripCat(t, imported)))
			if format == "csv" {
				original, err := os.ReadFile(source)
				require.NoError(t, err)
				originalRows, err := csv.NewReader(strings.NewReader(string(original))).ReadAll()
				require.NoError(t, err)
				catCmd := cat.Cmd{ReadPageSize: 2, SampleRatio: 1, Format: "csv", NoHeader: true, URI: imported}
				actualRows, err := csv.NewReader(strings.NewReader(testutils.CommandStdout(t, catCmd))).ReadAll()
				require.NoError(t, err)
				require.Equal(t, originalRows, actualRows)
			}
			roundTripSplitMerge(t, imported, expected)
			roundTripTranscode(t, imported, expected)
		})
	}
}

func roundTripSplitMerge(t *testing.T, imported string, expected []map[string]any) {
	t.Helper()
	for _, fileCount := range []int64{0, 5} {
		t.Run(fmt.Sprintf("split-%d", fileCount), func(t *testing.T) {
			partsDir := t.TempDir()
			splitCmd := split.Cmd{URI: imported, ReadPageSize: 2, NameFormat: filepath.Join(partsDir, "part-%02d.parquet")}
			if fileCount == 0 {
				splitCmd.RecordCount = 2
			} else {
				splitCmd.FileCount = fileCount
			}
			require.NoError(t, splitCmd.Run(context.Background()))
			parts, err := filepath.Glob(filepath.Join(partsDir, "part-*.parquet"))
			require.NoError(t, err)
			wantParts := 2
			if fileCount > 0 {
				wantParts = int(fileCount)
			}
			require.Len(t, parts, wantParts)
			var rows []map[string]any
			for _, part := range parts {
				require.True(t, testutils.HasSameSchema(imported, part))
				rows = append(rows, roundTripRows(t, roundTripCat(t, part))...)
			}
			require.Equal(t, expected, rows)
			for _, concurrent := range []bool{false, true} {
				t.Run(fmt.Sprintf("merge-concurrent-%t", concurrent), func(t *testing.T) {
					output := filepath.Join(t.TempDir(), "merged.parquet")
					mergeCmd := merge.Cmd{Source: parts, URI: output, ReadPageSize: 2, Concurrent: concurrent}
					require.NoError(t, mergeCmd.Run(context.Background()))
					require.True(t, testutils.HasSameSchema(imported, output))
					actual := roundTripRows(t, roundTripCat(t, output))
					if concurrent {
						require.ElementsMatch(t, expected, actual)
					} else {
						require.Equal(t, expected, actual)
					}
				})
			}
		})
	}
}

func roundTripTranscode(t *testing.T, imported string, expected []map[string]any) {
	t.Helper()
	for _, tc := range []struct {
		codec   string
		version int32
		crc     bool
	}{
		{"UNCOMPRESSED", 1, false}, {"SNAPPY", 2, true}, {"ZSTD", 1, true},
	} {
		t.Run("transcode-"+tc.codec, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "transcoded.parquet")
			cmd := transcode.Cmd{Source: imported, URI: output, ReadPageSize: 2, WriteOption: pio.WriteOption{
				CompressionCodec: tc.codec, DataPageVersion: tc.version, WriteCRC: tc.crc,
			}}
			require.NoError(t, cmd.Run(context.Background()))
			require.True(t, testutils.HasSameSchema(imported, output))
			require.Equal(t, expected, roundTripRows(t, roundTripCat(t, output)))
		})
	}
}
