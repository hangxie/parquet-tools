package inspect

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
	pschema "github.com/hangxie/parquet-tools/schema"
)

func TestInspectShowsSortingAndPageIndexes(t *testing.T) {
	type row struct {
		Value int32 `parquet:"name=value, type=INT32"`
	}

	path := filepath.Join(t.TempDir(), "indexes.parquet")
	fw, err := local.NewLocalFileWriter(path)
	require.NoError(t, err)
	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(row),
		writer.WithNP(1),
		writer.WithPageSize(32),
		writer.WithSortingColumns(&parquet.SortingColumn{ColumnIdx: 0, NullsFirst: true}),
	)
	require.NoError(t, err)
	for value := int32(0); value < 64; value++ {
		require.NoError(t, pw.WriteWithContext(context.Background(), row{Value: value}))
	}
	require.NoError(t, pw.WriteStopWithContext(context.Background()))
	require.NoError(t, fw.Close())

	rowGroupOutput, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, (Cmd{URI: path, RowGroup: new(0)}).Run(context.Background()))
	})
	var rowGroup map[string]any
	require.NoError(t, json.Unmarshal([]byte(rowGroupOutput), &rowGroup))
	rg := rowGroup["rowGroup"].(map[string]any)
	require.Equal(t, []any{map[string]any{
		"columnIndex": float64(0),
		"direction":   "ASC",
		"nullsFirst":  true,
	}}, rg["sortingColumns"])
	brief := rowGroup["columnChunks"].([]any)[0].(map[string]any)
	require.NotContains(t, brief, "dataPageOffset")
	require.NotContains(t, brief, "encodingStats")
	require.NotContains(t, brief, "fileOffset")
	require.NotContains(t, brief, "typeLength")

	columnOutput, _ := testutils.CaptureStdoutStderr(func() {
		require.NoError(t, (Cmd{URI: path, RowGroup: new(0), ColumnChunk: new(0)}).Run(context.Background()))
	})
	var column map[string]any
	require.NoError(t, json.Unmarshal([]byte(columnOutput), &column))
	chunk := column["columnChunk"].(map[string]any)
	require.Equal(t, "ASCENDING", chunk["columnIndex"].(map[string]any)["boundaryOrder"])
	require.NotEmpty(t, chunk["offsetIndex"].(map[string]any)["pageLocations"])
}

func TestColumnIndexMetadataDoesNotDecodeNullPageBounds(t *testing.T) {
	cmd := Cmd{}
	schemaNode := &pschema.SchemaNode{
		SchemaElement: parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_INT32)},
	}
	index := &parquet.ColumnIndex{
		NullPages:                 []bool{true, false},
		MinValues:                 [][]byte{{0, 0, 0, 0}, {1, 0, 0, 0}},
		MaxValues:                 [][]byte{{0, 0, 0, 0}, {2, 0, 0, 0}},
		BoundaryOrder:             parquet.BoundaryOrder_ASCENDING,
		NullCounts:                []int64{1, 0},
		RepetitionLevelHistograms: []int64{2, 3},
		DefinitionLevelHistograms: []int64{4, 5},
	}

	metadata := cmd.columnIndexMetadata(index, schemaNode)
	require.Equal(t, []any{nil, int32(1)}, metadata["minValues"])
	require.Equal(t, []any{nil, int32(2)}, metadata["maxValues"])
	require.Equal(t, []int64{1, 0}, metadata["nullCounts"])
	require.Equal(t, []int64{2, 3}, metadata["repetitionLevelHistograms"])
	require.Equal(t, []int64{4, 5}, metadata["definitionLevelHistograms"])
}

func TestDecodeIndexBoundsWithoutSchema(t *testing.T) {
	require.Equal(t, []any{nil}, (Cmd{}).decodeIndexBounds([][]byte{{1}}, nil, true, nil))
}

func TestOffsetIndexMetadataIncludesUnencodedByteCount(t *testing.T) {
	metadata := offsetIndexMetadata(&parquet.OffsetIndex{
		PageLocations:               []*parquet.PageLocation{nil},
		UnencodedByteArrayDataBytes: []int64{10, 20},
	})

	require.Empty(t, metadata["pageLocations"])
	require.Equal(t, []int64{10, 20}, metadata["unencodedByteArrayDataBytes"])
}
