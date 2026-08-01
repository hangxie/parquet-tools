package inspect

import (
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/stretchr/testify/require"
)

func TestAddRowGroupMetadata(t *testing.T) {
	output := map[string]any{}
	addRowGroupMetadata(output, &parquet.RowGroup{
		FileOffset:          new(int64(10)),
		TotalCompressedSize: new(int64(20)),
		Ordinal:             new(int16(3)),
		SortingColumns:      []*parquet.SortingColumn{{ColumnIdx: 1, Descending: true, NullsFirst: true}},
	})

	require.Equal(t, int64(10), output["fileOffset"])
	require.Equal(t, int64(20), output["totalCompressedSize"])
	require.Equal(t, int16(3), output["ordinal"])
	require.Equal(t, []map[string]any{{"columnIndex": int32(1), "direction": "DESC", "nullsFirst": true}}, output["sortingColumns"])
}

func TestAddFileMetadataDoesNotDuplicateTotalRows(t *testing.T) {
	output := map[string]any{"totalRows": int64(3)}
	addFileMetadata(output, &parquet.FileMetaData{NumRows: 3})

	require.Equal(t, int64(3), output["totalRows"])
	require.NotContains(t, output, "numRows")
}

func TestAddFileMetadataColumnOrders(t *testing.T) {
	testCases := map[string]struct {
		orders   []*parquet.ColumnOrder
		expected []string
	}{
		"absent": {},
		"type-defined": {
			orders:   []*parquet.ColumnOrder{{TYPE_ORDER: &parquet.TypeDefinedOrder{}}},
			expected: []string{"TYPE_DEFINED_ORDER"},
		},
		"undefined": {
			orders:   []*parquet.ColumnOrder{nil, {}},
			expected: []string{"UNDEFINED", "UNDEFINED"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			output := map[string]any{}
			addFileMetadata(output, &parquet.FileMetaData{ColumnOrders: tc.orders})
			if tc.expected == nil {
				require.NotContains(t, output, "columnOrders")
			} else {
				require.Equal(t, tc.expected, output["columnOrders"])
			}
		})
	}
}
