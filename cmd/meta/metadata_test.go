package meta

import (
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/stretchr/testify/require"
)

func TestSortingColumnsPreserveNullPlacementAndPrecedence(t *testing.T) {
	columns := sortingColumnMeta([]*parquet.SortingColumn{
		{ColumnIdx: 2, Descending: true, NullsFirst: false},
		{ColumnIdx: 0, Descending: false, NullsFirst: true},
	})

	require.Equal(t, []sortingMeta{
		{ColumnIndex: 2, Direction: "DESC", NullsFirst: false},
		{ColumnIndex: 0, Direction: "ASC", NullsFirst: true},
	}, columns)
}

func TestColumnOrderNames(t *testing.T) {
	testCases := map[string]struct {
		orders   []*parquet.ColumnOrder
		expected []string
	}{
		"absent": {
			expected: []string{},
		},
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
			require.Equal(t, tc.expected, columnOrderNames(tc.orders))
		})
	}
}
