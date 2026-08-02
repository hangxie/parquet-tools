package inspect

import (
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/stretchr/testify/require"
)

func TestAddKeyValueMetadata(t *testing.T) {
	value := "value"
	testCases := map[string]struct {
		metadata []*parquet.KeyValue
		expected any
	}{
		"absent": {},
		"nil-entry": {
			metadata: []*parquet.KeyValue{nil},
			expected: []map[string]any{},
		},
		"key-only": {
			metadata: []*parquet.KeyValue{{Key: "key"}},
			expected: []map[string]any{{"key": "key"}},
		},
		"key-and-value": {
			metadata: []*parquet.KeyValue{{Key: "key", Value: &value}},
			expected: []map[string]any{{"key": "key", "value": "value"}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			output := map[string]any{}
			addKeyValueMetadata(output, tc.metadata)
			if tc.expected == nil {
				require.NotContains(t, output, "keyValueMetadata")
			} else {
				require.Equal(t, tc.expected, output["keyValueMetadata"])
			}
		})
	}
}

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

func TestAddRowGroupMetadataSkipsNilSortingColumn(t *testing.T) {
	output := map[string]any{}
	addRowGroupMetadata(output, &parquet.RowGroup{
		SortingColumns: []*parquet.SortingColumn{nil, {ColumnIdx: 1}},
	})

	require.Equal(t, []map[string]any{{
		"columnIndex": int32(1),
		"direction":   "ASC",
		"nullsFirst":  false,
	}}, output["sortingColumns"])
}

func TestAddColumnMetadataOptionalStatistics(t *testing.T) {
	zMin, zMax, mMin, mMax := 1.0, 2.0, 3.0, 4.0
	output := map[string]any{}
	addColumnMetadata(output, &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{
		SizeStatistics: &parquet.SizeStatistics{
			RepetitionLevelHistogram: []int64{1, 2},
			DefinitionLevelHistogram: []int64{3, 4},
		},
		GeospatialStatistics: &parquet.GeospatialStatistics{
			GeospatialTypes: []int32{1, 2},
			Bbox: &parquet.BoundingBox{
				Xmin: 10, Xmax: 20, Ymin: 30, Ymax: 40,
				Zmin: &zMin, Zmax: &zMax, Mmin: &mMin, Mmax: &mMax,
			},
		},
	}})

	require.Equal(t, map[string]any{
		"repetitionLevelHistogram": []int64{1, 2},
		"definitionLevelHistogram": []int64{3, 4},
	}, output["sizeStatistics"])
	require.Equal(t, map[string]any{
		"geospatialTypes": []int32{1, 2},
		"boundingBox": map[string]any{
			"xMin": 10.0, "xMax": 20.0, "yMin": 30.0, "yMax": 40.0,
			"zMin": 1.0, "zMax": 2.0, "mMin": 3.0, "mMax": 4.0,
		},
	}, output["geospatialStatistics"])
}

func TestAddSchemaElementMetadataIgnoresNilElement(t *testing.T) {
	output := map[string]any{}
	addSchemaElementMetadata(output, nil)
	require.Empty(t, output)
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

func TestAddFileMetadataEncryptionAlgorithm(t *testing.T) {
	testCases := map[string]struct {
		algorithm *parquet.EncryptionAlgorithm
		expected  any
	}{
		"absent": {},
		"gcm": {
			algorithm: &parquet.EncryptionAlgorithm{AES_GCM_V1: &parquet.AesGcmV1{}},
			expected:  "AES_GCM_V1",
		},
		"gcm-ctr": {
			algorithm: &parquet.EncryptionAlgorithm{AES_GCM_CTR_V1: &parquet.AesGcmCtrV1{}},
			expected:  "AES_GCM_CTR_V1",
		},
		"unknown": {
			algorithm: &parquet.EncryptionAlgorithm{},
			expected:  "UNKNOWN",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			output := map[string]any{}
			addFileMetadata(output, &parquet.FileMetaData{EncryptionAlgorithm: tc.algorithm})
			if tc.expected == nil {
				require.NotContains(t, output, "encryptionAlgorithm")
			} else {
				require.Equal(t, tc.expected, output["encryptionAlgorithm"])
			}
		})
	}
}
