package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

func TestInspect(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    InspectCmd
		golden string
		errMsg string
	}{
		// file level
		"file/good":         {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet"}, golden: "inspect-good-file.json"},
		"file/dict-page":    {cmd: InspectCmd{ReadOption: rOpt, URI: "dict-page.parquet"}, golden: "inspect-dict-page-file.json"},
		"file/row-group":    {cmd: InspectCmd{ReadOption: rOpt, URI: "row-group.parquet"}, golden: "inspect-row-group-file.json"},
		"file/non-existent": {cmd: InspectCmd{ReadOption: rOpt, URI: "nonexistent.parquet"}, errMsg: "no such file or directory"},
		// row group level
		"row-group/good-rg-0":      {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0)}, golden: "inspect-good-row-group-0.json"},
		"row-group/row-group-rg-0": {cmd: InspectCmd{ReadOption: rOpt, URI: "row-group.parquet", RowGroup: common.ToPtr(0)}, golden: "inspect-row-group-rg-0.json"},
		"row-group/row-group-rg-1": {cmd: InspectCmd{ReadOption: rOpt, URI: "row-group.parquet", RowGroup: common.ToPtr(1)}, golden: "inspect-row-group-rg-1.json"},
		"row-group/negative-index": {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(-1)}, errMsg: "row group index -1 out of range"},
		"row-group/out-of-range":   {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(999)}, errMsg: "row group index 999 out of range"},
		"row-group/geospatial":     {cmd: InspectCmd{ReadOption: rOpt, URI: "geospatial.parquet", RowGroup: common.ToPtr(0)}, golden: "inspect-geospatial-row-group-0.json"},
		"row-group/nil-statistics": {cmd: InspectCmd{ReadOption: rOpt, URI: "nil-statistics.parquet", RowGroup: common.ToPtr(0)}, golden: "inspect-nil-statistics-row-group-0.json"},
		"row-group/all-types":      {cmd: InspectCmd{ReadOption: rOpt, URI: "all-types.parquet", RowGroup: common.ToPtr(0)}, golden: "inspect-all-types-row-group-0.json"},
		// column chunk level
		"column-chunk/good-col-0":             {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0)}, golden: "inspect-good-column-chunk-0.json"},
		"column-chunk/dict-page-col-0":        {cmd: InspectCmd{ReadOption: rOpt, URI: "dict-page.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0)}, golden: "inspect-dict-page-column-chunk-0.json"},
		"column-chunk/all-types-interval":     {cmd: InspectCmd{ReadOption: rOpt, URI: "all-types.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(39)}, golden: "inspect-all-types-interval-column.json"},
		"column-chunk/negative-column-index":  {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(-1)}, errMsg: "column chunk index -1 out of range"},
		"column-chunk/out-of-range-column":    {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(999)}, errMsg: "column chunk index 999 out of range"},
		"column-chunk/without-row-group":      {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", ColumnChunk: common.ToPtr(0)}, errMsg: "--column-chunk requires --row-group"},
		"column-chunk/negative-row-group":     {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(-1), ColumnChunk: common.ToPtr(0)}, errMsg: "row group index -1 out of range"},
		"column-chunk/out-of-range-row-group": {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(999), ColumnChunk: common.ToPtr(0)}, errMsg: "row group index 999 out of range"},
		// page level
		"page/good-page-0":                  {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(0)}, golden: "inspect-good-page-0.json"},
		"page/dict-page-page-0":             {cmd: InspectCmd{ReadOption: rOpt, URI: "dict-page.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(0)}, golden: "inspect-dict-page-page-0.json"},
		"page/row-group-rg1-page-0":         {cmd: InspectCmd{ReadOption: rOpt, URI: "row-group.parquet", RowGroup: common.ToPtr(1), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(0)}, golden: "inspect-row-group-rg1-page-0.json"},
		"page/data-page-v2-page-0":          {cmd: InspectCmd{ReadOption: rOpt, URI: "data-page-v2.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(0)}, golden: "inspect-data-page-v2-page-0.json"},
		"page/good-page-1":                  {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(1)}, golden: "inspect-good-page-1.json"},
		"page/row-group-page-5":             {cmd: InspectCmd{ReadOption: rOpt, URI: "row-group.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(5)}, golden: "inspect-row-group-page-5.json"},
		"page/negative-page-index":          {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(-1)}, errMsg: "page index -1 out of range"},
		"page/out-of-range-page":            {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(999)}, errMsg: "page index 999 out of range"},
		"page/without-row-group-and-column": {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", Page: common.ToPtr(0)}, errMsg: "--page requires both --row-group and --column-chunk"},
		"page/without-column":               {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), Page: common.ToPtr(0)}, errMsg: "--page requires both --row-group and --column-chunk"},
		"page/negative-row-group":           {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(-1), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(0)}, errMsg: "row group index -1 out of range"},
		"page/out-of-range-row-group":       {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(999), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(0)}, errMsg: "row group index 999 out of range"},
		"page/negative-column-chunk":        {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(-1), Page: common.ToPtr(0)}, errMsg: "column chunk index -1 out of range"},
		"page/out-of-range-column-chunk":    {cmd: InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(999), Page: common.ToPtr(0)}, errMsg: "column chunk index 999 out of range"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if tc.errMsg == "" {
				t.Parallel()
			}
			tc.cmd.URI = "../testdata/" + tc.cmd.URI
			if tc.errMsg != "" {
				err := tc.cmd.Run()
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				tc.golden = "../testdata/golden/" + tc.golden
				stdout, stderr := captureStdoutStderr(func() {
					require.NoError(t, tc.cmd.Run())
				})
				require.Equal(t, loadExpected(t, tc.golden), stdout)
				require.Equal(t, "", stderr)
			}
		})
	}
}

func TestGetStatValue(t *testing.T) {
	cmd := InspectCmd{}

	testCases := map[string]struct {
		value      []byte
		schemaNode *pschema.SchemaNode
		want       any
		wantNil    bool
		wantError  bool
	}{
		"nil-value": {
			value: nil,
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type: parquet.TypePtr(parquet.Type_INT32),
				},
			},
			wantNil: true,
		},
		"empty-value": {
			value: []byte{},
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type: parquet.TypePtr(parquet.Type_INT32),
				},
			},
			wantNil: true,
		},
		"geometry-with-data": {
			value: []byte{1, 2, 3, 4}, // Some non-empty data
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
					LogicalType: &parquet.LogicalType{
						GEOMETRY: &parquet.GeometryType{},
					},
				},
			},
			wantNil: true, // Should return nil for geospatial types
		},
		"geography-with-data": {
			value: []byte{1, 2, 3, 4}, // Some non-empty data
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
					LogicalType: &parquet.LogicalType{
						GEOGRAPHY: &parquet.GeographyType{},
					},
				},
			},
			wantNil: true, // Should return nil for geospatial types
		},
		"interval-with-data": {
			value: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, // 12 bytes for interval
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type:          parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
					ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL),
				},
			},
			wantNil: true, // Should return nil for interval types
		},
		"invalid-int32-data": {
			value: []byte{1}, // Too short for INT32 (needs 4 bytes)
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type: parquet.TypePtr(parquet.Type_INT32),
				},
			},
			wantError: true, // Should return error message
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := cmd.getStatValue(tc.value, tc.schemaNode)
			if tc.wantNil {
				require.Nil(t, result)
			} else if tc.wantError {
				require.NotNil(t, result)
				require.Contains(t, result, "failed to read data")
			} else {
				require.Equal(t, tc.want, result)
			}
		})
	}
}

func TestBuildStatistics(t *testing.T) {
	cmd := InspectCmd{}

	testCases := map[string]struct {
		statistics *parquet.Statistics
		schemaNode *pschema.SchemaNode
		want       map[string]any
	}{
		"all-fields": {
			statistics: &parquet.Statistics{
				NullCount:     common.ToPtr(int64(10)),
				DistinctCount: common.ToPtr(int64(5)),
				MinValue:      []byte{1, 0, 0, 0},
				MaxValue:      []byte{100, 0, 0, 0},
			},
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type: parquet.TypePtr(parquet.Type_INT32),
				},
			},
			want: map[string]any{
				"nullCount":     int64(10),
				"distinctCount": int64(5),
				"minValue":      int32(1),
				"maxValue":      int32(100),
			},
		},
		"without-distinct-count": {
			statistics: &parquet.Statistics{
				NullCount: common.ToPtr(int64(10)),
				MinValue:  []byte{1, 0, 0, 0},
				MaxValue:  []byte{100, 0, 0, 0},
			},
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type: parquet.TypePtr(parquet.Type_INT32),
				},
			},
			want: map[string]any{
				"nullCount": int64(10),
				"minValue":  int32(1),
				"maxValue":  int32(100),
			},
		},
		"without-null-count": {
			statistics: &parquet.Statistics{
				DistinctCount: common.ToPtr(int64(5)),
				MinValue:      []byte{1, 0, 0, 0},
				MaxValue:      []byte{100, 0, 0, 0},
			},
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type: parquet.TypePtr(parquet.Type_INT32),
				},
			},
			want: map[string]any{
				"distinctCount": int64(5),
				"minValue":      int32(1),
				"maxValue":      int32(100),
			},
		},
		"nil-schema-node": {
			statistics: &parquet.Statistics{
				NullCount:     common.ToPtr(int64(10)),
				DistinctCount: common.ToPtr(int64(5)),
			},
			schemaNode: nil,
			want: map[string]any{
				"nullCount":     int64(10),
				"distinctCount": int64(5),
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := cmd.buildStatistics(tc.statistics, tc.schemaNode)
			require.Equal(t, tc.want, result)
		})
	}
}

func TestResolvePathInSchema(t *testing.T) {
	cmd := InspectCmd{}

	testCases := map[string]struct {
		pathInSchema []string
		inExNameMap  map[string][]string
		want         []string
	}{
		"found-in-map": {
			pathInSchema: []string{"col1"},
			inExNameMap: map[string][]string{
				"col1": {"ExternalCol1"},
			},
			want: []string{"ExternalCol1"},
		},
		"not-found-in-map": {
			pathInSchema: []string{"col2"},
			inExNameMap: map[string][]string{
				"col1": {"ExternalCol1"},
			},
			want: []string{"col2"},
		},
		"nested-path-found": {
			pathInSchema: []string{"parent", "child"},
			inExNameMap: map[string][]string{
				"parent" + common.PAR_GO_PATH_DELIMITER + "child": {"ExternalParent", "ExternalChild"},
			},
			want: []string{"ExternalParent", "ExternalChild"},
		},
		"nested-path-not-found": {
			pathInSchema: []string{"parent", "child"},
			inExNameMap: map[string][]string{
				"other" + common.PAR_GO_PATH_DELIMITER + "path": {"External"},
			},
			want: []string{"parent", "child"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := cmd.resolvePathInSchema(tc.pathInSchema, tc.inExNameMap)
			require.Equal(t, tc.want, result)
		})
	}
}

func TestAddTypeInformation(t *testing.T) {
	cmd := InspectCmd{}

	testCases := map[string]struct {
		output     map[string]any
		schemaNode *pschema.SchemaNode
		want       map[string]any
	}{
		"with-converted-and-logical-type": {
			output: map[string]any{},
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type:          parquet.TypePtr(parquet.Type_INT32),
					ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
					LogicalType: &parquet.LogicalType{
						DATE: &parquet.DateType{},
					},
				},
			},
			want: map[string]any{
				"convertedType": "convertedtype=DATE",
				"logicalType":   "logicaltype=DATE",
			},
		},
		"nil-schema-node": {
			output:     map[string]any{},
			schemaNode: nil,
			want:       map[string]any{},
		},
		"no-types": {
			output: map[string]any{},
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type: parquet.TypePtr(parquet.Type_INT32),
				},
			},
			want: map[string]any{},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cmd.addTypeInformation(tc.output, tc.schemaNode)
			require.Equal(t, tc.want, tc.output)
		})
	}
}

func TestConvertPageHeaderInfo(t *testing.T) {
	cmd := InspectCmd{}

	testCases := map[string]struct {
		headerInfo reader.PageHeaderInfo
		schemaNode *pschema.SchemaNode
		want       PageInfo
	}{
		"data-page-without-crc": {
			headerInfo: reader.PageHeaderInfo{
				Index:            0,
				Offset:           1000,
				PageType:         parquet.PageType_DATA_PAGE,
				CompressedSize:   500,
				UncompressedSize: 600,
				NumValues:        100,
				Encoding:         parquet.Encoding_PLAIN,
				DefLevelEncoding: parquet.Encoding_RLE,
				RepLevelEncoding: parquet.Encoding_RLE,
				HasCRC:           false,
				HasStatistics:    false,
			},
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type: parquet.TypePtr(parquet.Type_INT32),
				},
			},
			want: PageInfo{
				Index:                   0,
				Offset:                  1000,
				Type:                    parquet.PageType_DATA_PAGE,
				CompressedSize:          500,
				UncompressedSize:        600,
				NumValues:               common.ToPtr(int32(100)),
				Encoding:                common.ToPtr(parquet.Encoding_PLAIN),
				DefinitionLevelEncoding: common.ToPtr(parquet.Encoding_RLE),
				RepetitionLevelEncoding: common.ToPtr(parquet.Encoding_RLE),
			},
		},
		"dictionary-page-with-nil-is-sorted": {
			headerInfo: reader.PageHeaderInfo{
				Index:            1,
				Offset:           2000,
				PageType:         parquet.PageType_DICTIONARY_PAGE,
				CompressedSize:   300,
				UncompressedSize: 400,
				NumValues:        50,
				Encoding:         parquet.Encoding_PLAIN,
				HasCRC:           true,
				CRC:              12345,
				IsSorted:         nil,
			},
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				},
			},
			want: PageInfo{
				Index:            1,
				Offset:           2000,
				Type:             parquet.PageType_DICTIONARY_PAGE,
				CompressedSize:   300,
				UncompressedSize: 400,
				HasCrc:           true,
				Crc:              12345,
				NumValues:        common.ToPtr(int32(50)),
				Encoding:         common.ToPtr(parquet.Encoding_PLAIN),
			},
		},
		"index-page": {
			headerInfo: reader.PageHeaderInfo{
				Index:            2,
				Offset:           3000,
				PageType:         parquet.PageType_INDEX_PAGE,
				CompressedSize:   200,
				UncompressedSize: 250,
				HasCRC:           false,
			},
			schemaNode: &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Type: parquet.TypePtr(parquet.Type_INT32),
				},
			},
			want: PageInfo{
				Index:            2,
				Offset:           3000,
				Type:             parquet.PageType_INDEX_PAGE,
				CompressedSize:   200,
				UncompressedSize: 250,
				Note:             "Index page (column index)",
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := cmd.convertPageHeaderInfo(tc.headerInfo, tc.schemaNode)
			require.Equal(t, tc.want, result)
		})
	}
}

func TestPrintJSON(t *testing.T) {
	cmd := InspectCmd{}

	testCases := map[string]struct {
		data      any
		wantError bool
	}{
		"valid-map": {
			data: map[string]any{
				"key1": "value1",
				"key2": 42,
			},
			wantError: false,
		},
		"valid-slice": {
			data: []any{
				"item1",
				"item2",
			},
			wantError: false,
		},
		"invalid-data": {
			data:      make(chan int),
			wantError: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := cmd.printJSON(tc.data)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
