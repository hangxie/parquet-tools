package inspect

import (
	"os"
	"strings"
	"testing"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

func TestInspect(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    Cmd
		golden string
		errMsg string
	}{
		// file level
		"file/good":         {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet"}, golden: "inspect-good-file.json"},
		"file/dict-page":    {cmd: Cmd{ReadOption: rOpt, URI: "dict-page.parquet"}, golden: "inspect-dict-page-file.json"},
		"file/row-group":    {cmd: Cmd{ReadOption: rOpt, URI: "row-group.parquet"}, golden: "inspect-row-group-file.json"},
		"file/non-existent": {cmd: Cmd{ReadOption: rOpt, URI: "nonexistent.parquet"}, errMsg: "no such file or directory"},
		// row group level
		"row-group/good-rg-0":      {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(0)}, golden: "inspect-good-row-group-0.json"},
		"row-group/row-group-rg-0": {cmd: Cmd{ReadOption: rOpt, URI: "row-group.parquet", RowGroup: new(0)}, golden: "inspect-row-group-rg-0.json"},
		"row-group/row-group-rg-1": {cmd: Cmd{ReadOption: rOpt, URI: "row-group.parquet", RowGroup: new(1)}, golden: "inspect-row-group-rg-1.json"},
		"row-group/negative-index": {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(-1)}, errMsg: "row group index -1 out of range"},
		"row-group/out-of-range":   {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(999)}, errMsg: "row group index 999 out of range"},
		"row-group/geospatial":     {cmd: Cmd{ReadOption: rOpt, URI: "geospatial.parquet", RowGroup: new(0)}, golden: "inspect-geospatial-row-group-0.json"},
		"row-group/nil-statistics": {cmd: Cmd{ReadOption: rOpt, URI: "nil-statistics.parquet", RowGroup: new(0)}, golden: "inspect-nil-statistics-row-group-0.json"},
		"row-group/all-types":      {cmd: Cmd{ReadOption: rOpt, URI: "all-types.parquet", RowGroup: new(0)}, golden: "inspect-all-types-row-group-0.json"},
		"row-group/bloom-filter":   {cmd: Cmd{ReadOption: rOpt, URI: "bloom-filter.parquet", RowGroup: new(0)}, golden: "inspect-bloom-filter-row-group-0.json"},
		// column chunk level
		"column-chunk/good-col-0":             {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(0), ColumnChunk: new(0)}, golden: "inspect-good-column-chunk-0.json"},
		"column-chunk/dict-page-col-0":        {cmd: Cmd{ReadOption: rOpt, URI: "dict-page.parquet", RowGroup: new(0), ColumnChunk: new(0)}, golden: "inspect-dict-page-column-chunk-0.json"},
		"column-chunk/all-types-interval":     {cmd: Cmd{ReadOption: rOpt, URI: "all-types.parquet", RowGroup: new(0), ColumnChunk: new(39)}, golden: "inspect-all-types-interval-column.json"},
		"column-chunk/bloom-filter-col-0":     {cmd: Cmd{ReadOption: rOpt, URI: "bloom-filter.parquet", RowGroup: new(0), ColumnChunk: new(0)}, golden: "inspect-bloom-filter-column-chunk-0.json"},
		"column-chunk/negative-column-index":  {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(0), ColumnChunk: new(-1)}, errMsg: "column chunk index -1 out of range"},
		"column-chunk/out-of-range-column":    {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(0), ColumnChunk: new(999)}, errMsg: "column chunk index 999 out of range"},
		"column-chunk/without-row-group":      {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", ColumnChunk: new(0)}, errMsg: "--column-chunk requires --row-group"},
		"column-chunk/negative-row-group":     {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(-1), ColumnChunk: new(0)}, errMsg: "row group index -1 out of range"},
		"column-chunk/out-of-range-row-group": {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(999), ColumnChunk: new(0)}, errMsg: "row group index 999 out of range"},
		// page level
		"page/good-page-0":                  {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(0), ColumnChunk: new(0), Page: new(0)}, golden: "inspect-good-page-0.json"},
		"page/dict-page-page-0":             {cmd: Cmd{ReadOption: rOpt, URI: "dict-page.parquet", RowGroup: new(0), ColumnChunk: new(0), Page: new(0)}, golden: "inspect-dict-page-page-0.json"},
		"page/dict-page-page-1":             {cmd: Cmd{ReadOption: rOpt, URI: "dict-page.parquet", RowGroup: new(0), ColumnChunk: new(0), Page: new(1)}, golden: "inspect-dict-page-page-1.json"},
		"page/row-group-rg1-page-0":         {cmd: Cmd{ReadOption: rOpt, URI: "row-group.parquet", RowGroup: new(1), ColumnChunk: new(0), Page: new(0)}, golden: "inspect-row-group-rg1-page-0.json"},
		"page/data-page-v2-page-0":          {cmd: Cmd{ReadOption: rOpt, URI: "data-page-v2.parquet", RowGroup: new(0), ColumnChunk: new(0), Page: new(0)}, golden: "inspect-data-page-v2-page-0.json"},
		"page/good-page-1":                  {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(0), ColumnChunk: new(0), Page: new(1)}, golden: "inspect-good-page-1.json"},
		"page/row-group-page-5":             {cmd: Cmd{ReadOption: rOpt, URI: "row-group.parquet", RowGroup: new(0), ColumnChunk: new(0), Page: new(5)}, golden: "inspect-row-group-page-5.json"},
		"page/negative-page-index":          {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(0), ColumnChunk: new(0), Page: new(-1)}, errMsg: "page index -1 out of range"},
		"page/out-of-range-page":            {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(0), ColumnChunk: new(0), Page: new(999)}, errMsg: "page index 999 out of range"},
		"page/without-row-group-and-column": {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", Page: new(0)}, errMsg: "--page requires both --row-group and --column-chunk"},
		"page/without-column":               {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(0), Page: new(0)}, errMsg: "--page requires both --row-group and --column-chunk"},
		"page/negative-row-group":           {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(-1), ColumnChunk: new(0), Page: new(0)}, errMsg: "row group index -1 out of range"},
		"page/out-of-range-row-group":       {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(999), ColumnChunk: new(0), Page: new(0)}, errMsg: "row group index 999 out of range"},
		"page/negative-column-chunk":        {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(0), ColumnChunk: new(-1), Page: new(0)}, errMsg: "column chunk index -1 out of range"},
		"page/out-of-range-column-chunk":    {cmd: Cmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: new(0), ColumnChunk: new(999), Page: new(0)}, errMsg: "column chunk index 999 out of range"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if tc.errMsg == "" {
				t.Parallel()
			}
			tc.cmd.URI = "../../testdata/" + tc.cmd.URI
			if tc.errMsg != "" {
				err := tc.cmd.Run()
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				tc.golden = "../../testdata/golden/" + tc.golden
				stdout, stderr := testutils.CaptureStdoutStderr(func() {
					require.NoError(t, tc.cmd.Run())
				})
				require.Equal(t, testutils.LoadExpected(t, tc.golden), stdout)
				require.Equal(t, "", stderr)
			}
		})
	}
}

func TestBuildStatistics(t *testing.T) {
	cmd := Cmd{}

	testCases := map[string]struct {
		statistics *parquet.Statistics
		schemaNode *pschema.SchemaNode
		want       map[string]any
	}{
		"all-fields": {
			statistics: &parquet.Statistics{
				NullCount:     new(int64(10)),
				DistinctCount: new(int64(5)),
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
				NullCount: new(int64(10)),
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
				DistinctCount: new(int64(5)),
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
				NullCount:     new(int64(10)),
				DistinctCount: new(int64(5)),
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
	cmd := Cmd{}

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
	cmd := Cmd{}

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
	cmd := Cmd{}

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
				NumValues:               new(int32(100)),
				Encoding:                new(parquet.Encoding_PLAIN),
				DefinitionLevelEncoding: new(parquet.Encoding_RLE),
				RepetitionLevelEncoding: new(parquet.Encoding_RLE),
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
				NumValues:        new(int32(50)),
				Encoding:         new(parquet.Encoding_PLAIN),
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
	cmd := Cmd{}

	testCases := map[string]struct {
		data      any
		wantError bool
		errMsg    string
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
			errMsg:    "unsupported type",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := cmd.printJSON(tc.data)
			if tc.wantError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestReadPageValuesEdgeCases(t *testing.T) {
	cmd := Cmd{}

	t.Run("index-page-returns-empty", func(t *testing.T) {
		col := &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{}}
		pages := []PageInfo{{Type: parquet.PageType_INDEX_PAGE}}

		values, err := cmd.readPageValues(nil, 0, 0, col, nil, pages, 0)
		require.NoError(t, err)
		require.Equal(t, []any{}, values)
	})

	t.Run("page-index-out-of-range", func(t *testing.T) {
		col := &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{}}
		pages := []PageInfo{{Type: parquet.PageType_DATA_PAGE}}

		_, err := cmd.readPageValues(nil, 0, 0, col, nil, pages, 5)
		require.Error(t, err)
		require.Contains(t, err.Error(), "page index 5 out of range")
	})

	t.Run("nil-num-values-error", func(t *testing.T) {
		fileReader, err := pio.NewParquetFileReader("../../testdata/good.parquet", pio.ReadOption{})
		require.NoError(t, err)
		defer func() { _ = fileReader.PFile.Close() }()

		col := fileReader.Footer.RowGroups[0].Columns[0]
		pathKey := strings.Join(col.MetaData.PathInSchema, common.PAR_GO_PATH_DELIMITER)

		schemaRoot, err := pschema.NewSchemaTree(fileReader, pschema.SchemaOption{})
		require.NoError(t, err)
		schemaNode := schemaRoot.GetPathMap()[pathKey]

		pages := []PageInfo{{Type: parquet.PageType_DATA_PAGE, NumValues: nil}}

		_, err = cmd.readPageValues(fileReader, 0, 0, col, schemaNode, pages, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unable to get numValues for page")
	})
}

func TestRunCorruptFile(t *testing.T) {
	tmpFile := t.TempDir() + "/corrupt.parquet"
	require.NoError(t, os.WriteFile(tmpFile, []byte("not a parquet file"), 0o644))

	err := Cmd{ReadOption: pio.ReadOption{}, URI: tmpFile}.Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "read footer")
}

func TestReadDictionaryPageValuesError(t *testing.T) {
	tmpFile := t.TempDir() + "/dict-error.parquet"
	data, err := os.ReadFile("../../testdata/dict-page.parquet")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(tmpFile, data, 0o644))

	pr, err := pio.NewParquetFileReader(tmpFile, pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = pr.PFile.Close() }()

	col := pr.Footer.RowGroups[0].Columns[0]
	pathKey := strings.Join(col.MetaData.PathInSchema, common.PAR_GO_PATH_DELIMITER)

	schemaRoot, err := pschema.NewSchemaTree(pr, pschema.SchemaOption{SkipPageEncoding: true})
	require.NoError(t, err)
	schemaNode := schemaRoot.GetPathMap()[pathKey]

	// Get the dictionary page info before corrupting the file
	pages, err := Cmd{}.readPages(pr, 0, 0, schemaNode)
	require.NoError(t, err)
	require.NotEmpty(t, pages)
	require.Equal(t, parquet.PageType_DICTIONARY_PAGE, pages[0].Type)

	// Truncate file so ReadDictionaryPageValues fails
	require.NoError(t, os.Truncate(tmpFile, 4))

	cmd := Cmd{}
	_, err = cmd.readDictionaryPageValues(pr, col, schemaNode, pages[0])
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read dictionary page values")
}
