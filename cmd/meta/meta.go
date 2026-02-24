package meta

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

// Cmd is a kong command for meta
type Cmd struct {
	FailOnInt96 bool   `help:"fail command if INT96 data type is present." name:"fail-on-int96" default:"false"`
	URI         string `arg:"" predictor:"file" help:"URI of Parquet file."`
	pio.ReadOption
}

type columnMeta struct {
	PathInSchema      []string
	Type              string
	ConvertedType     *string `json:",omitempty"`
	LogicalType       *string `json:",omitempty"`
	Encodings         []string
	CompressedSize    int64
	UncompressedSize  int64
	NumValues         int64
	NullCount         *int64  `json:",omitempty"`
	DistinctCount     *int64  `json:",omitempty"`
	MaxValue          any     `json:",omitempty"`
	MinValue          any     `json:",omitempty"`
	Index             *string `json:",omitempty"`
	BloomFilterOffset *int64  `json:",omitempty"`
	BloomFilterLength *int32  `json:",omitempty"`
	CompressionCodec  string
}

type rowGroupMeta struct {
	NumRows       int64
	TotalByteSize int64
	Columns       []columnMeta
}

type parquetMeta struct {
	NumRowGroups int
	RowGroups    []rowGroupMeta
}

// Run does actual meta job
func (c Cmd) Run() error {
	reader, err := pio.NewParquetFileReader(c.URI, c.ReadOption)
	if err != nil {
		return err
	}

	schemaRoot, err := pschema.NewSchemaTree(reader, pschema.SchemaOption{FailOnInt96: c.FailOnInt96, SkipPageEncoding: true})
	if err != nil {
		return err
	}

	inExNameMap := schemaRoot.GetInExNameMap()
	pathMap := schemaRoot.GetPathMap()
	bloomSizeMap := pschema.BloomFilterSizeMap(reader)

	rowGroups, err := c.buildRowGroups(reader.Footer.RowGroups, inExNameMap, pathMap, bloomSizeMap)
	if err != nil {
		return err
	}

	meta := parquetMeta{
		NumRowGroups: len(rowGroups),
		RowGroups:    rowGroups,
	}
	buf, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	fmt.Println(string(buf))

	return nil
}

func (c Cmd) buildRowGroups(rowGroups []*parquet.RowGroup, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode, bloomSizeMap map[string]int32) ([]rowGroupMeta, error) {
	result := make([]rowGroupMeta, len(rowGroups))
	for i, rg := range rowGroups {
		columns, err := c.buildColumns(rg, inExNameMap, pathMap, bloomSizeMap)
		if err != nil {
			return nil, err
		}
		result[i] = rowGroupMeta{
			NumRows:       rg.NumRows,
			TotalByteSize: rg.TotalByteSize,
			Columns:       columns,
		}
	}
	return result, nil
}

func (c Cmd) buildColumns(rg *parquet.RowGroup, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode, bloomSizeMap map[string]int32) ([]columnMeta, error) {
	columns := make([]columnMeta, len(rg.Columns))
	for i, col := range rg.Columns {
		column, err := c.buildColumnMeta(col, rg.SortingColumns, i, inExNameMap, pathMap, bloomSizeMap)
		if err != nil {
			return nil, err
		}
		columns[i] = column
	}
	return columns, nil
}

func (c Cmd) buildColumnMeta(col *parquet.ColumnChunk, sortingColumns []*parquet.SortingColumn, colIndex int, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode, bloomSizeMap map[string]int32) (columnMeta, error) {
	column := columnMeta{
		PathInSchema:      col.MetaData.PathInSchema,
		Type:              col.MetaData.Type.String(),
		ConvertedType:     nil,
		LogicalType:       nil,
		Encodings:         pschema.EncodingToString(col.MetaData.Encodings),
		CompressedSize:    col.MetaData.TotalCompressedSize,
		UncompressedSize:  col.MetaData.TotalUncompressedSize,
		NumValues:         col.MetaData.NumValues,
		MaxValue:          nil,
		MinValue:          nil,
		NullCount:         nil,
		DistinctCount:     nil,
		Index:             sortingToString(sortingColumns, colIndex),
		BloomFilterOffset: col.MetaData.BloomFilterOffset,
		CompressionCodec:  col.MetaData.Codec.String(),
	}

	pathKey := strings.Join(col.MetaData.PathInSchema, common.PAR_GO_PATH_DELIMITER)

	// Use the correct bitset-only size from the bloom filter size map
	if size, ok := bloomSizeMap[pathKey]; ok && size > 0 {
		column.BloomFilterLength = &size
	}

	if exPath, found := inExNameMap[pathKey]; found {
		column.PathInSchema = exPath
	}

	schemaNode := pathMap[pathKey]
	if schemaNode == nil {
		return columnMeta{}, fmt.Errorf("schema node not found for column path: [%s]", pathKey)
	}

	c.addTypeInformation(&column, schemaNode)

	if col.MetaData.Statistics != nil {
		c.addStatistics(&column, col.MetaData.Statistics, schemaNode)
	}

	// use bounding box for geospatial data if geospatial statistics exists
	if schemaNode.LogicalType != nil &&
		(schemaNode.LogicalType.IsSetGEOMETRY() || schemaNode.LogicalType.IsSetGEOGRAPHY()) &&
		col.MetaData.GeospatialStatistics != nil && col.MetaData.GeospatialStatistics.Bbox != nil {
		column.MinValue = []float64{
			col.MetaData.GeospatialStatistics.Bbox.Xmin,
			col.MetaData.GeospatialStatistics.Bbox.Ymin,
		}
		column.MaxValue = []float64{
			col.MetaData.GeospatialStatistics.Bbox.Xmax,
			col.MetaData.GeospatialStatistics.Bbox.Ymax,
		}
	}

	return column, nil
}

func (c Cmd) addTypeInformation(column *columnMeta, schemaNode *pschema.SchemaNode) {
	if ct := schemaNode.ConvertedTypeString(); ct != "" {
		column.ConvertedType = new(ct)
	}
	if lt := schemaNode.LogicalTypeString(); lt != "" {
		column.LogicalType = new(lt)
	}
}

func (c Cmd) addStatistics(column *columnMeta, statistics *parquet.Statistics, schemaNode *pschema.SchemaNode) {
	column.NullCount = statistics.NullCount
	column.DistinctCount = statistics.DistinctCount
	column.MaxValue = schemaNode.DecodeStatValue(statistics.MaxValue)
	column.MinValue = schemaNode.DecodeStatValue(statistics.MinValue)
}

func sortingToString(sortingColumns []*parquet.SortingColumn, columnIndex int) *string {
	for _, indexCol := range sortingColumns {
		if indexCol.ColumnIdx == int32(columnIndex) {
			if indexCol.Descending {
				return new("DESC")
			}
			return new("ASC")
		}
	}
	return nil
}
