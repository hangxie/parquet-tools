package cmd

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/types"

	pio "github.com/hangxie/parquet-tools/internal/io"
	pschema "github.com/hangxie/parquet-tools/internal/schema"
)

// MetaCmd is a kong command for meta
type MetaCmd struct {
	Base64      bool   `name:"base64" short:"b" help:"deprecated, will be removed in future version" default:"false"`
	FailOnInt96 bool   `help:"fail command if INT96 data type is present." name:"fail-on-int96" default:"false"`
	URI         string `arg:"" predictor:"file" help:"URI of Parquet file."`
	pio.ReadOption
}

type columnMeta struct {
	PathInSchema     []string
	Type             string
	ConvertedType    *string `json:",omitempty"`
	LogicalType      *string `json:",omitempty"`
	Encodings        []string
	CompressedSize   int64
	UncompressedSize int64
	NumValues        int64
	NullCount        *int64  `json:",omitempty"`
	DistinctCount    *int64  `json:",omitempty"`
	MaxValue         any     `json:",omitempty"`
	MinValue         any     `json:",omitempty"`
	Index            *string `json:",omitempty"`
	CompressionCodec string
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
func (c MetaCmd) Run() error {
	reader, err := pio.NewParquetFileReader(c.URI, c.ReadOption)
	if err != nil {
		return err
	}

	schemaRoot, err := pschema.NewSchemaTree(reader, pschema.SchemaOption{FailOnInt96: c.FailOnInt96})
	if err != nil {
		return err
	}

	inExNameMap, pathMap := c.buildSchemaMaps(schemaRoot)

	rowGroups := c.buildRowGroups(reader.Footer.RowGroups, inExNameMap, pathMap)

	meta := parquetMeta{
		NumRowGroups: len(rowGroups),
		RowGroups:    rowGroups,
	}
	buf, _ := json.Marshal(meta)
	fmt.Println(string(buf))

	return nil
}

func (c MetaCmd) buildSchemaMaps(schemaRoot *pschema.SchemaNode) (map[string][]string, map[string]*pschema.SchemaNode) {
	inExNameMap := map[string][]string{}
	queue := []*pschema.SchemaNode{schemaRoot}
	for len(queue) > 0 {
		node := queue[0]
		queue = append(queue[1:], node.Children...)
		inPath := strings.Join(node.InNamePath[1:], common.PAR_GO_PATH_DELIMITER)
		inExNameMap[inPath] = node.ExNamePath[1:]
	}
	pathMap := schemaRoot.GetPathMap()
	return inExNameMap, pathMap
}

func (c MetaCmd) buildRowGroups(rowGroups []*parquet.RowGroup, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode) []rowGroupMeta {
	result := make([]rowGroupMeta, len(rowGroups))
	for i, rg := range rowGroups {
		columns := c.buildColumns(rg, inExNameMap, pathMap)
		result[i] = rowGroupMeta{
			NumRows:       rg.NumRows,
			TotalByteSize: rg.TotalByteSize,
			Columns:       columns,
		}
	}
	return result
}

func (c MetaCmd) buildColumns(rg *parquet.RowGroup, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode) []columnMeta {
	columns := make([]columnMeta, len(rg.Columns))
	for i, col := range rg.Columns {
		columns[i] = c.buildColumnMeta(col, rg.SortingColumns, i, inExNameMap, pathMap)
	}
	return columns
}

func (c MetaCmd) buildColumnMeta(col *parquet.ColumnChunk, sortingColumns []*parquet.SortingColumn, colIndex int, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode) columnMeta {
	column := columnMeta{
		PathInSchema:     col.MetaData.PathInSchema,
		Type:             col.MetaData.Type.String(),
		ConvertedType:    nil,
		LogicalType:      nil,
		Encodings:        encodingToString(col.MetaData.Encodings),
		CompressedSize:   col.MetaData.TotalCompressedSize,
		UncompressedSize: col.MetaData.TotalUncompressedSize,
		NumValues:        col.MetaData.NumValues,
		MaxValue:         nil,
		MinValue:         nil,
		NullCount:        nil,
		DistinctCount:    nil,
		Index:            sortingToString(sortingColumns, colIndex),
		CompressionCodec: col.MetaData.Codec.String(),
	}

	pathKey := strings.Join(col.MetaData.PathInSchema, common.PAR_GO_PATH_DELIMITER)
	if exPath, found := inExNameMap[pathKey]; found {
		column.PathInSchema = exPath
	}

	schemaNode := pathMap[pathKey]
	if schemaNode != nil {
		c.addTypeInformation(&column, schemaNode)
	}

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

	return column
}

func (c MetaCmd) addTypeInformation(column *columnMeta, schemaNode *pschema.SchemaNode) {
	tagMap := schemaNode.GetTagMap()
	orderedTags := pschema.OrderedTags()

	var convertedTypeParts []string
	for _, tag := range orderedTags {
		if tag == "convertedtype" || tag == "scale" || tag == "precision" || tag == "length" {
			if value, found := tagMap[tag]; found {
				convertedTypeParts = append(convertedTypeParts, tag+"="+value)
			}
		}
	}

	if len(convertedTypeParts) > 0 {
		column.ConvertedType = common.ToPtr(strings.Join(convertedTypeParts, ", "))
	}

	var logicalTypeParts []string
	for _, tag := range orderedTags {
		if strings.HasPrefix(tag, "logicaltype") {
			if value, found := tagMap[tag]; found {
				logicalTypeParts = append(logicalTypeParts, tag+"="+value)
			}
		}
	}

	if len(logicalTypeParts) > 0 {
		column.LogicalType = common.ToPtr(strings.Join(logicalTypeParts, ", "))
	}
}

func (c MetaCmd) addStatistics(column *columnMeta, statistics *parquet.Statistics, schemaNode *pschema.SchemaNode) {
	column.NullCount = statistics.NullCount
	column.DistinctCount = statistics.DistinctCount

	maxValue := c.retrieveValue(statistics.MaxValue, *schemaNode.Type)
	minValue := c.retrieveValue(statistics.MinValue, *schemaNode.Type)
	if maxValue != nil {
		column.MaxValue = decodeMinMaxValue(maxValue, schemaNode)
	}
	if minValue != nil {
		column.MinValue = decodeMinMaxValue(minValue, schemaNode)
	}
}

func decodeMinMaxValue(value any, schemaNode *pschema.SchemaNode) any {
	if schemaNode.Type != nil {
		switch *schemaNode.Type {
		case parquet.Type_INT96:
			// INT96 (deprecated) is used for timestamp only
			return types.INT96ToTime(value.(string))
		case parquet.Type_FIXED_LEN_BYTE_ARRAY, parquet.Type_BYTE_ARRAY:
			if schemaNode.ConvertedType == nil && schemaNode.LogicalType == nil {
				// BYTE_ARRAY and FIXED_LENGTH_BYTE_ARRAY without logical or converted type
				return base64.StdEncoding.EncodeToString([]byte(value.(string)))
			}
		}
	}

	// backward compatibility
	if schemaNode.ConvertedType != nil {
		switch *schemaNode.ConvertedType {
		case parquet.ConvertedType_INTERVAL:
			return types.IntervalToString([]byte(value.(string)))
		case parquet.ConvertedType_DECIMAL:
			// all sorts of DECIMAL values: INT32, INT64, BYTE_ARRAY
			return types.ConvertDecimalValue(value, schemaNode.Type, int(*schemaNode.Precision), int(*schemaNode.Scale))
		case parquet.ConvertedType_DATE:
			return types.ConvertDateLogicalValue(value)
		case parquet.ConvertedType_TIME_MICROS, parquet.ConvertedType_TIME_MILLIS:
			return types.ConvertTimeLogicalValue(value, schemaNode.LogicalType.GetTIME())
		case parquet.ConvertedType_TIMESTAMP_MICROS, parquet.ConvertedType_TIMESTAMP_MILLIS:
			return types.ConvertTimestampValue(value, *schemaNode.ConvertedType)
		case parquet.ConvertedType_BSON:
			return types.ConvertBSONLogicalValue(value)
		}
	}

	if schemaNode.LogicalType != nil {
		switch {
		case schemaNode.LogicalType.IsSetDECIMAL():
			// all sorts of DECIMAL values: INT32, INT64, BYTE_ARRAY
			return types.ConvertDecimalValue(value, schemaNode.Type, int(*schemaNode.Precision), int(*schemaNode.Scale))
		case schemaNode.LogicalType.IsSetDATE():
			return types.ConvertDateLogicalValue(value)
		case schemaNode.LogicalType.IsSetTIME():
			return types.ConvertTimeLogicalValue(value, schemaNode.LogicalType.GetTIME())
		case schemaNode.LogicalType.IsSetTIMESTAMP():
			if schemaNode.LogicalType.TIMESTAMP.Unit.IsSetMILLIS() {
				return types.TIMESTAMP_MILLISToISO8601(value.(int64), false)
			}
			if schemaNode.LogicalType.TIMESTAMP.Unit.IsSetMICROS() {
				return types.TIMESTAMP_MICROSToISO8601(value.(int64), false)
			}
			return types.TIMESTAMP_NANOSToISO8601(value.(int64), false)
		case schemaNode.LogicalType.IsSetUUID():
			return types.ConvertUUIDValue(value)
		case schemaNode.LogicalType.IsSetBSON():
			return types.ConvertBSONLogicalValue(value)
		case schemaNode.LogicalType.IsSetFLOAT16():
			return types.ConvertFloat16LogicalValue(value)
		}
	}

	return value
}

func (c MetaCmd) retrieveValue(value []byte, parquetType parquet.Type) any {
	if value == nil {
		return nil
	}

	buf := bytes.NewReader(value)
	switch parquetType {
	case parquet.Type_BOOLEAN:
		var b bool
		if err := binary.Read(buf, binary.LittleEndian, &b); err != nil {
			return "failed to read data as BOOLEAN"
		}
		return b
	case parquet.Type_INT32:
		var i32 int32
		if err := binary.Read(buf, binary.LittleEndian, &i32); err != nil {
			return "failed to read data as INT32"
		}
		return i32
	case parquet.Type_INT64:
		var i64 int64
		if err := binary.Read(buf, binary.LittleEndian, &i64); err != nil {
			return "failed to read data as INT64"
		}
		return i64
	case parquet.Type_FLOAT:
		var f32 float32
		if err := binary.Read(buf, binary.LittleEndian, &f32); err != nil {
			return "failed to read data as FLOAT"
		}
		return f32
	case parquet.Type_DOUBLE:
		var f64 float64
		if err := binary.Read(buf, binary.LittleEndian, &f64); err != nil {
			return "failed to read data as DOUBLE"
		}
		return f64
	}
	return string(value)
}

func encodingToString(encodings []parquet.Encoding) []string {
	ret := make([]string, len(encodings))
	for i := range encodings {
		ret[i] = encodings[i].String()
	}
	return ret
}

func sortingToString(sortingColumns []*parquet.SortingColumn, columnIndex int) *string {
	for _, indexCol := range sortingColumns {
		if indexCol.ColumnIdx == int32(columnIndex) {
			if indexCol.Descending {
				return common.ToPtr("DESC")
			}
			return common.ToPtr("ASC")
		}
	}
	return nil
}
