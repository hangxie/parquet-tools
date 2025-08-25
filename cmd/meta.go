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
	pio.ReadOption
	Base64      bool   `name:"base64" short:"b" help:"deprecated, will be removed in future version" default:"false"`
	URI         string `arg:"" predictor:"file" help:"URI of Parquet file."`
	FailOnInt96 bool   `help:"fail command if INT96 data type is present." name:"fail-on-int96" default:"false"`
}

type columnMeta struct {
	PathInSchema     []string
	Type             string
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

	inExNameMap := map[string][]string{}
	queue := []*pschema.SchemaNode{schemaRoot}
	for len(queue) > 0 {
		node := queue[0]
		queue = append(queue[1:], node.Children...)
		inPath := strings.Join(node.InNamePath[1:], common.PAR_GO_PATH_DELIMITER)
		inExNameMap[inPath] = node.ExNamePath[1:]
	}

	pathMap := schemaRoot.GetPathMap()

	rowGroups := make([]rowGroupMeta, len(reader.Footer.RowGroups))
	for rgIndex, rg := range reader.Footer.RowGroups {
		columns := make([]columnMeta, len(rg.Columns))
		for colIndex, col := range rg.Columns {
			columns[colIndex] = columnMeta{
				PathInSchema:     col.MetaData.PathInSchema,
				Type:             col.MetaData.Type.String(),
				Encodings:        encodingToString(col.MetaData.Encodings),
				CompressedSize:   col.MetaData.TotalCompressedSize,
				UncompressedSize: col.MetaData.TotalUncompressedSize,
				NumValues:        col.MetaData.NumValues,
				MaxValue:         nil,
				MinValue:         nil,
				NullCount:        nil,
				DistinctCount:    nil,
				Index:            sortingToString(rg.SortingColumns, colIndex),
				CompressionCodec: col.MetaData.Codec.String(),
			}

			pathKey := strings.Join(col.MetaData.PathInSchema, common.PAR_GO_PATH_DELIMITER)
			if exPath, found := inExNameMap[pathKey]; found {
				// should always reach here unless the schema or meta is corrupted
				columns[colIndex].PathInSchema = exPath
			}
			schemaNode := pathMap[pathKey]

			if col.MetaData.Statistics == nil {
				// no statistics info
				continue
			}

			columns[colIndex].NullCount = col.MetaData.Statistics.NullCount
			columns[colIndex].DistinctCount = col.MetaData.Statistics.DistinctCount

			maxValue := c.retrieveValue(col.MetaData.Statistics.MaxValue, *schemaNode.Type)
			minValue := c.retrieveValue(col.MetaData.Statistics.MinValue, *schemaNode.Type)
			if maxValue != nil {
				columns[colIndex].MaxValue = decodeMinMaxValue(maxValue, schemaNode)
			}
			if minValue != nil {
				columns[colIndex].MinValue = decodeMinMaxValue(minValue, schemaNode)
			}
		}

		rowGroups[rgIndex] = rowGroupMeta{
			NumRows:       rg.NumRows,
			TotalByteSize: rg.TotalByteSize,
			Columns:       columns,
		}
	}

	meta := parquetMeta{
		NumRowGroups: len(rowGroups),
		RowGroups:    rowGroups,
	}
	buf, _ := json.Marshal(meta)
	fmt.Println(string(buf))

	return nil
}

func decodeMinMaxValue(value any, schemaNode *pschema.SchemaNode) any {
	if schemaNode.Type != nil && *schemaNode.Type == parquet.Type_INT96 {
		// INT96 (deprecated) is used for timestamp only
		return types.INT96ToTime(value.(string))
	}

	if schemaNode.ConvertedType != nil && *schemaNode.ConvertedType == parquet.ConvertedType_INTERVAL {
		// INTERVAL
		return types.IntervalToString([]byte(value.(string)))
	}

	// backward compatibility
	if schemaNode.ConvertedType != nil {
		switch *schemaNode.ConvertedType {
		case parquet.ConvertedType_DECIMAL:
			// all sorts of DECIMAL values: INT32, INT64, BYTE_ARRAY
			return types.ConvertDecimalValue(value, schemaNode.Type, int(*schemaNode.Precision), int(*schemaNode.Scale))
		case parquet.ConvertedType_TIME_MICROS, parquet.ConvertedType_TIME_MILLIS:
			// TIME
			return types.ConvertTimeLogicalValue(value, schemaNode.LogicalType.GetTIME())
		case parquet.ConvertedType_TIMESTAMP_MICROS, parquet.ConvertedType_TIMESTAMP_MILLIS:
			// TIMESTAMP
			return types.ConvertTimestampValue(value, *schemaNode.ConvertedType)
		}
	}

	if schemaNode.LogicalType != nil {
		switch {
		case schemaNode.LogicalType.IsSetDECIMAL():
			// DECIMAL
			return types.ConvertDecimalValue(value, schemaNode.Type, int(*schemaNode.Precision), int(*schemaNode.Scale))
		case schemaNode.LogicalType.TIME != nil:
			// TIME
			return types.ConvertTimeLogicalValue(value, schemaNode.LogicalType.GetTIME())
		case schemaNode.LogicalType.TIMESTAMP != nil:
			// TIMESTAMP
			if schemaNode.LogicalType.TIMESTAMP.Unit.IsSetMILLIS() {
				return types.TIMESTAMP_MILLISToISO8601(value.(int64), false)
			}
			if schemaNode.LogicalType.TIMESTAMP.Unit.IsSetMICROS() {
				return types.TIMESTAMP_MICROSToISO8601(value.(int64), false)
			}
			return types.TIMESTAMP_NANOSToISO8601(value.(int64), false)
		}
	}

	if schemaNode.ConvertedType == nil && schemaNode.LogicalType == nil && schemaNode.Type != nil &&
		(*schemaNode.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY || *schemaNode.Type == parquet.Type_BYTE_ARRAY) {
		// BYTE_ARRAY and FIXED_LENGTH_BYTE_ARRAY without logical or converted type
		return base64.StdEncoding.EncodeToString([]byte(value.(string)))
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
