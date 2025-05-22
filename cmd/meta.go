package cmd

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/common"
	"github.com/hangxie/parquet-go/parquet"
	"github.com/hangxie/parquet-go/types"

	pio "github.com/hangxie/parquet-tools/internal/io"
	pschema "github.com/hangxie/parquet-tools/internal/schema"
)

// MetaCmd is a kong command for meta
type MetaCmd struct {
	pio.ReadOption
	Base64      bool   `name:"base64" short:"b" help:"Encode min/max value." default:"false"`
	URI         string `arg:"" predictor:"file" help:"URI of Parquet file."`
	FailOnInt96 bool   `help:"fail command if INT96 data type presents." name:"fail-on-int96" default:"false"`
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

	riFieldMap := map[string]pschema.ReinterpretField{}
	for _, field := range schemaRoot.GetReinterpretFields(false) {
		riFieldMap[field.InPath] = field
	}

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

			if col.MetaData.Statistics == nil {
				// no statistics info
				continue
			}

			columns[colIndex].NullCount = col.MetaData.Statistics.NullCount
			columns[colIndex].DistinctCount = col.MetaData.Statistics.DistinctCount

			if col.MetaData.Type == parquet.Type_INT96 {
				// INT96 (deprecated) is used for timestamp only
				if maxValue := c.retrieveValue(col.MetaData.Statistics.MaxValue, col.MetaData.Type, false); maxValue != nil {
					columns[colIndex].MaxValue = types.INT96ToTime(maxValue.(string))
				}
				if minValue := c.retrieveValue(col.MetaData.Statistics.MinValue, col.MetaData.Type, false); minValue != nil {
					columns[colIndex].MinValue = types.INT96ToTime(minValue.(string))
				}
				continue
			}

			field, found := riFieldMap[pathKey]
			if !found {
				columns[colIndex].MaxValue = c.retrieveValue(col.MetaData.Statistics.MaxValue, col.MetaData.Type, c.Base64)
				columns[colIndex].MinValue = c.retrieveValue(col.MetaData.Statistics.MinValue, col.MetaData.Type, c.Base64)
				continue
			}

			// reformat decimal values
			var err error
			maxValue := c.retrieveValue(col.MetaData.Statistics.MaxValue, col.MetaData.Type, false)
			if columns[colIndex].MaxValue, err = pschema.DecimalToFloat(field, maxValue); err != nil {
				return err
			}

			minValue := c.retrieveValue(col.MetaData.Statistics.MinValue, col.MetaData.Type, false)
			if columns[colIndex].MinValue, err = pschema.DecimalToFloat(field, minValue); err != nil {
				return err
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

func (c MetaCmd) retrieveValue(value []byte, parquetType parquet.Type, base64Encode bool) any {
	if value == nil {
		return nil
	}

	buf := bytes.NewReader(value)
	var ret string
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
	if !base64Encode {
		ret = string(value)
	} else {
		ret = base64.StdEncoding.EncodeToString(value)
	}
	return ret
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
			var ret string
			if indexCol.Descending {
				ret = "DESC"
			} else {
				ret = "ASC"
			}
			return &ret
		}
	}
	return nil
}
