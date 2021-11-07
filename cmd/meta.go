package cmd

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/xitongsys/parquet-go/parquet"
)

// MetaCmd is a kong command for meta
type MetaCmd struct {
	CommonOption
	Base64 bool `name:"base64" short:"b" help:"Encode min/max value." default:"false"`
}

type columnMeta struct {
	PathInSchema     []string
	Type             string
	Encodings        []string
	CompressedSize   int64
	UncompressedSize int64
	NumValues        int64
	NullCount        *int64      `json:",omitempty"`
	DistinctCount    *int64      `json:",omitempty"`
	MaxValue         interface{} `json:",omitempty"`
	MinValue         interface{} `json:",omitempty"`
	Index            *string     `json:",omitempty"`
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
func (c *MetaCmd) Run(ctx *Context) error {
	reader, err := newParquetFileReader(c.URI)
	if err != nil {
		return err
	}

	rowGroups := make([]rowGroupMeta, len(reader.Footer.RowGroups))
	for rgIndex, rg := range reader.Footer.RowGroups {
		columns := make([]columnMeta, len(rg.Columns))
		for colIndex, col := range rg.Columns {
			columns[colIndex] = columnMeta{
				PathInSchema:     col.MetaData.PathInSchema,
				Type:             col.MetaData.Type.String(),
				Encodings:        make([]string, len(col.MetaData.Encodings)),
				CompressedSize:   col.MetaData.TotalCompressedSize,
				UncompressedSize: col.MetaData.TotalUncompressedSize,
				NumValues:        col.MetaData.NumValues,
				MaxValue:         nil,
				MinValue:         nil,
				NullCount:        nil,
				DistinctCount:    nil,
				Index:            nil,
			}
			if col.MetaData.Statistics != nil {
				columns[colIndex].MaxValue = c.retrieveValue(col.MetaData.Statistics.MaxValue, col.MetaData.Type, c.Base64)
				columns[colIndex].MinValue = c.retrieveValue(col.MetaData.Statistics.MinValue, col.MetaData.Type, c.Base64)
				columns[colIndex].NullCount = col.MetaData.Statistics.NullCount
				columns[colIndex].DistinctCount = col.MetaData.Statistics.DistinctCount
			}
			for i, encoding := range col.MetaData.Encodings {
				columns[colIndex].Encodings[i] = encoding.String()
			}

			for _, indexCol := range rg.SortingColumns {
				if indexCol.ColumnIdx == int32(colIndex) {
					columns[colIndex].Index = new(string)
					if indexCol.Descending {
						*columns[colIndex].Index = "DESC"
					} else {
						*columns[colIndex].Index = "ASC"
					}
					break
				}
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

func (c *MetaCmd) retrieveValue(value []byte, parquetType parquet.Type, base64Encode bool) interface{} {
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
