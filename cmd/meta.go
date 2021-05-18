package cmd

import (
	"encoding/json"
	"fmt"
)

// MetaCmd is a kong command for meta
type MetaCmd struct {
	CommonOption
	OriginalType bool `help:"Print logical types in OriginalType representation."`
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
	MaxValue         []byte  `json:",omitempty"`
	MinValue         []byte  `json:",omitempty"`
	Index            *string `json:",omitempty"`
}

type rowGroupMeta struct {
	NumRows       int64
	TotalByteSize int64
	Columns       []columnMeta
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
				NumValues:        col.MetaData.NumValues,
				NullCount:        col.MetaData.Statistics.NullCount,
				DistinctCount:    col.MetaData.Statistics.DistinctCount,
				MaxValue:         col.MetaData.Statistics.MaxValue,
				MinValue:         col.MetaData.Statistics.MinValue,
				CompressedSize:   col.MetaData.TotalCompressedSize,
				UncompressedSize: col.MetaData.TotalUncompressedSize,
				Index:            nil,
			}
			for i, encoding := range col.MetaData.Encodings {
				columns[colIndex].Encodings[i] = encoding.String()
			}
			// TODO find a parquet file with index to test this
			/*
				for _, indexCol := range rg.SortingColumns {
					if indexCol.ColumnIdx == int32(colIndex) {
						columns[colIndex].Index = new(string)
						if indexCol.Descending {
							*columns[colIndex].Index = "DESC"
						} else {
							*columns[colIndex].Index = "ACS"
						}
						break
					}
				}
			*/
		}
		rowGroups[rgIndex] = rowGroupMeta{
			NumRows:       rg.NumRows,
			TotalByteSize: rg.TotalByteSize,
			Columns:       columns,
		}
	}

	meta := struct {
		NumRowGroups int
		RowGroups    []rowGroupMeta
	}{
		NumRowGroups: len(rowGroups),
		RowGroups:    rowGroups,
	}
	buf, _ := json.Marshal(meta)
	fmt.Println(string(buf))

	return nil
}
