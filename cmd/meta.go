package cmd

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// MetaCmd is a kong command for meta
type MetaCmd struct {
	CommonOption
	Base64 bool `short:"b" help:"Encode min/max value." default:"false"`
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
	MaxValue         *string `json:",omitempty"`
	MinValue         *string `json:",omitempty"`
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
				CompressedSize:   col.MetaData.TotalCompressedSize,
				UncompressedSize: col.MetaData.TotalUncompressedSize,
				NumValues:        col.MetaData.NumValues,
				Index:            nil,
			}
			if col.MetaData.Statistics != nil {
				columns[colIndex].MaxValue = c.retrieveValue(col.MetaData.Statistics.MaxValue, c.Base64)
				columns[colIndex].MinValue = c.retrieveValue(col.MetaData.Statistics.MinValue, c.Base64)
				columns[colIndex].NullCount = col.MetaData.Statistics.NullCount
				columns[colIndex].DistinctCount = col.MetaData.Statistics.DistinctCount
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

func (c *MetaCmd) retrieveValue(value []byte, base64Encode bool) *string {
	if value == nil {
		return nil
	}

	if !base64Encode {
		ret := string(value)
		return &ret
	}

	ret := base64.StdEncoding.EncodeToString(value)
	return &ret
}
