package inspect

import (
	"encoding/base64"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

// PageInfo represents information about a page in a Parquet file.
type PageInfo struct {
	Index                      int               `json:"index"`
	Offset                     int64             `json:"offset"`
	Type                       parquet.PageType  `json:"type"`
	CompressedSize             int32             `json:"compressedSize"`
	UncompressedSize           int32             `json:"uncompressedSize"`
	HasCrc                     bool              `json:"hasCrc,omitempty"`
	Crc                        string            `json:"crc,omitempty"`
	NumValues                  *int32            `json:"numValues,omitempty"`
	Encoding                   *parquet.Encoding `json:"encoding,omitempty"`
	DefinitionLevelEncoding    *parquet.Encoding `json:"definitionLevelEncoding,omitempty"`
	RepetitionLevelEncoding    *parquet.Encoding `json:"repetitionLevelEncoding,omitempty"`
	NumNulls                   *int32            `json:"numNulls,omitempty"`
	NumRows                    *int32            `json:"numRows,omitempty"`
	DefinitionLevelsByteLength *int32            `json:"definitionLevelsByteLength,omitempty"`
	RepetitionLevelsByteLength *int32            `json:"repetitionLevelsByteLength,omitempty"`
	IsCompressed               *bool             `json:"isCompressed,omitempty"`
	IsSorted                   *bool             `json:"isSorted,omitempty"`
	Note                       string            `json:"note,omitempty"`
	Statistics                 map[string]any    `json:"statistics,omitempty"`
}

// Cmd is a kong command for inspect
type Cmd struct {
	URI         string `arg:"" predictor:"file" help:"URI of Parquet file."`
	RowGroup    *int   `help:"Row group index to inspect." placeholder:"INDEX"`
	ColumnChunk *int   `help:"Column chunk index to inspect." placeholder:"INDEX"`
	Page        *int   `help:"Page index to inspect." placeholder:"INDEX"`
	pio.ReadOption
}

// Run does actual inspect job
func (c Cmd) Run() error {
	// Validate parameter combinations
	if c.Page != nil && (c.RowGroup == nil || c.ColumnChunk == nil) {
		return fmt.Errorf("--page requires both --row-group and --column-chunk")
	}
	if c.ColumnChunk != nil && c.RowGroup == nil {
		return fmt.Errorf("--column-chunk requires --row-group")
	}

	reader, err := pio.NewParquetFileReader(c.URI, c.ReadOption)
	if err != nil {
		return err
	}
	defer func() {
		_ = reader.PFile.Close()
	}()

	schemaRoot, err := pschema.NewSchemaTree(reader, pschema.SchemaOption{FailOnInt96: false})
	if err != nil {
		return err
	}

	// Build schema maps for name resolution
	inExNameMap := schemaRoot.GetInExNameMap()
	pathMap := schemaRoot.GetPathMap()
	bloomSizeMap := pschema.BloomFilterSizeMap(reader)

	// Determine which level to inspect
	switch {
	case c.Page != nil:
		// Level 4: Show page details and values
		return c.inspectPage(reader, *c.RowGroup, *c.ColumnChunk, *c.Page, pathMap)
	case c.ColumnChunk != nil:
		// Level 3: Show column chunk details and pages
		return c.inspectColumnChunk(reader, *c.RowGroup, *c.ColumnChunk, inExNameMap, pathMap, bloomSizeMap)
	case c.RowGroup != nil:
		// Level 2: Show row group details and column chunks
		return c.inspectRowGroup(reader, *c.RowGroup, inExNameMap, pathMap, bloomSizeMap)
	default:
		// Level 1: Show file info and row groups
		return c.inspectFile(reader)
	}
}

// Level 1: File info and row groups with brief info
func (c Cmd) inspectFile(reader *reader.ParquetReader) error {
	footer := reader.Footer

	// Calculate totals and build row group brief info in a single loop
	totalRows := int64(0)
	compressedSize := int64(0)
	uncompressedSize := int64(0)
	rowGroupsBrief := make([]map[string]any, len(footer.RowGroups))

	for i, rg := range footer.RowGroups {
		rgCompressed := int64(0)
		rgUncompressed := int64(0)
		for _, col := range rg.Columns {
			rgCompressed += col.MetaData.TotalCompressedSize
			rgUncompressed += col.MetaData.TotalUncompressedSize
		}

		rowGroupsBrief[i] = map[string]any{
			"index":            i,
			"numRows":          rg.NumRows,
			"totalByteSize":    rg.TotalByteSize,
			"numColumns":       len(rg.Columns),
			"compressedSize":   rgCompressed,
			"uncompressedSize": rgUncompressed,
		}

		totalRows += rg.NumRows
		compressedSize += rgCompressed
		uncompressedSize += rgUncompressed
	}

	fileInfo := map[string]any{
		"version":          footer.Version,
		"numRowGroups":     len(footer.RowGroups),
		"totalRows":        totalRows,
		"compressedSize":   compressedSize,
		"uncompressedSize": uncompressedSize,
		"createdBy":        footer.CreatedBy,
	}
	if fc := reader.FileCrypto; fc != nil {
		if km := fc.GetKeyMetadata(); len(km) > 0 {
			fileInfo["footerKeyMetadata"] = base64.StdEncoding.EncodeToString(km)
		}
	} else if footer != nil {
		if km := footer.GetFooterSigningKeyMetadata(); len(km) > 0 {
			fileInfo["footerKeyMetadata"] = base64.StdEncoding.EncodeToString(km)
		}
	}

	output := map[string]any{
		"fileInfo":  fileInfo,
		"rowGroups": rowGroupsBrief,
	}

	return c.printJSON(output)
}
