package inspect

import (
	"context"
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"

	pschema "github.com/hangxie/parquet-tools/schema"
)

func (c Cmd) inspectRowGroup(reader *reader.ParquetReader, rowGroupIndex int, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode, bloomSizeMap map[string]int32) error {
	footer := reader.Footer

	if rowGroupIndex < 0 || rowGroupIndex >= len(footer.RowGroups) {
		return fmt.Errorf("row group index %d out of range [0, %d)", rowGroupIndex, len(footer.RowGroups))
	}

	rg := footer.RowGroups[rowGroupIndex]

	// Build column chunks brief info
	columnChunksBrief := make([]map[string]any, len(rg.Columns))
	for i, col := range rg.Columns {
		columnChunksBrief[i] = c.buildColumnChunkBrief(i, col, inExNameMap, pathMap, bloomSizeMap)
	}

	rowGroup := map[string]any{
		"index":         rowGroupIndex,
		"numRows":       rg.NumRows,
		"totalByteSize": rg.TotalByteSize,
		"numColumns":    len(rg.Columns),
	}
	addRowGroupMetadata(rowGroup, rg)
	output := map[string]any{
		"rowGroup":     rowGroup,
		"columnChunks": columnChunksBrief,
	}

	return c.printJSON(output)
}

func (c Cmd) buildColumnChunkBrief(index int, col *parquet.ColumnChunk, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode, bloomSizeMap map[string]int32) map[string]any {
	pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)
	pathInSchema := c.resolvePathInSchema(col.MetaData.PathInSchema, inExNameMap)
	schemaNode := pathMap[pathKey]

	columnChunk := map[string]any{
		"index":            index,
		"pathInSchema":     pathInSchema,
		"type":             col.MetaData.Type.String(),
		"encodings":        pschema.EncodingToString(col.MetaData.Encodings),
		"compressionCodec": col.MetaData.Codec.String(),
		"numValues":        col.MetaData.NumValues,
		"compressedSize":   col.MetaData.TotalCompressedSize,
		"uncompressedSize": col.MetaData.TotalUncompressedSize,
	}

	c.addTypeInformation(columnChunk, schemaNode)
	c.addEncryptionInfo(columnChunk, col)

	// Add bloom filter info if available, using correct bitset-only size
	if col.MetaData.IsSetBloomFilterOffset() {
		columnChunk["bloomFilterOffset"] = col.MetaData.GetBloomFilterOffset()
		if size, ok := bloomSizeMap[pathKey]; ok && size > 0 {
			columnChunk["bloomFilterLength"] = size
		}
	}

	// Add statistics if available
	if col.MetaData.Statistics != nil {
		stats := c.buildStatistics(col.MetaData.Statistics, schemaNode)
		if len(stats) > 0 {
			columnChunk["statistics"] = stats
		}
	}

	return columnChunk
}

// Level 3: Column chunk details and pages with brief info
func (c Cmd) inspectColumnChunk(ctx context.Context, reader *reader.ParquetReader, rowGroupIndex, columnChunkIndex int, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode, bloomSizeMap map[string]int32) error {
	footer := reader.Footer

	if rowGroupIndex < 0 || rowGroupIndex >= len(footer.RowGroups) {
		return fmt.Errorf("row group index %d out of range [0, %d)", rowGroupIndex, len(footer.RowGroups))
	}

	rg := footer.RowGroups[rowGroupIndex]

	if columnChunkIndex < 0 || columnChunkIndex >= len(rg.Columns) {
		return fmt.Errorf("column chunk index %d out of range [0, %d)", columnChunkIndex, len(rg.Columns))
	}

	col := rg.Columns[columnChunkIndex]
	pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)
	pathInSchema := c.resolvePathInSchema(col.MetaData.PathInSchema, inExNameMap)
	schemaNode := pathMap[pathKey]

	// Build column chunk details
	columnChunkDetails := map[string]any{
		"rowGroupIndex":    rowGroupIndex,
		"columnChunkIndex": columnChunkIndex,
		"pathInSchema":     pathInSchema,
		"type":             col.MetaData.Type.String(),
		"encodings":        pschema.EncodingToString(col.MetaData.Encodings),
		"compressionCodec": col.MetaData.Codec.String(),
		"numValues":        col.MetaData.NumValues,
		"compressedSize":   col.MetaData.TotalCompressedSize,
		"uncompressedSize": col.MetaData.TotalUncompressedSize,
	}

	if col.MetaData.IsSetBloomFilterOffset() {
		columnChunkDetails["bloomFilterOffset"] = col.MetaData.GetBloomFilterOffset()
		if size, ok := bloomSizeMap[pathKey]; ok && size > 0 {
			columnChunkDetails["bloomFilterLength"] = size
		}
	}

	c.addTypeInformation(columnChunkDetails, schemaNode)
	if schemaNode != nil {
		addSchemaElementMetadata(columnChunkDetails, &schemaNode.SchemaElement)
	}
	addColumnMetadata(columnChunkDetails, col)
	c.addEncryptionInfo(columnChunkDetails, col)

	// Add statistics
	if col.MetaData.Statistics != nil {
		stats := c.buildStatistics(col.MetaData.Statistics, schemaNode)
		if len(stats) > 0 {
			columnChunkDetails["statistics"] = stats
		}
	}

	if err := c.addPageIndexes(ctx, reader, rowGroupIndex, columnChunkIndex, columnChunkDetails, schemaNode); err != nil {
		return err
	}

	// Read pages - this requires reading the actual data from the file
	pages, err := c.readPages(ctx, reader, rowGroupIndex, columnChunkIndex, schemaNode)
	if err != nil {
		return fmt.Errorf("failed to read pages: %w", err)
	}

	output := map[string]any{
		"columnChunk": columnChunkDetails,
		"pages":       pages,
	}

	return c.printJSON(output)
}

// Level 4: Page details and values
func (c Cmd) inspectPage(ctx context.Context, reader *reader.ParquetReader, rowGroupIndex, columnChunkIndex, pageIndex int, pathMap map[string]*pschema.SchemaNode) error {
	footer := reader.Footer

	if rowGroupIndex < 0 || rowGroupIndex >= len(footer.RowGroups) {
		return fmt.Errorf("row group index %d out of range [0, %d)", rowGroupIndex, len(footer.RowGroups))
	}
	rg := footer.RowGroups[rowGroupIndex]

	if columnChunkIndex < 0 || columnChunkIndex >= len(rg.Columns) {
		return fmt.Errorf("column chunk index %d out of range [0, %d)", columnChunkIndex, len(rg.Columns))
	}
	col := rg.Columns[columnChunkIndex]
	pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)
	schemaNode := pathMap[pathKey]

	// Read pages
	pages, err := c.readPages(ctx, reader, rowGroupIndex, columnChunkIndex, schemaNode)
	if err != nil {
		return fmt.Errorf("failed to read pages: %w", err)
	}

	if pageIndex < 0 || pageIndex >= len(pages) {
		return fmt.Errorf("page index %d out of range [0, %d)", pageIndex, len(pages))
	}
	page := pages[pageIndex]

	// Read page values
	values, err := c.readPageValues(ctx, reader, rowGroupIndex, columnChunkIndex, col, schemaNode, pages, pageIndex)
	if err != nil {
		return fmt.Errorf("failed to read page values: %w", err)
	}

	output := map[string]any{
		"page":   page,
		"values": values,
	}

	return c.printJSON(output)
}
