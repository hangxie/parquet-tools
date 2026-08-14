package inspect

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"

	pschema "github.com/hangxie/parquet-tools/schema"
)

func (c Cmd) readPages(ctx context.Context, pr *reader.ParquetReader, rowGroupIndex, columnChunkIndex int, schemaNode *pschema.SchemaNode) ([]PageInfo, error) {
	pageHeaders, err := pr.GetAllPageHeadersWithContext(ctx, rowGroupIndex, columnChunkIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to read page headers: %w", err)
	}

	// Convert PageHeaderInfo to our output format
	pages := make([]PageInfo, len(pageHeaders))
	for i, headerInfo := range pageHeaders {
		pages[i] = c.convertPageHeaderInfo(headerInfo, schemaNode)
	}

	return pages, nil
}

// convertPageHeaderInfo converts PageHeaderInfo from parquet-go to our JSON output format
func (c Cmd) convertPageHeaderInfo(headerInfo reader.PageHeaderInfo, schemaNode *pschema.SchemaNode) PageInfo {
	pageInfo := PageInfo{
		Index:            headerInfo.Index,
		Offset:           headerInfo.Offset,
		Type:             headerInfo.PageType,
		CompressedSize:   headerInfo.CompressedSize,
		UncompressedSize: headerInfo.UncompressedSize,
	}

	if headerInfo.HasCRC {
		pageInfo.HasCrc = true
		pageInfo.Crc = fmt.Sprintf("%08x", uint32(headerInfo.CRC))
	}

	switch headerInfo.PageType {
	case parquet.PageType_DATA_PAGE:
		pageInfo.NumValues = &headerInfo.NumValues
		pageInfo.Encoding = &headerInfo.Encoding
		pageInfo.DefinitionLevelEncoding = &headerInfo.DefLevelEncoding
		pageInfo.RepetitionLevelEncoding = &headerInfo.RepLevelEncoding

		if headerInfo.HasStatistics {
			pageInfo.Statistics = c.buildStatistics(headerInfo.Statistics, schemaNode)
		}

	case parquet.PageType_DATA_PAGE_V2:
		pageInfo.NumValues = &headerInfo.NumValues
		pageInfo.NumNulls = &headerInfo.NumNulls
		pageInfo.NumRows = &headerInfo.NumRows
		pageInfo.Encoding = &headerInfo.Encoding
		pageInfo.DefinitionLevelsByteLength = &headerInfo.DefLevelBytes
		pageInfo.RepetitionLevelsByteLength = &headerInfo.RepLevelBytes
		pageInfo.IsCompressed = headerInfo.IsCompressed

		if headerInfo.HasStatistics {
			pageInfo.Statistics = c.buildStatistics(headerInfo.Statistics, schemaNode)
		}

	case parquet.PageType_DICTIONARY_PAGE:
		pageInfo.NumValues = &headerInfo.NumValues
		pageInfo.Encoding = &headerInfo.Encoding
		pageInfo.IsSorted = headerInfo.IsSorted

	case parquet.PageType_INDEX_PAGE:
		pageInfo.Note = "Index page (column index)"
	}

	return pageInfo
}

func (c Cmd) readPageValues(ctx context.Context, pr *reader.ParquetReader, rowGroupIndex, columnChunkIndex int, col *parquet.ColumnChunk, schemaNode *pschema.SchemaNode, pages []PageInfo, pageIndex int) ([]any, error) {
	if pageIndex < 0 || pageIndex >= len(pages) {
		return nil, fmt.Errorf("page index %d out of range [0, %d)", pageIndex, len(pages))
	}

	pageInfo := pages[pageIndex]

	// Handle different page types
	switch pageInfo.Type {
	case parquet.PageType_DICTIONARY_PAGE:
		return c.readDictionaryPageValues(ctx, pr, col, schemaNode, pageInfo)
	case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
		// Continue to process data pages below
	default:
		// For other page types (INDEX_PAGE, etc.), return empty
		return []any{}, nil
	}

	if pageInfo.NumValues == nil {
		return nil, fmt.Errorf("unable to get numValues for page")
	}

	var rowsBeforeRowGroup int64
	for i := range rowGroupIndex {
		rowsBeforeRowGroup += pr.Footer.RowGroups[i].NumRows
	}
	rowGroupRows := pr.Footer.RowGroups[rowGroupIndex].NumRows

	// Without a row range the row group is the smallest readable unit.
	firstRow, numRows, located := pageRowRange(ctx, pr, rowGroupIndex, columnChunkIndex, pages, pageIndex, rowGroupRows)
	if !located {
		firstRow, numRows = 0, rowGroupRows
	}

	values, err := readColumnRows(ctx, pr, columnChunkIndex, rowsBeforeRowGroup+firstRow, numRows)
	if err != nil {
		return nil, err
	}
	if !located {
		values = pageValueSlice(values, pages, pageIndex)
	}

	return c.convertValuesToJSON(values, schemaNode), nil
}

// readColumnRows reads numRows rows of one column, skipRows rows into the file.
func readColumnRows(ctx context.Context, pr *reader.ParquetReader, columnChunkIndex int, skipRows, numRows int64) ([]any, error) {
	// The reader is shared, so leave no column buffer cursor behind.
	defer func() {
		_ = pr.ReadStopWithContext(context.WithoutCancel(ctx))
		clear(pr.ColumnBuffers)
	}()

	if skipRows > 0 {
		if err := pr.SkipRowsByIndexWithContext(ctx, int64(columnChunkIndex), skipRows); err != nil {
			return nil, fmt.Errorf("failed to skip to page: %w", err)
		}
	}

	values, _, _, err := pr.ReadColumnByIndexWithContext(ctx, int64(columnChunkIndex), numRows)
	if err != nil {
		return nil, fmt.Errorf("failed to read column values: %w", err)
	}
	return values, nil
}

// pageRowRange locates a data page in its row group as (firstRow, numRows).
// Page headers count values, not rows, and v2 num_rows is too often wrong.
func pageRowRange(ctx context.Context, pr *reader.ParquetReader, rowGroupIndex, columnChunkIndex int, pages []PageInfo, pageIndex int, rowGroupRows int64) (int64, int64, bool) {
	// One small read, saves reading the row group's earlier pages.
	index, err := pr.ReadOffsetIndexWithContext(ctx, rowGroupIndex, columnChunkIndex)
	if err == nil && index != nil {
		if firstRow, numRows, ok := offsetIndexRowRange(index.PageLocations, pages[pageIndex].Offset, rowGroupRows); ok {
			return firstRow, numRows, true
		}
	}
	if columnHasRepetition(pr, columnChunkIndex) {
		return 0, 0, false
	}
	return pageValueOffset(pages, pageIndex), int64(*pages[pageIndex].NumValues), true
}

// Page locations cover data pages only, so match on offset, not on index.
func offsetIndexRowRange(locations []*parquet.PageLocation, pageOffset, rowGroupRows int64) (int64, int64, bool) {
	for i, location := range locations {
		if location == nil || location.Offset != pageOffset {
			continue
		}
		endRow := rowGroupRows
		if i+1 < len(locations) && locations[i+1] != nil {
			endRow = locations[i+1].FirstRowIndex
		}
		if location.FirstRowIndex < 0 || endRow > rowGroupRows || endRow <= location.FirstRowIndex {
			return 0, 0, false
		}
		return location.FirstRowIndex, endRow - location.FirstRowIndex, true
	}
	return 0, 0, false
}

// An unresolvable path counts as repeated, keeping the caller conservative.
func columnHasRepetition(pr *reader.ParquetReader, columnChunkIndex int) bool {
	if pr.SchemaHandler == nil || columnChunkIndex < 0 || columnChunkIndex >= len(pr.SchemaHandler.ValueColumns) {
		return true
	}
	level, err := pr.SchemaHandler.MaxRepetitionLevel(common.StrToPath(pr.SchemaHandler.ValueColumns[columnChunkIndex]))
	return err != nil || level > 0
}

// pageValueSlice cuts one page out of its row group's values.
func pageValueSlice(values []any, pages []PageInfo, pageIndex int) []any {
	start := pageValueOffset(pages, pageIndex)
	end := min(start+int64(*pages[pageIndex].NumValues), int64(len(values)))
	if start >= end {
		return []any{}
	}
	return values[start:end]
}

func pageValueOffset(pages []PageInfo, pageIndex int) int64 {
	var offset int64
	for i := range pageIndex {
		if pages[i].NumValues == nil {
			continue
		}
		if pages[i].Type != parquet.PageType_DATA_PAGE && pages[i].Type != parquet.PageType_DATA_PAGE_V2 {
			continue
		}
		offset += int64(*pages[i].NumValues)
	}
	return offset
}

func (c Cmd) readDictionaryPageValues(ctx context.Context, pr *reader.ParquetReader, col *parquet.ColumnChunk, schemaNode *pschema.SchemaNode, pageInfo PageInfo) ([]any, error) {
	meta := col.MetaData

	values, err := pr.ReadDictionaryPageValuesWithContext(ctx, pageInfo.Offset, meta.Codec, meta.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to read dictionary page values: %w", err)
	}

	return c.convertValuesToJSON(values, schemaNode), nil
}
