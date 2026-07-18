package inspect

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"

	pschema "github.com/hangxie/parquet-tools/schema"
)

func (c Cmd) readPages(pr *reader.ParquetReader, rowGroupIndex, columnChunkIndex int, schemaNode *pschema.SchemaNode) ([]PageInfo, error) {
	pageHeaders, err := pr.GetAllPageHeaders(rowGroupIndex, columnChunkIndex)
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

func (c Cmd) readPageValues(pr *reader.ParquetReader, rowGroupIndex, columnChunkIndex int, col *parquet.ColumnChunk, schemaNode *pschema.SchemaNode, pages []PageInfo, pageIndex int) ([]any, error) {
	meta := col.MetaData

	if pageIndex < 0 || pageIndex >= len(pages) {
		return nil, fmt.Errorf("page index %d out of range [0, %d)", pageIndex, len(pages))
	}

	pageInfo := pages[pageIndex]

	// Handle different page types
	switch pageInfo.Type {
	case parquet.PageType_DICTIONARY_PAGE:
		return c.readDictionaryPageValues(pr, col, schemaNode, pageInfo)
	case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
		// Continue to process data pages below
	default:
		// For other page types (INDEX_PAGE, etc.), return empty
		return []any{}, nil
	}

	// Calculate total values before this row group and in this row group
	var valuesBeforeRG int64
	for i := range rowGroupIndex {
		valuesBeforeRG += pr.Footer.RowGroups[i].NumRows
	}

	// Create a fresh column reader to read the entire column
	freshReader, err := reader.NewParquetColumnReader(pr.PFile, reader.WithNP(4))
	if err != nil {
		return nil, fmt.Errorf("failed to create fresh reader: %w", err)
	}
	defer func() { _ = freshReader.ReadStop() }()

	// Calculate total number of rows in the file
	totalRows := int64(0)
	for _, rg := range pr.Footer.RowGroups {
		totalRows += rg.NumRows
	}

	// Read ALL values from the entire file for this column
	allValuesInFile, _, _, err := freshReader.ReadColumnByIndex(int64(columnChunkIndex), totalRows)
	if err != nil {
		return nil, fmt.Errorf("failed to read column values: %w", err)
	}

	// Extract only the values for this row group
	rgStartIdx := valuesBeforeRG
	rgEndIdx := min(valuesBeforeRG+meta.NumValues, int64(len(allValuesInFile)))

	allValues := allValuesInFile[rgStartIdx:rgEndIdx]

	// Calculate the start index for this page
	var startIdx int64
	for i := range pageIndex {
		if pages[i].Type != parquet.PageType_DATA_PAGE && pages[i].Type != parquet.PageType_DATA_PAGE_V2 {
			continue
		}
		if pages[i].NumValues == nil {
			continue
		}
		startIdx += int64(*pages[i].NumValues)
	}

	// Extract values for just this page
	if pageInfo.NumValues == nil {
		return nil, fmt.Errorf("unable to get numValues for page")
	}

	endIdx := min(startIdx+int64(*pageInfo.NumValues), int64(len(allValues)))

	// Convert values to appropriate JSON types
	pageValues := make([]any, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		pageValues[i-startIdx] = allValues[i]
	}

	return c.convertValuesToJSON(pageValues, schemaNode), nil
}

func (c Cmd) readDictionaryPageValues(pr *reader.ParquetReader, col *parquet.ColumnChunk, schemaNode *pschema.SchemaNode, pageInfo PageInfo) ([]any, error) {
	meta := col.MetaData

	values, err := pr.ReadDictionaryPageValues(pageInfo.Offset, meta.Codec, meta.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to read dictionary page values: %w", err)
	}

	return c.convertValuesToJSON(values, schemaNode), nil
}
