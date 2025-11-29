package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/encoding"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/types"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

// PageInfo represents information about a page in a Parquet file
type PageInfo struct {
	Index                      int               `json:"index"`
	Offset                     int64             `json:"offset"`
	Type                       parquet.PageType  `json:"type"`
	CompressedSize             int32             `json:"compressedSize"`
	UncompressedSize           int32             `json:"uncompressedSize"`
	HasCrc                     bool              `json:"hasCrc,omitempty"`
	Crc                        int32             `json:"crc,omitempty"`
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

// InspectCmd is a kong command for inspect
type InspectCmd struct {
	URI         string `arg:"" predictor:"file" help:"URI of Parquet file."`
	RowGroup    *int   `name:"row-group" help:"Row group index to inspect."`
	ColumnChunk *int   `name:"column-chunk" help:"Column chunk index to inspect (requires --row-group)."`
	Page        *int   `name:"page" help:"Page index to inspect (requires --row-group and --column-chunk)."`
	pio.ReadOption
}

// Run does actual inspect job
func (c InspectCmd) Run() error {
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
	inExNameMap, pathMap := c.buildSchemaMaps(schemaRoot)

	// Determine which level to inspect
	switch {
	case c.Page != nil:
		// Level 4: Show page details and values
		return c.inspectPage(reader, *c.RowGroup, *c.ColumnChunk, *c.Page, pathMap)
	case c.ColumnChunk != nil:
		// Level 3: Show column chunk details and pages
		return c.inspectColumnChunk(reader, *c.RowGroup, *c.ColumnChunk, inExNameMap, pathMap)
	case c.RowGroup != nil:
		// Level 2: Show row group details and column chunks
		return c.inspectRowGroup(reader, *c.RowGroup, inExNameMap, pathMap)
	default:
		// Level 1: Show file info and row groups
		return c.inspectFile(reader)
	}
}

func (c InspectCmd) buildSchemaMaps(schemaRoot *pschema.SchemaNode) (map[string][]string, map[string]*pschema.SchemaNode) {
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

// Level 1: File info and row groups with brief info
func (c InspectCmd) inspectFile(reader *reader.ParquetReader) error {
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

	output := map[string]any{
		"fileInfo": map[string]any{
			"version":          footer.Version,
			"numRowGroups":     len(footer.RowGroups),
			"totalRows":        totalRows,
			"compressedSize":   compressedSize,
			"uncompressedSize": uncompressedSize,
			"createdBy":        footer.CreatedBy,
		},
		"rowGroups": rowGroupsBrief,
	}

	return c.printJSON(output)
}

// Level 2: Row group details and column chunks with brief info
func (c InspectCmd) inspectRowGroup(reader *reader.ParquetReader, rowGroupIndex int, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode) error {
	footer := reader.Footer

	if rowGroupIndex < 0 || rowGroupIndex >= len(footer.RowGroups) {
		return fmt.Errorf("row group index %d out of range [0, %d)", rowGroupIndex, len(footer.RowGroups))
	}

	rg := footer.RowGroups[rowGroupIndex]

	// Build column chunks brief info
	columnChunksBrief := make([]map[string]any, len(rg.Columns))
	for i, col := range rg.Columns {
		columnChunksBrief[i] = c.buildColumnChunkBrief(i, col, inExNameMap, pathMap)
	}

	output := map[string]any{
		"rowGroup": map[string]any{
			"index":         rowGroupIndex,
			"numRows":       rg.NumRows,
			"totalByteSize": rg.TotalByteSize,
			"numColumns":    len(rg.Columns),
		},
		"columnChunks": columnChunksBrief,
	}

	return c.printJSON(output)
}

func (c InspectCmd) buildColumnChunkBrief(index int, col *parquet.ColumnChunk, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode) map[string]any {
	pathKey := strings.Join(col.MetaData.PathInSchema, common.PAR_GO_PATH_DELIMITER)
	pathInSchema := c.resolvePathInSchema(col.MetaData.PathInSchema, inExNameMap)
	schemaNode := pathMap[pathKey]

	columnChunk := map[string]any{
		"index":            index,
		"pathInSchema":     pathInSchema,
		"type":             col.MetaData.Type.String(),
		"encodings":        encodingToString(col.MetaData.Encodings),
		"compressionCodec": col.MetaData.Codec.String(),
		"numValues":        col.MetaData.NumValues,
		"compressedSize":   col.MetaData.TotalCompressedSize,
		"uncompressedSize": col.MetaData.TotalUncompressedSize,
	}

	c.addTypeInformation(columnChunk, schemaNode)

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
func (c InspectCmd) inspectColumnChunk(reader *reader.ParquetReader, rowGroupIndex, columnChunkIndex int, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode) error {
	footer := reader.Footer

	if rowGroupIndex < 0 || rowGroupIndex >= len(footer.RowGroups) {
		return fmt.Errorf("row group index %d out of range [0, %d)", rowGroupIndex, len(footer.RowGroups))
	}

	rg := footer.RowGroups[rowGroupIndex]

	if columnChunkIndex < 0 || columnChunkIndex >= len(rg.Columns) {
		return fmt.Errorf("column chunk index %d out of range [0, %d)", columnChunkIndex, len(rg.Columns))
	}

	col := rg.Columns[columnChunkIndex]
	pathKey := strings.Join(col.MetaData.PathInSchema, common.PAR_GO_PATH_DELIMITER)
	pathInSchema := c.resolvePathInSchema(col.MetaData.PathInSchema, inExNameMap)
	schemaNode := pathMap[pathKey]

	// Build column chunk details
	columnChunkDetails := map[string]any{
		"rowGroupIndex":    rowGroupIndex,
		"columnChunkIndex": columnChunkIndex,
		"pathInSchema":     pathInSchema,
		"type":             col.MetaData.Type.String(),
		"encodings":        encodingToString(col.MetaData.Encodings),
		"compressionCodec": col.MetaData.Codec.String(),
		"numValues":        col.MetaData.NumValues,
		"compressedSize":   col.MetaData.TotalCompressedSize,
		"uncompressedSize": col.MetaData.TotalUncompressedSize,
		"dataPageOffset":   col.MetaData.DataPageOffset,
	}

	if col.MetaData.DictionaryPageOffset != nil {
		columnChunkDetails["dictionaryPageOffset"] = *col.MetaData.DictionaryPageOffset
	}

	if col.MetaData.IndexPageOffset != nil {
		columnChunkDetails["indexPageOffset"] = *col.MetaData.IndexPageOffset
	}

	c.addTypeInformation(columnChunkDetails, schemaNode)

	// Add statistics
	if col.MetaData.Statistics != nil {
		stats := c.buildStatistics(col.MetaData.Statistics, schemaNode)
		if len(stats) > 0 {
			columnChunkDetails["statistics"] = stats
		}
	}

	// Read pages - this requires reading the actual data from the file
	pages, err := c.readPages(reader, rowGroupIndex, columnChunkIndex, schemaNode)
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
func (c InspectCmd) inspectPage(reader *reader.ParquetReader, rowGroupIndex, columnChunkIndex, pageIndex int, pathMap map[string]*pschema.SchemaNode) error {
	footer := reader.Footer

	if rowGroupIndex < 0 || rowGroupIndex >= len(footer.RowGroups) {
		return fmt.Errorf("row group index %d out of range [0, %d)", rowGroupIndex, len(footer.RowGroups))
	}
	rg := footer.RowGroups[rowGroupIndex]

	if columnChunkIndex < 0 || columnChunkIndex >= len(rg.Columns) {
		return fmt.Errorf("column chunk index %d out of range [0, %d)", columnChunkIndex, len(rg.Columns))
	}
	col := rg.Columns[columnChunkIndex]
	pathKey := strings.Join(col.MetaData.PathInSchema, common.PAR_GO_PATH_DELIMITER)
	schemaNode := pathMap[pathKey]

	// Read pages
	pages, err := c.readPages(reader, rowGroupIndex, columnChunkIndex, schemaNode)
	if err != nil {
		return fmt.Errorf("failed to read pages: %w", err)
	}

	if pageIndex < 0 || pageIndex >= len(pages) {
		return fmt.Errorf("page index %d out of range [0, %d)", pageIndex, len(pages))
	}
	page := pages[pageIndex]

	// Read page values
	values, err := c.readPageValues(reader, rowGroupIndex, columnChunkIndex, col, schemaNode, pages, pageIndex)
	if err != nil {
		return fmt.Errorf("failed to read page values: %w", err)
	}

	output := map[string]any{
		"page":   page,
		"values": values,
	}

	return c.printJSON(output)
}

func (c InspectCmd) getConvertedType(schemaNode *pschema.SchemaNode) string {
	tagMap := schemaNode.GetTagMap()
	orderedTags := pschema.OrderedTags()

	convertedTypeTags := map[string]struct{}{
		"convertedtype": {},
		"scale":         {},
		"precision":     {},
		"length":        {},
	}

	var convertedTypeParts []string
	for _, tag := range orderedTags {
		if _, ok := convertedTypeTags[tag]; !ok {
			continue
		}
		if value, found := tagMap[tag]; found {
			convertedTypeParts = append(convertedTypeParts, tag+"="+value)
		}
	}

	return strings.Join(convertedTypeParts, ", ")
}

func (c InspectCmd) getLogicalType(schemaNode *pschema.SchemaNode) string {
	tagMap := schemaNode.GetTagMap()
	orderedTags := pschema.OrderedTags()

	var logicalTypeParts []string
	for _, tag := range orderedTags {
		if !strings.HasPrefix(tag, "logicaltype") {
			continue
		}
		if value, found := tagMap[tag]; found {
			logicalTypeParts = append(logicalTypeParts, tag+"="+value)
		}
	}

	return strings.Join(logicalTypeParts, ", ")
}

func (c InspectCmd) getStatValue(value []byte, schemaNode *pschema.SchemaNode) any {
	if len(value) == 0 {
		return nil
	}

	// Check if this is a type where min/max don't apply
	isGeospatial := schemaNode.LogicalType != nil && (schemaNode.LogicalType.IsSetGEOMETRY() || schemaNode.LogicalType.IsSetGEOGRAPHY())
	isInterval := schemaNode.ConvertedType != nil && *schemaNode.ConvertedType == parquet.ConvertedType_INTERVAL
	if isGeospatial || isInterval {
		return nil
	}

	var val any
	// For BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY, statistics bytes don't include the length prefix
	if *schemaNode.Type == parquet.Type_BYTE_ARRAY || *schemaNode.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		val = string(value)
	} else {
		// For other types, use parquet-go's encoding functions to decode the raw bytes
		buf := bytes.NewReader(value)
		vals, err := encoding.ReadPlain(buf, *schemaNode.Type, 1, 0)
		if err != nil {
			return fmt.Sprintf("failed to read data as %s: %v", schemaNode.Type.String(), err)
		}
		if len(vals) == 0 {
			return nil
		}
		val = vals[0]
	}

	return types.ParquetTypeToJSONTypeWithLogical(
		val,
		schemaNode.Type, schemaNode.ConvertedType, schemaNode.LogicalType,
		int(schemaNode.GetPrecision()), int(schemaNode.GetScale()))
}

func (c InspectCmd) readPages(pr *reader.ParquetReader, rowGroupIndex, columnChunkIndex int, schemaNode *pschema.SchemaNode) ([]PageInfo, error) {
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
func (c InspectCmd) convertPageHeaderInfo(headerInfo reader.PageHeaderInfo, schemaNode *pschema.SchemaNode) PageInfo {
	pageInfo := PageInfo{
		Index:            headerInfo.Index,
		Offset:           headerInfo.Offset,
		Type:             headerInfo.PageType,
		CompressedSize:   headerInfo.CompressedSize,
		UncompressedSize: headerInfo.UncompressedSize,
	}

	if headerInfo.HasCRC {
		pageInfo.HasCrc = true
		pageInfo.Crc = headerInfo.CRC
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

func (c InspectCmd) readPageValues(pr *reader.ParquetReader, rowGroupIndex, columnChunkIndex int, col *parquet.ColumnChunk, schemaNode *pschema.SchemaNode, pages []PageInfo, pageIndex int) ([]any, error) {
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
	var valuesBeforeRG int64 = 0
	for i := range rowGroupIndex {
		valuesBeforeRG += pr.Footer.RowGroups[i].NumRows
	}

	// Create a fresh column reader to read the entire column
	freshReader, err := reader.NewParquetColumnReader(pr.PFile, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create fresh reader: %w", err)
	}
	defer func() { _ = freshReader.ReadStopWithError() }()

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
	var startIdx int64 = 0
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

func (c InspectCmd) readDictionaryPageValues(pr *reader.ParquetReader, col *parquet.ColumnChunk, schemaNode *pschema.SchemaNode, pageInfo PageInfo) ([]any, error) {
	meta := col.MetaData

	values, err := pr.ReadDictionaryPageValues(pageInfo.Offset, meta.Codec, meta.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to read dictionary page values: %w", err)
	}

	return c.convertValuesToJSON(values, schemaNode), nil
}

// Helper functions

// printJSON marshals data to JSON and prints it
func (c InspectCmd) printJSON(data any) error {
	buf, err := json.Marshal(data)
	if err != nil {
		return err
	}
	fmt.Println(string(buf))
	return nil
}

// convertValuesToJSON converts raw parquet values to JSON-friendly types
func (c InspectCmd) convertValuesToJSON(values []any, schemaNode *pschema.SchemaNode) []any {
	result := make([]any, len(values))
	precision, scale := int(schemaNode.GetPrecision()), int(schemaNode.GetScale())
	for i, val := range values {
		result[i] = types.ParquetTypeToJSONTypeWithLogical(
			val,
			schemaNode.Type, schemaNode.ConvertedType, schemaNode.LogicalType,
			precision, scale)
	}
	return result
}

// resolvePathInSchema resolves internal path to external path using the schema map
func (c InspectCmd) resolvePathInSchema(pathInSchema []string, inExNameMap map[string][]string) []string {
	pathKey := strings.Join(pathInSchema, common.PAR_GO_PATH_DELIMITER)
	if exPath, found := inExNameMap[pathKey]; found {
		return exPath
	}
	return pathInSchema
}

// addTypeInformation adds converted and logical type information to the output map
func (c InspectCmd) addTypeInformation(output map[string]any, schemaNode *pschema.SchemaNode) {
	if schemaNode == nil {
		return
	}
	if convertedType := c.getConvertedType(schemaNode); convertedType != "" {
		output["convertedType"] = convertedType
	}
	if logicalType := c.getLogicalType(schemaNode); logicalType != "" {
		output["logicalType"] = logicalType
	}
}

// buildStatistics creates a statistics map from parquet statistics
func (c InspectCmd) buildStatistics(statistics *parquet.Statistics, schemaNode *pschema.SchemaNode) map[string]any {
	stats := map[string]any{}

	if statistics.NullCount != nil {
		stats["nullCount"] = *statistics.NullCount
	}
	if statistics.DistinctCount != nil {
		stats["distinctCount"] = *statistics.DistinctCount
	}

	if schemaNode != nil {
		if minVal := c.getStatValue(statistics.MinValue, schemaNode); minVal != nil {
			stats["minValue"] = minVal
		}
		if maxVal := c.getStatValue(statistics.MaxValue, schemaNode); maxVal != nil {
			stats["maxValue"] = maxVal
		}
	}

	return stats
}
