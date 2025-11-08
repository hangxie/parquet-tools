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
		return c.inspectPage(reader, *c.RowGroup, *c.ColumnChunk, *c.Page, inExNameMap, pathMap)
	case c.ColumnChunk != nil:
		// Level 3: Show column chunk details and pages
		return c.inspectColumnChunk(reader, *c.RowGroup, *c.ColumnChunk, inExNameMap, pathMap)
	case c.RowGroup != nil:
		// Level 2: Show row group details and column chunks
		return c.inspectRowGroup(reader, *c.RowGroup, inExNameMap, pathMap)
	default:
		// Level 1: Show file info and row groups
		return c.inspectFile(reader, inExNameMap, pathMap)
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
func (c InspectCmd) inspectFile(reader *reader.ParquetReader, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode) error {
	footer := reader.Footer

	// Calculate totals
	totalRows := int64(0)
	compressedSize := int64(0)
	uncompressedSize := int64(0)

	for _, rg := range footer.RowGroups {
		totalRows += rg.NumRows
		for _, col := range rg.Columns {
			compressedSize += col.MetaData.TotalCompressedSize
			uncompressedSize += col.MetaData.TotalUncompressedSize
		}
	}

	// Build row group brief info
	rowGroupsBrief := make([]map[string]any, len(footer.RowGroups))
	for i, rg := range footer.RowGroups {
		rgCompressed := int64(0)
		rgUncompressed := int64(0)
		for _, col := range rg.Columns {
			rgCompressed += col.MetaData.TotalCompressedSize
			rgUncompressed += col.MetaData.TotalUncompressedSize
		}

		rowGroupsBrief[i] = map[string]any{
			"index":             i,
			"num_rows":          rg.NumRows,
			"total_byte_size":   rg.TotalByteSize,
			"num_columns":       len(rg.Columns),
			"compressed_size":   rgCompressed,
			"uncompressed_size": rgUncompressed,
		}
	}

	output := map[string]any{
		"file_info": map[string]any{
			"version":           footer.Version,
			"num_row_groups":    len(footer.RowGroups),
			"total_rows":        totalRows,
			"compressed_size":   compressedSize,
			"uncompressed_size": uncompressedSize,
			"created_by":        footer.CreatedBy,
		},
		"row_groups": rowGroupsBrief,
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
		"row_group": map[string]any{
			"index":           rowGroupIndex,
			"num_rows":        rg.NumRows,
			"total_byte_size": rg.TotalByteSize,
			"num_columns":     len(rg.Columns),
		},
		"column_chunks": columnChunksBrief,
	}

	return c.printJSON(output)
}

func (c InspectCmd) buildColumnChunkBrief(index int, col *parquet.ColumnChunk, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode) map[string]any {
	pathKey := strings.Join(col.MetaData.PathInSchema, common.PAR_GO_PATH_DELIMITER)
	pathInSchema := c.resolvePathInSchema(col.MetaData.PathInSchema, inExNameMap)
	schemaNode := pathMap[pathKey]

	columnChunk := map[string]any{
		"index":             index,
		"path_in_schema":    pathInSchema,
		"type":              col.MetaData.Type.String(),
		"encodings":         encodingToString(col.MetaData.Encodings),
		"compression_codec": col.MetaData.Codec.String(),
		"num_values":        col.MetaData.NumValues,
		"compressed_size":   col.MetaData.TotalCompressedSize,
		"uncompressed_size": col.MetaData.TotalUncompressedSize,
	}

	c.addTypeInformation(columnChunk, schemaNode)

	// Add statistics if available
	if col.MetaData.Statistics != nil {
		c.addColumnStatistics(columnChunk, col.MetaData.Statistics, schemaNode)
	}

	return columnChunk
}

func (c InspectCmd) addColumnStatistics(columnChunk map[string]any, statistics *parquet.Statistics, schemaNode *pschema.SchemaNode) {
	stats := c.buildStatistics(statistics, schemaNode)
	if len(stats) > 0 {
		columnChunk["statistics"] = stats
	}
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
		"row_group_index":    rowGroupIndex,
		"column_chunk_index": columnChunkIndex,
		"path_in_schema":     pathInSchema,
		"type":               col.MetaData.Type.String(),
		"encodings":          encodingToString(col.MetaData.Encodings),
		"compression_codec":  col.MetaData.Codec.String(),
		"num_values":         col.MetaData.NumValues,
		"compressed_size":    col.MetaData.TotalCompressedSize,
		"uncompressed_size":  col.MetaData.TotalUncompressedSize,
		"data_page_offset":   col.MetaData.DataPageOffset,
	}

	if col.MetaData.DictionaryPageOffset != nil {
		columnChunkDetails["dictionary_page_offset"] = *col.MetaData.DictionaryPageOffset
	}

	if col.MetaData.IndexPageOffset != nil {
		columnChunkDetails["index_page_offset"] = *col.MetaData.IndexPageOffset
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
	pages, err := c.readPages(reader, col, schemaNode)
	if err != nil {
		return fmt.Errorf("failed to read pages: %w", err)
	}

	output := map[string]any{
		"column_chunk": columnChunkDetails,
		"pages":        pages,
	}

	return c.printJSON(output)
}

// Level 4: Page details and values
func (c InspectCmd) inspectPage(reader *reader.ParquetReader, rowGroupIndex, columnChunkIndex, pageIndex int, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode) error {
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
	pages, err := c.readPages(reader, col, schemaNode)
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

	var convertedTypeParts []string
	for _, tag := range orderedTags {
		if tag == "convertedtype" || tag == "scale" || tag == "precision" || tag == "length" {
			if value, found := tagMap[tag]; found {
				convertedTypeParts = append(convertedTypeParts, tag+"="+value)
			}
		}
	}

	if len(convertedTypeParts) > 0 {
		return strings.Join(convertedTypeParts, ", ")
	}
	return ""
}

func (c InspectCmd) getLogicalType(schemaNode *pschema.SchemaNode) string {
	tagMap := schemaNode.GetTagMap()
	orderedTags := pschema.OrderedTags()

	var logicalTypeParts []string
	for _, tag := range orderedTags {
		if strings.HasPrefix(tag, "logicaltype") {
			if value, found := tagMap[tag]; found {
				logicalTypeParts = append(logicalTypeParts, tag+"="+value)
			}
		}
	}

	if len(logicalTypeParts) > 0 {
		return strings.Join(logicalTypeParts, ", ")
	}
	return ""
}

func (c InspectCmd) getStatValue(value []byte, schemaNode *pschema.SchemaNode) any {
	if value == nil {
		return nil
	}

	// Check if this is a type where min/max don't apply
	isGeospatial := schemaNode.LogicalType != nil && (schemaNode.LogicalType.IsSetGEOMETRY() || schemaNode.LogicalType.IsSetGEOGRAPHY())
	isInterval := schemaNode.ConvertedType != nil && *schemaNode.ConvertedType == parquet.ConvertedType_INTERVAL
	if isGeospatial || isInterval {
		return nil
	}

	// For BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY, check if they need special handling
	if *schemaNode.Type == parquet.Type_BYTE_ARRAY || *schemaNode.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		// Check for special logical types that need decoding
		needsDecoding := false

		// UUID, BSON, JSON, and DECIMAL types need special handling
		if schemaNode.LogicalType != nil {
			if schemaNode.LogicalType.IsSetUUID() ||
				schemaNode.LogicalType.IsSetJSON() ||
				schemaNode.LogicalType.IsSetBSON() ||
				schemaNode.LogicalType.IsSetDECIMAL() {
				needsDecoding = true
			}
		}

		// Also check converted type for DECIMAL
		if schemaNode.ConvertedType != nil {
			if *schemaNode.ConvertedType == parquet.ConvertedType_DECIMAL ||
				*schemaNode.ConvertedType == parquet.ConvertedType_JSON ||
				*schemaNode.ConvertedType == parquet.ConvertedType_BSON {
				needsDecoding = true
			}
		}

		// If it's a plain string (UTF8), return as string
		if !needsDecoding {
			return string(value)
		}

		// For types that need decoding, pass raw bytes to ParquetTypeToJSONTypeWithLogical
		// Statistics bytes don't include the length prefix, so we pass them as-is
		precision, scale := int(schemaNode.GetPrecision()), int(schemaNode.GetScale())
		return types.ParquetTypeToJSONTypeWithLogical(
			string(value),
			schemaNode.Type, schemaNode.ConvertedType, schemaNode.LogicalType,
			precision, scale)
	}

	// For numeric types, use parquet-go's encoding functions
	buf := bytes.NewReader(value)
	vals, err := encoding.ReadPlain(buf, *schemaNode.Type, 1, 0)
	if err != nil {
		return fmt.Sprintf("failed to read data as %s: %v", schemaNode.Type.String(), err)
	}
	if len(vals) == 0 {
		return nil
	}

	precision, scale := int(schemaNode.GetPrecision()), int(schemaNode.GetScale())
	return types.ParquetTypeToJSONTypeWithLogical(
		vals[0],
		schemaNode.Type, schemaNode.ConvertedType, schemaNode.LogicalType,
		precision, scale)
}

func (c InspectCmd) readPages(pr *reader.ParquetReader, col *parquet.ColumnChunk, schemaNode *pschema.SchemaNode) ([]map[string]any, error) {
	// Use the new ReadAllPageHeaders function from v2.4.0
	pageHeaders, err := reader.ReadAllPageHeaders(pr.PFile, col)
	if err != nil {
		return nil, fmt.Errorf("failed to read page headers: %w", err)
	}

	// Convert PageHeaderInfo to our output format
	pages := make([]map[string]any, len(pageHeaders))
	for i, headerInfo := range pageHeaders {
		pages[i] = c.convertPageHeaderInfo(headerInfo, schemaNode)
	}

	return pages, nil
}

// convertPageHeaderInfo converts PageHeaderInfo from v2.4.0 to our JSON output format
func (c InspectCmd) convertPageHeaderInfo(headerInfo reader.PageHeaderInfo, schemaNode *pschema.SchemaNode) map[string]any {
	pageInfo := map[string]any{
		"index":             headerInfo.Index,
		"offset":            headerInfo.Offset,
		"type":              headerInfo.PageType.String(),
		"compressed_size":   headerInfo.CompressedSize,
		"uncompressed_size": headerInfo.UncompressedSize,
	}

	if headerInfo.HasCRC {
		pageInfo["has_crc"] = true
		pageInfo["crc"] = headerInfo.CRC
	}

	switch headerInfo.PageType {
	case parquet.PageType_DATA_PAGE:
		pageInfo["num_values"] = headerInfo.NumValues
		pageInfo["encoding"] = headerInfo.Encoding.String()
		pageInfo["definition_level_encoding"] = headerInfo.DefLevelEncoding.String()
		pageInfo["repetition_level_encoding"] = headerInfo.RepLevelEncoding.String()

		if headerInfo.HasStatistics {
			c.addPageStatistics(pageInfo, headerInfo.Statistics, schemaNode)
		}

	case parquet.PageType_DATA_PAGE_V2:
		pageInfo["num_values"] = headerInfo.NumValues
		pageInfo["num_nulls"] = headerInfo.NumNulls
		pageInfo["num_rows"] = headerInfo.NumRows
		pageInfo["encoding"] = headerInfo.Encoding.String()
		pageInfo["definition_levels_byte_length"] = headerInfo.DefLevelBytes
		pageInfo["repetition_levels_byte_length"] = headerInfo.RepLevelBytes
		if headerInfo.IsCompressed != nil {
			pageInfo["is_compressed"] = *headerInfo.IsCompressed
		}

		if headerInfo.HasStatistics {
			c.addPageStatistics(pageInfo, headerInfo.Statistics, schemaNode)
		}

	case parquet.PageType_DICTIONARY_PAGE:
		pageInfo["num_values"] = headerInfo.NumValues
		pageInfo["encoding"] = headerInfo.Encoding.String()
		if headerInfo.IsSorted != nil {
			pageInfo["is_sorted"] = *headerInfo.IsSorted
		}

	case parquet.PageType_INDEX_PAGE:
		pageInfo["note"] = "Index page (column index)"
	}

	return pageInfo
}

func (c InspectCmd) addPageStatistics(pageInfo map[string]any, statistics *parquet.Statistics, schemaNode *pschema.SchemaNode) {
	stats := c.buildStatistics(statistics, schemaNode)
	if len(stats) > 0 {
		pageInfo["statistics"] = stats
	}
}

func (c InspectCmd) readPageValues(pr *reader.ParquetReader, rowGroupIndex, columnChunkIndex int, col *parquet.ColumnChunk, schemaNode *pschema.SchemaNode, pages []map[string]any, pageIndex int) ([]any, error) {
	meta := col.MetaData

	if pageIndex < 0 || pageIndex >= len(pages) {
		return nil, fmt.Errorf("page index %d out of range [0, %d)", pageIndex, len(pages))
	}

	pageInfo := pages[pageIndex]

	// Check page type
	pageType, ok := pageInfo["type"].(string)
	if !ok {
		return nil, fmt.Errorf("unable to determine page type")
	}

	// Handle dictionary pages separately
	if pageType == "DICTIONARY_PAGE" {
		return c.readDictionaryPageValues(pr, col, schemaNode, pageInfo)
	}

	// For data pages, we need to read all values from the column chunk
	// and then extract the values for this specific page
	if pageType != "DATA_PAGE" && pageType != "DATA_PAGE_V2" {
		// For other page types (INDEX_PAGE, etc.), return empty
		return []any{}, nil
	}

	// Calculate total values before this row group and in this row group
	var valuesBeforeRG int64 = 0
	for i := 0; i < rowGroupIndex; i++ {
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
	rgEndIdx := valuesBeforeRG + meta.NumValues
	if rgEndIdx > int64(len(allValuesInFile)) {
		rgEndIdx = int64(len(allValuesInFile))
	}

	allValues := allValuesInFile[rgStartIdx:rgEndIdx]

	// Calculate the start index for this page
	var startIdx int64 = 0
	for i := 0; i < pageIndex; i++ {
		if numVals, ok := pages[i]["num_values"].(int32); ok {
			pType, _ := pages[i]["type"].(string)
			if pType == "DATA_PAGE" || pType == "DATA_PAGE_V2" {
				startIdx += int64(numVals)
			}
		}
	}

	// Extract values for just this page
	numVals, ok := pageInfo["num_values"].(int32)
	if !ok {
		return nil, fmt.Errorf("unable to get num_values for page")
	}

	endIdx := startIdx + int64(numVals)
	if endIdx > int64(len(allValues)) {
		endIdx = int64(len(allValues))
	}

	// Convert values to appropriate JSON types
	pageValues := make([]interface{}, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		pageValues[i-startIdx] = allValues[i]
	}

	return c.convertValuesToJSON(pageValues, schemaNode), nil
}

func (c InspectCmd) readDictionaryPageValues(pr *reader.ParquetReader, col *parquet.ColumnChunk, schemaNode *pschema.SchemaNode, pageInfo map[string]any) ([]any, error) {
	meta := col.MetaData

	// Get page offset
	offset, ok := pageInfo["offset"].(int64)
	if !ok {
		return nil, fmt.Errorf("unable to get page offset")
	}

	// Use the new ReadDictionaryPageValues function from v2.4.0
	values, err := pr.ReadDictionaryPageValues(offset, meta.Codec, meta.Type)
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
func (c InspectCmd) convertValuesToJSON(values []interface{}, schemaNode *pschema.SchemaNode) []any {
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
		output["converted_type"] = convertedType
	}
	if logicalType := c.getLogicalType(schemaNode); logicalType != "" {
		output["logical_type"] = logicalType
	}
}

// buildStatistics creates a statistics map from parquet statistics
func (c InspectCmd) buildStatistics(statistics *parquet.Statistics, schemaNode *pschema.SchemaNode) map[string]any {
	stats := map[string]any{}

	if statistics.NullCount != nil {
		stats["null_count"] = *statistics.NullCount
	}
	if statistics.DistinctCount != nil {
		stats["distinct_count"] = *statistics.DistinctCount
	}

	if schemaNode != nil {
		if minVal := c.getStatValue(statistics.MinValue, schemaNode); minVal != nil {
			stats["min_value"] = minVal
		}
		if maxVal := c.getStatValue(statistics.MaxValue, schemaNode); maxVal != nil {
			stats["max_value"] = maxVal
		}
	}

	return stats
}
