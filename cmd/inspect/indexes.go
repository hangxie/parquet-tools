package inspect

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"

	pschema "github.com/hangxie/parquet-tools/schema"
)

func (c Cmd) addPageIndexes(ctx context.Context, pr *reader.ParquetReader, rowGroupIndex, columnIndex int, output map[string]any, schemaNode *pschema.SchemaNode) error {
	column, err := pr.ReadColumnIndexWithContext(ctx, rowGroupIndex, columnIndex)
	if err != nil {
		return fmt.Errorf("read column index: %w", err)
	}
	if column != nil {
		output["columnIndex"] = c.columnIndexMetadata(column, schemaNode)
	}

	offset, err := pr.ReadOffsetIndexWithContext(ctx, rowGroupIndex, columnIndex)
	if err != nil {
		return fmt.Errorf("read offset index: %w", err)
	}
	if offset != nil {
		output["offsetIndex"] = offsetIndexMetadata(offset)
	}
	return nil
}

func (c Cmd) columnIndexMetadata(index *parquet.ColumnIndex, schemaNode *pschema.SchemaNode) map[string]any {
	output := map[string]any{
		"boundaryOrder": index.BoundaryOrder.String(),
		"nullPages":     index.NullPages,
		"minValues":     c.decodeIndexBounds(index.MinValues, index.NullPages, true, schemaNode),
		"maxValues":     c.decodeIndexBounds(index.MaxValues, index.NullPages, false, schemaNode),
	}
	if index.NullCounts != nil {
		output["nullCounts"] = index.NullCounts
	}
	if index.RepetitionLevelHistograms != nil {
		output["repetitionLevelHistograms"] = index.RepetitionLevelHistograms
	}
	if index.DefinitionLevelHistograms != nil {
		output["definitionLevelHistograms"] = index.DefinitionLevelHistograms
	}
	return output
}

func (c Cmd) decodeIndexBounds(bounds [][]byte, nullPages []bool, minimum bool, schemaNode *pschema.SchemaNode) []any {
	result := make([]any, len(bounds))
	if schemaNode == nil {
		return result
	}
	for i, bound := range bounds {
		if i < len(nullPages) && nullPages[i] {
			continue
		}
		statistics := &parquet.Statistics{}
		if minimum {
			statistics.MinValue = bound
			result[i], _ = schemaNode.DecodeStatistics(statistics)
		} else {
			statistics.MaxValue = bound
			_, result[i] = schemaNode.DecodeStatistics(statistics)
		}
		result[i] = normalizeNegativeZero(result[i])
	}
	return result
}

func offsetIndexMetadata(index *parquet.OffsetIndex) map[string]any {
	locations := make([]map[string]any, 0, len(index.PageLocations))
	for _, location := range index.PageLocations {
		if location != nil {
			locations = append(locations, map[string]any{
				"offset":             location.Offset,
				"compressedPageSize": location.CompressedPageSize,
				"firstRowIndex":      location.FirstRowIndex,
			})
		}
	}
	output := map[string]any{"pageLocations": locations}
	if index.UnencodedByteArrayDataBytes != nil {
		output["unencodedByteArrayDataBytes"] = index.UnencodedByteArrayDataBytes
	}
	return output
}
