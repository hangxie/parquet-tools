package schema

import (
	"github.com/hangxie/parquet-go/v3/parquet"
	pgschema "github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

// DecodeStatistics decodes a parquet.Statistics object into human-readable min/max values.
// Returns (nil, nil) for nil/empty statistics, nodes with undefined sort order, or on error.
func (node *SchemaNode) DecodeStatistics(stats *parquet.Statistics) (min, max any) {
	if node.UndefinedSortOrder {
		return nil, nil
	}
	rawMin, rawMax, err := pgschema.DecodeStatisticsMinMax(&node.SchemaElement, stats)
	if err != nil {
		return nil, nil
	}
	return types.ConvertToJSONType(rawMin, &node.SchemaElement),
		types.ConvertToJSONType(rawMax, &node.SchemaElement)
}
