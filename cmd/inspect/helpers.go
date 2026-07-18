package inspect

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/types"

	pschema "github.com/hangxie/parquet-tools/schema"
)

func (c Cmd) printJSON(data any) error {
	buf, err := json.Marshal(data)
	if err != nil {
		return err
	}
	fmt.Println(string(buf))
	return nil
}

// convertValuesToJSON converts raw parquet values to JSON-friendly types
func (c Cmd) convertValuesToJSON(values []any, schemaNode *pschema.SchemaNode) []any {
	result := make([]any, len(values))
	for i, val := range values {
		result[i] = types.ConvertToJSONType(val, &schemaNode.SchemaElement)
	}
	return result
}

// resolvePathInSchema resolves internal path to external path using the schema map
func (c Cmd) resolvePathInSchema(pathInSchema []string, inExNameMap map[string][]string) []string {
	pathKey := strings.Join(pathInSchema, common.ParGoPathDelimiter)
	if exPath, found := inExNameMap[pathKey]; found {
		return exPath
	}
	return pathInSchema
}

// addEncryptionInfo adds encryptionMode and keyMetadata to the output map when present
func (c Cmd) addEncryptionInfo(output map[string]any, col *parquet.ColumnChunk) {
	cm := col.GetCryptoMetadata()
	if cm == nil {
		return
	}
	switch {
	case cm.ENCRYPTION_WITH_FOOTER_KEY != nil:
		output["encryptionMode"] = "FOOTER_KEY"
	case cm.ENCRYPTION_WITH_COLUMN_KEY != nil:
		output["encryptionMode"] = "COLUMN_KEY"
		if km := cm.ENCRYPTION_WITH_COLUMN_KEY.GetKeyMetadata(); len(km) > 0 {
			output["keyMetadata"] = base64.StdEncoding.EncodeToString(km)
		}
	}
}

// addTypeInformation adds converted and logical type information to the output map
func (c Cmd) addTypeInformation(output map[string]any, schemaNode *pschema.SchemaNode) {
	if schemaNode == nil {
		return
	}
	if ct := schemaNode.ConvertedTypeString(); ct != "" {
		output["convertedType"] = ct
	}
	if lt := schemaNode.LogicalTypeString(); lt != "" {
		output["logicalType"] = lt
	}
}

// buildStatistics creates a statistics map from parquet statistics
func (c Cmd) buildStatistics(statistics *parquet.Statistics, schemaNode *pschema.SchemaNode) map[string]any {
	stats := map[string]any{}

	if statistics.NullCount != nil {
		stats["nullCount"] = *statistics.NullCount
	}
	if statistics.DistinctCount != nil {
		stats["distinctCount"] = *statistics.DistinctCount
	}

	if schemaNode != nil {
		min, max := schemaNode.DecodeStatistics(statistics)
		if min != nil {
			stats["minValue"] = normalizeNegativeZero(min)
		}
		if max != nil {
			stats["maxValue"] = normalizeNegativeZero(max)
		}
	}

	return stats
}

// normalizeNegativeZero converts IEEE 754 negative zero to positive zero for consistent JSON output.
func normalizeNegativeZero(v any) any {
	switch f := v.(type) {
	case float32:
		if math.Signbit(float64(f)) && f == 0 {
			return float32(0)
		}
	case float64:
		if math.Signbit(f) && f == 0 {
			return float64(0)
		}
	}
	return v
}
