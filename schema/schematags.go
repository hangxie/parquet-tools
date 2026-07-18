package schema

import (
	"fmt"
	"maps"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func (s *SchemaNode) GetTagMap() map[string]string {
	tagMap := map[string]string{
		"repetitiontype": repetitionTypeStr(s.SchemaElement),
		"type":           typeStr(s.SchemaElement),
		"name":           s.Name,
	}

	if len(s.ExNamePath) != 0 {
		tagMap["name"] = s.ExNamePath[len(s.ExNamePath)-1]
	}

	if len(s.InNamePath) != 0 {
		tagMap["inname"] = s.InNamePath[len(s.InNamePath)-1]
	}

	if tagMap["type"] == "STRUCT" && s.LogicalType == nil && s.ConvertedType == nil {
		return tagMap
	}

	if s.Type != nil && *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		tagMap["length"] = fmt.Sprint(*s.TypeLength)
	}

	if s.LogicalType != nil {
		s.updateTagFromLogicalType(tagMap)
	}

	// Add custom parquet-go writer directives before updateTagFromConvertedType
	// so that LIST/MAP elements can include valueencoding/keyencoding tags
	if s.Encoding != "" {
		tagMap["encoding"] = s.Encoding
	}
	if s.CompressionCodec != "" {
		tagMap["compression"] = s.CompressionCodec
	}
	if s.OmitStats != "" {
		tagMap["omitstats"] = s.OmitStats
	}
	if s.BloomFilter != "" {
		tagMap["bloomfilter"] = s.BloomFilter
	}
	if s.BloomFilterSize != "" {
		tagMap["bloomfiltersize"] = s.BloomFilterSize
	}

	if s.ConvertedType != nil {
		s.updateTagFromConvertedType(tagMap)
	}

	return tagMap
}

func (s *SchemaNode) getTagMapWithPrefix(prefix string) map[string]string {
	tagMap := s.GetTagMap()
	ret := map[string]string{}
	for _, tag := range orderedTags {
		if tag == "name" || strings.HasPrefix(tag, "key") || strings.HasPrefix(tag, "value") {
			// these are tags that should never have prefix
			continue
		}
		if val, found := tagMap[tag]; found {
			ret[prefix+tag] = val
		}
	}

	return ret
}

func (s *SchemaNode) updateTagForList(tagMap map[string]string) {
	if len(s.Children) == 0 {
		return
	}

	if s.Children[0].LogicalType != nil {
		// LIST => Element (of scalar type)
		s.Children[0].Name = "Element"
		*s.Children[0].RepetitionType = parquet.FieldRepetitionType_REQUIRED
		maps.Copy(tagMap, s.Children[0].getTagMapWithPrefix("value"))
		return
	}

	if len(s.Children[0].Children) > 1 {
		// LIST => Element (of STRUCT)
		s.Children[0].Name = "Element"
		s.Children[0].Type = nil
		s.Children[0].ConvertedType = nil
		*s.Children[0].RepetitionType = parquet.FieldRepetitionType_REQUIRED
		return
	}

	if len(s.Children[0].Children) == 1 {
		// LIST => List => Element
		maps.Copy(tagMap, s.Children[0].Children[0].getTagMapWithPrefix("value"))
		s.Children = s.Children[0].Children
		s.Children[0].Name = "Element"
		return
	}
}

func (s *SchemaNode) updateTagForMap(tagMap map[string]string) {
	if len(s.Children) == 0 || s.Children[0] == nil || len(s.Children[0].Children) == 0 {
		// meaningless interim layer
		return
	}
	if s.Children[0].ConvertedType != nil && *s.Children[0].ConvertedType != parquet.ConvertedType_MAP_KEY_VALUE {
		// child nodes have been processed
		return
	}

	// MAP has schema structure of MAP->MAP_KEY_VALUE->(Field1, Field2)
	// expected output is MAP->(Key, Value)
	maps.Copy(tagMap, s.Children[0].Children[0].getTagMapWithPrefix("key"))
	maps.Copy(tagMap, s.Children[0].Children[1].getTagMapWithPrefix("value"))
	s.Children = s.Children[0].Children[0:2]
	s.Children[0].Name = "Key"
	s.Children[1].Name = "Value"
}

func (s *SchemaNode) updateTagFromConvertedType(tagMap map[string]string) {
	if s.ConvertedType == nil {
		return
	}

	tagMap["convertedtype"] = s.ConvertedType.String()

	switch *s.ConvertedType {
	case parquet.ConvertedType_LIST:
		s.updateTagForList(tagMap)
	case parquet.ConvertedType_MAP:
		s.updateTagForMap(tagMap)
	case parquet.ConvertedType_DECIMAL:
		tagMap["scale"] = fmt.Sprint(*s.Scale)
		tagMap["precision"] = fmt.Sprint(*s.Precision)
		if *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			tagMap["length"] = fmt.Sprint(*s.TypeLength)
		}
	case parquet.ConvertedType_INTERVAL:
		tagMap["length"] = "12"
	}
}

func (s *SchemaNode) updateTagFromLogicalType(tagMap map[string]string) {
	if s.LogicalType == nil {
		return
	}

	switch {
	case s.LogicalType.IsSetBSON():
		tagMap["logicaltype"] = "BSON"
	case s.LogicalType.IsSetDATE():
		tagMap["logicaltype"] = "DATE"
	case s.LogicalType.IsSetDECIMAL():
		tagMap["logicaltype"] = "DECIMAL"
		tagMap["logicaltype.precision"] = fmt.Sprint(s.LogicalType.DECIMAL.Precision)
		tagMap["logicaltype.scale"] = fmt.Sprint(s.LogicalType.DECIMAL.Scale)
	case s.LogicalType.IsSetENUM():
		tagMap["logicaltype"] = "ENUM"
	case s.LogicalType.IsSetFLOAT16():
		tagMap["logicaltype"] = "FLOAT16"
	case s.LogicalType.IsSetGEOGRAPHY():
		tagMap["logicaltype"] = "GEOGRAPHY"
	case s.LogicalType.IsSetGEOMETRY():
		tagMap["logicaltype"] = "GEOMETRY"
	case s.LogicalType.IsSetINTEGER():
		tagMap["logicaltype"] = "INTEGER"
		tagMap["logicaltype.bitwidth"] = fmt.Sprint(s.LogicalType.INTEGER.BitWidth)
		tagMap["logicaltype.issigned"] = fmt.Sprint(s.LogicalType.INTEGER.IsSigned)
	case s.LogicalType.IsSetJSON():
		tagMap["logicaltype"] = "JSON"
	case s.LogicalType.IsSetVARIANT():
		// VARIANT is a semi-structured logical type introduced in newer parquet-format
		tagMap["logicaltype"] = "VARIANT"
		for _, child := range s.Children {
			if child.Encoding != "" {
				tagMap["encoding"] = child.Encoding
			}
			if child.CompressionCodec != "" {
				tagMap["compression"] = child.CompressionCodec
			}
			if tagMap["encoding"] != "" && tagMap["compression"] != "" {
				break
			}
		}
	case s.LogicalType.IsSetSTRING():
		tagMap["logicaltype"] = "STRING"
	case s.LogicalType.IsSetTIME():
		tagMap["logicaltype"] = "TIME"
		tagMap["logicaltype.isadjustedtoutc"] = fmt.Sprint(s.LogicalType.TIME.IsAdjustedToUTC)
		tagMap["logicaltype.unit"] = timeUnitToTag(s.LogicalType.TIME.Unit)
	case s.LogicalType.IsSetTIMESTAMP():
		tagMap["logicaltype"] = "TIMESTAMP"
		tagMap["logicaltype.isadjustedtoutc"] = fmt.Sprint(s.LogicalType.TIMESTAMP.IsAdjustedToUTC)
		tagMap["logicaltype.unit"] = timeUnitToTag(s.LogicalType.TIMESTAMP.Unit)
	case s.LogicalType.IsSetUUID():
		tagMap["logicaltype"] = "UUID"
	case s.LogicalType.IsSetUNKNOWN():
		tagMap["logicaltype"] = "UNKNOWN"
	}
}

func (s *SchemaNode) GetPathMap() map[string]*SchemaNode {
	retVal := map[string]*SchemaNode{}
	queue := []*SchemaNode{s}
	for len(queue) > 0 {
		node := queue[0]
		queue = append(queue[1:], node.Children...)
		retVal[strings.Join(node.InNamePath[1:], common.ParGoPathDelimiter)] = node
	}
	return retVal
}

// GetInExNameMap returns a map from internal name path to external name path
// for all nodes in the schema tree via BFS traversal.
func (s *SchemaNode) GetInExNameMap() map[string][]string {
	retVal := map[string][]string{}
	queue := []*SchemaNode{s}
	for len(queue) > 0 {
		node := queue[0]
		queue = append(queue[1:], node.Children...)
		inPath := strings.Join(node.InNamePath[1:], common.ParGoPathDelimiter)
		retVal[inPath] = node.ExNamePath[1:]
	}
	return retVal
}

func typeStr(se parquet.SchemaElement) string {
	if se.Type != nil {
		return se.Type.String()
	}
	if se.LogicalType != nil && se.LogicalType.IsSetVARIANT() {
		return "VARIANT"
	}
	if se.ConvertedType != nil {
		return se.ConvertedType.String()
	}
	return "STRUCT"
}

func repetitionTypeStr(se parquet.SchemaElement) string {
	if se.RepetitionType == nil {
		return "REQUIRED"
	}
	return se.RepetitionType.String()
}

func timeUnitToTag(timeUnit *parquet.TimeUnit) string {
	if timeUnit == nil {
		return ""
	}
	if timeUnit.IsSetNANOS() {
		return "NANOS"
	}
	if timeUnit.IsSetMICROS() {
		return "MICROS"
	}
	if timeUnit.IsSetMILLIS() {
		return "MILLIS"
	}
	return "UNKNOWN_UNIT"
}

// ConvertedTypeString returns the formatted converted type string from schema tags.
// Returns "" if node is nil or has no converted type information.
func (node *SchemaNode) ConvertedTypeString() string {
	if node == nil {
		return ""
	}

	tagMap := node.GetTagMap()

	var parts []string
	for _, tag := range orderedTags {
		if tag != "convertedtype" && tag != "scale" && tag != "precision" && tag != "length" {
			continue
		}
		if value, found := tagMap[tag]; found {
			parts = append(parts, tag+"="+value)
		}
	}

	return strings.Join(parts, ", ")
}

// LogicalTypeString returns the formatted logical type string from schema tags.
// Returns "" if node is nil or has no logical type information.
func (node *SchemaNode) LogicalTypeString() string {
	if node == nil {
		return ""
	}

	tagMap := node.GetTagMap()

	var parts []string
	for _, tag := range orderedTags {
		if !strings.HasPrefix(tag, "logicaltype") {
			continue
		}
		if value, found := tagMap[tag]; found {
			parts = append(parts, tag+"="+value)
		}
	}

	return strings.Join(parts, ", ")
}
