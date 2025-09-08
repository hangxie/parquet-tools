package schema

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
)

// this represents order of tags in JSON schema and go struct
var orderedTags = []string{
	"name",
	"type",
	"keytype",
	"keyconvertedtype",
	"keyscale",
	"keyprecision",
	"valuetype",
	"valueconvertedtype",
	"valuescale",
	"valueprecision",
	"convertedtype",
	"scale",
	"precision",
	"length",
	"logicaltype",
	"logicaltype.precision",
	"logicaltype.scale",
	"logicaltype.isadjustedtoutc",
	"logicaltype.unit",
	"repetitiontype",
}

// OrderedTags returns a copy of the ordered tags list for external use
func OrderedTags() []string {
	result := make([]string, len(orderedTags))
	copy(result, orderedTags)
	return result
}

type SchemaNode struct {
	parquet.SchemaElement
	Children   []*SchemaNode `json:"children,omitempty"`
	InNamePath []string      `json:"-"`
	ExNamePath []string      `json:"-"`
}

type SchemaOption struct {
	FailOnInt96 bool
}

func NewSchemaTree(reader *reader.ParquetReader, option SchemaOption) (*SchemaNode, error) {
	schemas := reader.SchemaHandler.SchemaElements
	root := &SchemaNode{
		SchemaElement: *schemas[0],
		Children:      []*SchemaNode{},
		InNamePath:    []string{schemas[0].Name},
		ExNamePath:    strings.Split(reader.SchemaHandler.InPathToExPath[schemas[0].Name], common.PAR_GO_PATH_DELIMITER)[:1],
	}
	stack := []*SchemaNode{root}

	for pos := 1; len(stack) > 0; {
		node := stack[len(stack)-1]
		if option.FailOnInt96 && node.Type != nil && *node.Type == parquet.Type_INT96 {
			return nil, fmt.Errorf("field %s has type INT96 which is not supported", node.Name)
		}
		if len(node.Children) < int(node.GetNumChildren()) {
			childNode := &SchemaNode{
				SchemaElement: *schemas[pos],
				Children:      []*SchemaNode{},
			}

			// append() does not always return new slice, so we need to copy the old slice
			childNode.InNamePath = make([]string, len(node.InNamePath)+1)
			copy(childNode.InNamePath, node.InNamePath)
			childNode.InNamePath[len(node.InNamePath)] = schemas[pos].Name

			inPathKey := strings.Join(childNode.InNamePath, common.PAR_GO_PATH_DELIMITER)
			childNode.ExNamePath = strings.Split(reader.SchemaHandler.InPathToExPath[inPathKey], common.PAR_GO_PATH_DELIMITER)

			node.Children = append(node.Children, childNode)
			stack = append(stack, childNode)
			pos++
		} else {
			stack = stack[:len(stack)-1]
			if len(node.Children) == 0 {
				node.Children = nil
			}
		}
	}

	queue := []*SchemaNode{root}
	for len(queue) > 0 {
		node := queue[0]
		queue = append(queue[1:], node.Children...)
		node.Name = node.ExNamePath[len(node.ExNamePath)-1]
	}
	return root, nil
}

func (s *SchemaNode) GetTagMap() map[string]string {
	tagMap := map[string]string{
		"repetitiontype": repetitionTyeStr(s.SchemaElement),
		"type":           typeStr(s.SchemaElement),
		"name":           s.Name,
	}

	if len(s.ExNamePath) != 0 {
		tagMap["name"] = s.ExNamePath[len(s.ExNamePath)-1]
	}

	if tagMap["type"] == "STRUCT" {
		return tagMap
	}

	if s.Type != nil && *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		tagMap["length"] = fmt.Sprint(*s.TypeLength)
	}

	if s.LogicalType != nil {
		s.updateTagFromLogicalType(tagMap)
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
	if s.LogicalType != nil {
		if s.LogicalType.IsSetDECIMAL() {
			tagMap["logicaltype"] = "DECIMAL"
			tagMap["logicaltype.precision"] = fmt.Sprint(s.LogicalType.DECIMAL.Precision)
			tagMap["logicaltype.scale"] = fmt.Sprint(s.LogicalType.DECIMAL.Scale)
		} else if s.LogicalType.IsSetDATE() {
			tagMap["logicaltype"] = "DATE"
		} else if s.LogicalType.IsSetTIME() {
			tagMap["logicaltype"] = "TIME"
			tagMap["logicaltype.isadjustedtoutc"] = fmt.Sprint(s.LogicalType.TIME.IsAdjustedToUTC)
			tagMap["logicaltype.unit"] = timeUnitToTag(s.LogicalType.TIME.Unit)
		} else if s.LogicalType.IsSetTIMESTAMP() {
			tagMap["logicaltype"] = "TIMESTAMP"
			tagMap["logicaltype.isadjustedtoutc"] = fmt.Sprint(s.LogicalType.TIMESTAMP.IsAdjustedToUTC)
			tagMap["logicaltype.unit"] = timeUnitToTag(s.LogicalType.TIMESTAMP.Unit)
		} else if s.LogicalType.IsSetUUID() {
			tagMap["logicaltype"] = "UUID"
		} else if s.LogicalType.IsSetBSON() {
			tagMap["logicaltype"] = "BSON"
		} else if s.LogicalType.IsSetSTRING() {
			tagMap["logicaltype"] = "STRING"
		}
	}
}

func (s *SchemaNode) GetPathMap() map[string]*SchemaNode {
	retVal := map[string]*SchemaNode{}
	queue := []*SchemaNode{s}
	for len(queue) > 0 {
		node := queue[0]
		queue = append(queue[1:], node.Children...)
		retVal[strings.Join(node.InNamePath[1:], common.PAR_GO_PATH_DELIMITER)] = node
	}
	return retVal
}

func typeStr(se parquet.SchemaElement) string {
	if se.Type != nil {
		return se.Type.String()
	}
	if se.ConvertedType != nil {
		return se.ConvertedType.String()
	}
	return "STRUCT"
}

func repetitionTyeStr(se parquet.SchemaElement) string {
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

func (s SchemaNode) GoStruct() (string, error) {
	goStruct, err := goStructNode{s}.String()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("type %s %s", s.InNamePath[0], goStruct), nil
}

func (s SchemaNode) JSONSchema() string {
	schema, _ := json.Marshal(jsonSchemaNode{s}.Schema())
	return string(schema)
}

func (s SchemaNode) CSVSchema() (string, error) {
	jsonSchema := jsonSchemaNode{s}.Schema()
	schema := make([]string, len(jsonSchema.Fields))
	for i, f := range jsonSchema.Fields {
		if len(f.Fields) != 0 {
			return "", fmt.Errorf("CSV supports flat schema only")
		}
		if strings.Contains(f.Tag, "repetitiontype=REPEATED") {
			return "", fmt.Errorf("CSV does not support column in LIST type")
		}
		if strings.Contains(f.Tag, "repetitiontype=OPTIONAL") {
			return "", fmt.Errorf("CSV does not support optional column")
		}
		schema[i] = strings.Replace(f.Tag, ", repetitiontype=REQUIRED", "", 1)
	}
	return strings.Join(schema, "\n"), nil
}
