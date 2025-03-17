package internal

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/types"
)

// this represents order of tags in JSON schema and go struct
var orderedTags []string = []string{
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

type SchemaNode struct {
	parquet.SchemaElement
	Parent   []string      `json:"-"`
	Children []*SchemaNode `json:"children,omitempty"`
}

type SchemaOption struct {
	FailOnInt96 bool
}

type ReinterpretField struct {
	ParquetType   parquet.Type
	ConvertedType parquet.ConvertedType
	Precision     int
	Scale         int
}

func NewSchemaTree(reader *reader.ParquetReader, option SchemaOption) (*SchemaNode, error) {
	schemas := reader.SchemaHandler.SchemaElements
	var stack []*SchemaNode
	root := &SchemaNode{
		SchemaElement: *schemas[0],
		Parent:        []string{},
		Children:      []*SchemaNode{},
	}
	stack = append(stack, root)

	for pos := 1; len(stack) > 0; {
		node := stack[len(stack)-1]
		if option.FailOnInt96 && node.Type != nil && *node.Type == parquet.Type_INT96 {
			return nil, fmt.Errorf("field %s has type INT96 which is not supported", node.Name)
		}
		if len(node.Children) < int(node.GetNumChildren()) {
			childNode := &SchemaNode{
				SchemaElement: *schemas[pos],
				Parent:        append(node.Parent, node.Name),
				Children:      []*SchemaNode{},
			}
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

	return root, nil
}

func (s *SchemaNode) getTagMap() map[string]string {
	tagMap := map[string]string{}
	tagMap["name"] = s.Name
	tagMap["repetitiontype"] = repetitionTyeStr(s.SchemaElement)
	tagMap["type"] = typeStr(s.SchemaElement)

	if tagMap["type"] == "STRUCT" {
		return tagMap
	}

	if s.Type != nil && *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY && s.ConvertedType == nil {
		tagMap["length"] = fmt.Sprint(*s.TypeLength)
		return tagMap
	}

	s.updateTagFromConvertedType(tagMap)
	s.updateTagFromLogicalType(tagMap)

	return tagMap
}

func (s *SchemaNode) getTagMapWithPrefix(prefix string) map[string]string {
	tagMap := s.getTagMap()
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
		for k, v := range s.Children[0].getTagMapWithPrefix("value") {
			tagMap[k] = v
		}
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
		for k, v := range s.Children[0].Children[0].getTagMapWithPrefix("value") {
			tagMap[k] = v
		}
		// s.Children[0] = s.Children[0].Children[0]
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
	for k, v := range s.Children[0].Children[0].getTagMapWithPrefix("key") {
		tagMap[k] = v
	}
	for k, v := range s.Children[0].Children[1].getTagMapWithPrefix("value") {
		tagMap[k] = v
	}
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
		if s.LogicalType.IsSetDECIMAL() && tagMap["convertedtype"] != "DECIMAL" {
			// Do not populate localtype fields for DECIMAL type
			tagMap["logicaltype"] = "DECIMAL"
			tagMap["logicaltype.precision"] = fmt.Sprint(s.LogicalType.DECIMAL.Precision)
			tagMap["logicaltype.scale"] = fmt.Sprint(s.LogicalType.DECIMAL.Scale)
		} else if s.LogicalType.IsSetDATE() {
			// Do not populate localtype fields for DATE type
		} else if s.LogicalType.IsSetTIME() {
			tagMap["logicaltype"] = "TIME"
			tagMap["logicaltype.isadjustedtoutc"] = fmt.Sprint(s.LogicalType.TIME.IsAdjustedToUTC)
			tagMap["logicaltype.unit"] = TimeUnitToTag(s.LogicalType.TIME.Unit)
			delete(tagMap, "convertedtype")
		} else if s.LogicalType.IsSetTIMESTAMP() {
			tagMap["logicaltype"] = "TIMESTAMP"
			tagMap["logicaltype.isadjustedtoutc"] = fmt.Sprint(s.LogicalType.TIMESTAMP.IsAdjustedToUTC)
			tagMap["logicaltype.unit"] = TimeUnitToTag(s.LogicalType.TIMESTAMP.Unit)
			delete(tagMap, "convertedtype")
		}
	}
}

func (s *SchemaNode) GetReinterpretFields(rootPath string, noInterimLayer bool) map[string]ReinterpretField {
	reinterpretFields := make(map[string]ReinterpretField)
	for _, child := range s.Children {
		currentPath := rootPath + "." + child.Name
		if child.Type == nil && child.ConvertedType == nil && child.NumChildren != nil {
			// STRUCT
			for k, v := range child.GetReinterpretFields(currentPath, noInterimLayer) {
				reinterpretFields[k] = v
			}
			continue
		}

		if child.Type != nil && *child.Type == parquet.Type_INT96 {
			reinterpretFields[currentPath] = ReinterpretField{
				ParquetType:   parquet.Type_INT96,
				ConvertedType: parquet.ConvertedType_TIMESTAMP_MICROS,
				Precision:     0,
				Scale:         0,
			}
			continue
		}

		if child.ConvertedType != nil {
			switch *child.ConvertedType {
			case parquet.ConvertedType_MAP, parquet.ConvertedType_LIST:
				if noInterimLayer {
					child = child.Children[0]
				}
				fallthrough
			case parquet.ConvertedType_MAP_KEY_VALUE:
				for k, v := range child.GetReinterpretFields(currentPath, noInterimLayer) {
					reinterpretFields[k] = v
				}
			case parquet.ConvertedType_DECIMAL, parquet.ConvertedType_INTERVAL:
				reinterpretFields[currentPath] = ReinterpretField{
					ParquetType:   *child.Type,
					ConvertedType: *child.ConvertedType,
					Precision:     int(*child.Precision),
					Scale:         int(*child.Scale),
				}
			}
		}
	}

	return reinterpretFields
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

func DecimalToFloat(fieldAttr ReinterpretField, iface interface{}) (*float64, error) {
	if iface == nil {
		return nil, nil
	}

	switch value := iface.(type) {
	case int64:
		f64 := float64(value) / math.Pow10(fieldAttr.Scale)
		return &f64, nil
	case int32:
		f64 := float64(value) / math.Pow10(fieldAttr.Scale)
		return &f64, nil
	case string:
		buf := StringToBytes(fieldAttr, value)
		f64, err := strconv.ParseFloat(types.DECIMAL_BYTE_ARRAY_ToString(buf, fieldAttr.Precision, fieldAttr.Scale), 64)
		if err != nil {
			return nil, err
		}
		return &f64, nil
	}
	return nil, fmt.Errorf("unknown type: %T", iface)
}

func StringToBytes(fieldAttr ReinterpretField, value string) []byte {
	// INTERVAL uses LittleEndian, DECIMAL uses BigEndian
	// make sure all decimal-like value are all BigEndian
	buf := []byte(value)
	if fieldAttr.ConvertedType == parquet.ConvertedType_INTERVAL {
		for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
			buf[i], buf[j] = buf[j], buf[i]
		}
	}
	return buf
}

func TimeUnitToTag(timeUnit *parquet.TimeUnit) string {
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
	return fmt.Sprintf("type %s %s", s.Name, goStruct), nil
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

func equals[T comparable](a, b *T) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func schemaElementEquals(s, v parquet.SchemaElement) bool {
	return s.Name == v.Name &&
		equals(s.Type, v.Type) &&
		equals(s.TypeLength, v.TypeLength) &&
		equals(s.ConvertedType, v.ConvertedType) &&
		equals(s.NumChildren, v.NumChildren) &&
		equals(s.ConvertedType, v.ConvertedType) &&
		equals(s.Scale, v.Scale) &&
		equals(s.Precision, v.Precision) &&
		equals(s.FieldID, v.FieldID) &&
		equals(s.LogicalType, v.LogicalType)
}

func (s SchemaNode) Equals(v SchemaNode) bool {
	if len(s.Parent) != len(v.Parent) || len(s.Children) != len(v.Children) {
		return false
	}

	if len(s.Parent) != 0 {
		// do not compare attributes of top level node as different libraries behave differently
		if !schemaElementEquals(s.SchemaElement, v.SchemaElement) {
			return false
		}

		for i := range s.Parent[1:] {
			if s.Parent[i+1] != v.Parent[i+1] {
				return false
			}
		}
	}

	for i := range s.Children {
		if !s.Children[i].Equals(*v.Children[i]) {
			return false
		}
	}
	return true
}
