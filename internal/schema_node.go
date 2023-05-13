package internal

import (
	"fmt"
	"math"
	"reflect"
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

type ReinterpretField struct {
	ParquetType   parquet.Type
	ConvertedType parquet.ConvertedType
	Precision     int
	Scale         int
}

func NewSchemaTree(reader *reader.ParquetReader) *SchemaNode {
	schemas := reader.SchemaHandler.SchemaElements
	stack := []*SchemaNode{}
	root := &SchemaNode{
		SchemaElement: *schemas[0],
		Parent:        []string{},
		Children:      []*SchemaNode{},
	}
	stack = append(stack, root)

	for pos := 1; len(stack) > 0; {
		node := stack[len(stack)-1]
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

	return root
}

func (s *SchemaNode) getTagMap() map[string]string {
	tagMap := map[string]string{}
	if s == nil {
		return tagMap
	}
	tagMap["name"] = s.Name
	tagMap["repetitiontype"] = repetitionTyeStr(s.SchemaElement)
	tagMap["type"] = typeStr(s.SchemaElement)

	if tagMap["type"] == "STRUCT" {
		return tagMap
	}

	if s.Type != nil && *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY && s.ConvertedType == nil {
		tagMap["type"] = typeStr(s.SchemaElement)
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
	if len(s.Children) == 0 || s.Children[0].Type != nil {
		return
	}
	// LIST has schema structure LIST->List->Element
	// expected output is LIST->Element
	for k, v := range s.Children[0].Children[0].getTagMapWithPrefix("value") {
		tagMap[k] = v
	}
	s.Children = s.Children[0].Children[:1]
	s.Children[0].Name = "Element"
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
	return nil, fmt.Errorf("unknown type: %s", reflect.TypeOf(iface))
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
