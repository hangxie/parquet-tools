package cmd

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xitongsys/parquet-go/parquet"
)

var (
	formatRaw  string = "raw"
	formatJSON string = "json"
	formatGo   string = "go"
)

// SchemaCmd is a kong command for schema
type SchemaCmd struct {
	ReadOption
	Format string `short:"f" help:"Schema format (raw/json/go)." enum:"raw,json,go" default:"json"`
}

// Run does actual schema job
func (c *SchemaCmd) Run(ctx *Context) error {
	reader, err := newParquetFileReader(c.ReadOption)
	if err != nil {
		return err
	}
	defer reader.PFile.Close()

	schemaRoot := newSchemaTree(reader)
	switch c.Format {
	case formatRaw:
		res, _ := json.Marshal(*schemaRoot)
		fmt.Printf("%s\n", res)
	case formatJSON:
		s := schemaRoot.jsonSchema()
		res, _ := json.Marshal(s)
		fmt.Printf("%s\n", res)
	case formatGo:
		snippet := schemaRoot.goStruct(false)
		fmt.Printf("type %s %s\n", schemaRoot.Name, snippet)
	default:
		return fmt.Errorf("unknown schema format [%s]", c.Format)
	}

	return nil
}

type schemaNode struct {
	parquet.SchemaElement
	Children []*schemaNode `json:"children,omitempty"`
}

func typeStr(se parquet.SchemaElement) string {
	if se.Type != nil {
		return se.Type.String()
	}
	if se.ConvertedType != nil {
		switch *se.ConvertedType {
		case parquet.ConvertedType_LIST:
			return "LIST"
		case parquet.ConvertedType_MAP:
			return "MAP"
		default:
			return se.ConvertedType.String()
		}
	}
	return "STRUCT"
}

func repetitionTyeStr(se parquet.SchemaElement) string {
	if se.RepetitionType == nil {
		return "REQUIRED"
	}
	return se.RepetitionType.String()
}

var goTypeStrMap map[parquet.Type]string = map[parquet.Type]string{
	parquet.Type_BOOLEAN:              "bool",
	parquet.Type_INT32:                "int32",
	parquet.Type_INT64:                "int64",
	parquet.Type_INT96:                "string",
	parquet.Type_FLOAT:                "float32",
	parquet.Type_DOUBLE:               "float64",
	parquet.Type_BYTE_ARRAY:           "string",
	parquet.Type_FIXED_LEN_BYTE_ARRAY: "string",
}

func goTypeStr(se parquet.SchemaElement) string {
	if se.Type != nil {
		if typeStr, ok := goTypeStrMap[*se.Type]; ok {
			return typeStr
		}
	}
	return ""
}

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

type jsonSchemaNode struct {
	Tag    string
	Fields []*jsonSchemaNode `json:",omitempty"`
}

func (s *schemaNode) jsonSchema() *jsonSchemaNode {
	tagMap := s.getTagMap()

	annotations := []string{}
	for _, tag := range orderedTags {
		// keytype and valuetype are for go struct tag only
		if strings.HasPrefix(tag, "key") || strings.HasPrefix(tag, "value") {
			continue
		}
		if val, found := tagMap[tag]; found {
			annotations = append(annotations, tag+"="+val)
		}
	}
	ret := &jsonSchemaNode{
		Tag:    strings.Join(annotations, ", "),
		Fields: make([]*jsonSchemaNode, len(s.Children)),
	}

	for index, child := range s.Children {
		ret.Fields[index] = child.jsonSchema()
	}

	return ret
}

func (s *schemaNode) goStruct(withName bool) string {
	res := ""
	if s.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
		res = "*"
	} else if s.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED {
		res = "[]"
	}

	if s.Type == nil && s.ConvertedType == nil {
		res += "struct {\n"
		for _, cNode := range s.Children {
			res += cNode.goStruct(true) + "\n"
		}
		res += "}"
	} else if s.ConvertedType != nil && *s.ConvertedType == parquet.ConvertedType_LIST {
		// Parquet uses LIST -> "List"" -> actual element type
		// oo struct will be []<actual element type>
		res += "[]" + s.Children[0].Children[0].goStruct(false)
	} else if s.ConvertedType != nil && *s.ConvertedType == parquet.ConvertedType_MAP {
		// Parquet uses MAP -> "Map_Key_Value" -> [key type, value type]
		// go struct will be map[<key type>]<value type>
		res += "map[" + goTypeStr(s.Children[0].Children[0].SchemaElement) + "]" + s.Children[0].Children[1].goStruct(false)
	} else {
		res += goTypeStr(s.SchemaElement)
	}

	if withName {
		res = s.Name + " " + res + " " + s.getStructTags()
	}
	return res
}

func (s *schemaNode) getStructTags() string {
	tagMap := s.getTagMap()

	annotations := []string{}
	for _, tag := range orderedTags {
		if val, found := tagMap[tag]; found {
			// repetitiontype=REQUIRED is redundant in go struct
			if !(tag == "repetitiontype" && val == "REQUIRED") {
				annotations = append(annotations, tag+"="+val)
			}
		}
	}

	return fmt.Sprintf("`parquet:\"%s\"`", strings.Join(annotations, ", "))
}

func (s *schemaNode) updateTagFromConvertedType(tagMap map[string]string) {
	if s.ConvertedType != nil {
		tagMap["convertedtype"] = s.ConvertedType.String()

		switch *s.ConvertedType {
		case parquet.ConvertedType_LIST:
			// LIST has schema structure of LIST->List->Field1
			// expected output is LIST->Element
			delete(tagMap, "convertedtype")
			element := s.Children[0].Children[0].SchemaElement
			for k, v := range getTagMapAsChild(element, "value") {
				tagMap[k] = v
			}
			s.Children = s.Children[0].Children[:1]
			s.Children[0].Name = "Element"
		case parquet.ConvertedType_MAP:
			// MAP has schema structure of MAP->MAP_KEY_VALUE->(Field1, Field2)
			// expected output is MAP->(Key, Value)
			delete(tagMap, "convertedtype")
			key := s.Children[0].Children[0].SchemaElement
			value := s.Children[0].Children[1].SchemaElement
			for k, v := range getTagMapAsChild(key, "key") {
				tagMap[k] = v
			}
			for k, v := range getTagMapAsChild(value, "value") {
				tagMap[k] = v
			}
			s.Children = s.Children[0].Children[0:2]
			s.Children[0].Name = "Key"
			s.Children[1].Name = "Value"
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
}

func (s *schemaNode) updateTagFromLogicalType(tagMap map[string]string) {
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
			tagMap["logicaltype.unit"] = timeUnitToTag(s.LogicalType.TIME.Unit)
			delete(tagMap, "convertedtype")
		} else if s.LogicalType.IsSetTIMESTAMP() {
			tagMap["logicaltype"] = "TIMESTAMP"
			tagMap["logicaltype.isadjustedtoutc"] = fmt.Sprint(s.LogicalType.TIMESTAMP.IsAdjustedToUTC)
			tagMap["logicaltype.unit"] = timeUnitToTag(s.LogicalType.TIMESTAMP.Unit)
			delete(tagMap, "convertedtype")
		}
	}
}

func (s *schemaNode) getTagMap() map[string]string {
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

func getTagMapAsChild(se parquet.SchemaElement, prefix string) map[string]string {
	element := schemaNode{
		se,
		[]*schemaNode{},
	}
	tagMap := element.getTagMap()
	ret := map[string]string{}
	for _, tag := range orderedTags {
		if tag == "name" || strings.HasPrefix(tag, "key") || strings.HasPrefix(tag, "value") {
			continue
		}
		if val, found := tagMap[tag]; found {
			ret[prefix+tag] = val
		}
	}

	return ret
}
