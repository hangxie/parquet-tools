package cmd

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
)

var (
	formatRaw  string = "raw"
	formatJSON string = "json"
	formatGo   string = "go"
)

// SchemaCmd is a kong command for schema
type SchemaCmd struct {
	CommonOption
	Format string `short:"f" help:"Schema format (raw/json/go)." enum:"raw,json,go" default:"json"`
}

// Run does actual schema job
func (c *SchemaCmd) Run(ctx *Context) error {
	reader, err := newParquetFileReader(c.URI)
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
		snippet := schemaRoot.goStruct(true)
		// remove annotation for top level
		re := regexp.MustCompile("} `[^`\n]*`$")
		fmt.Printf("type %s\n", re.ReplaceAll([]byte(snippet), []byte("}")))
	default:
		return fmt.Errorf("unknown schema format [%s]", c.Format)
	}

	return nil
}

// Codes below are copied from parquet-go with refactor:
// https://github.com/xitongsys/parquet-go/blob/master/tool/parquet-tools/schematool/schematool.go

type schemaNode struct {
	parquet.SchemaElement
	Children []*schemaNode `json:"children,omitempty"`
}

func newSchemaTree(reader *reader.ParquetReader) *schemaNode {
	schemas := reader.SchemaHandler.SchemaElements
	stack := []*schemaNode{}
	root := &schemaNode{
		SchemaElement: *schemas[0],
		Children:      []*schemaNode{},
	}
	stack = append(stack, root)

	pos := 1
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		if len(node.Children) < int(node.SchemaElement.GetNumChildren()) {
			childNode := &schemaNode{
				SchemaElement: *schemas[pos],
				Children:      []*schemaNode{},
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

func typeStr(se parquet.SchemaElement) string {
	ret := ""
	if se.Type != nil {
		ret = se.Type.String()
	} else if se.ConvertedType != nil {
		ret = se.ConvertedType.String()
	}
	return ret
}

func repetitionTyeStr(se parquet.SchemaElement) string {
	repetitionType := "REQUIRED"
	if se.RepetitionType != nil {
		repetitionType = se.RepetitionType.String()
	}
	return repetitionType
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
	if se.Type == nil {
		return ""
	}

	if typeStr, ok := goTypeStrMap[*se.Type]; ok {
		return typeStr
	}
	return ""
}

type jsonSchemaNode struct {
	Tag    string
	Fields []*jsonSchemaNode `json:",omitempty"`
}

func (s *schemaNode) jsonSchema() *jsonSchemaNode {
	ret := &jsonSchemaNode{}
	repetitionType := repetitionTyeStr(s.SchemaElement)

	if s.Type == nil && s.ConvertedType == nil {
		ret.Tag = fmt.Sprintf("name=%s, repetitiontype=%s", s.Name, repetitionType)
	} else if s.ConvertedType != nil && *s.ConvertedType == parquet.ConvertedType_MAP && s.Children != nil {
		// MAP has schema structure of MAP->MAP_KEY_VALUE->(Field1, Field2)
		// expected output is MAP->(Key, Value)
		ret.Tag = fmt.Sprintf("name=%s, type=MAP, repetitiontype=%s", s.Name, repetitionType)
		s.Children = s.Children[0].Children[0:2]
		s.Children[0].Name = "Key"
		s.Children[1].Name = "Value"
	} else if s.ConvertedType != nil && *s.ConvertedType == parquet.ConvertedType_LIST && s.Children != nil {
		// LIST has schema structure of LIST->List->Field1
		// expected output is LIST->Element
		ret.Tag = fmt.Sprintf("name=%s, type=LIST, repetitiontype=%s", s.Name, repetitionType)
		s.Children = s.Children[0].Children[:1]
		s.Children[0].Name = "Element"
	} else if s.ConvertedType != nil && *s.ConvertedType == parquet.ConvertedType_DECIMAL {
		if *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			ret.Tag = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, scale=%d, precision=%d, length=%d, repetitiontype=%s",
				s.Name, s.Type.String(), s.ConvertedType.String(), s.GetScale(), s.GetPrecision(), s.GetTypeLength(), repetitionType)
		} else {
			ret.Tag = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, scale=%d, precision=%d, repetitiontype=%s",
				s.Name, s.Type.String(), s.ConvertedType.String(), s.GetScale(), s.GetPrecision(), repetitionType)
		}
	} else if *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY && s.ConvertedType == nil {
		ret.Tag = fmt.Sprintf("name=%s, type=%s, length=%d, repetitiontype=%s",
			s.Name, s.Type.String(), s.GetTypeLength(), repetitionType)
	} else if s.ConvertedType != nil {
		ret.Tag = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, repetitiontype=%s",
			s.Name, typeStr(s.SchemaElement), s.ConvertedType.String(), repetitionType)
	} else {
		ret.Tag = fmt.Sprintf("name=%s, type=%s, repetitiontype=%s", s.Name, typeStr(s.SchemaElement), repetitionType)
	}

	if len(s.Children) == 0 {
		ret.Fields = nil
	} else {
		ret.Fields = make([]*jsonSchemaNode, len(s.Children))
		for index, child := range s.Children {
			ret.Fields[index] = child.jsonSchema()
		}
	}

	return ret
}

func (s *schemaNode) goStruct(withName bool) string {
	res := ""
	if withName {
		res = strings.Title(s.GetName())
	}

	repetitionStr := ""
	if s.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
		repetitionStr = "*"
	} else if s.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED {
		repetitionStr = "[]"
	}
	if withName {
		repetitionStr = " " + repetitionStr
	}

	// regexp for removing uncessary value type tag
	re := regexp.MustCompile("([^}]) `.*`")

	if s.Type == nil && s.ConvertedType == nil {
		res += repetitionStr + "struct {\n"
		for _, cNode := range s.Children {
			res += cNode.goStruct(true) + "\n"
		}
		res += "}"
	} else if s.ConvertedType != nil && *s.ConvertedType == parquet.ConvertedType_MAP && s.Children != nil {
		res += repetitionStr + "map[" + goTypeStr(s.Children[0].Children[0].SchemaElement) + "]" + s.Children[0].Children[1].goStruct(false)
		res = string(re.ReplaceAll([]byte(res), []byte("$1")))
	} else if s.ConvertedType != nil && *s.ConvertedType == parquet.ConvertedType_LIST && s.Children != nil {
		cNode := s.Children[0].Children[0]
		res += repetitionStr + "[]" + cNode.goStruct(false)
		res = string(re.ReplaceAll([]byte(res), []byte("$1")))
	} else {
		res += repetitionStr + goTypeStr(s.SchemaElement)
	}

	res += " " + s.getStructTags()
	return res
}

func (s *schemaNode) getStructTags() string {
	repetitionStr := "REQUIRED"
	if s.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
		repetitionStr = "OPTIONAL"
	} else if s.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED {
		repetitionStr = "REPEATED"
	}

	if s.Type == nil && s.ConvertedType == nil {
		// STRUCT
		return fmt.Sprintf("`parquet:\"name=%s, repetitiontype=%s\"`", s.Name, repetitionStr)
	}

	if s.ConvertedType != nil && *s.ConvertedType == parquet.ConvertedType_MAP && s.Children != nil {
		// MAP
		return fmt.Sprintf("`parquet:\"name=%s, type=MAP, repetitiontype=%s, keytype=%s, valuetype=%s\"`",
			s.Name, repetitionStr, typeStr(s.Children[0].Children[0].SchemaElement), typeStr(s.Children[0].Children[1].SchemaElement))
	}

	if s.ConvertedType != nil && *s.ConvertedType == parquet.ConvertedType_LIST && s.Children != nil {
		// LIST
		return fmt.Sprintf("`parquet:\"name=%s, type=LIST, repetitiontype=%s, valuetype=%s\"`",
			s.Name, repetitionStr, typeStr(s.Children[0].Children[0].SchemaElement))
	}

	if *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY && s.ConvertedType == nil {
		// plain FIXED_LEN_BYTE_ARRAY
		return fmt.Sprintf("`parquet:\"name=%s, type=%s, length=%d, repetitiontype=%s\"`",
			s.Name, s.Type, s.GetTypeLength(), repetitionStr)
	}

	if s.ConvertedType != nil && *s.ConvertedType == parquet.ConvertedType_DECIMAL {
		// DECIMAL with FIXED_LEN_BYTE_ARRAY
		if *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return fmt.Sprintf("`parquet:\"name=%s, type=%s, convertedtype=%s, scale=%d, precision=%d, length=%d, repetitiontype=%s\"`",
				s.Name, s.Type, s.ConvertedType, s.GetScale(), s.GetPrecision(), s.GetTypeLength(), repetitionStr)
		}
		// DECIMAL
		return fmt.Sprintf("`parquet:\"name=%s, type=%s, convertedtype=%s, scale=%d, precision=%d, repetitiontype=%s\"`",
			s.Name, s.Type, s.Type, s.GetScale(), s.GetPrecision(), repetitionStr)
	}

	if s.ConvertedType != nil {
		// with type and converted type
		return fmt.Sprintf("`parquet:\"name=%s, type=%s, convertedtype=%s, repetitiontype=%s\"`",
			s.Name, typeStr(s.SchemaElement), s.ConvertedType.String(), repetitionStr)
	}

	// with type only
	return fmt.Sprintf("`parquet:\"name=%s, type=%s, repetitiontype=%s\"`", s.Name, typeStr(s.SchemaElement), repetitionStr)
}
