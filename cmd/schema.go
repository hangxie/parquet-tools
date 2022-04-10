package cmd

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/xitongsys/parquet-go/parquet"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
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
		snippet := schemaRoot.goStruct(true)
		// remove annotation for top level
		re := regexp.MustCompile("} `[^`\n]*`$")
		snippet = re.ReplaceAllString(snippet, "}")
		fmt.Printf("type %s\n", snippet)
	default:
		return fmt.Errorf("unknown schema format [%s]", c.Format)
	}

	return nil
}

// Codes below copied idea from:
// https://github.com/xitongsys/parquet-go/blob/master/tool/parquet-tools/schematool/schematool.go

type schemaNode struct {
	parquet.SchemaElement
	Children []*schemaNode `json:"children,omitempty"`
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

type jsonSchemaNode struct {
	Tag    string
	Fields []*jsonSchemaNode `json:",omitempty"`
}

func (s *schemaNode) jsonSchema() *jsonSchemaNode {
	ret := &jsonSchemaNode{}
	repetitionType := repetitionTyeStr(s.SchemaElement)

	if s.Type == nil && s.ConvertedType == nil {
		// STRUCT
		ret.Tag = fmt.Sprintf("name=%s, repetitiontype=%s", s.Name, repetitionType)
	} else if s.ConvertedType != nil {
		switch *s.ConvertedType {
		case parquet.ConvertedType_LIST:
			// LIST has schema structure of LIST->List->Field1
			// expected output is LIST->Element
			ret.Tag = fmt.Sprintf("name=%s, type=LIST, repetitiontype=%s", s.Name, repetitionType)
			s.Children = s.Children[0].Children[:1]
			s.Children[0].Name = "Element"
		case parquet.ConvertedType_MAP:
			// MAP has schema structure of MAP->MAP_KEY_VALUE->(Field1, Field2)
			// expected output is MAP->(Key, Value)
			ret.Tag = fmt.Sprintf("name=%s, type=MAP, repetitiontype=%s", s.Name, repetitionType)
			s.Children = s.Children[0].Children[0:2]
			s.Children[0].Name = "Key"
			s.Children[1].Name = "Value"
		case parquet.ConvertedType_DECIMAL:
			if *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
				ret.Tag = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, scale=%d, precision=%d, length=%d, repetitiontype=%s",
					s.Name, s.Type.String(), s.ConvertedType.String(), s.GetScale(), s.GetPrecision(), s.GetTypeLength(), repetitionType)
			} else {
				ret.Tag = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, scale=%d, precision=%d, repetitiontype=%s",
					s.Name, s.Type.String(), s.ConvertedType.String(), s.GetScale(), s.GetPrecision(), repetitionType)
			}
		default:
			ret.Tag = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, repetitiontype=%s",
				s.Name, typeStr(s.SchemaElement), s.ConvertedType.String(), repetitionType)
		}
	} else if *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY && s.ConvertedType == nil {
		ret.Tag = fmt.Sprintf("name=%s, type=%s, length=%d, repetitiontype=%s",
			s.Name, s.Type.String(), s.GetTypeLength(), repetitionType)
	} else {
		ret.Tag = fmt.Sprintf("name=%s, type=%s, repetitiontype=%s", s.Name, typeStr(s.SchemaElement), repetitionType)
	}

	ret.Fields = make([]*jsonSchemaNode, len(s.Children))
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
		res = cases.Title(language.Und, cases.NoLower).String(s.GetName()) + " " + res + " " + s.getStructTags()
	}
	return res
}

func (s *schemaNode) getStructTags() string {
	repetitionStr := repetitionTyeStr(s.SchemaElement)
	if s.Type == nil && s.ConvertedType == nil {
		// STRUCT
		return fmt.Sprintf("`parquet:\"name=%s, repetitiontype=%s\"`", s.Name, repetitionStr)
	}

	if s.ConvertedType != nil {
		switch *s.ConvertedType {
		case parquet.ConvertedType_LIST:
			return fmt.Sprintf("`parquet:\"name=%s, type=LIST, repetitiontype=%s, valuetype=%s\"`",
				s.Name, repetitionStr, typeStr(s.Children[0].Children[0].SchemaElement))
		case parquet.ConvertedType_MAP:
			return fmt.Sprintf("`parquet:\"name=%s, type=MAP, repetitiontype=%s, keytype=%s, valuetype=%s\"`",
				s.Name, repetitionStr, typeStr(s.Children[0].Children[0].SchemaElement), typeStr(s.Children[0].Children[1].SchemaElement))
		case parquet.ConvertedType_DECIMAL:
			// DECIMAL with FIXED_LEN_BYTE_ARRAY
			if *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
				return fmt.Sprintf("`parquet:\"name=%s, type=%s, convertedtype=%s, scale=%d, precision=%d, length=%d, repetitiontype=%s\"`",
					s.Name, s.Type, s.ConvertedType, s.GetScale(), s.GetPrecision(), s.GetTypeLength(), repetitionStr)
			}
			// DECIMAL with BYTE_ARRAY, INT32, INT63
			return fmt.Sprintf("`parquet:\"name=%s, type=%s, convertedtype=%s, scale=%d, precision=%d, repetitiontype=%s\"`",
				s.Name, s.Type, s.Type, s.GetScale(), s.GetPrecision(), repetitionStr)
		default:
			// with type and converted type
			return fmt.Sprintf("`parquet:\"name=%s, type=%s, convertedtype=%s, repetitiontype=%s\"`",
				s.Name, typeStr(s.SchemaElement), s.ConvertedType.String(), repetitionStr)

		}
	} else if *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		// plain FIXED_LEN_BYTE_ARRAY without converted type
		return fmt.Sprintf("`parquet:\"name=%s, type=%s, length=%d, repetitiontype=%s\"`",
			s.Name, s.Type, s.GetTypeLength(), repetitionStr)
	}

	// with type only
	return fmt.Sprintf("`parquet:\"name=%s, type=%s, repetitiontype=%s\"`", s.Name, typeStr(s.SchemaElement), repetitionStr)
}
