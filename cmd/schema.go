package cmd

import (
	"encoding/json"
	"fmt"

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
	Format string `help:"Schema format." enum:"raw,json,go" default:"json"`
}

// Run does actual schema job
func (c *SchemaCmd) Run(ctx *Context) error {
	reader, err := newParquetFileReader(c.URI)
	if err != nil {
		return err
	}

	if err := reader.ReadFooter(); err != nil {
		return fmt.Errorf("failed to read footer from Parquet file: %s", err.Error())
	}

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
		fmt.Printf("%s\n", schemaRoot.goStruct())
	default:
		return fmt.Errorf("unknown schema format [%s]", c.Format)
	}

	return nil
}

// Codes below are copied from parquet-go with refactor:
// https://github.com/xitongsys/parquet-go/blob/master/tool/parquet-tools/schematool/schematool.go

type schemaNode struct {
	parquet.SchemaElement
	Children []*schemaNode
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

type jsonSchemaNode struct {
	Tag    string
	Fields []*jsonSchemaNode
}

func (s *schemaNode) jsonSchema() *jsonSchemaNode {
	ret := &jsonSchemaNode{}
	repetitionType := repetitionTyeStr(s.SchemaElement)

	if s.Type == nil && s.ConvertedType == nil {
		ret.Tag = fmt.Sprintf("name=%s, repetitiontype=%s", s.Name, repetitionType)
	} else if s.ConvertedType != nil && *s.ConvertedType == parquet.ConvertedType_MAP && s.Children != nil {
		// MAP has schema structure of MAP->MAP_KEY_VALUE->(Key, Value)
		// expected output is MAP->(Key, Value)
		ret.Tag = fmt.Sprintf("name=%s, type=MAP, repetitiontype=%s", s.Name, repetitionType)
		s.Children = s.Children[0].Children[0:2]
		s.Children[0].Name = "Key"
		s.Children[1].Name = "Value"
	} else if s.ConvertedType != nil && *s.ConvertedType == parquet.ConvertedType_LIST && s.Children != nil {
		// LIST has schema structure of LIST->List->Element
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

func (s *schemaNode) goStruct() string {
	// TODO output go struct with tags
	return "TBD"
}
