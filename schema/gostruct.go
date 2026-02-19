package schema

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/hangxie/parquet-go/v2/parquet"
)

type goStructNode struct {
	SchemaNode
	ForceCamelCase bool
}

var goTypeStrMap = map[parquet.Type]string{
	parquet.Type_BOOLEAN:              "bool",
	parquet.Type_INT32:                "int32",
	parquet.Type_INT64:                "int64",
	parquet.Type_INT96:                "string",
	parquet.Type_FLOAT:                "float32",
	parquet.Type_DOUBLE:               "float64",
	parquet.Type_BYTE_ARRAY:           "string",
	parquet.Type_FIXED_LEN_BYTE_ARRAY: "string",
}

func (n goStructNode) asScalar() (string, error) {
	if n.Type == nil {
		return "", fmt.Errorf("type not set")
	}
	if typeStr, ok := goTypeStrMap[*n.Type]; ok {
		return typeStr, nil
	}
	return "", fmt.Errorf("unknown type: %d", *n.Type)
}

func (n goStructNode) asStruct() (string, error) {
	var typeStr strings.Builder
	typeStr.WriteString("struct {\n")
	for _, child := range n.Children {
		structStr, err := goStructNode{
			SchemaNode:     *child,
			ForceCamelCase: n.ForceCamelCase,
		}.stringWithName()
		if err != nil {
			return "", err
		}
		typeStr.WriteString(structStr + "\n")
	}
	typeStr.WriteString("}")
	return typeStr.String(), nil
}

func (n goStructNode) asList() (string, error) {
	var typeStr string
	var err error
	if n.Children[0].LogicalType != nil {
		// LIST => Element (of scalar type)
		n.Children[0].Name = "Element"
		*n.Children[0].RepetitionType = parquet.FieldRepetitionType_REQUIRED
		typeStr, err = goStructNode{
			SchemaNode:     *n.Children[0],
			ForceCamelCase: n.ForceCamelCase,
		}.String()
	} else if len(n.Children[0].Children) > 1 {
		// LIST => Element (of STRUCT)
		n.Children[0].Name = "Element"
		n.Children[0].Type = nil
		n.Children[0].ConvertedType = nil
		*n.Children[0].RepetitionType = parquet.FieldRepetitionType_REQUIRED
		typeStr, err = goStructNode{
			SchemaNode:     *n.Children[0],
			ForceCamelCase: n.ForceCamelCase,
		}.String()
	} else if len(n.Children[0].Children) == 1 {
		// LIST => List => Element
		// go struct will be []<actual element type>
		elementNode := n.Children[0].Children[0]
		typeStr, err = goStructNode{
			SchemaNode:     *elementNode,
			ForceCamelCase: n.ForceCamelCase,
		}.String()
	}
	if err != nil {
		return "", err
	}
	return "[]" + typeStr, nil
}

func (n goStructNode) asMap() (string, error) {
	// Parquet uses MAP -> "Map_Key_Value" -> [key type, value type]
	// go struct will be map[<key type>]<value type>
	keyStr, err := goStructNode{
		SchemaNode:     *n.Children[0].Children[0],
		ForceCamelCase: n.ForceCamelCase,
	}.asScalar()
	if err != nil {
		return "", err
	}
	valueStr, err := goStructNode{
		SchemaNode:     *n.Children[0].Children[1],
		ForceCamelCase: n.ForceCamelCase,
	}.String()
	if err != nil {
		return "", err
	}
	return "map[" + keyStr + "]" + valueStr, nil
}

func (n goStructNode) String() (string, error) {
	typePrefix := ""
	switch n.GetRepetitionType() {
	case parquet.FieldRepetitionType_OPTIONAL:
		typePrefix = "*"
	case parquet.FieldRepetitionType_REPEATED:
		typePrefix = "[]"
	}

	var typeStr string
	var err error
	switch {
	case n.LogicalType != nil && n.LogicalType.IsSetVARIANT():
		typeStr = "any"
		if typePrefix == "*" {
			typePrefix = ""
		}
	case n.Type == nil && n.ConvertedType == nil:
		typeStr, err = n.asStruct()
	case n.ConvertedType != nil && *n.ConvertedType == parquet.ConvertedType_LIST:
		typeStr, err = n.asList()
	case n.ConvertedType != nil && *n.ConvertedType == parquet.ConvertedType_MAP:
		typeStr, err = n.asMap()
	default:
		typeStr, err = goStructNode{
			SchemaNode:     n.SchemaNode,
			ForceCamelCase: n.ForceCamelCase,
		}.asScalar()
	}
	if err != nil {
		return "", err
	}
	return typePrefix + typeStr, nil
}

func (n goStructNode) stringWithName() (string, error) {
	typeStr, err := n.String()
	if err != nil {
		return "", err
	}
	structTags, err := n.getStructTags()
	if err != nil {
		return "", err
	}
	if n.ForceCamelCase {
		typeStr = snakeToCamel(n.InNamePath[len(n.InNamePath)-1]) + " " + typeStr + " " + structTags
	} else {
		typeStr = n.InNamePath[len(n.InNamePath)-1] + " " + typeStr + " " + structTags
	}
	return typeStr, nil
}

func (n goStructNode) getStructTags() (string, error) {
	tagMap := n.SchemaNode.GetTagMap()
	if _, found := tagMap["valuetype"]; !found &&
		n.ConvertedType != nil && *n.ConvertedType == parquet.ConvertedType_LIST {
		// make sure LIST always has "valuetype"
		tagMap["valuetype"] = "STRUCT"
	}

	if tag, found := tagMap["type"]; found && tag == "LIST" {
		if tag, found = tagMap["valuetype"]; found && (tag == "LIST" || tag == "MAP") {
			return "", fmt.Errorf("go struct does not support LIST of %s in [%s]", tag, strings.Join(n.InNamePath, "."))
		}
	} else if found && tag == "MAP" {
		if tag, found = tagMap["keytype"]; found && (tag == "LIST" || tag == "MAP") {
			return "", fmt.Errorf("go struct does not support %s as MAP key in [%s]", tag, strings.Join(n.InNamePath, "."))
		}
		if tag, found = tagMap["valuetype"]; found && (tag == "LIST" || tag == "MAP") {
			return "", fmt.Errorf("go struct does not support %s as MAP value in [%s]", tag, strings.Join(n.InNamePath, "."))
		}
	}

	annotations := make([]string, 0, len(orderedTags))
	for _, tag := range orderedTags {
		if tag == "inname" {
			// inname is for raw JSON schema only, not for go struct tags
			continue
		}
		if val, found := tagMap[tag]; found &&
			!(tag == "repetitiontype" && val == "REQUIRED") {
			// repetitiontype=REQUIRED is redundant in go struct
			annotations = append(annotations, tag+"="+val)
		}
	}

	return fmt.Sprintf("`parquet:\"%s\"`", strings.Join(annotations, ", ")), nil
}

// snakeToCamel converts snake_case strings to CamelCase
func snakeToCamel(s string) string {
	if s == "" {
		return s
	}

	parts := strings.Split(s, "_")
	var result strings.Builder

	for _, part := range parts {
		if len(part) > 0 {
			runes := []rune(part)
			runes[0] = unicode.ToUpper(runes[0])
			result.WriteString(string(runes))
		}
	}

	return result.String()
}
