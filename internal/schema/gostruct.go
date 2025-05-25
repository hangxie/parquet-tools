package schema

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v2/parquet"
)

type goStructNode struct {
	SchemaNode
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
	typeStr := "struct {\n"
	for _, child := range n.Children {
		structStr, err := goStructNode{*child}.stringWithName()
		if err != nil {
			return "", err
		}
		typeStr += structStr + "\n"
	}
	typeStr += "}"
	return typeStr, nil
}

func (n goStructNode) asList() (string, error) {
	var typeStr string
	var err error
	if n.Children[0].LogicalType != nil {
		// LIST => Element (of scalar type)
		n.Children[0].Name = "Element"
		*n.Children[0].RepetitionType = parquet.FieldRepetitionType_REQUIRED
		typeStr, err = goStructNode{*n.Children[0]}.String()
	} else if len(n.Children[0].Children) > 1 {
		// LIST => Element (of STRUCT)
		n.Children[0].Name = "Element"
		n.Children[0].Type = nil
		n.Children[0].ConvertedType = nil
		*n.Children[0].RepetitionType = parquet.FieldRepetitionType_REQUIRED
		typeStr, err = goStructNode{*n.Children[0]}.String()
	} else if len(n.Children[0].Children) == 1 {
		// LIST => List => Element
		// go struct will be []<actual element type>
		elementNode := n.Children[0].Children[0]
		if elementNode.ConvertedType != nil &&
			(*elementNode.ConvertedType == parquet.ConvertedType_MAP ||
				*elementNode.ConvertedType == parquet.ConvertedType_LIST) {
			return "", fmt.Errorf(
				"go struct does not support composite type as list element in field [%s]",
				strings.Join(n.InNamePath, "."))
		}
		typeStr, err = goStructNode{*elementNode}.String()
	}
	if err != nil {
		return "", err
	}
	return "[]" + typeStr, nil
}

func (n goStructNode) asMap() (string, error) {
	// go struct tag does not support LIST or MAP as type of key/value
	if n.Children[0].Children[0].ConvertedType != nil {
		keyConvertedType := *n.Children[0].Children[0].ConvertedType
		if keyConvertedType == parquet.ConvertedType_MAP ||
			keyConvertedType == parquet.ConvertedType_LIST {
			return "", fmt.Errorf("go struct does not support composite type as map key in field [%s]", strings.Join(n.InNamePath, "."))
		}
	}

	if n.Children[0].Children[1].ConvertedType != nil {
		valueConvertedType := *n.Children[0].Children[1].ConvertedType
		if valueConvertedType == parquet.ConvertedType_MAP ||
			valueConvertedType == parquet.ConvertedType_LIST {
			return "", fmt.Errorf("go struct does not support composite type as map value in field [%s]", strings.Join(n.InNamePath, "."))
		}
	}
	// Parquet uses MAP -> "Map_Key_Value" -> [key type, value type]
	// go struct will be map[<key type>]<value type>
	keyStr, err := goStructNode{*n.Children[0].Children[0]}.asScalar()
	if err != nil {
		return "", err
	}
	valueStr, err := goStructNode{*n.Children[0].Children[1]}.String()
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
	case n.Type == nil && n.ConvertedType == nil:
		typeStr, err = n.asStruct()
	case n.ConvertedType != nil && *n.ConvertedType == parquet.ConvertedType_LIST:
		typeStr, err = n.asList()
	case n.ConvertedType != nil && *n.ConvertedType == parquet.ConvertedType_MAP:
		typeStr, err = n.asMap()
	default:
		typeStr, err = goStructNode{n.SchemaNode}.asScalar()
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
	typeStr = n.InNamePath[len(n.InNamePath)-1] + " " + typeStr + " " + n.getStructTags()
	return typeStr, nil
}

func (n goStructNode) getStructTags() string {
	tagMap := n.SchemaNode.getTagMap()
	if _, found := tagMap["valuetype"]; !found &&
		n.ConvertedType != nil && *n.ConvertedType == parquet.ConvertedType_LIST {
		// make sure LISt always has "valuetype"
		tagMap["valuetype"] = "STRUCT"
	}

	annotations := make([]string, 0, len(orderedTags))
	for _, tag := range orderedTags {
		if val, found := tagMap[tag]; found &&
			!(tag == "repetitiontype" && val == "REQUIRED") {
			// repetitiontype=REQUIRED is redundant in go struct
			annotations = append(annotations, tag+"="+val)
		}
	}

	return fmt.Sprintf("`parquet:\"%s\"`", strings.Join(annotations, ", "))
}
