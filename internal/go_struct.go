package internal

import (
	"fmt"
	"strings"

	"github.com/xitongsys/parquet-go/parquet"
)

type GoStructNode struct {
	SchemaNode
}

func NewGoStructNode(s SchemaNode) GoStructNode {
	return GoStructNode{s}
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

func (n GoStructNode) asScalar() (string, error) {
	if n.Type == nil {
		return "", fmt.Errorf("type not set")
	}
	if typeStr, ok := goTypeStrMap[*n.Type]; ok {
		return typeStr, nil
	}
	return "", fmt.Errorf("unknown type: %d", *n.Type)
}

func (n GoStructNode) asStruct() (string, error) {
	typeStr := "struct {\n"
	for _, child := range n.Children {
		structStr, err := NewGoStructNode(*child).stringWithName()
		if err != nil {
			return "", err
		}
		typeStr += structStr + "\n"
	}
	typeStr += "}"
	return typeStr, nil
}

func (n GoStructNode) asList() (string, error) {
	var typeStr string
	var err error
	if n.Children[0].Type == nil {
		// Parquet uses LIST -> "List"" -> actual element type
		// oo struct will be []<actual element type>
		typeStr, err = NewGoStructNode(*n.Children[0].Children[0]).String()
	} else {
		// TODO find test case
		// https://github.com/hangxie/parquet-tools/issues/187
		typeStr, err = NewGoStructNode(*n.Children[0]).String()
	}
	if err != nil {
		return "", err
	}
	return "[]" + typeStr, nil
}

func (n GoStructNode) asMap() (string, error) {
	// go struct tag does not support LIST or MAP as type of key/value
	if n.Children[0].Children[0].ConvertedType != nil {
		keyConvertedType := *n.Children[0].Children[0].ConvertedType
		if keyConvertedType == parquet.ConvertedType_MAP || keyConvertedType == parquet.ConvertedType_LIST {
			return "", fmt.Errorf("go struct does not support composite type as map key in field [%s.%s]", strings.Join(n.Parent, "."), n.Name)
		}
	}

	if n.Children[0].Children[1].ConvertedType != nil {
		valueConvertedType := *n.Children[0].Children[1].ConvertedType
		if valueConvertedType == parquet.ConvertedType_MAP || valueConvertedType == parquet.ConvertedType_LIST {
			return "", fmt.Errorf("go struct does not support composite type as map value in field [%s.%s]", strings.Join(n.Parent, "."), n.Name)
		}
	}
	// Parquet uses MAP -> "Map_Key_Value" -> [key type, value type]
	// go struct will be map[<key type>]<value type>
	keyStr, err := NewGoStructNode(*n.Children[0].Children[0]).asScalar()
	if err != nil {
		return "", err
	}
	valueStr, err := NewGoStructNode(*n.Children[0].Children[1]).String()
	if err != nil {
		return "", err
	}
	return "map[" + keyStr + "]" + valueStr, nil
}

func (n GoStructNode) String() (string, error) {
	typePrefix := ""
	if n.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
		typePrefix = "*"
	} else if n.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED {
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
		typeStr, err = NewGoStructNode(n.SchemaNode).asScalar()
	}
	if err != nil {
		return "", err
	}
	return typePrefix + typeStr, nil
}

func (n GoStructNode) stringWithName() (string, error) {
	typeStr, err := n.String()
	if err != nil {
		return "", err
	}
	typeStr = n.Name + " " + typeStr + " " + n.getStructTags()
	return typeStr, nil
}

func (n GoStructNode) getStructTags() string {
	tagMap := n.SchemaNode.getTagMap()
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
