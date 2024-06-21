package internal

import "strings"

type JSONSchema struct {
	Tag    string
	Fields []JSONSchema `json:",omitempty"`
}

type JSONSchemaNode struct {
	SchemaNode
}

func NewJSONSchemaNode(s SchemaNode) JSONSchemaNode {
	return JSONSchemaNode{s}
}

func (s JSONSchemaNode) Schema() JSONSchema {
	tagMap := s.SchemaNode.getTagMap()

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
	ret := JSONSchema{
		Tag:    strings.Join(annotations, ", "),
		Fields: make([]JSONSchema, len(s.Children)),
	}

	for index, child := range s.Children {
		ret.Fields[index] = NewJSONSchemaNode(*child).Schema()
	}

	return ret
}
