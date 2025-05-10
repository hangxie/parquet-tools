package schema

import "strings"

type JSONSchema struct {
	Tag    string
	Fields []JSONSchema `json:",omitempty"`
}

type jsonSchemaNode struct {
	SchemaNode
}

func (s jsonSchemaNode) Schema() JSONSchema {
	// these are tag/value pairs to be ignored as they are default values
	type tagValPair struct {
		tag string
		val string
	}

	tagsToIgnore := map[tagValPair]struct{}{
		{"type", "STRUCT"}:             {},
		{"repetitiontype", "REQUIRED"}: {},
		{"convertedtype", "LIST"}:      {},
		{"convertedtype", "MAP"}:       {},
	}
	tagMap := s.SchemaNode.getTagMap()

	var annotations []string
	for _, tag := range orderedTags {
		// keytype and valuetype are for go struct tag only
		if strings.HasPrefix(tag, "key") || strings.HasPrefix(tag, "value") {
			continue
		}
		if val, found := tagMap[tag]; found {
			if _, found := tagsToIgnore[tagValPair{tag, val}]; found {
				continue
			}
			annotations = append(annotations, tag+"="+val)
		}
	}
	ret := JSONSchema{
		Tag:    strings.Join(annotations, ", "),
		Fields: make([]JSONSchema, len(s.Children)),
	}

	for index, child := range s.Children {
		ret.Fields[index] = jsonSchemaNode{*child}.Schema()
	}

	return ret
}
