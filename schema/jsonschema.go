package schema

import (
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
)

type JSONSchema struct {
	Tag    string
	Fields []JSONSchema `json:",omitempty"`
}

type jsonSchemaNode struct {
	SchemaNode
}

func (s jsonSchemaNode) Schema() JSONSchema {
	clone := s.cloneForRendering()
	return jsonSchemaNode{*clone}.schema()
}

func (s jsonSchemaNode) schema() JSONSchema {
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
	tagMap := s.getTagMap()
	s.normalizeForRendering()

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

	if s.LogicalType != nil && s.LogicalType.IsSetVARIANT() {
		ret.Fields = nil
		return ret
	}

	for index, child := range s.Children {
		ret.Fields[index] = jsonSchemaNode{*child}.schema()
	}

	return ret
}

func (s *jsonSchemaNode) normalizeForRendering() {
	if s.ConvertedType == nil || len(s.Children) == 0 || s.Children[0] == nil {
		return
	}

	switch *s.ConvertedType {
	case parquet.ConvertedType_LIST:
		s.normalizeListForRendering()
	case parquet.ConvertedType_MAP:
		s.normalizeMapForRendering()
	}
}

func (s *jsonSchemaNode) normalizeListForRendering() {
	element := s.Children[0]
	switch {
	case element.LogicalType != nil:
		element.Name = "Element"
		element.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)
	case len(element.Children) > 1:
		element.Name = "Element"
		element.Type = nil
		element.ConvertedType = nil
		element.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)
	case len(element.Children) == 1:
		s.Children = element.Children
		s.Children[0].Name = "Element"
	}
}

func (s *jsonSchemaNode) normalizeMapForRendering() {
	keyValue := s.Children[0]
	if len(keyValue.Children) < 2 ||
		(keyValue.ConvertedType != nil && *keyValue.ConvertedType != parquet.ConvertedType_MAP_KEY_VALUE) {
		return
	}

	s.Children = keyValue.Children[:2]
	s.Children[0].Name = "Key"
	s.Children[1].Name = "Value"
}
