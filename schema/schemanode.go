package schema

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

var orderedTags = []string{
	"name",
	"inname",
	"type",
	"keytype",
	"keyconvertedtype",
	"keyscale",
	"keyprecision",
	"keyencoding",
	"keycompression",
	"valuetype",
	"valueconvertedtype",
	"valuescale",
	"valueprecision",
	"valueencoding",
	"valuecompression",
	"convertedtype",
	"scale",
	"precision",
	"length",
	"logicaltype",
	"logicaltype.precision",
	"logicaltype.scale",
	"logicaltype.isadjustedtoutc",
	"logicaltype.unit",
	"logicaltype.bitwidth",
	"logicaltype.issigned",
	"repetitiontype",
	"encoding",
	"compression",
	"omitstats",
	"bloomfilter",
	"bloomfiltersize",
}

// OrderedTags returns a copy of the ordered tags list for external use
func OrderedTags() []string {
	result := make([]string, len(orderedTags))
	copy(result, orderedTags)
	return result
}

type SchemaNode struct {
	parquet.SchemaElement
	Children   []*SchemaNode `json:"children,omitempty"`
	InNamePath []string      `json:"-"`
	ExNamePath []string      `json:"-"`
	// Custom parquet-go writer directives (not part of Parquet format)
	Encoding           string `json:"encoding,omitempty"`          // Data page encoding (PLAIN, RLE, etc)
	OmitStats          string `json:"-"`                           // Control statistics generation (true/false)
	CompressionCodec   string `json:"compression_codec,omitempty"` // Compression codec (SNAPPY, GZIP, etc)
	BloomFilter        string `json:"-"`                           // Enable bloom filter (true/false)
	BloomFilterSize    string `json:"-"`                           // Bloom filter size in bytes
	UndefinedSortOrder bool   `json:"-"`                           // True for children of types with undefined sort order (e.g., VARIANT)
}

type SchemaOption struct {
	FailOnInt96      bool
	SkipPageEncoding bool
	SkipBloomFilter  bool
}

// cloneForRendering copies the tree state that schema renderers normalize.
func (s *SchemaNode) cloneForRendering() *SchemaNode {
	if s == nil {
		return nil
	}

	clone := *s
	clone.Type = copyPointer(s.Type)
	clone.RepetitionType = copyPointer(s.RepetitionType)
	clone.ConvertedType = copyPointer(s.ConvertedType)
	clone.Children = make([]*SchemaNode, len(s.Children))
	for i, child := range s.Children {
		clone.Children[i] = child.cloneForRendering()
	}
	return &clone
}

// isRepeated reports whether the node is a REPEATED field.
func (s *SchemaNode) isRepeated() bool {
	return s != nil && s.RepetitionType != nil && *s.RepetitionType == parquet.FieldRepetitionType_REPEATED
}

// isList reports whether the node is a LIST group. converted_type is optional, so a group
// annotated only by logical type also counts in the layouts parquet-go reads as a list.
func (s *SchemaNode) isList() bool {
	if s.GetConvertedType() == parquet.ConvertedType_LIST {
		return true
	}
	return s.LogicalType != nil && s.LogicalType.IsSetLIST() &&
		len(s.Children) == 1 && s.Children[0].isListWrapper(s)
}

// isMap reports whether the node is a MAP group that parquet-go reads as a Go map. The reader
// matches the key_value, key and value names, so any other naming must render as a struct.
func (s *SchemaNode) isMap() bool {
	annotated := s.GetConvertedType() == parquet.ConvertedType_MAP || (s.LogicalType != nil && s.LogicalType.IsSetMAP())
	if !annotated || len(s.Children) != 1 || s.Children[0] == nil {
		return false
	}
	keyValue := s.Children[0]
	return keyValue.inName() == "Key_value" && len(keyValue.Children) == 2 &&
		keyValue.Children[0].inName() == "Key" && keyValue.Children[1].inName() == "Value"
}

// isUnreadMapGroup reports whether the node carries a MAP or MAP_KEY_VALUE converted type
// on a layout parquet-go does not read as a map.
func (s *SchemaNode) isUnreadMapGroup() bool {
	ct := s.GetConvertedType()
	return s.Type == nil && s.ConvertedType != nil && !s.isMap() &&
		(ct == parquet.ConvertedType_MAP || ct == parquet.ConvertedType_MAP_KEY_VALUE)
}

// inName is the reader's internal name for the node, which renderers never rename.
func (s *SchemaNode) inName() string {
	if s == nil {
		return ""
	}
	if len(s.InNamePath) != 0 {
		return s.InNamePath[len(s.InNamePath)-1]
	}
	// a tree decoded from raw JSON has no paths; derive the name as the reader does
	return common.StringToVariableName(s.Name)
}

// isListWrapper reports whether the node is the repeated wrapper of a 3-level LIST rather than
// the element itself, per the spec's backward-compatibility rules.
func (s *SchemaNode) isListWrapper(list *SchemaNode) bool {
	return s.isRepeated() && len(s.Children) == 1 && !s.Children[0].isRepeated() &&
		s.exName() != "array" && s.exName() != list.exName()+"_tuple"
}

// exName is the file's name for the node; renderers may rename Name.
func (s *SchemaNode) exName() string {
	if len(s.ExNamePath) != 0 {
		return s.ExNamePath[len(s.ExNamePath)-1]
	}
	return s.Name
}

// isLegacyListElement reports whether the node is the element of a 2-level LIST,
// ie a repeated scalar sitting directly under the LIST group instead of the
// 3-level LIST => List => Element layers.
func (s *SchemaNode) isLegacyListElement() bool {
	return s != nil && s.Type != nil && s.LogicalType == nil && len(s.Children) == 0 && s.isRepeated()
}

func copyPointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func (s SchemaNode) GoStruct(forceCamelCase bool) (string, error) {
	goStruct, err := goStructNode{
		SchemaNode:     s,
		ForceCamelCase: forceCamelCase,
	}.String()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("type %s %s", s.InNamePath[0], goStruct), nil
}

func (s SchemaNode) JSONSchema() string {
	schema, _ := json.Marshal(jsonSchemaNode{s}.Schema())
	return string(schema)
}

func (s SchemaNode) CSVSchema() (string, error) {
	jsonSchema := jsonSchemaNode{s}.Schema()
	schema := make([]string, len(jsonSchema.Fields))
	for i, f := range jsonSchema.Fields {
		if len(f.Fields) != 0 {
			return "", fmt.Errorf("CSV supports flat schema only")
		}
		if strings.Contains(f.Tag, "repetitiontype=REPEATED") {
			return "", fmt.Errorf("CSV does not support column in LIST type")
		}
		if strings.Contains(f.Tag, "repetitiontype=OPTIONAL") {
			return "", fmt.Errorf("CSV does not support optional column")
		}
		tag := strings.Replace(f.Tag, ", repetitiontype=REQUIRED", "", 1)
		// Remove inname tag from CSV schema as it's Go-specific
		tag = removeTagFromString(tag, "inname")
		schema[i] = tag
	}
	return strings.Join(schema, "\n"), nil
}

// removeTagFromString removes a tag and its value from a tag string
// e.g., removeTagFromString("name=foo, inname=Foo, type=INT32", "inname") -> "name=foo, type=INT32"
func removeTagFromString(tagString, tagName string) string {
	// Pattern: either ", tagName=value" or "tagName=value, "
	parts := strings.Split(tagString, ", ")
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		if !strings.HasPrefix(part, tagName+"=") {
			filtered = append(filtered, part)
		}
	}
	return strings.Join(filtered, ", ")
}
