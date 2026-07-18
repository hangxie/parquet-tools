package schema

import (
	"encoding/json"
	"fmt"
	"strings"

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
