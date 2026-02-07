package schema

import (
	"encoding/json"
	"fmt"
	"maps"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"golang.org/x/sync/errgroup"
)

// this represents order of tags in JSON schema and go struct
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
	Encoding         string `json:"encoding,omitempty"`          // Data page encoding (PLAIN, RLE, etc)
	OmitStats        string `json:"-"`                           // Control statistics generation (true/false)
	CompressionCodec string `json:"compression_codec,omitempty"` // Compression codec (SNAPPY, GZIP, etc)
}

type SchemaOption struct {
	FailOnInt96          bool
	SkipPageEncoding     bool
	WithCompressionCodec bool
}

// readFirstDataPageEncoding reads the first data page header to get its encoding.
// Uses the parquet-go library's GetFirstDataPageHeader which efficiently reads only
// the first data page, skipping any dictionary pages.
func readFirstDataPageEncoding(pr *reader.ParquetReader, rowGroupIndex, columnIndex int) (parquet.Encoding, error) {
	// Use parquet-go's GetFirstDataPageHeader which correctly handles:
	// - Dictionary pages at DataPageOffset
	// - Proper offset calculation including header sizes
	// - CRC and other page header variations
	headerInfo, err := pr.GetFirstDataPageHeader(rowGroupIndex, columnIndex)
	if err != nil {
		return 0, fmt.Errorf("read first data page header: %w", err)
	}

	return headerInfo.Encoding, nil
}

// buildEncodingMap extracts encoding information from row groups by reading the first data page header.
// For each column, it reads the page header at DataPageOffset to get the actual data page encoding.
// Note: Parquet files should use consistent encodings across row groups for the same column.
// This function reads columns in parallel to speed up remote file access.
func buildEncodingMap(pr *reader.ParquetReader) map[string]string {
	result := make(map[string]string)

	// Use the first row group to extract encodings
	if len(pr.Footer.RowGroups) == 0 {
		return result
	}

	columns := pr.Footer.RowGroups[0].Columns

	// Use a mutex to protect concurrent writes to the result map
	var mu sync.Mutex

	// Process columns in parallel, use runtime.NumCPU() to match available cores
	g := new(errgroup.Group)
	g.SetLimit(runtime.NumCPU())

	for colIndex, col := range columns {
		colIndex, col := colIndex, col // capture loop variables
		g.Go(func() error {
			pathKey := strings.Join(col.MetaData.PathInSchema, common.PAR_GO_PATH_DELIMITER)

			// Clone the reader to get a dedicated file handle for concurrent access
			// This is necessary because io.ReadSeeker operations (Seek/Read) are not thread-safe
			clonedFile, err := pr.PFile.Clone()
			if err != nil {
				// If cloning fails, skip this column's encoding
				return nil
			}
			defer func() {
				_ = clonedFile.Close()
			}()

			// Create a temporary reader with the cloned file
			clonedReader := &reader.ParquetReader{
				PFile:         clonedFile,
				Footer:        pr.Footer,
				SchemaHandler: pr.SchemaHandler,
			}

			// Read just the first data page header to get encoding
			encoding, err := readFirstDataPageEncoding(clonedReader, 0, colIndex)
			if err != nil {
				// If we can't read the data page encoding, omit it from the schema.
				// This lets the writer choose an appropriate default encoding for the type.
				// Note: We don't try to guess from col.MetaData.Encodings because it mixes
				// data encodings with definition/repetition level encodings (RLE, BIT_PACKED).
				return nil
			}

			mu.Lock()
			result[pathKey] = encoding.String()
			mu.Unlock()
			return nil
		})
	}

	// Wait for all goroutines to complete, ignoring errors since we handle them inline
	_ = g.Wait()
	return result
}

// buildCompressionCodecMap extracts compression codec information from the footer metadata.
// This is a fast operation as it only reads from the already-loaded footer.
func buildCompressionCodecMap(pr *reader.ParquetReader) map[string]string {
	result := make(map[string]string)

	if len(pr.Footer.RowGroups) == 0 {
		return result
	}

	// Use the first row group to extract compression codecs
	columns := pr.Footer.RowGroups[0].Columns
	for _, col := range columns {
		pathKey := strings.Join(col.MetaData.PathInSchema, common.PAR_GO_PATH_DELIMITER)
		result[pathKey] = col.MetaData.Codec.String()
	}

	return result
}

func NewSchemaTree(reader *reader.ParquetReader, option SchemaOption) (*SchemaNode, error) {
	// Extract encoding information from the parquet file unless SkipPageEncoding is set
	var encodingMap map[string]string
	if !option.SkipPageEncoding {
		encodingMap = buildEncodingMap(reader)
	}

	// Extract compression codec information if requested
	var compressionCodecMap map[string]string
	if option.WithCompressionCodec {
		compressionCodecMap = buildCompressionCodecMap(reader)
	}

	schemas := reader.SchemaHandler.SchemaElements
	root := &SchemaNode{
		SchemaElement: *schemas[0],
		Children:      []*SchemaNode{},
		InNamePath:    []string{schemas[0].Name},
		ExNamePath:    strings.Split(reader.SchemaHandler.InPathToExPath[schemas[0].Name], common.PAR_GO_PATH_DELIMITER)[:1],
	}
	stack := []*SchemaNode{root}

	for pos := 1; len(stack) > 0; {
		node := stack[len(stack)-1]
		if option.FailOnInt96 && node.Type != nil && *node.Type == parquet.Type_INT96 {
			return nil, fmt.Errorf("field %s has type INT96 which is not supported", node.Name)
		}
		if len(node.Children) < int(node.GetNumChildren()) {
			childNode := &SchemaNode{
				SchemaElement: *schemas[pos],
				Children:      []*SchemaNode{},
			}

			// append() does not always return new slice, so we need to copy the old slice
			childNode.InNamePath = make([]string, len(node.InNamePath)+1)
			copy(childNode.InNamePath, node.InNamePath)
			childNode.InNamePath[len(node.InNamePath)] = schemas[pos].Name

			inPathKey := strings.Join(childNode.InNamePath, common.PAR_GO_PATH_DELIMITER)
			childNode.ExNamePath = strings.Split(reader.SchemaHandler.InPathToExPath[inPathKey], common.PAR_GO_PATH_DELIMITER)

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

	queue := []*SchemaNode{root}
	for len(queue) > 0 {
		node := queue[0]
		queue = append(queue[1:], node.Children...)
		node.Name = node.ExNamePath[len(node.ExNamePath)-1]

		// Populate encoding and compression codec information for leaf nodes
		if node.Type != nil {
			pathKey := strings.Join(node.InNamePath[1:], common.PAR_GO_PATH_DELIMITER)
			if encodingMap != nil {
				if encoding, found := encodingMap[pathKey]; found {
					node.Encoding = encoding
				}
			}
			if compressionCodecMap != nil {
				if codec, found := compressionCodecMap[pathKey]; found {
					node.CompressionCodec = codec
				}
			}
		}
	}
	return root, nil
}

func (s *SchemaNode) GetTagMap() map[string]string {
	tagMap := map[string]string{
		"repetitiontype": repetitionTyeStr(s.SchemaElement),
		"type":           typeStr(s.SchemaElement),
		"name":           s.Name,
	}

	if len(s.ExNamePath) != 0 {
		tagMap["name"] = s.ExNamePath[len(s.ExNamePath)-1]
	}

	if len(s.InNamePath) != 0 {
		tagMap["inname"] = s.InNamePath[len(s.InNamePath)-1]
	}

	if tagMap["type"] == "STRUCT" && s.LogicalType == nil && s.ConvertedType == nil {
		return tagMap
	}

	if s.Type != nil && *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		tagMap["length"] = fmt.Sprint(*s.TypeLength)
	}

	if s.LogicalType != nil {
		s.updateTagFromLogicalType(tagMap)
	}

	// Add custom parquet-go writer directives before updateTagFromConvertedType
	// so that LIST/MAP elements can include valueencoding/keyencoding tags
	if s.Encoding != "" {
		tagMap["encoding"] = s.Encoding
	}
	if s.CompressionCodec != "" {
		tagMap["compression"] = s.CompressionCodec
	}
	if s.OmitStats != "" {
		tagMap["omitstats"] = s.OmitStats
	}

	if s.ConvertedType != nil {
		s.updateTagFromConvertedType(tagMap)
	}

	return tagMap
}

func (s *SchemaNode) getTagMapWithPrefix(prefix string) map[string]string {
	tagMap := s.GetTagMap()
	ret := map[string]string{}
	for _, tag := range orderedTags {
		if tag == "name" || strings.HasPrefix(tag, "key") || strings.HasPrefix(tag, "value") {
			// these are tags that should never have prefix
			continue
		}
		if val, found := tagMap[tag]; found {
			ret[prefix+tag] = val
		}
	}

	return ret
}

func (s *SchemaNode) updateTagForList(tagMap map[string]string) {
	if len(s.Children) == 0 {
		return
	}

	if s.Children[0].LogicalType != nil {
		// LIST => Element (of scalar type)
		s.Children[0].Name = "Element"
		*s.Children[0].RepetitionType = parquet.FieldRepetitionType_REQUIRED
		maps.Copy(tagMap, s.Children[0].getTagMapWithPrefix("value"))
		return
	}

	if len(s.Children[0].Children) > 1 {
		// LIST => Element (of STRUCT)
		s.Children[0].Name = "Element"
		s.Children[0].Type = nil
		s.Children[0].ConvertedType = nil
		*s.Children[0].RepetitionType = parquet.FieldRepetitionType_REQUIRED
		return
	}

	if len(s.Children[0].Children) == 1 {
		// LIST => List => Element
		maps.Copy(tagMap, s.Children[0].Children[0].getTagMapWithPrefix("value"))
		s.Children = s.Children[0].Children
		s.Children[0].Name = "Element"
		return
	}
}

func (s *SchemaNode) updateTagForMap(tagMap map[string]string) {
	if len(s.Children) == 0 || s.Children[0] == nil || len(s.Children[0].Children) == 0 {
		// meaningless interim layer
		return
	}
	if s.Children[0].ConvertedType != nil && *s.Children[0].ConvertedType != parquet.ConvertedType_MAP_KEY_VALUE {
		// child nodes have been processed
		return
	}

	// MAP has schema structure of MAP->MAP_KEY_VALUE->(Field1, Field2)
	// expected output is MAP->(Key, Value)
	maps.Copy(tagMap, s.Children[0].Children[0].getTagMapWithPrefix("key"))
	maps.Copy(tagMap, s.Children[0].Children[1].getTagMapWithPrefix("value"))
	s.Children = s.Children[0].Children[0:2]
	s.Children[0].Name = "Key"
	s.Children[1].Name = "Value"
}

func (s *SchemaNode) updateTagFromConvertedType(tagMap map[string]string) {
	if s.ConvertedType == nil {
		return
	}

	tagMap["convertedtype"] = s.ConvertedType.String()

	switch *s.ConvertedType {
	case parquet.ConvertedType_LIST:
		s.updateTagForList(tagMap)
	case parquet.ConvertedType_MAP:
		s.updateTagForMap(tagMap)
	case parquet.ConvertedType_DECIMAL:
		tagMap["scale"] = fmt.Sprint(*s.Scale)
		tagMap["precision"] = fmt.Sprint(*s.Precision)
		if *s.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			tagMap["length"] = fmt.Sprint(*s.TypeLength)
		}
	case parquet.ConvertedType_INTERVAL:
		tagMap["length"] = "12"
	}
}

func (s *SchemaNode) updateTagFromLogicalType(tagMap map[string]string) {
	if s.LogicalType == nil {
		return
	}

	switch {
	case s.LogicalType.IsSetBSON():
		tagMap["logicaltype"] = "BSON"
	case s.LogicalType.IsSetDATE():
		tagMap["logicaltype"] = "DATE"
	case s.LogicalType.IsSetDECIMAL():
		tagMap["logicaltype"] = "DECIMAL"
		tagMap["logicaltype.precision"] = fmt.Sprint(s.LogicalType.DECIMAL.Precision)
		tagMap["logicaltype.scale"] = fmt.Sprint(s.LogicalType.DECIMAL.Scale)
	case s.LogicalType.IsSetENUM():
		tagMap["logicaltype"] = "ENUM"
	case s.LogicalType.IsSetFLOAT16():
		tagMap["logicaltype"] = "FLOAT16"
	case s.LogicalType.IsSetGEOGRAPHY():
		tagMap["logicaltype"] = "GEOGRAPHY"
	case s.LogicalType.IsSetGEOMETRY():
		tagMap["logicaltype"] = "GEOMETRY"
	case s.LogicalType.IsSetINTEGER():
		tagMap["logicaltype"] = "INTEGER"
		tagMap["logicaltype.bitwidth"] = fmt.Sprint(s.LogicalType.INTEGER.BitWidth)
		tagMap["logicaltype.issigned"] = fmt.Sprint(s.LogicalType.INTEGER.IsSigned)
	case s.LogicalType.IsSetJSON():
		tagMap["logicaltype"] = "JSON"
	case s.LogicalType.IsSetVARIANT():
		// VARIANT is a semi-structured logical type introduced in newer parquet-format
		tagMap["logicaltype"] = "VARIANT"
		for _, child := range s.Children {
			if child.Encoding != "" {
				tagMap["encoding"] = child.Encoding
			}
			if child.CompressionCodec != "" {
				tagMap["compression"] = child.CompressionCodec
			}
			if tagMap["encoding"] != "" && tagMap["compression"] != "" {
				break
			}
		}
	case s.LogicalType.IsSetSTRING():
		tagMap["logicaltype"] = "STRING"
	case s.LogicalType.IsSetTIME():
		tagMap["logicaltype"] = "TIME"
		tagMap["logicaltype.isadjustedtoutc"] = fmt.Sprint(s.LogicalType.TIME.IsAdjustedToUTC)
		tagMap["logicaltype.unit"] = timeUnitToTag(s.LogicalType.TIME.Unit)
	case s.LogicalType.IsSetTIMESTAMP():
		tagMap["logicaltype"] = "TIMESTAMP"
		tagMap["logicaltype.isadjustedtoutc"] = fmt.Sprint(s.LogicalType.TIMESTAMP.IsAdjustedToUTC)
		tagMap["logicaltype.unit"] = timeUnitToTag(s.LogicalType.TIMESTAMP.Unit)
	case s.LogicalType.IsSetUUID():
		tagMap["logicaltype"] = "UUID"
	case s.LogicalType.IsSetUNKNOWN():
		tagMap["logicaltype"] = "UNKNOWN"
	}
}

func (s *SchemaNode) GetPathMap() map[string]*SchemaNode {
	retVal := map[string]*SchemaNode{}
	queue := []*SchemaNode{s}
	for len(queue) > 0 {
		node := queue[0]
		queue = append(queue[1:], node.Children...)
		retVal[strings.Join(node.InNamePath[1:], common.PAR_GO_PATH_DELIMITER)] = node
	}
	return retVal
}

func typeStr(se parquet.SchemaElement) string {
	if se.Type != nil {
		return se.Type.String()
	}
	if se.LogicalType != nil && se.LogicalType.IsSetVARIANT() {
		return "VARIANT"
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

func timeUnitToTag(timeUnit *parquet.TimeUnit) string {
	if timeUnit == nil {
		return ""
	}
	if timeUnit.IsSetNANOS() {
		return "NANOS"
	}
	if timeUnit.IsSetMICROS() {
		return "MICROS"
	}
	if timeUnit.IsSetMILLIS() {
		return "MILLIS"
	}
	return "UNKNOWN_UNIT"
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

// EncodingToString converts a slice of parquet encodings to sorted strings.
func EncodingToString(encodings []parquet.Encoding) []string {
	ret := make([]string, len(encodings))
	for i := range encodings {
		ret[i] = encodings[i].String()
	}
	sort.Strings(ret)
	return ret
}
