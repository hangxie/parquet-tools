package retype

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/types"
	"go.mongodb.org/mongo-driver/v2/bson"

	pschema "github.com/hangxie/parquet-tools/schema"
)

// RuleID identifies a retype rule.
type RuleID int

const (
	// RuleInt96ToTimestamp converts INT96 columns to TIMESTAMP_NANOS.
	RuleInt96ToTimestamp RuleID = iota
	// RuleBsonToString converts BSON columns to plain strings.
	RuleBsonToString
	// RuleJsonToString removes JSON logical type from columns.
	RuleJsonToString
	// RuleFloat16ToFloat32 converts FLOAT16 columns to FLOAT32.
	RuleFloat16ToFloat32
	// RuleVariantToString converts VARIANT columns to plain strings.
	RuleVariantToString
	// RuleUuidToString converts UUID columns to plain strings.
	RuleUuidToString
	// RuleRepeatedToList converts legacy repeated primitives to LIST format.
	RuleRepeatedToList
	// RuleGeoToBinary removes GEOGRAPHY and GEOMETRY logical types.
	RuleGeoToBinary
)

// getActiveRules returns the list of rules enabled by CLI flags.
func (c Cmd) getActiveRules() []*RetypeRule {
	var rules []*RetypeRule

	if c.Int96ToTimestamp {
		rules = append(rules, RuleRegistry[RuleInt96ToTimestamp])
	}
	if c.BsonToString {
		rules = append(rules, RuleRegistry[RuleBsonToString])
	}
	if c.JsonToString {
		rules = append(rules, RuleRegistry[RuleJsonToString])
	}
	if c.Float16ToFloat32 {
		rules = append(rules, RuleRegistry[RuleFloat16ToFloat32])
	}
	if c.VariantToString {
		rules = append(rules, RuleRegistry[RuleVariantToString])
	}
	if c.UuidToString {
		rules = append(rules, RuleRegistry[RuleUuidToString])
	}
	if c.RepeatedToList {
		rules = append(rules, RuleRegistry[RuleRepeatedToList])
	}
	if c.GeoToBinary {
		rules = append(rules, RuleRegistry[RuleGeoToBinary])
	}

	return rules
}

// RetypeRule defines a transformation rule for converting Parquet column types.
// Each rule specifies how to match columns, transform their schema, and optionally convert data.
type RetypeRule struct {
	// Name is the identifier for this rule (e.g., "int96-to-timestamp")
	Name string

	// MatchSchema returns true if the schema node should be transformed by this rule
	MatchSchema func(node, parent *pschema.SchemaNode) bool

	// TransformSchema modifies the schema node in place
	TransformSchema func(*pschema.SchemaNode)

	// ConvertData converts a field value. Returns the converted value.
	// If nil, no data conversion is performed (schema-only change).
	ConvertData func(value any) (any, error)

	// TargetType is the Go type for converted fields.
	// If nil, the type remains unchanged (string to string conversions).
	TargetType reflect.Type

	// InputKind enforces the input kind for the rule.
	// If set to reflect.Invalid (default), any input kind is accepted.
	// Used to filter out false positive name matches in nested structures.
	InputKind reflect.Kind
}

// listElementWrapper wraps a primitive value for 3-level LIST structure.
type listElementWrapper struct {
	Element any `parquet:"element"`
}

// listWrapper wraps the list for 3-level LIST structure.
type listWrapper struct {
	List []listElementWrapper `parquet:"list"`
}

// RuleRegistry contains all available retype rules indexed by name.
var RuleRegistry = map[RuleID]*RetypeRule{
	RuleInt96ToTimestamp: {
		Name: "int96-to-timestamp",
		MatchSchema: func(node, parent *pschema.SchemaNode) bool {
			return node.Type != nil && *node.Type == parquet.Type_INT96
		},
		TransformSchema: func(node *pschema.SchemaNode) {
			node.Type = new(parquet.Type_INT64)
			node.LogicalType = &parquet.LogicalType{
				TIMESTAMP: &parquet.TimestampType{
					IsAdjustedToUTC: true,
					Unit: &parquet.TimeUnit{
						NANOS: &parquet.NanoSeconds{},
					},
				},
			}
			node.ConvertedType = nil
		},
		ConvertData: func(value any) (any, error) {
			s, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("expected string for INT96, got %T", value)
			}
			return int96ToNanos(s)
		},
		TargetType: reflect.TypeFor[int64](),
		InputKind:  reflect.String,
	},
	RuleBsonToString: {
		Name: "bson-to-string",
		MatchSchema: func(node, parent *pschema.SchemaNode) bool {
			return node.LogicalType != nil && node.LogicalType.IsSetBSON()
		},
		TransformSchema: func(node *pschema.SchemaNode) {
			// Remove BSON logical type, making it a plain BYTE_ARRAY (string)
			node.LogicalType = &parquet.LogicalType{
				STRING: &parquet.StringType{},
			}
			node.ConvertedType = nil
		},
		ConvertData: func(value any) (any, error) {
			s, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("expected string for BSON, got %T", value)
			}
			return bsonToJSONString(s)
		},
		TargetType: nil, // string -> string
		InputKind:  reflect.String,
	},
	RuleJsonToString: {
		Name: "json-to-string",
		MatchSchema: func(node, parent *pschema.SchemaNode) bool {
			return node.LogicalType != nil && node.LogicalType.IsSetJSON()
		},
		TransformSchema: func(node *pschema.SchemaNode) {
			// Remove JSON logical type, making it a plain BYTE_ARRAY (string)
			node.LogicalType = &parquet.LogicalType{
				STRING: &parquet.StringType{},
			}
			node.ConvertedType = nil
		},
		ConvertData: nil, // No data conversion needed
		TargetType:  nil, // string -> string
	},
	RuleFloat16ToFloat32: {
		Name: "float16-to-float32",
		MatchSchema: func(node, parent *pschema.SchemaNode) bool {
			return node.LogicalType != nil && node.LogicalType.IsSetFLOAT16()
		},
		TransformSchema: func(node *pschema.SchemaNode) {
			node.Type = new(parquet.Type_FLOAT)
			node.LogicalType = nil
			node.TypeLength = nil
		},
		ConvertData: func(value any) (any, error) {
			s, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("expected string for FLOAT16, got %T", value)
			}
			if len(s) != 2 {
				return nil, fmt.Errorf("float16 requires 2 bytes, got %d", len(s))
			}
			return types.ConvertFloat16LogicalValue(s), nil
		},
		TargetType: reflect.TypeFor[float32](),
		InputKind:  reflect.String,
	},
	RuleVariantToString: {
		Name: "variant-to-string",
		MatchSchema: func(node, parent *pschema.SchemaNode) bool {
			return node.LogicalType != nil && node.LogicalType.IsSetVARIANT()
		},
		TransformSchema: func(node *pschema.SchemaNode) {
			// Remove VARIANT logical type, making it a plain BYTE_ARRAY (string)
			node.Type = new(parquet.Type_BYTE_ARRAY)
			node.LogicalType = &parquet.LogicalType{
				STRING: &parquet.StringType{},
			}
			node.ConvertedType = new(parquet.ConvertedType_UTF8)
		},
		ConvertData: func(value any) (any, error) {
			jsonData, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal VARIANT to JSON: %w", err)
			}
			return string(jsonData), nil
		},
		TargetType: reflect.TypeFor[string](),
	},
	RuleUuidToString: {
		Name: "uuid-to-string",
		MatchSchema: func(node, parent *pschema.SchemaNode) bool {
			return node.LogicalType != nil && node.LogicalType.IsSetUUID()
		},
		TransformSchema: func(node *pschema.SchemaNode) {
			// Remove UUID logical type, making it a plain BYTE_ARRAY (string)
			node.Type = new(parquet.Type_BYTE_ARRAY)
			node.LogicalType = &parquet.LogicalType{
				STRING: &parquet.StringType{},
			}
			node.ConvertedType = new(parquet.ConvertedType_UTF8)
			node.TypeLength = nil
		},
		ConvertData: func(value any) (any, error) {
			s, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("expected string for UUID, got %T", value)
			}
			if len(s) != 16 {
				return nil, fmt.Errorf("UUID requires 16 bytes, got %d", len(s))
			}
			b := []byte(s)
			return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
		},
		TargetType: reflect.TypeFor[string](),
		InputKind:  reflect.String,
	},
	RuleRepeatedToList: {
		Name: "repeated-to-list",
		MatchSchema: func(node, parent *pschema.SchemaNode) bool {
			// Check if parent is MAP or LIST
			if parent != nil {
				if parent.LogicalType != nil && (parent.LogicalType.IsSetMAP() || parent.LogicalType.IsSetLIST()) {
					return false
				}
				if parent.ConvertedType != nil && (*parent.ConvertedType == parquet.ConvertedType_MAP || *parent.ConvertedType == parquet.ConvertedType_LIST) {
					return false
				}
			}

			return node.RepetitionType != nil && *node.RepetitionType == parquet.FieldRepetitionType_REPEATED &&
				(node.ConvertedType == nil || *node.ConvertedType != parquet.ConvertedType_LIST) &&
				(node.LogicalType == nil || !node.LogicalType.IsSetLIST()) &&
				(node.LogicalType == nil || !node.LogicalType.IsSetMAP())
		},
		TransformSchema: func(node *pschema.SchemaNode) {
			// Force copy to avoid aliasing when appending
			inPath := node.InNamePath[:len(node.InNamePath):len(node.InNamePath)]
			exPath := node.ExNamePath[:len(node.ExNamePath):len(node.ExNamePath)]

			// Create element node (copy of original primitive)
			// Element is REQUIRED (not OPTIONAL) to match parquet-go's expected LIST structure
			element := &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Name:           "element",
					Type:           node.Type,
					TypeLength:     node.TypeLength,
					RepetitionType: new(parquet.FieldRepetitionType_REQUIRED),
					ConvertedType:  node.ConvertedType,
					Scale:          node.Scale,
					Precision:      node.Precision,
					FieldID:        nil, // Clear FieldID to avoid conflicts
					LogicalType:    node.LogicalType,
				},
				// Children should be nil for primitive
				InNamePath:       append(inPath, "List", "Element"),
				ExNamePath:       append(exPath, "list", "element"),
				Encoding:         node.Encoding,
				CompressionCodec: node.CompressionCodec,
				OmitStats:        node.OmitStats,
			}

			// Create list node (repeated group)
			list := &pschema.SchemaNode{
				SchemaElement: parquet.SchemaElement{
					Name:           "list",
					RepetitionType: new(parquet.FieldRepetitionType_REPEATED),
				},
				Children:   []*pschema.SchemaNode{element},
				InNamePath: append(inPath, "List"),
				ExNamePath: append(exPath, "list"),
			}

			// Transform original node to LIST Group
			node.Type = nil
			node.TypeLength = nil
			node.RepetitionType = new(parquet.FieldRepetitionType_REQUIRED)
			node.ConvertedType = new(parquet.ConvertedType_LIST)
			node.LogicalType = &parquet.LogicalType{LIST: &parquet.ListType{}}
			node.Children = []*pschema.SchemaNode{list}

			// Clear leaf properties from the group node
			node.Scale = nil
			node.Precision = nil
			node.FieldID = nil
			node.Encoding = ""
			node.CompressionCodec = ""
			node.OmitStats = ""
		},
		// ConvertData is needed to restructure the data into 3-level LIST format.
		ConvertData: func(value any) (any, error) {
			val := reflect.ValueOf(value)
			if val.Kind() != reflect.Slice {
				return nil, fmt.Errorf("expected slice for repeated field, got %T", value)
			}
			if val.IsNil() {
				return nil, nil
			}

			// Create slice of elements
			list := make([]listElementWrapper, val.Len())
			for i := range val.Len() {
				list[i] = listElementWrapper{
					Element: val.Index(i).Interface(),
				}
			}

			// Wrap in 3-level structure: Field -> List -> Element
			return listWrapper{
				List: list,
			}, nil
		},
		TargetType: reflect.TypeFor[listWrapper](),
	},
	RuleGeoToBinary: {
		Name: "geo-to-binary",

		MatchSchema: func(node, parent *pschema.SchemaNode) bool {
			return node.LogicalType != nil && (node.LogicalType.IsSetGEOMETRY() || node.LogicalType.IsSetGEOGRAPHY())
		},
		TransformSchema: func(node *pschema.SchemaNode) {
			node.LogicalType = nil
			node.ConvertedType = nil
		},
		ConvertData: nil,
		TargetType:  nil,
	},
}

// applyRule recursively applies a transformation rule to matching schema nodes.
// Returns a set of field names (external names) that were matched for O(1) lookup during data conversion.
func applyRule(s, parent *pschema.SchemaNode, match func(node, parent *pschema.SchemaNode) bool, transform func(*pschema.SchemaNode)) map[string]struct{} {
	matchedFields := make(map[string]struct{})

	// Process this node if it matches
	if match(s, parent) {
		transform(s)
		// Record the internal field name; ReadByNumber's dynamic structs use InName as
		// the Go struct field name, so findConverterForField must match it.
		if len(s.InNamePath) > 0 {
			matchedFields[s.InNamePath[len(s.InNamePath)-1]] = struct{}{}
		}
	}

	// Recursively process children
	for _, child := range s.Children {
		maps.Copy(matchedFields, applyRule(child, s, match, transform))
	}

	return matchedFields
}

// int96ToNanos converts an INT96 encoded string to nanoseconds since epoch.
func int96ToNanos(value string) (any, error) {
	timestamp, err := types.INT96ToTime(value)
	if err != nil {
		return nil, fmt.Errorf("INT96 conversion failed: %w", err)
	}
	return timestamp.UnixNano(), nil
}

// bsonToJSONString converts BSON binary data to a JSON string.
func bsonToJSONString(value string) (any, error) {
	bsonData := []byte(value)
	var doc bson.M
	if err := bson.Unmarshal(bsonData, &doc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal BSON: %w", err)
	}
	jsonData, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal to JSON: %w", err)
	}
	return string(jsonData), nil
}
