package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/types"
	"github.com/hangxie/parquet-go/v2/writer"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/sync/errgroup"

	"github.com/hangxie/parquet-go/v2/common"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

// RetypeCmd is a kong command for retype
type RetypeCmd struct {
	Int96ToTimestamp bool   `name:"int96-to-timestamp" help:"Convert INT96 columns to TIMESTAMP_NANOS." default:"false"`
	BsonToString     bool   `name:"bson-to-string" help:"Convert BSON columns to plain strings (JSON encoded)." default:"false"`
	JsonToString     bool   `name:"json-to-string" help:"Remove JSON logical type from columns." default:"false"`
	Float16ToFloat32 bool   `name:"float16-to-float32" help:"Convert FLOAT16 columns to FLOAT32." default:"false"`
	ReadPageSize     int    `help:"Page size to read from Parquet." default:"1000"`
	Source           string `short:"s" help:"Source Parquet file to retype." required:"true"`
	URI              string `arg:"" predictor:"file" help:"URI of output Parquet file."`
	pio.ReadOption
	pio.WriteOption
}

// Run does actual retype job
func (c RetypeCmd) Run() error {
	if c.ReadPageSize < 1 {
		return fmt.Errorf("invalid read page size %d, needs to be at least 1", c.ReadPageSize)
	}

	// Open source file
	fileReader, err := pio.NewParquetFileReader(c.Source, c.ReadOption)
	if err != nil {
		return fmt.Errorf("failed to read from [%s]: %w", c.Source, err)
	}
	defer func() {
		_ = fileReader.PFile.Close()
	}()

	// Get schema from source
	schemaTree, err := pschema.NewSchemaTree(fileReader, pschema.SchemaOption{WithCompressionCodec: true})
	if err != nil {
		return err
	}

	// Get active rules and apply them to schema
	activeRules := c.getActiveRules()
	matchedFields := make([]map[string]struct{}, len(activeRules))
	for i, rule := range activeRules {
		matchedFields[i] = applyRule(schemaTree, rule.MatchSchema, rule.TransformSchema)
	}

	// Create converter for data transformation
	converter := NewConverter(activeRules, matchedFields)

	// Generate JSON schema from (possibly modified) SchemaTree
	schemaJson := schemaTree.JSONSchema()

	// Create output file with new settings
	fileWriter, err := pio.NewGenericWriter(c.URI, c.WriteOption, schemaJson)
	if err != nil {
		return fmt.Errorf("failed to write to [%s]: %w", c.URI, err)
	}
	defer func() {
		_ = fileWriter.WriteStop()
		_ = fileWriter.PFile.Close()
	}()

	// Dedicated goroutine for output to ensure output integrity
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writerGroup, _ := errgroup.WithContext(ctx)
	writerChan := make(chan any, c.ReadPageSize)
	writerGroup.Go(func() error {
		return c.writer(ctx, fileWriter, writerChan)
	})

	// Single reader goroutine
	readerGroup, _ := errgroup.WithContext(ctx)
	readerGroup.Go(func() error {
		return c.reader(ctx, fileReader, converter, writerChan)
	})

	if err := readerGroup.Wait(); err != nil {
		return err
	}
	close(writerChan)

	if err := writerGroup.Wait(); err != nil {
		return err
	}

	if err := fileWriter.WriteStop(); err != nil {
		return fmt.Errorf("failed to end write [%s]: %w", c.URI, err)
	}
	if err := fileWriter.PFile.Close(); err != nil {
		return fmt.Errorf("failed to close [%s]: %w", c.URI, err)
	}

	return nil
}

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
)

// getActiveRules returns the list of rules enabled by CLI flags.
func (c RetypeCmd) getActiveRules() []*RetypeRule {
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

	return rules
}

func (c RetypeCmd) writer(ctx context.Context, fileWriter *writer.ParquetWriter, writerChan chan any) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case row, more := <-writerChan:
			if !more {
				return nil
			}

			if err := fileWriter.Write(row); err != nil {
				return fmt.Errorf("failed to write data to [%s]: %w", c.URI, err)
			}
		}
	}
}

func (c RetypeCmd) reader(ctx context.Context, fileReader *reader.ParquetReader, converter *Converter, writerChan chan any) error {
	for {
		rows, err := fileReader.ReadByNumber(c.ReadPageSize)
		if err != nil {
			return fmt.Errorf("failed to read from [%s]: %w", c.Source, err)
		}
		if len(rows) == 0 {
			return nil
		}
		for _, row := range rows {
			row, err = converter.Convert(row)
			if err != nil {
				return fmt.Errorf("failed to convert row: %w", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case writerChan <- row:
			}
		}
	}
}

// RetypeRule defines a transformation rule for converting Parquet column types.
// Each rule specifies how to match columns, transform their schema, and optionally convert data.
type RetypeRule struct {
	// Name is the identifier for this rule (e.g., "int96-to-timestamp")
	Name string

	// MatchSchema returns true if the schema node should be transformed by this rule
	MatchSchema func(*pschema.SchemaNode) bool

	// TransformSchema modifies the schema node in place
	TransformSchema func(*pschema.SchemaNode)

	// ConvertData converts a field value. Returns the converted value.
	// If nil, no data conversion is performed (schema-only change).
	ConvertData func(value string) (any, error)

	// TargetType is the Go type for converted fields.
	// If nil, the type remains unchanged (string to string conversions).
	TargetType reflect.Type
}

// RuleRegistry contains all available retype rules indexed by name.
var RuleRegistry = map[RuleID]*RetypeRule{
	RuleInt96ToTimestamp: {
		Name: "int96-to-timestamp",
		MatchSchema: func(node *pschema.SchemaNode) bool {
			return node.Type != nil && *node.Type == parquet.Type_INT96
		},
		TransformSchema: func(node *pschema.SchemaNode) {
			node.Type = common.ToPtr(parquet.Type_INT64)
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
		ConvertData: int96ToNanos,
		TargetType:  reflect.TypeFor[int64](),
	},
	RuleBsonToString: {
		Name: "bson-to-string",
		MatchSchema: func(node *pschema.SchemaNode) bool {
			return node.LogicalType != nil && node.LogicalType.IsSetBSON()
		},
		TransformSchema: func(node *pschema.SchemaNode) {
			// Remove BSON logical type, making it a plain BYTE_ARRAY (string)
			node.LogicalType = &parquet.LogicalType{
				STRING: &parquet.StringType{},
			}
			node.ConvertedType = nil
		},
		ConvertData: bsonToJSONString,
		TargetType:  nil, // string -> string
	},
	RuleJsonToString: {
		Name: "json-to-string",
		MatchSchema: func(node *pschema.SchemaNode) bool {
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
		MatchSchema: func(node *pschema.SchemaNode) bool {
			return node.LogicalType != nil && node.LogicalType.IsSetFLOAT16()
		},
		TransformSchema: func(node *pschema.SchemaNode) {
			node.Type = common.ToPtr(parquet.Type_FLOAT)
			node.LogicalType = nil
			node.TypeLength = nil
		},
		ConvertData: func(value string) (any, error) {
			if len(value) != 2 {
				return nil, fmt.Errorf("float16 requires 2 bytes, got %d", len(value))
			}
			return types.ConvertFloat16LogicalValue(value), nil
		},
		TargetType: reflect.TypeFor[float32](),
	},
}

// applyRule recursively applies a transformation rule to matching schema nodes.
// Returns a set of field names (external names) that were matched for O(1) lookup during data conversion.
func applyRule(s *pschema.SchemaNode, match func(*pschema.SchemaNode) bool, transform func(*pschema.SchemaNode)) map[string]struct{} {
	matchedFields := make(map[string]struct{})

	// Process this node if it matches
	if match(s) {
		transform(s)
		// Record the field name for data conversion
		if len(s.ExNamePath) > 0 {
			matchedFields[s.ExNamePath[len(s.ExNamePath)-1]] = struct{}{}
		}
	}

	// Recursively process children
	for _, child := range s.Children {
		maps.Copy(matchedFields, applyRule(child, match, transform))
	}

	return matchedFields
}

// int96ToNanos converts an INT96 encoded string to nanoseconds since epoch.
func int96ToNanos(value string) (any, error) {
	timestamp, err := types.INT96ToTimeWithError(value)
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

// FieldConverter pairs a rule with its matched fields.
type FieldConverter struct {
	Rule   *RetypeRule
	Fields map[string]struct{}
}

// Converter handles data conversion for multiple rules.
type Converter struct {
	converters      []*FieldConverter
	typeCache       map[reflect.Type]reflect.Type
	needsTypeChange bool
}

// NewConverter creates a converter for the given rules and their matched fields.
func NewConverter(rules []*RetypeRule, matchedFields []map[string]struct{}) *Converter {
	converters := make([]*FieldConverter, 0, len(rules))
	needsTypeChange := false

	for i, rule := range rules {
		if rule.ConvertData == nil {
			continue
		}
		converters = append(converters, &FieldConverter{
			Rule:   rule,
			Fields: matchedFields[i],
		})
		if rule.TargetType != nil {
			needsTypeChange = true
		}
	}

	return &Converter{
		converters:      converters,
		typeCache:       make(map[reflect.Type]reflect.Type),
		needsTypeChange: needsTypeChange,
	}
}

// Convert transforms a row according to all active rules.
func (c *Converter) Convert(row any) (any, error) {
	if len(c.converters) == 0 {
		return row, nil
	}
	return c.convertValue(reflect.ValueOf(row))
}

// convertValue recursively converts a reflect.Value.
func (c *Converter) convertValue(srcVal reflect.Value) (any, error) {
	if srcVal.Kind() == reflect.Pointer {
		if srcVal.IsNil() {
			return nil, nil
		}
		result, err := c.convertValue(srcVal.Elem())
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, nil
		}
		resultVal := reflect.ValueOf(result)
		if resultVal.Kind() == reflect.Pointer {
			return result, nil
		}
		ptr := reflect.New(resultVal.Type())
		ptr.Elem().Set(resultVal)
		return ptr.Interface(), nil
	}

	switch srcVal.Kind() {
	case reflect.Struct:
		return c.convertStruct(srcVal)
	case reflect.Slice:
		return c.convertSlice(srcVal)
	case reflect.Map:
		return c.convertMap(srcVal)
	default:
		return srcVal.Interface(), nil
	}
}

// convertStruct converts a struct value.
func (c *Converter) convertStruct(srcVal reflect.Value) (any, error) {
	srcType := srcVal.Type()
	targetType := c.getOrCreateTargetType(srcType)
	targetVal := reflect.New(targetType).Elem()

	for i := range srcType.NumField() {
		srcField := srcType.Field(i)
		srcFieldVal := srcVal.Field(i)
		targetFieldVal := targetVal.Field(i)

		converter := c.findConverterForField(srcField.Name)
		if converter != nil {
			converted, err := c.convertField(srcFieldVal, converter.Rule, srcField.Name)
			if err != nil {
				return nil, err
			}
			if converted != nil {
				convertedVal := reflect.ValueOf(converted)
				targetFieldVal.Set(convertedVal)
			}
		} else {
			// Recursively convert nested types
			converted, err := c.convertValue(srcFieldVal)
			if err != nil {
				return nil, err
			}
			if converted != nil {
				convertedVal := reflect.ValueOf(converted)
				if convertedVal.Kind() == reflect.Pointer && targetFieldVal.Kind() != reflect.Pointer {
					convertedVal = convertedVal.Elem()
				}
				targetFieldVal.Set(convertedVal)
			}
		}
	}

	return targetVal.Addr().Interface(), nil
}

// convertField applies a rule's conversion to a single field.
func (c *Converter) convertField(srcVal reflect.Value, rule *RetypeRule, fieldName string) (any, error) {
	if srcVal.Kind() == reflect.String {
		result, err := rule.ConvertData(srcVal.String())
		if err != nil {
			return nil, fmt.Errorf("failed to convert field [%s]: %w", fieldName, err)
		}
		return result, nil
	}

	if srcVal.Kind() == reflect.Pointer {
		if srcVal.IsNil() {
			return c.nilPointerForRule(rule), nil
		}
		result, err := rule.ConvertData(srcVal.Elem().String())
		if err != nil {
			return nil, fmt.Errorf("failed to convert field [%s]: %w", fieldName, err)
		}
		// Wrap in pointer
		resultVal := reflect.ValueOf(result)
		ptr := reflect.New(resultVal.Type())
		ptr.Elem().Set(resultVal)
		return ptr.Interface(), nil
	}

	return nil, fmt.Errorf("unexpected type for field [%s]: %s", fieldName, srcVal.Kind())
}

// nilPointerForRule returns a nil pointer of the appropriate type.
func (c *Converter) nilPointerForRule(rule *RetypeRule) any {
	if rule.TargetType != nil {
		return reflect.Zero(reflect.PointerTo(rule.TargetType)).Interface()
	}
	return (*string)(nil)
}

// convertSlice converts each element of a slice.
func (c *Converter) convertSlice(srcVal reflect.Value) (any, error) {
	if srcVal.IsNil() {
		return nil, nil
	}

	elemType := c.getOrCreateTargetTypeForField(srcVal.Type().Elem())
	targetSlice := reflect.MakeSlice(reflect.SliceOf(elemType), srcVal.Len(), srcVal.Len())

	// Check for Element/element fields (Parquet LIST elements)
	converter := c.findConverterForField("Element")
	if converter == nil {
		converter = c.findConverterForField("element")
	}

	for i := range srcVal.Len() {
		elem := srcVal.Index(i)

		if converter != nil && elem.Kind() == reflect.String {
			// Direct string element conversion
			result, err := converter.Rule.ConvertData(elem.String())
			if err != nil {
				return nil, fmt.Errorf("failed to convert list element [%d]: %w", i, err)
			}
			targetSlice.Index(i).Set(reflect.ValueOf(result))
		} else {
			converted, err := c.convertValue(elem)
			if err != nil {
				return nil, err
			}
			if converted != nil {
				convertedVal := reflect.ValueOf(converted)
				if convertedVal.Kind() == reflect.Pointer && elemType.Kind() != reflect.Pointer {
					convertedVal = convertedVal.Elem()
				}
				targetSlice.Index(i).Set(convertedVal)
			}
		}
	}

	return targetSlice.Interface(), nil
}

// convertMap converts each value of a map.
func (c *Converter) convertMap(srcVal reflect.Value) (any, error) {
	if srcVal.IsNil() {
		return nil, nil
	}

	keyType := srcVal.Type().Key()
	valueType := c.getOrCreateTargetTypeForField(srcVal.Type().Elem())
	targetMap := reflect.MakeMap(reflect.MapOf(keyType, valueType))

	// Check for Value/value fields (Parquet MAP values)
	converter := c.findConverterForField("Value")
	if converter == nil {
		converter = c.findConverterForField("value")
	}

	iter := srcVal.MapRange()
	for iter.Next() {
		key := iter.Key()
		val := iter.Value()

		if converter != nil && val.Kind() == reflect.String {
			// Direct string value conversion
			result, err := converter.Rule.ConvertData(val.String())
			if err != nil {
				return nil, fmt.Errorf("failed to convert map value [%v]: %w", key.Interface(), err)
			}
			targetMap.SetMapIndex(key, reflect.ValueOf(result))
		} else {
			converted, err := c.convertValue(val)
			if err != nil {
				return nil, err
			}
			if converted != nil {
				convertedVal := reflect.ValueOf(converted)
				if convertedVal.Kind() == reflect.Pointer && valueType.Kind() != reflect.Pointer {
					convertedVal = convertedVal.Elem()
				}
				targetMap.SetMapIndex(key, convertedVal)
			}
		}
	}

	return targetMap.Interface(), nil
}

// getOrCreateTargetType creates a target struct type with converted field types.
func (c *Converter) getOrCreateTargetType(srcType reflect.Type) reflect.Type {
	if cached, ok := c.typeCache[srcType]; ok {
		return cached
	}

	fields := make([]reflect.StructField, srcType.NumField())
	for i := range srcType.NumField() {
		srcField := srcType.Field(i)
		fields[i] = srcField

		converter := c.findConverterForField(srcField.Name)
		if converter != nil && converter.Rule.TargetType != nil {
			targetType := converter.Rule.TargetType
			if srcField.Type.Kind() == reflect.Pointer {
				fields[i].Type = reflect.PointerTo(targetType)
			} else {
				fields[i].Type = targetType
			}
		} else {
			fields[i].Type = c.getOrCreateTargetTypeForField(srcField.Type)
		}
	}

	targetType := reflect.StructOf(fields)
	c.typeCache[srcType] = targetType
	return targetType
}

// getOrCreateTargetTypeForField creates target types for nested fields.
func (c *Converter) getOrCreateTargetTypeForField(srcType reflect.Type) reflect.Type {
	switch srcType.Kind() {
	case reflect.Struct:
		return c.getOrCreateTargetType(srcType)
	case reflect.Slice:
		elemType := c.getOrCreateTargetTypeForField(srcType.Elem())
		return reflect.SliceOf(elemType)
	case reflect.Map:
		keyType := srcType.Key()
		valueType := c.getOrCreateTargetTypeForField(srcType.Elem())
		return reflect.MapOf(keyType, valueType)
	case reflect.Pointer:
		elemType := c.getOrCreateTargetTypeForField(srcType.Elem())
		return reflect.PointerTo(elemType)
	default:
		return srcType
	}
}

// findConverterForField returns the converter that handles the given field name.
func (c *Converter) findConverterForField(fieldName string) *FieldConverter {
	for _, conv := range c.converters {
		if _, ok := conv.Fields[fieldName]; ok {
			return conv
		}
	}
	return nil
}
