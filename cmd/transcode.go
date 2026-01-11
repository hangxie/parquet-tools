package cmd

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/writer"
	"golang.org/x/sync/errgroup"

	"github.com/hangxie/parquet-tools/cmd/typeconv"
	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

// TranscodeCmd is a kong command for transcode
type TranscodeCmd struct {
	FailOnInt96      bool     `help:"Fail if INT96 fields are detected in the source file." default:"false"`
	FieldCompression []string `help:"Field-specific compression in 'field.path=CODEC' format. Can be specified multiple times."`
	FieldEncoding    []string `help:"Field-specific encoding in 'field.path=ENCODING' format. Can be specified multiple times."`
	FieldType        []string `help:"Field type conversion in 'field.path=PRIMITIVE:LOGICAL' format. Can be specified multiple times."`
	OmitStats        string   `help:"Control statistics (true/false). Leave empty to keep original." default:""`
	ReadPageSize     int      `help:"Page size to read from Parquet." default:"1000"`
	Source           string   `short:"s" help:"Source Parquet file to transcode." required:"true"`
	URI              string   `arg:"" predictor:"file" help:"URI of output Parquet file."`
	pio.ReadOption
	pio.WriteOption
}

// parseFieldEncodings parses field-specific encoding specifications from "field.path=ENCODING" format
// and returns a map from field path to encoding. Field paths use "." as delimiter.
func (c TranscodeCmd) parseFieldEncodings() (map[string]string, error) {
	result := make(map[string]string)
	for _, spec := range c.FieldEncoding {
		parts := strings.SplitN(spec, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid field encoding format [%s], expected 'field.path=ENCODING'", spec)
		}
		fieldPath := strings.TrimSpace(parts[0])
		encoding := strings.TrimSpace(parts[1])

		if fieldPath == "" {
			return nil, fmt.Errorf("empty field path in [%s]", spec)
		}
		if encoding == "" {
			return nil, fmt.Errorf("empty encoding in [%s]", spec)
		}

		// Validate encoding
		if _, err := parquet.EncodingFromString(strings.ToUpper(encoding)); err != nil {
			validEncodings := []string{
				"PLAIN", "RLE", "BIT_PACKED", "DELTA_BINARY_PACKED",
				"DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY", "BYTE_STREAM_SPLIT",
				"RLE_DICTIONARY", "PLAIN_DICTIONARY",
			}
			return nil, fmt.Errorf("invalid encoding [%s] for field [%s]: %w, valid encodings: %s", encoding, fieldPath, err, strings.Join(validEncodings, ", "))
		}
		// PLAIN_DICTIONARY is only allowed in v1 data pages
		if strings.ToUpper(encoding) == "PLAIN_DICTIONARY" && c.DataPageVersion != 1 {
			return nil, fmt.Errorf("PLAIN_DICTIONARY encoding is only allowed with data page version 1, use RLE_DICTIONARY instead for field [%s]", fieldPath)
		}

		result[fieldPath] = strings.ToUpper(encoding)
	}
	return result, nil
}

// parseFieldCompressions parses field-specific compression specifications from "field.path=CODEC" format
// and returns a map from field path to compression codec. Field paths use "." as delimiter.
func (c TranscodeCmd) parseFieldCompressions() (map[string]string, error) {
	result := make(map[string]string)
	validCodecs := []string{
		"UNCOMPRESSED", "SNAPPY", "GZIP", "LZ4", "LZ4_RAW", "ZSTD", "BROTLI",
	}
	for _, spec := range c.FieldCompression {
		parts := strings.SplitN(spec, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid field compression format [%s], expected 'field.path=CODEC'", spec)
		}
		fieldPath := strings.TrimSpace(parts[0])
		codec := strings.TrimSpace(parts[1])

		if fieldPath == "" {
			return nil, fmt.Errorf("empty field path in [%s]", spec)
		}
		if codec == "" {
			return nil, fmt.Errorf("empty compression codec in [%s]", spec)
		}

		// Validate compression codec
		codec = strings.ToUpper(codec)
		isValid := false
		for _, validCodec := range validCodecs {
			if codec == validCodec {
				isValid = true
				break
			}
		}
		if !isValid {
			return nil, fmt.Errorf("invalid compression codec [%s] for field [%s], valid codecs: %s", codec, fieldPath, strings.Join(validCodecs, ", "))
		}

		result[fieldPath] = codec
	}
	return result, nil
}

// parseFieldTypes parses field-specific type specifications from "field.path=PRIMITIVE:LOGICAL" format
// and returns a map from field path to TypeSpec. Field paths use "." as delimiter.
func (c TranscodeCmd) parseFieldTypes() (map[string]*typeconv.TypeSpec, error) {
	result := make(map[string]*typeconv.TypeSpec)
	for _, spec := range c.FieldType {
		parts := strings.SplitN(spec, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid field type format [%s], expected 'field.path=PRIMITIVE:LOGICAL'", spec)
		}
		fieldPath := strings.TrimSpace(parts[0])
		typeSpec := strings.TrimSpace(parts[1])

		if fieldPath == "" {
			return nil, fmt.Errorf("empty field path in [%s]", spec)
		}
		if typeSpec == "" {
			return nil, fmt.Errorf("empty type specification in [%s]", spec)
		}

		parsed, err := typeconv.ParseTypeSpec(typeSpec)
		if err != nil {
			return nil, fmt.Errorf("invalid type specification for field [%s]: %w", fieldPath, err)
		}

		result[fieldPath] = parsed
	}
	return result, nil
}

// buildConverters creates type converters for specified fields and validates the conversions
func (c TranscodeCmd) buildConverters(schemaTree *pschema.SchemaNode, fieldTypes map[string]*typeconv.TypeSpec) (map[string]typeconv.Converter, error) {
	converters := make(map[string]typeconv.Converter)
	pathMap := schemaTree.GetPathMap()

	for fieldPath, targetType := range fieldTypes {
		node, found := pathMap[fieldPath]
		if !found {
			return nil, fmt.Errorf("field [%s] not found in schema", fieldPath)
		}
		if node.Type == nil {
			return nil, fmt.Errorf("field [%s] is not a leaf field (no primitive type)", fieldPath)
		}

		// Build source type info
		sourceInfo := typeconv.SourceTypeInfo{
			Primitive: node.Type.String(),
		}

		// Extract primitive length for FIXED_LEN_BYTE_ARRAY
		if node.TypeLength != nil {
			sourceInfo.PrimitiveLen = int(*node.TypeLength)
		}

		// Extract logical type info
		sourceInfo.Logical = c.extractLogicalType(node)
		if node.LogicalType != nil && node.LogicalType.IsSetDECIMAL() {
			sourceInfo.Precision = int(node.LogicalType.DECIMAL.Precision)
			sourceInfo.Scale = int(node.LogicalType.DECIMAL.Scale)
		} else if node.ConvertedType != nil && *node.ConvertedType == parquet.ConvertedType_DECIMAL {
			if node.Precision != nil {
				sourceInfo.Precision = int(*node.Precision)
			}
			if node.Scale != nil {
				sourceInfo.Scale = int(*node.Scale)
			}
		}

		// Build converter
		converter, err := typeconv.BuildConverter(sourceInfo, targetType)
		if err != nil {
			return nil, fmt.Errorf("cannot convert field [%s]: %w", fieldPath, err)
		}

		converters[fieldPath] = converter
	}

	return converters, nil
}

// extractLogicalType extracts the logical type name from a schema node
func (c TranscodeCmd) extractLogicalType(node *pschema.SchemaNode) string {
	if node.LogicalType != nil {
		switch {
		case node.LogicalType.IsSetSTRING():
			return "STRING"
		case node.LogicalType.IsSetDECIMAL():
			return "DECIMAL"
		case node.LogicalType.IsSetTIMESTAMP():
			unit := node.LogicalType.TIMESTAMP.Unit
			if unit.IsSetNANOS() {
				return "TIMESTAMP_NANOS"
			}
			if unit.IsSetMICROS() {
				return "TIMESTAMP_MICROS"
			}
			if unit.IsSetMILLIS() {
				return "TIMESTAMP_MILLIS"
			}
		case node.LogicalType.IsSetDATE():
			return "DATE"
		case node.LogicalType.IsSetTIME():
			return "TIME"
		case node.LogicalType.IsSetUUID():
			return "UUID"
		case node.LogicalType.IsSetJSON():
			return "JSON"
		case node.LogicalType.IsSetBSON():
			return "BSON"
		case node.LogicalType.IsSetENUM():
			return "ENUM"
		}
	}

	// Check converted type as fallback
	if node.ConvertedType != nil {
		switch *node.ConvertedType {
		case parquet.ConvertedType_UTF8:
			return "STRING"
		case parquet.ConvertedType_DECIMAL:
			return "DECIMAL"
		case parquet.ConvertedType_TIMESTAMP_MILLIS:
			return "TIMESTAMP_MILLIS"
		case parquet.ConvertedType_TIMESTAMP_MICROS:
			return "TIMESTAMP_MICROS"
		case parquet.ConvertedType_DATE:
			return "DATE"
		}
	}

	return "NONE"
}

func (c TranscodeCmd) modifySchemaTree(schemaTree *pschema.SchemaNode, fieldEncodings, fieldCompressions map[string]string, fieldTypes map[string]*typeconv.TypeSpec) error {
	// Add custom parquet-go writer directives (encoding, compression, omitstats)
	// Only apply to leaf nodes (not struct/group types)
	if schemaTree.Type != nil {
		// Build field path from ExNamePath (skip root element)
		fieldPath := strings.Join(schemaTree.ExNamePath[1:], ".")

		// Apply field type conversion if specified
		if targetType, found := fieldTypes[fieldPath]; found {
			c.applyTypeConversion(schemaTree, targetType)
		}

		// Determine the effective type for encoding compatibility check
		effectiveType := schemaTree.Type.String()

		// Apply field-specific encoding if specified
		if encoding, found := fieldEncodings[fieldPath]; found {
			allowed := c.getAllowedEncodings(effectiveType)
			if !c.isEncodingCompatible(encoding, effectiveType) {
				return fmt.Errorf("encoding %s is not compatible with field [%s] of type %s, allowed encodings: %s", encoding, fieldPath, effectiveType, strings.Join(allowed, ", "))
			}
			schemaTree.Encoding = encoding
		}

		// Apply field-specific compression if specified
		if compression, found := fieldCompressions[fieldPath]; found {
			schemaTree.CompressionCodec = compression
		}

		// Apply omit stats if specified
		if c.OmitStats != "" {
			schemaTree.OmitStats = c.OmitStats
		}
	}

	// Recursively process children
	for _, child := range schemaTree.Children {
		if err := c.modifySchemaTree(child, fieldEncodings, fieldCompressions, fieldTypes); err != nil {
			return err
		}
	}
	return nil
}

// applyTypeConversion modifies a schema node to reflect the target type
func (c TranscodeCmd) applyTypeConversion(node *pschema.SchemaNode, target *typeconv.TypeSpec) {
	// Update primitive type
	newType, _ := parquet.TypeFromString(target.Primitive)
	node.Type = &newType

	// Update type length for FIXED_LEN_BYTE_ARRAY
	if target.Primitive == "FIXED_LEN_BYTE_ARRAY" {
		length := int32(target.PrimitiveLen)
		node.TypeLength = &length
	} else {
		node.TypeLength = nil
	}

	// Update logical type
	node.LogicalType = nil
	node.ConvertedType = nil
	node.Precision = nil
	node.Scale = nil

	switch target.Logical {
	case "NONE":
		// No logical type
	case "STRING":
		node.LogicalType = &parquet.LogicalType{STRING: &parquet.StringType{}}
		ct := parquet.ConvertedType_UTF8
		node.ConvertedType = &ct
	case "DECIMAL":
		precision := int32(target.Precision)
		scale := int32(target.Scale)
		node.LogicalType = &parquet.LogicalType{
			DECIMAL: &parquet.DecimalType{
				Precision: precision,
				Scale:     scale,
			},
		}
		ct := parquet.ConvertedType_DECIMAL
		node.ConvertedType = &ct
		node.Precision = &precision
		node.Scale = &scale
	case "TIMESTAMP_NANOS":
		isAdjustedToUTC := true
		node.LogicalType = &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				IsAdjustedToUTC: isAdjustedToUTC,
				Unit:            &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			},
		}
	case "TIMESTAMP_MICROS":
		isAdjustedToUTC := true
		node.LogicalType = &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				IsAdjustedToUTC: isAdjustedToUTC,
				Unit:            &parquet.TimeUnit{MICROS: &parquet.MicroSeconds{}},
			},
		}
		ct := parquet.ConvertedType_TIMESTAMP_MICROS
		node.ConvertedType = &ct
	case "TIMESTAMP_MILLIS":
		isAdjustedToUTC := true
		node.LogicalType = &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				IsAdjustedToUTC: isAdjustedToUTC,
				Unit:            &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
			},
		}
		ct := parquet.ConvertedType_TIMESTAMP_MILLIS
		node.ConvertedType = &ct
	case "DATE":
		node.LogicalType = &parquet.LogicalType{DATE: &parquet.DateType{}}
		ct := parquet.ConvertedType_DATE
		node.ConvertedType = &ct
	}

	// Clear encoding for type-converted fields (let writer choose appropriate default)
	node.Encoding = ""
}

func (c TranscodeCmd) getAllowedEncodings(dataType string) []string {
	dataType = strings.ToUpper(dataType)

	// Encoding compatibility matrix: maps data type to compatible encodings
	// Per Parquet spec and parquet-go validation rules:
	// - RLE: BOOLEAN, INT32, INT64 only
	// - DELTA_BINARY_PACKED: INT32, INT64 only
	// - DELTA_BYTE_ARRAY, DELTA_LENGTH_BYTE_ARRAY: BYTE_ARRAY only
	// - BYTE_STREAM_SPLIT: FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY
	// - PLAIN_DICTIONARY: All types (v1 data pages only, validated in parseFieldEncodings)
	compatibilityMap := map[string][]string{
		"BOOLEAN":              {"BIT_PACKED", "PLAIN_DICTIONARY", "RLE", "RLE_DICTIONARY"},
		"BYTE_ARRAY":           {"DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		"DOUBLE":               {"BYTE_STREAM_SPLIT", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		"FIXED_LEN_BYTE_ARRAY": {"BYTE_STREAM_SPLIT", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		"FLOAT":                {"BYTE_STREAM_SPLIT", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		"INT32":                {"BIT_PACKED", "BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED", "PLAIN_DICTIONARY", "RLE", "RLE_DICTIONARY"},
		"INT64":                {"BIT_PACKED", "BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED", "PLAIN_DICTIONARY", "RLE", "RLE_DICTIONARY"},
	}

	allowed, exists := compatibilityMap[dataType]
	if !exists {
		return []string{"PLAIN"}
	}

	// PLAIN encoding works with all types, so always include it
	return append([]string{"PLAIN"}, allowed...)
}

func (c TranscodeCmd) isEncodingCompatible(encoding, dataType string) bool {
	// If no type specified, it's a struct/group, skip encoding
	if dataType == "" {
		return false
	}

	encoding = strings.ToUpper(encoding)
	dataType = strings.ToUpper(dataType)

	// PLAIN encoding works with all types
	if encoding == "PLAIN" {
		return true
	}

	// Encoding compatibility matrix: maps data type to compatible encodings
	// Per Parquet spec and parquet-go validation rules:
	// - RLE: BOOLEAN, INT32, INT64 only
	// - DELTA_BINARY_PACKED: INT32, INT64 only
	// - DELTA_BYTE_ARRAY, DELTA_LENGTH_BYTE_ARRAY: BYTE_ARRAY only
	// - BYTE_STREAM_SPLIT: FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY
	// - PLAIN_DICTIONARY: All types (v1 data pages only, validated in parseFieldEncodings)
	compatibilityMap := map[string][]string{
		"BOOLEAN":              {"BIT_PACKED", "PLAIN_DICTIONARY", "RLE", "RLE_DICTIONARY"},
		"BYTE_ARRAY":           {"DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		"DOUBLE":               {"BYTE_STREAM_SPLIT", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		"FIXED_LEN_BYTE_ARRAY": {"BYTE_STREAM_SPLIT", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		"FLOAT":                {"BYTE_STREAM_SPLIT", "PLAIN_DICTIONARY", "RLE_DICTIONARY"},
		"INT32":                {"BIT_PACKED", "BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED", "PLAIN_DICTIONARY", "RLE", "RLE_DICTIONARY"},
		"INT64":                {"BIT_PACKED", "BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED", "PLAIN_DICTIONARY", "RLE", "RLE_DICTIONARY"},
	}

	compatibleEncodings, exists := compatibilityMap[dataType]
	if !exists {
		return false
	}

	for _, compatibleEncoding := range compatibleEncodings {
		if encoding == compatibleEncoding {
			return true
		}
	}

	return false
}

func (c TranscodeCmd) writer(ctx context.Context, fileWriter *writer.ParquetWriter, writerChan chan any) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			row, more := <-writerChan
			if !more {
				return nil
			}

			if err := fileWriter.Write(row); err != nil {
				return fmt.Errorf("failed to write data to [%s]: %w", c.URI, err)
			}
		}
	}
}

func (c TranscodeCmd) reader(ctx context.Context, fileReader *reader.ParquetReader, writerChan chan any, converters map[string]typeconv.Converter) error {
	for {
		rows, err := fileReader.ReadByNumber(c.ReadPageSize)
		if err != nil {
			return fmt.Errorf("failed to read from [%s]: %w", c.Source, err)
		}
		if len(rows) == 0 {
			return nil
		}
		for _, row := range rows {
			// Apply type conversions if any
			if len(converters) > 0 {
				row, err = c.transformRow(row, converters)
				if err != nil {
					return fmt.Errorf("failed to transform row: %w", err)
				}
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				writerChan <- row
			}
		}
	}
}

// transformRow applies type converters to a row
func (c TranscodeCmd) transformRow(row any, converters map[string]typeconv.Converter) (any, error) {
	rowMap, ok := row.(map[string]any)
	if !ok {
		// Try to convert struct to map using reflection
		rowMap, ok = structToMap(row)
		if !ok {
			return nil, fmt.Errorf("expected map[string]any or struct, got %T", row)
		}
	}

	// Apply converters to matching fields
	for fieldPath, converter := range converters {
		if err := c.applyConverterToField(rowMap, fieldPath, converter); err != nil {
			return nil, err
		}
	}

	return rowMap, nil
}

// structToMap converts a struct to map[string]any using reflection
// Returns the map and true if successful, nil and false otherwise
func structToMap(v any) (map[string]any, bool) {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil, false
	}

	result := make(map[string]any)
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		fieldVal := val.Field(i)

		// Use Go field name (parquet-go uses field names, not json tags)
		name := field.Name

		// Skip unexported fields
		if !fieldVal.CanInterface() {
			continue
		}

		result[name] = fieldVal.Interface()
	}

	return result, true
}

// applyConverterToField applies a converter to a field in the row, handling nested paths
func (c TranscodeCmd) applyConverterToField(rowMap map[string]any, fieldPath string, converter typeconv.Converter) error {
	parts := strings.Split(fieldPath, ".")
	return c.applyConverterRecursive(rowMap, parts, converter)
}

// applyConverterRecursive recursively navigates the row structure and applies the converter
func (c TranscodeCmd) applyConverterRecursive(current any, pathParts []string, converter typeconv.Converter) error {
	if len(pathParts) == 0 {
		return nil
	}

	currentMap, ok := current.(map[string]any)
	if !ok {
		return fmt.Errorf("expected map[string]any at path, got %T", current)
	}

	fieldName := pathParts[0]
	value, exists := currentMap[fieldName]
	if !exists {
		// Field doesn't exist in this row (might be null or missing)
		return nil
	}

	if len(pathParts) == 1 {
		// This is the leaf field, apply the converter
		converted, err := converter(value)
		if err != nil {
			return fmt.Errorf("conversion failed for field [%s]: %w", fieldName, err)
		}
		currentMap[fieldName] = converted
		return nil
	}

	// Navigate deeper
	return c.applyConverterRecursive(value, pathParts[1:], converter)
}

// Run does actual transcode job
func (c TranscodeCmd) Run() error {
	if c.ReadPageSize < 1 {
		return fmt.Errorf("invalid read page size %d, needs to be at least 1", c.ReadPageSize)
	}

	// Parse and validate field-specific encodings
	fieldEncodings, err := c.parseFieldEncodings()
	if err != nil {
		return err
	}

	// Parse and validate field-specific compressions
	fieldCompressions, err := c.parseFieldCompressions()
	if err != nil {
		return err
	}

	// Parse and validate field-specific types
	fieldTypes, err := c.parseFieldTypes()
	if err != nil {
		return err
	}

	// Open source file
	fileReader, err := pio.NewParquetFileReader(c.Source, c.ReadOption)
	if err != nil {
		return fmt.Errorf("failed to read from [%s]: %w", c.Source, err)
	}
	defer func() {
		_ = fileReader.PFile.Close()
	}()

	// Get schema from source (don't fail on INT96 if we're converting it)
	failOnInt96 := c.FailOnInt96
	if len(fieldTypes) > 0 {
		// Check if any INT96 fields are being converted
		for _, ts := range fieldTypes {
			if ts.Logical == "TIMESTAMP_NANOS" || ts.Logical == "TIMESTAMP_MICROS" || ts.Logical == "TIMESTAMP_MILLIS" {
				// User is converting INT96 to timestamp, so don't fail on INT96
				failOnInt96 = false
				break
			}
		}
	}
	schemaTree, err := pschema.NewSchemaTree(fileReader, pschema.SchemaOption{FailOnInt96: failOnInt96})
	if err != nil {
		return err
	}

	// Build type converters before modifying schema
	var converters map[string]typeconv.Converter
	if len(fieldTypes) > 0 {
		converters, err = c.buildConverters(schemaTree, fieldTypes)
		if err != nil {
			return err
		}
	}

	// Modify schema tree: custom writer directives (encoding, compression, omitstats, type)
	// Preserve source encodings by default, but allow user-specified values to override
	if c.OmitStats != "" || len(fieldEncodings) > 0 || len(fieldCompressions) > 0 || len(fieldTypes) > 0 {
		if err := c.modifySchemaTree(schemaTree, fieldEncodings, fieldCompressions, fieldTypes); err != nil {
			return err
		}
	}

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
		return c.reader(ctx, fileReader, writerChan, converters)
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
