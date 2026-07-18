package retype

import (
	"fmt"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

// Cmd is a kong command for retype.
type Cmd struct {
	Int96ToTimestamp bool   `help:"Convert INT96 columns to TIMESTAMP_NANOS." name:"int96-to-timestamp" default:"false"`
	BsonToString     bool   `help:"Convert BSON columns to plain strings (JSON encoded)." default:"false"`
	JsonToString     bool   `help:"Remove JSON logical type from columns." default:"false"`
	Float16ToFloat32 bool   `help:"Convert FLOAT16 columns to FLOAT32." name:"float16-to-float32" default:"false"`
	VariantToString  bool   `help:"Convert VARIANT columns to plain strings (JSON encoded)." default:"false"`
	UuidToString     bool   `help:"Convert UUID columns to plain strings." default:"false"`
	RepeatedToList   bool   `help:"Convert legacy repeated primitive columns to LIST format." default:"false"`
	GeoToBinary      bool   `help:"Remove GEOGRAPHY and GEOMETRY logical types (keep as plain BYTE_ARRAY)." default:"false"`
	ReadPageSize     int    `help:"Page size to read from Parquet." default:"1000"`
	Source           string `short:"s" help:"Source Parquet file to retype." required:"true"`
	URI              string `arg:"" predictor:"file" help:"URI of output Parquet file."`
	pio.ReadOption
	pio.WriteOption
}

// Run does actual retype job
func (c Cmd) Run() (retErr error) {
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
	schemaTree, err := pschema.NewSchemaTree(fileReader, pschema.SchemaOption{})
	if err != nil {
		return err
	}

	// Get active rules and apply them to schema
	activeRules := c.getActiveRules()
	matchedFields := make([]map[string]struct{}, len(activeRules))
	for i, rule := range activeRules {
		matchedFields[i] = applyRule(schemaTree, nil, rule.MatchSchema, rule.TransformSchema)
	}

	// Create converter for data transformation
	converter := NewConverter(activeRules, matchedFields)

	// Generate JSON schema from (possibly modified) SchemaTree
	schemaJSON := schemaTree.JSONSchema()

	// Create output file with new settings
	fileWriter, err := pio.NewGenericWriter(c.URI, c.WriteOption, schemaJSON)
	if err != nil {
		return fmt.Errorf("failed to write to [%s]: %w", c.URI, err)
	}
	defer func() {
		if err := fileWriter.WriteStop(); err != nil && retErr == nil {
			retErr = fmt.Errorf("failed to end write [%s]: %w", c.URI, err)
		}
		if err := fileWriter.PFile.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("failed to close [%s]: %w", c.URI, err)
		}
	}()

	return pio.RunPipeline(fileReader, fileWriter, c.Source, c.URI, c.ReadPageSize, converter.Convert)
}
