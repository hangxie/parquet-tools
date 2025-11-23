package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/writer"
	"golang.org/x/sync/errgroup"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

// TranscodeCmd is a kong command for transcode
type TranscodeCmd struct {
	FailOnInt96   bool     `help:"Fail if INT96 fields are detected in the source file." default:"false"`
	FieldEncoding []string `help:"Field-specific encoding in 'field.path=ENCODING' format. Can be specified multiple times."`
	OmitStats     string   `help:"Control statistics (true/false). Leave empty to keep original." default:""`
	ReadPageSize  int      `help:"Page size to read from Parquet." default:"1000"`
	Source        string   `short:"s" help:"Source Parquet file to transcode." required:"true"`
	URI           string   `arg:"" predictor:"file" help:"URI of output Parquet file."`
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
			return nil, fmt.Errorf("invalid encoding [%s] for field [%s]: %w", encoding, fieldPath, err)
		}
		// PLAIN_DICTIONARY is only allowed in v1 data pages
		if strings.ToUpper(encoding) == "PLAIN_DICTIONARY" && c.DataPageVersion != 1 {
			return nil, fmt.Errorf("PLAIN_DICTIONARY encoding is only allowed with data page version 1, use RLE_DICTIONARY instead for field [%s]", fieldPath)
		}

		result[fieldPath] = strings.ToUpper(encoding)
	}
	return result, nil
}

func (c TranscodeCmd) modifySchemaTree(schemaTree *pschema.SchemaNode, fieldEncodings map[string]string) error {
	// Add custom parquet-go writer directives (encoding, omitstats)
	// Only apply to leaf nodes (not struct/group types)
	if schemaTree.Type != nil {
		// Build field path from ExNamePath (skip root element)
		fieldPath := strings.Join(schemaTree.ExNamePath[1:], ".")

		// Apply field-specific encoding if specified
		if encoding, found := fieldEncodings[fieldPath]; found {
			if !c.isEncodingCompatible(encoding, schemaTree.Type.String()) {
				return fmt.Errorf("encoding %s is not compatible with field [%s] of type %s", encoding, fieldPath, schemaTree.Type.String())
			}
			schemaTree.Encoding = encoding
		}

		// Apply omit stats if specified
		if c.OmitStats != "" {
			schemaTree.OmitStats = c.OmitStats
		}
	}

	// Recursively process children
	for _, child := range schemaTree.Children {
		if err := c.modifySchemaTree(child, fieldEncodings); err != nil {
			return err
		}
	}
	return nil
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

func (c TranscodeCmd) reader(ctx context.Context, fileReader *reader.ParquetReader, writerChan chan any) error {
	for {
		rows, err := fileReader.ReadByNumber(c.ReadPageSize)
		if err != nil {
			return fmt.Errorf("failed to read from [%s]: %w", c.Source, err)
		}
		if len(rows) == 0 {
			return nil
		}
		for _, row := range rows {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				writerChan <- row
			}
		}
	}
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

	// Open source file
	fileReader, err := pio.NewParquetFileReader(c.Source, c.ReadOption)
	if err != nil {
		return fmt.Errorf("failed to read from [%s]: %w", c.Source, err)
	}
	defer func() {
		_ = fileReader.PFile.Close()
	}()

	// Get schema from source
	schemaTree, err := pschema.NewSchemaTree(fileReader, pschema.SchemaOption{FailOnInt96: c.FailOnInt96})
	if err != nil {
		return err
	}

	// Clear encoding from source file - we'll only use encoding if explicitly specified
	var clearEncodingRecursive func(*pschema.SchemaNode)
	clearEncodingRecursive = func(node *pschema.SchemaNode) {
		node.Encoding = ""
		for _, child := range node.Children {
			clearEncodingRecursive(child)
		}
	}
	clearEncodingRecursive(schemaTree)

	// Modify schema tree: custom writer directives (encoding, omitstats)
	// This will add user-specified encoding if provided
	if c.OmitStats != "" || len(fieldEncodings) > 0 {
		if err := c.modifySchemaTree(schemaTree, fieldEncodings); err != nil {
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
		return c.reader(ctx, fileReader, writerChan)
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
