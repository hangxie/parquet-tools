package transcode

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

// Cmd is a kong command for transcode
type Cmd struct {
	FailOnInt96      bool     `help:"Fail if INT96 fields are detected in the source file." default:"false"`
	FieldCompression []string `help:"Field-specific compression in 'field.path=CODEC' format. Can be specified multiple times."`
	FieldEncoding    []string `help:"Field-specific encoding in 'field.path=ENCODING' format. Can be specified multiple times."`
	OmitStats        string   `help:"Control statistics (true/false). Leave empty to keep original." default:""`
	ReadPageSize     int      `help:"Page size to read from Parquet." default:"1000"`
	Source           string   `short:"s" help:"Source Parquet file to transcode." required:"true"`
	URI              string   `arg:"" predictor:"file" help:"URI of output Parquet file."`
	pio.ReadOption
	pio.WriteOption
}

// parseFieldEncodings parses field-specific encoding specifications from "field.path=ENCODING" format
// and returns a map from field path to encoding. Field paths use "." as delimiter.
func (c Cmd) parseFieldEncodings() (map[string]string, error) {
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

		// Delta encodings are only allowed in v2 data pages
		if (strings.ToUpper(encoding) == "DELTA_BINARY_PACKED" ||
			strings.ToUpper(encoding) == "DELTA_BYTE_ARRAY" ||
			strings.ToUpper(encoding) == "DELTA_LENGTH_BYTE_ARRAY") && c.DataPageVersion != 2 {
			return nil, fmt.Errorf("[%s] encoding is only allowed with data page version 2 for field [%s]", encoding, fieldPath)
		}

		result[fieldPath] = strings.ToUpper(encoding)
	}
	return result, nil
}

// parseFieldCompressions parses field-specific compression specifications from "field.path=CODEC" format
// and returns a map from field path to compression codec. Field paths use "." as delimiter.
func (c Cmd) parseFieldCompressions() (map[string]string, error) {
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

func (c Cmd) modifySchemaTree(schemaTree *pschema.SchemaNode, fieldEncodings, fieldCompressions map[string]string, globalCompression string) error {
	// Add custom parquet-go writer directives (encoding, compression, omitstats)
	// Only apply to leaf nodes (not struct/group types)
	if schemaTree.Type != nil {
		// Build field path from ExNamePath (skip root element)
		fieldPath := strings.Join(schemaTree.ExNamePath[1:], ".")

		// Apply field-specific encoding if specified
		if encoding, found := fieldEncodings[fieldPath]; found {
			allowed := pschema.GetAllowedEncodings(schemaTree.Type.String())
			if !pschema.IsEncodingCompatible(encoding, schemaTree.Type.String()) {
				return fmt.Errorf("encoding [%s] is not compatible with field [%s] of type [%s], allowed encodings: %s", encoding, fieldPath, schemaTree.Type.String(), strings.Join(allowed, ", "))
			}
			schemaTree.Encoding = encoding
		}

		// Apply field-specific compression if specified
		if compression, found := fieldCompressions[fieldPath]; found {
			schemaTree.CompressionCodec = compression
		} else if globalCompression != "" {
			// Apply global compression override
			schemaTree.CompressionCodec = globalCompression
		}

		// Apply omit stats if specified
		if c.OmitStats != "" {
			schemaTree.OmitStats = c.OmitStats
		}
	}

	// Recursively process children
	for _, child := range schemaTree.Children {
		if err := c.modifySchemaTree(child, fieldEncodings, fieldCompressions, globalCompression); err != nil {
			return err
		}
	}
	return nil
}

func (c Cmd) writer(ctx context.Context, fileWriter *writer.ParquetWriter, writerChan chan any) error {
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

func (c Cmd) reader(ctx context.Context, fileReader *reader.ParquetReader, writerChan chan any) error {
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
func (c Cmd) Run() (retErr error) {
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

	// Open source file
	fileReader, err := pio.NewParquetFileReader(c.Source, c.ReadOption)
	if err != nil {
		return fmt.Errorf("failed to read from [%s]: %w", c.Source, err)
	}
	defer func() {
		_ = fileReader.PFile.Close()
	}()

	// Get schema from source
	schemaTree, err := pschema.NewSchemaTree(fileReader, pschema.SchemaOption{FailOnInt96: c.FailOnInt96, WithCompressionCodec: true})
	if err != nil {
		return err
	}

	// Modify schema tree: custom writer directives (encoding, compression, omitstats)
	// Preserve source encodings by default, but allow user-specified values to override
	if err := c.modifySchemaTree(schemaTree, fieldEncodings, fieldCompressions, c.Compression); err != nil {
		return err
	}

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

	return nil
}
