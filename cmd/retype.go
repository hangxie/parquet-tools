package cmd

import (
	"context"
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/types"
	"github.com/hangxie/parquet-go/v2/writer"
	"golang.org/x/sync/errgroup"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

// RetypeCmd is a kong command for retype
type RetypeCmd struct {
	Int96ToTimestamp bool   `name:"int96-to-timestamp" help:"Convert INT96 columns to TIMESTAMP_NANOS." default:"false"`
	ReadPageSize     int    `help:"Page size to read from Parquet." default:"1000"`
	Source           string `short:"s" help:"Source Parquet file to retype." required:"true"`
	URI              string `arg:"" predictor:"file" help:"URI of output Parquet file."`
	pio.ReadOption
	pio.WriteOption
}

func (c RetypeCmd) writer(ctx context.Context, fileWriter *writer.ParquetWriter, writerChan chan any) error {
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

func (c RetypeCmd) reader(ctx context.Context, fileReader *reader.ParquetReader, int96Fields map[string]struct{}, writerChan chan any) error {
	for {
		rows, err := fileReader.ReadByNumber(c.ReadPageSize)
		if err != nil {
			return fmt.Errorf("failed to read from [%s]: %w", c.Source, err)
		}
		if len(rows) == 0 {
			return nil
		}
		for _, row := range rows {
			// Convert INT96 fields to int64 nanoseconds
			if c.Int96ToTimestamp && len(int96Fields) > 0 {
				row, err = convertStructWithInt96(row, int96Fields)
				if err != nil {
					return fmt.Errorf("failed to convert INT96 fields: %w", err)
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

// targetStructTypeCache caches dynamically created struct types for INT96 conversion.
// Key is the source struct type, value is the target struct type with INT96 fields converted to int64.
var targetStructTypeCache = make(map[reflect.Type]reflect.Type)

// getOrCreateTargetType creates a new struct type where INT96 (string) fields are replaced with int64.
func getOrCreateTargetType(srcType reflect.Type, int96Fields map[string]struct{}) reflect.Type {
	if cached, ok := targetStructTypeCache[srcType]; ok {
		return cached
	}

	fields := make([]reflect.StructField, srcType.NumField())
	for i := range srcType.NumField() {
		srcField := srcType.Field(i)
		fields[i] = srcField

		if _, isInt96 := int96Fields[srcField.Name]; isInt96 {
			// Replace string with int64, *string with *int64
			if srcField.Type.Kind() == reflect.String {
				fields[i].Type = reflect.TypeFor[int64]()
			} else if srcField.Type.Kind() == reflect.Pointer && srcField.Type.Elem().Kind() == reflect.String {
				fields[i].Type = reflect.TypeFor[*int64]()
			}
		}
	}

	targetType := reflect.StructOf(fields)
	targetStructTypeCache[srcType] = targetType
	return targetType
}

// convertStructWithInt96 creates a new struct instance with INT96 fields converted from string to int64.
// This preserves the struct type (rather than using a map) for proper parquet-go serialization.
func convertStructWithInt96(v any, int96Fields map[string]struct{}) (any, error) {
	srcVal := reflect.ValueOf(v)

	// Handle pointer to struct
	if srcVal.Kind() == reflect.Pointer {
		if srcVal.IsNil() {
			return nil, nil
		}
		srcVal = srcVal.Elem()
	}

	if srcVal.Kind() != reflect.Struct {
		return v, nil
	}

	srcType := srcVal.Type()
	targetType := getOrCreateTargetType(srcType, int96Fields)
	targetVal := reflect.New(targetType).Elem()

	for i := range srcType.NumField() {
		srcField := srcType.Field(i)
		srcFieldVal := srcVal.Field(i)
		targetFieldVal := targetVal.Field(i)

		if _, isInt96 := int96Fields[srcField.Name]; isInt96 {
			// Convert INT96 string to int64 nanoseconds
			if srcFieldVal.Kind() == reflect.String {
				timestamp, err := types.INT96ToTimeWithError(srcFieldVal.String())
				if err != nil {
					return nil, fmt.Errorf("failed to convert INT96 field [%s]: %w", srcField.Name, err)
				}
				targetFieldVal.SetInt(timestamp.UnixNano())
			} else if srcFieldVal.Kind() == reflect.Pointer {
				// Handle *string (optional INT96)
				if srcFieldVal.IsNil() {
					// Leave as zero value (nil pointer)
				} else {
					timestamp, err := types.INT96ToTimeWithError(srcFieldVal.Elem().String())
					if err != nil {
						return nil, fmt.Errorf("failed to convert INT96 field [%s]: %w", srcField.Name, err)
					}
					ptr := reflect.New(reflect.TypeFor[int64]())
					ptr.Elem().SetInt(timestamp.UnixNano())
					targetFieldVal.Set(ptr)
				}
			} else {
				return nil, fmt.Errorf("unexpected type for INT96 field [%s]: %s", srcField.Name, srcFieldVal.Kind())
			}
		} else {
			targetFieldVal.Set(srcFieldVal)
		}
	}

	return targetVal.Addr().Interface(), nil
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
	schemaTree, err := pschema.NewSchemaTree(fileReader, pschema.SchemaOption{})
	if err != nil {
		return err
	}

	// Find INT96 fields and convert schema if requested
	var int96Fields map[string]struct{}
	if c.Int96ToTimestamp {
		int96Fields = schemaTree.ConvertInt96ToTimestamp()
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
		return c.reader(ctx, fileReader, int96Fields, writerChan)
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
