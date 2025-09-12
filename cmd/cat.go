package cmd

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/rand"
	"runtime"
	"strings"

	"github.com/hangxie/parquet-go/v2/marshal"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/types"
	"golang.org/x/sync/errgroup"

	pio "github.com/hangxie/parquet-tools/internal/io"
	pschema "github.com/hangxie/parquet-tools/internal/schema"
)

// CatCmd is a kong command for cat
type CatCmd struct {
	Concurrent   bool    `help:"enable concurrent output" default:"false"`
	FailOnInt96  bool    `help:"fail command if INT96 data type is present." name:"fail-on-int96" default:"false"`
	Format       string  `short:"f" help:"output format (json/jsonl/csv/tsv)" enum:"json,jsonl,csv,tsv" default:"json"`
	GeoFormat    string  `help:"experimental, output format (geojson/hex/base64) for geospatial fields" enum:"geojson,hex,base64" default:"geojson"`
	Limit        uint64  `short:"l" help:"Max number of rows to output, 0 means no limit." default:"0"`
	NoHeader     bool    `help:"(CSV/TSV only) do not output field name as header" default:"false"`
	ReadPageSize int     `help:"Page size to read from Parquet." default:"1000"`
	SampleRatio  float32 `short:"s" help:"Sample ratio (0.0-1.0)." default:"1.0"`
	Skip         int64   `short:"k" help:"Skip rows before apply other logics." default:"0"`
	SkipPageSize int64   `help:"deprecated, will be removed in future release." default:"100000"`
	URI          string  `arg:"" predictor:"file" help:"URI of Parquet file."`
	pio.ReadOption

	// reusable CSV writer components
	csvBuf    *strings.Builder
	csvWriter *csv.Writer
}

var delimiter = map[string]struct {
	begin          string
	lineDelimiter  string
	fieldDelimiter rune
	end            string
}{
	"json":  {"[", ",", ' ', "]"},
	"jsonl": {"", "\n", ' ', ""},
	"csv":   {"", "\n", ',', ""},
	"tsv":   {"", "\n", '\t', ""},
}

// Run does actual cat job
func (c CatCmd) Run() error {
	switch c.GeoFormat {
	case "hex":
		types.SetGeometryJSONMode(types.GeospatialModeHex)
		types.SetGeographyJSONMode(types.GeospatialModeHex)
	case "base64":
		types.SetGeometryJSONMode(types.GeospatialModeBase64)
		types.SetGeographyJSONMode(types.GeospatialModeBase64)
	case "geojson", "":
		types.SetGeometryJSONMode(types.GeospatialModeGeoJSON)
		types.SetGeographyJSONMode(types.GeospatialModeGeoJSON)
	default:
		return fmt.Errorf("unknown geo format: %s", c.GeoFormat)
	}

	if c.ReadPageSize < 1 {
		return fmt.Errorf("invalid read page size %d, needs to be at least 1", c.ReadPageSize)
	}
	if c.Skip < 0 {
		return fmt.Errorf("invalid skip %d, needs to be greater than or equal to 0", c.Skip)
	}
	if c.Limit == 0 {
		c.Limit = ^uint64(0)
	}
	// note that sampling rate at 0.0 is allowed, while it does not output anything
	if c.SampleRatio < 0.0 || c.SampleRatio > 1.0 {
		return fmt.Errorf("invalid sampling %f, needs to be between 0.0 and 1.0", c.SampleRatio)
	}
	if _, ok := delimiter[c.Format]; !ok {
		// should never reach here
		return fmt.Errorf("unknown format: %s", c.Format)
	}

	fileReader, err := pio.NewParquetFileReader(c.URI, c.ReadOption)
	if err != nil {
		return err
	}
	defer func() {
		_ = fileReader.PFile.Close()
	}()

	return c.outputRows(fileReader)
}

func (c *CatCmd) outputHeader(schemaRoot *pschema.SchemaNode) ([]string, error) {
	if c.Format != "csv" && c.Format != "tsv" {
		// only CSV and TSV need header
		return nil, nil
	}

	fieldList := make([]string, len(schemaRoot.Children))
	for index, child := range schemaRoot.Children {
		if len(child.Children) != 0 {
			return nil, fmt.Errorf("field [%s] is not scalar type, cannot output in %s format", child.Name, c.Format)
		}
		if child.LogicalType != nil && (child.LogicalType.IsSetGEOGRAPHY() || child.LogicalType.IsSetGEOMETRY()) {
			return nil, fmt.Errorf("field [%s] is not scalar type, cannot output in %s format", child.Name, c.Format)
		}
		fieldList[index] = child.ExNamePath[len(child.ExNamePath)-1]
	}
	headerList := make([]string, len(schemaRoot.Children))
	_ = copy(headerList, fieldList)
	line, err := c.valuesToCSV(headerList)
	if err != nil {
		return nil, err
	}

	if !c.NoHeader {
		fmt.Print(line)
	}
	return fieldList, nil
}

func (c *CatCmd) retrieveFieldDef(fileReader *reader.ParquetReader) ([]string, error) {
	schemaRoot, err := pschema.NewSchemaTree(fileReader, pschema.SchemaOption{FailOnInt96: c.FailOnInt96})
	if err != nil {
		return nil, err
	}

	// Initialize CSV writer for reuse if needed
	if c.Format == "csv" || c.Format == "tsv" {
		c.csvBuf = new(strings.Builder)
		c.csvWriter = csv.NewWriter(c.csvBuf)
		c.csvWriter.Comma = delimiter[c.Format].fieldDelimiter
	}

	// CSV and TSV do not support nested schema
	fieldList, err := c.outputHeader(schemaRoot)
	if err != nil {
		return nil, err
	}

	return fieldList, nil
}

func (c *CatCmd) outputSingleRow(rowStruct any, fieldList []string) error {
	switch c.Format {
	case "json", "jsonl":
		buf, _ := json.Marshal(rowStruct)
		fmt.Print(string(buf))
	case "csv", "tsv":
		flatValues := rowStruct.(map[string]any)
		values := make([]string, len(flatValues))
		for index, field := range fieldList {
			switch val := flatValues[field].(type) {
			case nil:
				// nil is just empty in CSV/TSV
			default:
				values[index] = fmt.Sprint(val)
			}
		}

		line, err := c.valuesToCSV(values)
		if err != nil {
			return err
		}
		fmt.Print(strings.TrimRight(line, "\n"))
	default:
		return fmt.Errorf("unsupported format: %s", c.Format)
	}

	return nil
}

func (c CatCmd) encoder(ctx context.Context, rowChan, outputChan chan any, schemaHandler *schema.SchemaHandler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			row, more := <-rowChan
			if !more {
				return nil
			}
			rowStruct, err := marshal.ConvertToJSONFriendly(row, schemaHandler)
			if err != nil {
				return err
			}
			outputChan <- rowStruct
		}
	}
}

func (c CatCmd) printer(ctx context.Context, outputChan chan any, fieldList []string) error {
	fmt.Print(delimiter[c.Format].begin)
	defer func() {
		fmt.Println(delimiter[c.Format].end)
	}()

	isFirstRow := true
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			row, more := <-outputChan
			if !more {
				return nil
			}

			if isFirstRow {
				isFirstRow = false
			} else {
				fmt.Print(delimiter[c.Format].lineDelimiter)
			}
			if err := c.outputSingleRow(row, fieldList); err != nil {
				return err
			}
		}
	}
}

func (c CatCmd) outputRows(fileReader *reader.ParquetReader) error {
	fieldList, err := c.retrieveFieldDef(fileReader)
	if err != nil {
		return err
	}

	// skip rows
	if err := fileReader.SkipRows(c.Skip); err != nil {
		return err
	}

	concurrency := 1
	if c.Concurrent {
		concurrency = runtime.NumCPU()
	}

	// dedicated goroutine for output to ensure output integrity
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outputGroup, _ := errgroup.WithContext(ctx)
	outputChan := make(chan any, concurrency)
	outputGroup.Go(func() error {
		return c.printer(ctx, outputChan, fieldList)
	})

	rowGroup, _ := errgroup.WithContext(ctx)
	rowChan := make(chan any, concurrency)
	// goroutines to encode rows
	for range concurrency {
		rowGroup.Go(func() error {
			return c.encoder(ctx, rowChan, outputChan, fileReader.SchemaHandler)
		})
	}

	// Output rows one by one to avoid running out of memory with a jumbo list
	for counter := uint64(0); counter < c.Limit; {
		rows, err := fileReader.ReadByNumber(c.ReadPageSize)
		if err != nil {
			return fmt.Errorf("failed to cat: %w", err)
		}
		if len(rows) == 0 {
			break
		}

		for i := 0; i < len(rows) && counter < c.Limit; i++ {
			if rand.Float32() >= c.SampleRatio {
				continue
			}
			// there is no known error at this moment
			rowChan <- rows[i]
			counter++
		}
	}
	close(rowChan)
	if err := rowGroup.Wait(); err != nil {
		return err
	}

	close(outputChan)
	if err := outputGroup.Wait(); err != nil {
		return err
	}

	return nil
}

func (c *CatCmd) valuesToCSV(values []string) (string, error) {
	// there is no standard for CSV, use go's CSV module to maintain minimum compatibility
	c.csvBuf.Reset()
	if err := c.csvWriter.Write(values); err != nil {
		// this should never happen
		return "", err
	}
	c.csvWriter.Flush()
	return c.csvBuf.String(), nil
}
