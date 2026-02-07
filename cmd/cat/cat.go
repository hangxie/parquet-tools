package cat

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"

	"github.com/hangxie/parquet-go/v2/marshal"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/types"
	"golang.org/x/sync/errgroup"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

// Cmd is a kong command for cat
type Cmd struct {
	Concurrent   bool    `help:"enable concurrent output" default:"false"`
	FailOnInt96  bool    `help:"fail command if INT96 data type is present." name:"fail-on-int96" default:"false"`
	Format       string  `short:"f" help:"output format (json/jsonl/csv/tsv)" enum:"json,jsonl,csv,tsv" default:"json"`
	GeoFormat    string  `help:"experimental, output format (geojson/hex/base64) for geospatial fields" enum:"geojson,hex,base64" default:"geojson"`
	Limit        uint64  `short:"l" help:"Max number of rows to output, 0 means no limit." default:"0"`
	NoHeader     bool    `help:"(CSV/TSV only) do not output field name as header" default:"false"`
	ReadPageSize int     `help:"Page size to read from Parquet." default:"1000"`
	SampleRatio  float32 `short:"s" help:"Sample ratio (0.0-1.0)." default:"1.0"`
	Skip         int64   `short:"k" help:"Skip rows before apply other logics." default:"0"`
	URI          string  `arg:"" predictor:"file" help:"URI of Parquet file."`
	pio.ReadOption
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
func (c Cmd) Run() error {
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
		return fmt.Errorf("unknown geo format: [%s]", c.GeoFormat)
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
		return fmt.Errorf("unknown format: [%s]", c.Format)
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

func (c *Cmd) outputHeader(schemaRoot *pschema.SchemaNode) ([]string, error) {
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

	strBuilder := new(strings.Builder)
	csvWriter := csv.NewWriter(strBuilder)
	csvWriter.Comma = delimiter[c.Format].fieldDelimiter
	line, err := c.valuesToCSV(fieldList, strBuilder, csvWriter)
	if err != nil {
		return nil, err
	}

	if !c.NoHeader {
		fmt.Print(line)
	}
	return fieldList, nil
}

func (c *Cmd) retrieveFieldDef(fileReader *reader.ParquetReader) ([]string, error) {
	schemaRoot, err := pschema.NewSchemaTree(fileReader, pschema.SchemaOption{FailOnInt96: c.FailOnInt96})
	if err != nil {
		return nil, err
	}

	// CSV and TSV do not support nested schema
	fieldList, err := c.outputHeader(schemaRoot)
	if err != nil {
		return nil, err
	}

	return fieldList, nil
}

func mapToStrList(flatValues map[string]any, fieldList []string) []string {
	values := make([]string, len(flatValues))
	for index, field := range fieldList {
		switch val := flatValues[field].(type) {
		case nil:
			// nil is just empty in CSV/TSV
		default:
			values[index] = fmt.Sprint(val)
		}
	}
	return values
}

func (c Cmd) encoder(ctx context.Context, rowChan chan any, outputChan chan string, schemaHandler *schema.SchemaHandler, fieldList []string) error {
	strBuilder := new(strings.Builder)
	csvWriter := csv.NewWriter(strBuilder)
	csvWriter.Comma = delimiter[c.Format].fieldDelimiter
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case row, more := <-rowChan:
			if !more {
				return nil
			}
			rowStruct, err := marshal.ConvertToJSONFriendly(row, schemaHandler)
			if err != nil {
				return err
			}

			// Format the row as a string based on the format
			var formattedRow string
			switch c.Format {
			case "json", "jsonl":
				buf, err := json.Marshal(rowStruct)
				if err != nil {
					return err
				}
				formattedRow = string(buf)
			case "csv", "tsv":
				values := mapToStrList(rowStruct.(map[string]any), fieldList)
				line, err := c.valuesToCSV(values, strBuilder, csvWriter)
				if err != nil {
					return err
				}
				formattedRow = strings.TrimRight(line, "\n")
			default:
				return fmt.Errorf("unsupported format: [%s]", c.Format)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case outputChan <- formattedRow:
			}
		}
	}
}

func (c Cmd) printer(ctx context.Context, outputChan chan string) error {
	fmt.Print(delimiter[c.Format].begin)
	defer func() {
		fmt.Print(delimiter[c.Format].end + "\n")
	}()

	isFirstRow := true
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case formattedRow, more := <-outputChan:
			if !more {
				return nil
			}

			if isFirstRow {
				isFirstRow = false
			} else {
				fmt.Print(delimiter[c.Format].lineDelimiter)
			}
			fmt.Print(formattedRow)
		}
	}
}

func (c Cmd) outputRows(fileReader *reader.ParquetReader) error {
	fieldList, err := c.retrieveFieldDef(fileReader)
	if err != nil {
		return err
	}

	// skip rows
	if err := fileReader.SkipRows(c.Skip); err != nil {
		return err
	}

	// Always use at least 2 goroutines: encoder + printer
	// In non-concurrent mode, use single encoder, otherwise use multiple encoders
	concurrency := 1
	if c.Concurrent {
		concurrency = runtime.NumCPU()
	}

	// Use a single errgroup for all goroutines
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)

	// Use appropriate buffer size
	bufferSize := concurrency * c.ReadPageSize
	outputChan := make(chan string, bufferSize)
	rowChan := make(chan any, bufferSize)

	// Start printer goroutine (always exactly 1)
	g.Go(func() error {
		return c.printer(gctx, outputChan)
	})

	// Start encoder goroutines (number is determined by concurrency)
	var encodersWg sync.WaitGroup
	encodersWg.Add(concurrency)

	for range concurrency {
		g.Go(func() error {
			defer encodersWg.Done()
			return c.encoder(gctx, rowChan, outputChan, fileReader.SchemaHandler, fieldList)
		})
	}

	// Start a producer goroutine to avoid blocking main thread
	g.Go(func() error {
		defer close(rowChan)

		for counter := uint64(0); counter < c.Limit; {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}

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
				select {
				case rowChan <- rows[i]:
					counter++
				case <-gctx.Done():
					return gctx.Err()
				}
			}
		}
		return nil
	})

	// Wait for encoders to complete
	encodersWg.Wait()
	close(outputChan)

	// Wait for all goroutines to complete
	return g.Wait()
}

func (c *Cmd) valuesToCSV(values []string, strBuilder *strings.Builder, csvWriter *csv.Writer) (string, error) {
	// there is no standard for CSV, use go's CSV module to maintain minimum compatibility
	strBuilder.Reset()
	if err := csvWriter.Write(values); err != nil {
		// this should never happen
		return "", err
	}
	csvWriter.Flush()
	return strBuilder.String(), nil
}
