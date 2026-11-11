package importcmd

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	pio "github.com/hangxie/parquet-tools/io"
)

const (
	defaultJSONLMaxLineSize = 16 * 1024 * 1024
	maxJSONLDelimiterSize   = 2
)

// Cmd is a kong command for import
type Cmd struct {
	FieldDelimiter   string `name:"field-delimiter" help:"Delimiter separating nested field path components in field and column parameters" default:"."`
	Format           string `help:"Source file formats (csv/json/jsonl)." short:"f" enum:"csv,json,jsonl" default:"csv"`
	JSONLMaxLineSize int    `name:"jsonl-max-line-size" help:"Maximum JSONL record size in bytes, excluding the line delimiter." default:"16777216"`
	Schema           string `required:"" short:"m" predictor:"file" help:"Schema file name."`
	SkipHeader       bool   `help:"Skip first line of CSV files" default:"false"`
	Source           string `required:"" short:"s" predictor:"file" help:"Source file name."`
	URI              string `arg:"" predictor:"file" help:"URI of Parquet file."`
	ValueMode        string `name:"value-mode" help:"input value representation (interpreted/raw); raw uses physical values and base64 for byte-backed columns, preserving text columns." enum:"interpreted,raw" default:"interpreted"`
	pio.WriteOption
}

// Run does actual import job
func (c Cmd) Run(ctx context.Context) error {
	mode, err := pio.ParseValueMode(c.ValueMode)
	if err != nil {
		return err
	}
	c.WriteOption.ValueMode = mode
	if err := pio.ValidateFieldDelimiter(c.FieldDelimiter); err != nil {
		return err
	}
	c.WriteOption.FieldDelimiter = c.FieldDelimiter
	switch c.Format {
	case "csv":
		return c.importCSV(ctx)
	case "json":
		return c.importJSON(ctx)
	case "jsonl":
		return c.importJSONL(ctx)
	}
	return fmt.Errorf("[%s] is not a recognized source format", c.Format)
}

func (c Cmd) importCSV(ctx context.Context) error {
	schemaData, err := os.ReadFile(c.Schema)
	if err != nil {
		return fmt.Errorf("failed to load schema from [%s]: %w", c.Schema, err)
	}

	var schema []string
	for line := range strings.SplitSeq(string(schemaData), "\n") {
		line = strings.Trim(line, "\r\n\t ")
		if line == "" {
			continue
		}
		schema = append(schema, line)
	}

	csvFile, err := os.Open(c.Source)
	if err != nil {
		return fmt.Errorf("failed to open CSV file [%s]: %w", c.Source, err)
	}
	defer func() {
		_ = csvFile.Close()
	}()
	csvReader := csv.NewReader(csvFile)

	parquetWriter, err := pio.NewCSVWriter(ctx, c.URI, c.WriteOption, schema)
	if err != nil {
		return fmt.Errorf("failed to create CSV writer: %w", err)
	}
	finished := false
	defer func() {
		if !finished {
			pio.AbortWrite(&parquetWriter.ParquetWriter)
		}
	}()

	if c.SkipHeader {
		_, _ = csvReader.Read()
	}
	for {
		fields, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV record from [%s]: %w", c.Source, err)
		}
		parquetFields := make([]*string, len(fields))
		for i := range fields {
			parquetFields[i] = &fields[i]
		}
		if err = parquetWriter.WriteStringWithContext(ctx, parquetFields); err != nil {
			return fmt.Errorf("failed to write [%v] to parquet: %w", fields, err)
		}
	}
	finished = true
	return pio.FinishWrite(ctx, &parquetWriter.ParquetWriter, c.URI)
}

func (c Cmd) importJSON(ctx context.Context) error {
	schemaData, err := os.ReadFile(c.Schema)
	if err != nil {
		return fmt.Errorf("failed to load schema from [%s]: %w", c.Schema, err)
	}

	jsonData, err := os.ReadFile(c.Source)
	if err != nil {
		return fmt.Errorf("failed to load source from [%s]: %w", c.Source, err)
	}

	var dummy map[string]any
	if err := json.Unmarshal(schemaData, &dummy); err != nil {
		return fmt.Errorf("content of [%s] is not a valid schema JSON", c.Schema)
	}

	var records []json.RawMessage
	if err := json.Unmarshal(jsonData, &records); err != nil {
		return fmt.Errorf("content of [%s] is not a valid JSON array: %w", c.Source, err)
	}

	parquetWriter, err := pio.NewJSONWriter(ctx, c.URI, c.WriteOption, string(schemaData))
	if err != nil {
		return fmt.Errorf("failed to create JSON writer: %w", err)
	}
	finished := false
	defer func() {
		if !finished {
			pio.AbortWrite(&parquetWriter.ParquetWriter)
		}
	}()

	for _, record := range records {
		if err := parquetWriter.WriteWithContext(ctx, string(record)); err != nil {
			return fmt.Errorf("failed to write to parquet file: %w", err)
		}
	}

	finished = true
	return pio.FinishWrite(ctx, &parquetWriter.ParquetWriter, c.URI)
}

func (c Cmd) importJSONL(ctx context.Context) error {
	maxLineSize := c.JSONLMaxLineSize
	if maxLineSize == 0 {
		maxLineSize = defaultJSONLMaxLineSize
	}
	if maxLineSize < 0 {
		return errors.New("JSONL maximum line size must be greater than zero")
	}
	if maxLineSize > math.MaxInt-maxJSONLDelimiterSize {
		return errors.New("JSONL maximum line size is too large")
	}

	schemaData, err := os.ReadFile(c.Schema)
	if err != nil {
		return fmt.Errorf("failed to load schema from [%s]: %w", c.Schema, err)
	}

	var dummy map[string]any
	if err := json.Unmarshal(schemaData, &dummy); err != nil {
		return fmt.Errorf("content of [%s] is not a valid schema JSON", c.Schema)
	}

	jsonlFile, err := os.Open(c.Source)
	if err != nil {
		return fmt.Errorf("failed to open source file [%s]: %w", c.Source, err)
	}
	defer func() {
		_ = jsonlFile.Close()
	}()
	scanner := bufio.NewScanner(jsonlFile)
	scannerBufferSize := maxLineSize + maxJSONLDelimiterSize
	scanner.Buffer(
		make([]byte, 0, min(bufio.MaxScanTokenSize, scannerBufferSize)),
		scannerBufferSize,
	)
	scanner.Split(bufio.ScanLines)

	parquetWriter, err := pio.NewJSONWriter(ctx, c.URI, c.WriteOption, string(schemaData))
	if err != nil {
		return fmt.Errorf("failed to create JSON writer: %w", err)
	}
	finished := false
	defer func() {
		if !finished {
			pio.AbortWrite(&parquetWriter.ParquetWriter)
		}
	}()

	for scanner.Scan() {
		jsonData := scanner.Bytes()
		if len(jsonData) > maxLineSize {
			return fmt.Errorf(
				"JSONL line exceeds maximum line size of %d bytes in [%s]",
				maxLineSize,
				c.Source,
			)
		}
		if err := json.Unmarshal(jsonData, &dummy); err != nil {
			return fmt.Errorf("invalid JSON string: %s", string(jsonData))
		}

		if err := parquetWriter.WriteWithContext(ctx, string(jsonData)); err != nil {
			return fmt.Errorf("failed to write to parquet file: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf(
			"failed to read JSONL source file [%s] with maximum line size %d bytes: %w",
			c.Source,
			maxLineSize,
			err,
		)
	}
	finished = true
	return pio.FinishWrite(ctx, &parquetWriter.ParquetWriter, c.URI)
}
