package importcmd

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	parquetSource "github.com/hangxie/parquet-go/v2/source"

	pio "github.com/hangxie/parquet-tools/io"
)

// Cmd is a kong command for import
type Cmd struct {
	Format     string `help:"Source file formats (csv/json/jsonl)." short:"f" enum:"csv,json,jsonl" default:"csv"`
	Schema     string `required:"" short:"m" predictor:"file" help:"Schema file name."`
	SkipHeader bool   `help:"Skip first line of CSV files" default:"false"`
	Source     string `required:"" short:"s" predictor:"file" help:"Source file name."`
	URI        string `arg:"" predictor:"file" help:"URI of Parquet file."`
	pio.WriteOption
}

// Run does actual import job
func (c Cmd) Run() error {
	switch c.Format {
	case "csv":
		return c.importCSV()
	case "json":
		return c.importJSON()
	case "jsonl":
		return c.importJSONL()
	}
	return fmt.Errorf("[%s] is not a recognized source format", c.Format)
}

func (c Cmd) closeWriter(pf parquetSource.ParquetFileWriter) error {
	// retry on particular errors according to https://github.com/colinmarc/hdfs/blob/v2.4.0/file_writer.go#L220-L226
	var err error
	for range 10 {
		err = pf.Close()
		if err != nil && strings.Contains(err.Error(), "replication in progress") {
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
	return err
}

func (c Cmd) importCSV() error {
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

	parquetWriter, err := pio.NewCSVWriter(c.URI, c.WriteOption, schema)
	if err != nil {
		return fmt.Errorf("failed to create CSV writer: %w", err)
	}

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
		if err = parquetWriter.WriteString(parquetFields); err != nil {
			return fmt.Errorf("failed to write [%v] to parquet: %w", fields, err)
		}
	}
	if err := parquetWriter.WriteStop(); err != nil {
		return fmt.Errorf("failed to close Parquet writer [%s]: %w", c.URI, err)
	}

	if err := c.closeWriter(parquetWriter.PFile); err != nil {
		return fmt.Errorf("failed to close Parquet file [%s]: %w", c.URI, err)
	}

	return nil
}

func (c Cmd) importJSON() error {
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

	parquetWriter, err := pio.NewJSONWriter(c.URI, c.WriteOption, string(schemaData))
	if err != nil {
		return fmt.Errorf("failed to create JSON writer: %w", err)
	}

	for _, record := range records {
		if err := parquetWriter.Write(string(record)); err != nil {
			return fmt.Errorf("failed to write to parquet file: %w", err)
		}
	}

	if err := parquetWriter.WriteStop(); err != nil {
		return fmt.Errorf("failed to close Parquet writer [%s]: %w", c.URI, err)
	}
	if err := c.closeWriter(parquetWriter.PFile); err != nil {
		return fmt.Errorf("failed to close Parquet file [%s]: %w", c.URI, err)
	}

	return nil
}

func (c Cmd) importJSONL() error {
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
	scanner.Split(bufio.ScanLines)

	parquetWriter, err := pio.NewJSONWriter(c.URI, c.WriteOption, string(schemaData))
	if err != nil {
		return fmt.Errorf("failed to create JSON writer: %w", err)
	}

	for scanner.Scan() {
		jsonData := scanner.Bytes()
		if err := json.Unmarshal(jsonData, &dummy); err != nil {
			return fmt.Errorf("invalid JSON string: %s", string(jsonData))
		}

		if err := parquetWriter.Write(string(jsonData)); err != nil {
			return fmt.Errorf("failed to write to parquet file: %w", err)
		}
	}
	if err := parquetWriter.WriteStop(); err != nil {
		return fmt.Errorf("failed to close Parquet writer [%s]: %w", c.URI, err)
	}
	if err := c.closeWriter(parquetWriter.PFile); err != nil {
		return fmt.Errorf("failed to close Parquet file [%s]: %w", c.URI, err)
	}

	return nil
}
