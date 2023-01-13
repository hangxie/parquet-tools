package cmd

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// ImportCmd is a kong command for import
type ImportCmd struct {
	WriteOption
	Source     string `required:"" short:"s" predictor:"file" help:"Source file name."`
	Format     string `help:"Source file formats (csv/json/jsonl)." short:"f" enum:"csv,json,jsonl" default:"csv"`
	Schema     string `required:"" short:"m" predictor:"file" help:"Schema file name."`
	SkipHeader bool   `help:"Skip first line of CSV files" default:"false"`
}

// Run does actual import job
func (c *ImportCmd) Run(ctx *Context) error {
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

func (c *ImportCmd) importCSV() error {
	schemaData, err := os.ReadFile(c.Schema)
	if err != nil {
		return fmt.Errorf("failed to load schema from %s: %s", c.Schema, err.Error())
	}
	schema := []string{}
	for _, line := range strings.Split(string(schemaData), "\n") {
		line = strings.Trim(line, "\r\n\t ")
		if line != "" {
			schema = append(schema, line)
		}
	}

	csvFile, err := os.Open(c.Source)
	if err != nil {
		return fmt.Errorf("failed to open CSV file %s: %s", c.Source, err.Error())
	}
	defer csvFile.Close()
	csvReader := csv.NewReader(csvFile)

	parquetWriter, err := newCSVWriter(c.WriteOption, schema)
	if err != nil {
		return fmt.Errorf("failed to create CSV writer: %s", err.Error())
	}

	if c.SkipHeader {
		_, _ = csvReader.Read()
	}
	for {
		fields, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		parquetFields := make([]*string, len(fields))
		for i := 0; i < len(fields); i++ {
			parquetFields[i] = &fields[i]
		}
		if err = parquetWriter.WriteString(parquetFields); err != nil {
			return fmt.Errorf("failed to write [%v] to parquet: %s", fields, err.Error())
		}
	}
	if err := parquetWriter.WriteStop(); err != nil {
		return fmt.Errorf("failed to close Parquet writer %s: %s", c.URI, err.Error())
	}
	if err := parquetWriter.PFile.Close(); err != nil {
		return fmt.Errorf("failed to close Parquet file %s: %s", c.URI, err.Error())
	}

	return nil
}

func (c *ImportCmd) importJSON() error {
	schemaData, err := os.ReadFile(c.Schema)
	if err != nil {
		return fmt.Errorf("failed to load schema from %s: %s", c.Schema, err.Error())
	}

	jsonData, err := os.ReadFile(c.Source)
	if err != nil {
		return fmt.Errorf("failed to load source from %s: %s", c.Source, err.Error())
	}

	var dummy map[string]interface{}
	if err := json.Unmarshal([]byte(schemaData), &dummy); err != nil {
		return fmt.Errorf("content of %s is not a valid schema JSON", c.Schema)
	}
	if err := json.Unmarshal(jsonData, &dummy); err != nil {
		return fmt.Errorf("invalid JSON string: %s", string(jsonData))
	}

	parquetWriter, err := newJSONWriter(c.WriteOption, string(schemaData))
	if err != nil {
		return fmt.Errorf("failed to create JSON writer: %s", err.Error())
	}

	if err := parquetWriter.Write(string(jsonData)); err != nil {
		return fmt.Errorf("failed to write to parquet file: %s", err.Error())
	}

	if err := parquetWriter.WriteStop(); err != nil {
		return fmt.Errorf("failed to close Parquet writer %s: %s", c.URI, err.Error())
	}
	if err := parquetWriter.PFile.Close(); err != nil {
		return fmt.Errorf("failed to close Parquet file %s: %s", c.URI, err.Error())
	}

	return nil
}

func (c *ImportCmd) importJSONL() error {
	schemaData, err := os.ReadFile(c.Schema)
	if err != nil {
		return fmt.Errorf("failed to load schema from %s: %s", c.Schema, err.Error())
	}

	var dummy map[string]interface{}
	if err := json.Unmarshal([]byte(schemaData), &dummy); err != nil {
		return fmt.Errorf("content of %s is not a valid schema JSON", c.Schema)
	}

	jsonlFile, err := os.Open(c.Source)
	if err != nil {
		return fmt.Errorf("failed to open source file %s", c.Source)
	}
	defer jsonlFile.Close()
	scanner := bufio.NewScanner(jsonlFile)
	scanner.Split(bufio.ScanLines)

	parquetWriter, err := newJSONWriter(c.WriteOption, string(schemaData))
	if err != nil {
		return fmt.Errorf("failed to create JSON writer: %s", err.Error())
	}

	for scanner.Scan() {
		jsonData := scanner.Bytes()
		if err := json.Unmarshal(jsonData, &dummy); err != nil {
			return fmt.Errorf("invalid JSON string: %s", string(jsonData))
		}

		if err := parquetWriter.Write(string(jsonData)); err != nil {
			return fmt.Errorf("failed to write to parquet file: %s", err.Error())
		}
	}
	if err := parquetWriter.WriteStop(); err != nil {
		return fmt.Errorf("failed to close Parquet writer %s: %s", c.URI, err.Error())
	}
	if err := parquetWriter.PFile.Close(); err != nil {
		return fmt.Errorf("failed to close Parquet file %s: %s", c.URI, err.Error())
	}

	return nil
}
