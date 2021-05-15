package cmd

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

// ImportCmd is a kong command for import
type ImportCmd struct {
	CommonOption
	Source string `required:"" help:"Source file name."`
	Format string `help:"Source file format." enum:"csv,json" default:"csv"`
	Schema string `required:"" help:"Schema file name."`
}

// Run does actual import job
func (c *ImportCmd) Run(ctx *Context) error {
	schemaData, err := ioutil.ReadFile(c.Schema)
	if err != nil {
		return fmt.Errorf("failed to load schema from %s: %s", c.Schema, err.Error())
	}

	switch c.Format {
	case "csv":
		return c.importCSV(c.Source, c.URI, string(schemaData))
	case "json":
		fmt.Println("TBD")
	default:
		return fmt.Errorf("[%s] is not a recognized source format", c.Format)
	}
	return nil
}

func (c *ImportCmd) importCSV(source string, target string, schemaData string) error {
	schema := []string{}
	for _, line := range strings.Split(string(schemaData), "\n") {
		line = strings.TrimFunc(line, func(r rune) bool {
			return r == ' ' || r == '\r' || r == '\t' || r == '\n'
		})
		if line != "" {
			schema = append(schema, line)
		}
	}

	csvFile, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("failed to open CSV file %s: %s", source, err.Error())
	}
	defer csvFile.Close()
	csvReader := csv.NewReader(csvFile)

	parquetWriter, err := newCSVWriter(target, schema)
	if err != nil {
		return err
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
		return fmt.Errorf("failed to close Parquet writer %s: %s", target, err.Error())
	}
	if err := parquetWriter.PFile.Close(); err != nil {
		return fmt.Errorf("failed to close Parquet file %s: %s", target, err.Error())
	}

	return nil
}
