package cmd

import (
	"encoding/json"
	"fmt"

	pio "github.com/hangxie/parquet-tools/internal/io"
	pschema "github.com/hangxie/parquet-tools/internal/schema"
)

var (
	formatRaw  = "raw"
	formatJSON = "json"
	formatGo   = "go"
	formatCSV  = "csv"
)

// SchemaCmd is a kong command for schema
type SchemaCmd struct {
	pio.ReadOption
	Format string `short:"f" help:"Schema format (raw/json/go/csv)." enum:"raw,json,go,csv" default:"json"`
	URI    string `arg:"" predictor:"file" help:"URI of Parquet file."`
}

// Run does actual schema job
func (c SchemaCmd) Run() error {
	reader, err := pio.NewParquetFileReader(c.URI, c.ReadOption)
	if err != nil {
		return err
	}
	defer func() {
		_ = reader.PFile.Close()
	}()

	schemaRoot, err := pschema.NewSchemaTree(reader, pschema.SchemaOption{FailOnInt96: false})
	if err != nil {
		return err
	}

	switch c.Format {
	case formatRaw:
		schema, _ := json.Marshal(*schemaRoot)
		fmt.Println(string(schema))
	case formatJSON:
		fmt.Println(schemaRoot.JSONSchema())
	case formatGo:
		goStruct, err := schemaRoot.GoStruct()
		if err != nil {
			return err
		}
		fmt.Println(goStruct)
	case formatCSV:
		schema, err := schemaRoot.CSVSchema()
		if err != nil {
			return err
		}
		fmt.Println(schema)
	default:
		return fmt.Errorf("unknown schema format [%s]", c.Format)
	}

	return nil
}
