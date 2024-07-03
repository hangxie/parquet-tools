package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/hangxie/parquet-tools/internal"
)

var (
	formatRaw  string = "raw"
	formatJSON string = "json"
	formatGo   string = "go"
	formatCSV  string = "csv"
)

// SchemaCmd is a kong command for schema
type SchemaCmd struct {
	internal.ReadOption
	Format string `short:"f" help:"Schema format (raw/json/go/csv)." enum:"raw,json,go,csv" default:"json"`
}

// Run does actual schema job
func (c SchemaCmd) Run() error {
	reader, err := internal.NewParquetFileReader(c.ReadOption)
	if err != nil {
		return err
	}
	defer reader.PFile.Close()

	schemaRoot := internal.NewSchemaTree(reader)
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
