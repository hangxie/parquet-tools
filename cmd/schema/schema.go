package schema

import (
	"encoding/json"
	"fmt"
	"go/format"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

var (
	formatRaw  = "raw"
	formatJSON = "json"
	formatGo   = "go"
	formatCSV  = "csv"
)

// Cmd is a kong command for schema
type Cmd struct {
	CamelCase            bool   `help:"enforce go struct field name to be CamelCase" default:"false"`
	Format               string `short:"f" help:"Schema format (raw/json/go/csv)." enum:"raw,json,go,csv" default:"json"`
	SkipPageEncoding     bool   `help:"skip reading page encoding information" name:"skip-page-encoding" default:"false"`
	ShowCompressionCodec bool   `help:"show compression codec for each column" name:"show-compression-codec" default:"false"`
	URI                  string `arg:"" predictor:"file" help:"URI of Parquet file."`
	pio.ReadOption
}

// Run does actual schema job
func (c Cmd) Run() error {
	reader, err := pio.NewParquetFileReader(c.URI, c.ReadOption)
	if err != nil {
		return err
	}
	defer func() {
		_ = reader.PFile.Close()
	}()

	schemaRoot, err := pschema.NewSchemaTree(reader, pschema.SchemaOption{FailOnInt96: false, SkipPageEncoding: c.SkipPageEncoding, WithCompressionCodec: c.ShowCompressionCodec})
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
		goStruct, err := schemaRoot.GoStruct(c.CamelCase)
		if err != nil {
			return err
		}
		formatted, err := format.Source([]byte(goStruct))
		if err != nil {
			return err
		}
		fmt.Println(string(formatted))
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
