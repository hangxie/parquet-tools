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
)

// SchemaCmd is a kong command for schema
type SchemaCmd struct {
	internal.ReadOption
	Format string `short:"f" help:"Schema format (raw/json/go)." enum:"raw,json,go" default:"json"`
}

// Run does actual schema job
func (c SchemaCmd) Run(ctx *Context) error {
	reader, err := internal.NewParquetFileReader(c.ReadOption)
	if err != nil {
		return err
	}
	defer reader.PFile.Close()

	schemaRoot := internal.NewSchemaTree(reader)
	switch c.Format {
	case formatRaw:
		res, _ := json.Marshal(*schemaRoot)
		fmt.Printf("%s\n", res)
	case formatJSON:
		s := internal.NewJSONSchemaNode(*schemaRoot).Schema()
		res, _ := json.Marshal(s)
		fmt.Printf("%s\n", res)
	case formatGo:
		snippet, err := internal.NewGoStructNode(*schemaRoot).String()
		if err != nil {
			return err
		}
		fmt.Printf("type %s %s\n", schemaRoot.Name, snippet)
	default:
		return fmt.Errorf("unknown schema format [%s]", c.Format)
	}

	return nil
}
