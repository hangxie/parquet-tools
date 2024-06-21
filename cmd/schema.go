package cmd

import (
	"encoding/json"
	"fmt"
	"strings"

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
	case formatCSV:
		jsonSchema := internal.NewJSONSchemaNode(*schemaRoot).Schema()
		s := make([]string, len(jsonSchema.Fields))
		for i, f := range jsonSchema.Fields {
			if len(f.Fields) != 0 {
				return fmt.Errorf("CSV supports flat schema only")
			}
			if strings.Contains(f.Tag, "repetitiontype=REPEATED") {
				return fmt.Errorf("CSV does not support column in LIST type")
			}
			if strings.Contains(f.Tag, "repetitiontype=OPTIONAL") {
				return fmt.Errorf("CSV does not support optional column")
			}
			s[i] = strings.Replace(f.Tag, ", repetitiontype=REQUIRED", "", 1)
		}
		fmt.Println(strings.Join(s, "\n"))
	default:
		return fmt.Errorf("unknown schema format [%s]", c.Format)
	}

	return nil
}
