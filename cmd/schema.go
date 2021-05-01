package cmd

import (
	"fmt"
)

// SchemaCmd is a kong command for schema
type SchemaCmd struct {
	CommonOption
	JSON     bool `help:"Output JSON format." default:"true"`
	GoStruct bool `help:"Output go struct format."`
}

// Run does actual schema job
func (c *SchemaCmd) Run(ctx *Context) error {
	fmt.Println("schema", c.URI)
	return nil
}
