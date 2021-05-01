package cmd

import (
	"fmt"
)

// MetaCmd is a kong command for meta
type MetaCmd struct {
	CommonOption
	OriginalType bool `help:"Print logical types in OriginalType representation."`
}

// Run does actual meta job
func (c *MetaCmd) Run(ctx *Context) error {
	fmt.Println("meta", c.URI)
	return nil
}
