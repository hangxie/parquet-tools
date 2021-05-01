package cmd

import (
	"fmt"
)

// RowCountCmd is a kong command for rowcount
type RowCountCmd struct {
	CommonOption
	Detailed bool `help:"Detailed rowcount."`
}

// Run does actual rowcount job
func (c *RowCountCmd) Run(ctx *Context) error {
	fmt.Println("rowcount", c.URI)
	return nil
}
