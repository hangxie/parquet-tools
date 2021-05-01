package cmd

import (
	"fmt"
)

// CatCmd is a kong command for cat
type CatCmd struct {
	CommonOption
	JSON bool `help:"Output JSON format."`
}

// Run does actual cat job
func (c *CatCmd) Run(ctx *Context) error {
	fmt.Println("cat", c.URI)
	return nil
}
