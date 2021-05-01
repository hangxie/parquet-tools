package cmd

import (
	"fmt"
)

// SizeCmd is a kong command for size
type SizeCmd struct {
	CommonOption
	Uncompressed bool `help:"Output uncompressed size."`
}

// Run does actual size job
func (c *SizeCmd) Run(ctx *Context) error {
	fmt.Println("size", c.URI)
	return nil
}
