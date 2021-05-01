package cmd

import (
	"fmt"
)

// DumpCmd is a kong command for dump
type DumpCmd struct {
	CommonOption
	Colume      []string `help:"Dump only the given column, can be specified more than once."`
	DisableData bool     `help:"Do not dump colume data."`
	DisableMeta bool     `help:"Do not dump row group and page metadata."`
}

// Run does actual dump job
func (c *DumpCmd) Run(ctx *Context) error {
	fmt.Println("dump", c.URI)
	return nil
}
