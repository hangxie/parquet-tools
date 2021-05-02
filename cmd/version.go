package cmd

import (
	"fmt"
)

// VersionCmd is a kong command for version
type VersionCmd struct {
}

// Run does actual version job
func (c *VersionCmd) Run(ctx *Context) error {
	fmt.Println("Version: ", ctx.Version)
	fmt.Println("Build Time: ", ctx.Build)
	return nil
}
