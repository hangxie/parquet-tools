package cmd

import (
	"encoding/json"
	"fmt"
)

// VersionCmd is a kong command for version
type VersionCmd struct {
	JSON bool `short:"j" help:"Output in JSON format." default:"false"`
}

// Run does actual version job
func (c *VersionCmd) Run(ctx *Context) error {
	if c.JSON {
		v := struct {
			Version   string
			BuildTime string
		}{
			Version:   ctx.Version,
			BuildTime: ctx.Build,
		}
		buf, _ := json.Marshal(v)
		fmt.Println(string(buf))
	} else {
		fmt.Println("Version:", ctx.Version)
		fmt.Println("Build Time:", ctx.Build)
	}
	return nil
}
