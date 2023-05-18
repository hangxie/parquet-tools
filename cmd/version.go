package cmd

import (
	"encoding/json"
	"fmt"
)

// VersionCmd is a kong command for version
type VersionCmd struct {
	JSON      bool `short:"j" help:"Output in JSON format." default:"false"`
	BuildTime bool `short:"b" help:"Output build time as well." default:"false"`
}

// Run does actual version job
func (c VersionCmd) Run(ctx *Context) error {
	if ctx == nil {
		return fmt.Errorf("cannot retrieve build information")
	}

	if !c.JSON {
		fmt.Println(ctx.Version)
		if c.BuildTime {
			fmt.Println(ctx.Build)
		}
		return nil
	}

	v := struct {
		Version   string
		BuildTime *string `json:",omitempty"`
	}{
		Version:   ctx.Version,
		BuildTime: nil,
	}
	if c.BuildTime {
		v.BuildTime = &ctx.Build
	}
	buf, _ := json.Marshal(v)
	fmt.Println(string(buf))

	return nil
}
