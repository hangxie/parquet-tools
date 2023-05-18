package cmd

import (
	"encoding/json"
	"fmt"
)

var (
	version string
	build   string
)

// VersionCmd is a kong command for version
type VersionCmd struct {
	JSON      bool `short:"j" help:"Output in JSON format." default:"false"`
	BuildTime bool `short:"b" help:"Output build time as well." default:"false"`
}

// Run does actual version job
func (c VersionCmd) Run() error {
	if !c.JSON {
		fmt.Println(version)
		if c.BuildTime {
			fmt.Println(build)
		}
		return nil
	}

	v := struct {
		Version   string
		BuildTime *string `json:",omitempty"`
	}{
		Version:   version,
		BuildTime: nil,
	}
	if c.BuildTime {
		v.BuildTime = &build
	}
	buf, _ := json.Marshal(v)
	fmt.Println(string(buf))

	return nil
}
