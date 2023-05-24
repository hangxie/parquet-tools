package cmd

import (
	"encoding/json"
	"fmt"
)

var (
	version string
	build   string
	gitHash string
)

// VersionCmd is a kong command for version
type VersionCmd struct {
	JSON      bool `short:"j" help:"Output in JSON format." default:"false"`
	BuildTime bool `short:"b" help:"Output build time as well." default:"false"`
	GitHash   bool `short:"g" help:"Output git hash." default:"false"`
}

// Run does actual version job
func (c VersionCmd) Run() error {
	if !c.JSON {
		fmt.Println(version)
		if c.BuildTime {
			fmt.Println(build)
		}
		if c.GitHash {
			fmt.Println(gitHash)
		}
		return nil
	}

	v := struct {
		Version   string
		BuildTime *string `json:",omitempty"`
		GitHash   *string `json:",omitempty"`
	}{
		Version:   version,
		BuildTime: nil,
		GitHash:   nil,
	}
	if c.BuildTime {
		v.BuildTime = &build
	}
	if c.GitHash {
		v.GitHash = &gitHash
	}
	buf, _ := json.Marshal(v)
	fmt.Println(string(buf))

	return nil
}
