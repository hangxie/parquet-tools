package cmd

import (
	"encoding/json"
	"fmt"
)

var (
	// semantic version
	version string
	// build time in ISO-8601 format
	build string
	// git hash for this build
	gitHash string
	// where the executable came from, can be:
	// - "source" or "" for build from source
	// - "github" for from github release
	// - "bottle" for from homebrew bottles
	source string
)

// VersionCmd is a kong command for version
type VersionCmd struct {
	JSON      bool `short:"j" help:"Output in JSON format." default:"false"`
	BuildTime bool `short:"b" help:"Output build time as well." default:"false"`
	GitHash   bool `short:"g" help:"Output git hash." default:"false"`
	Source    bool `short:"s" help:"Source of the executable." default:"false"`
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
		if c.Source {
			fmt.Println(source)
		}
		return nil
	}

	v := struct {
		Version   string
		BuildTime *string `json:",omitempty"`
		GitHash   *string `json:",omitempty"`
		Source    *string `json:",omitempty"`
	}{
		Version:   version,
		BuildTime: nil,
		GitHash:   nil,
		Source:    nil,
	}
	if c.BuildTime {
		v.BuildTime = &build
	}
	if c.GitHash {
		v.GitHash = &gitHash
	}
	if c.Source {
		v.Source = &source
	}
	buf, _ := json.Marshal(v)
	fmt.Println(string(buf))

	return nil
}
