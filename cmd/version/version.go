package version

import (
	"encoding/json"
	"fmt"

	pio "github.com/hangxie/parquet-tools/io"
)

var (
	// semantic version
	version string
	// build time in ISO-8601 format
	build string
	// where the executable came from, can be:
	// - "source" or "" for build from source
	// - "github" for from github release
	// - "Homebrew" for from homebrew bottles
	source string
)

// Cmd is a kong command for version
type Cmd struct {
	JSON      bool `short:"j" help:"Output in JSON format." default:"false"`
	All       bool `short:"a" help:"Output all version details." default:"false"`
	BuildTime bool `short:"b" help:"Output build time." default:"false"`
	Source    bool `short:"s" help:"Source of the executable." default:"false"`
	pio.ReadOption
}

// Run does actual version job
func (c Cmd) Run() error {
	if c.All {
		c.BuildTime = true
		c.Source = true
	}

	if !c.JSON {
		fmt.Println(version)
		if c.BuildTime {
			fmt.Println(build)
		}
		if c.Source {
			fmt.Println(source)
		}
		return nil
	}

	v := struct {
		Version   string
		BuildTime *string `json:",omitempty"`
		Source    *string `json:",omitempty"`
	}{
		Version:   version,
		BuildTime: nil,
		Source:    nil,
	}
	if c.BuildTime {
		v.BuildTime = &build
	}
	if c.Source {
		v.Source = &source
	}
	buf, _ := json.Marshal(v)
	fmt.Println(string(buf))

	return nil
}
