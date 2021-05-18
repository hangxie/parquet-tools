package main

import (
	"github.com/alecthomas/kong"

	"github.com/hangxie/parquet-tools/cmd"
)

var (
	version string
	build   string
)

var cli struct {
	Cat      cmd.CatCmd      `cmd:"" help:"Prints the content of a Parquet file, data only."`
	Import   cmd.ImportCmd   `cmd:"" help:"Create Parquet file from other source data."`
	Meta     cmd.MetaCmd     `cmd:"" help:"Prints the metadata."`
	RowCount cmd.RowCountCmd `cmd:"" help:"Prints the count of rows."`
	Schema   cmd.SchemaCmd   `cmd:"" help:"Prints the schema."`
	Size     cmd.SizeCmd     `cmd:"" help:"Prints the size."`
	Version  cmd.VersionCmd  `cmd:"" help:"Show build version."`
}

func main() {
	cmdCtx := cmd.Context{
		Version: version,
		Build:   build,
	}
	ctx := kong.Parse(&cli)
	err := ctx.Run(&cmdCtx)
	ctx.FatalIfErrorf(err)
}
