package main

import (
	"os"

	"github.com/alecthomas/kong"
	"github.com/posener/complete"
	"github.com/willabides/kongplete"

	"github.com/hangxie/parquet-tools/cmd"
)

var (
	version string
	build   string
)

var cli struct {
	Cat              cmd.CatCmd                   `cmd:"" help:"Prints the content of a Parquet file, data only."`
	Import           cmd.ImportCmd                `cmd:"" help:"Create Parquet file from other source data."`
	Meta             cmd.MetaCmd                  `cmd:"" help:"Prints the metadata."`
	RowCount         cmd.RowCountCmd              `cmd:"" help:"Prints the count of rows."`
	Schema           cmd.SchemaCmd                `cmd:"" help:"Prints the schema."`
	ShellCompletions kongplete.InstallCompletions `cmd:"" help:"Install/uninstall shell completions"`
	Size             cmd.SizeCmd                  `cmd:"" help:"Prints the size."`
	Version          cmd.VersionCmd               `cmd:"" help:"Show build version."`
}

func main() {
	parser := kong.Must(&cli, kong.UsageOnError(), kong.ConfigureHelp(kong.HelpOptions{Compact: true}))
	kongplete.Complete(parser, kongplete.WithPredictor("file", complete.PredictFiles("*")))

	ctx, err := parser.Parse(os.Args[1:])
	parser.FatalIfErrorf(err)
	ctx.FatalIfErrorf(ctx.Run(&cmd.Context{
		Version: version,
		Build:   build,
	}))
}
