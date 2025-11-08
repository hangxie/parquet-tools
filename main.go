package main

import (
	"os"

	"github.com/alecthomas/kong"
	"github.com/posener/complete"
	"github.com/willabides/kongplete"

	"github.com/hangxie/parquet-tools/cmd"
)

var cli struct {
	Cat              cmd.CatCmd                   `cmd:"" help:"Prints the content of a Parquet file, data only."`
	Import           cmd.ImportCmd                `cmd:"" help:"Create Parquet file from other source data."`
	Inspect          cmd.InspectCmd               `cmd:"" help:"Inspect Parquet file structure in detail."`
	Merge            cmd.MergeCmd                 `cmd:"" help:"Merge multiple parquet files into one."`
	Meta             cmd.MetaCmd                  `cmd:"" help:"Prints the metadata."`
	RowCount         cmd.RowCountCmd              `cmd:"" help:"Prints the count of rows."`
	Schema           cmd.SchemaCmd                `cmd:"" help:"Prints the schema."`
	ShellCompletions kongplete.InstallCompletions `cmd:"" help:"Install/uninstall shell completions"`
	Size             cmd.SizeCmd                  `cmd:"" help:"Prints the size."`
	Split            cmd.SplitCmd                 `cmd:"" help:"Split into multiple parquet files."`
	Version          cmd.VersionCmd               `cmd:"" help:"Show build version."`
}

func main() {
	parser := kong.Must(
		&cli,
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{Compact: true}),
		kong.Description("A utility to inspect Parquet files, for full usage see https://github.com/hangxie/parquet-tools/blob/main/README.md"),
	)
	kongplete.Complete(parser, kongplete.WithPredictor("file", complete.PredictFiles("*")))

	ctx, err := parser.Parse(os.Args[1:])
	parser.FatalIfErrorf(err)
	ctx.FatalIfErrorf(ctx.Run())
}
