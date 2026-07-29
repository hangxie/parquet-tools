package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/alecthomas/kong"
	"github.com/posener/complete"
	"github.com/willabides/kongplete"

	"github.com/hangxie/parquet-tools/cmd/cat"
	importcmd "github.com/hangxie/parquet-tools/cmd/import"
	"github.com/hangxie/parquet-tools/cmd/inspect"
	"github.com/hangxie/parquet-tools/cmd/merge"
	"github.com/hangxie/parquet-tools/cmd/meta"
	"github.com/hangxie/parquet-tools/cmd/retype"
	"github.com/hangxie/parquet-tools/cmd/rowcount"
	"github.com/hangxie/parquet-tools/cmd/schema"
	"github.com/hangxie/parquet-tools/cmd/size"
	"github.com/hangxie/parquet-tools/cmd/split"
	"github.com/hangxie/parquet-tools/cmd/transcode"
	"github.com/hangxie/parquet-tools/cmd/version"
	pio "github.com/hangxie/parquet-tools/io"
)

type cli struct {
	Cat              cat.Cmd                      `cmd:"" help:"Prints the content of a Parquet file, data only."`
	Import           importcmd.Cmd                `cmd:"" help:"Create Parquet file from other source data."`
	Inspect          inspect.Cmd                  `cmd:"" help:"Inspect Parquet file structure in detail."`
	Merge            merge.Cmd                    `cmd:"" help:"Merge multiple parquet files into one."`
	Meta             meta.Cmd                     `cmd:"" help:"Prints the metadata."`
	Retype           retype.Cmd                   `cmd:"" help:"Change column data type."`
	RowCount         rowcount.Cmd                 `cmd:"" help:"Prints the count of rows."`
	Schema           schema.Cmd                   `cmd:"" help:"Prints the schema."`
	ShellCompletions kongplete.InstallCompletions `cmd:"" help:"Install/uninstall shell completions"`
	Size             size.Cmd                     `cmd:"" help:"Prints the size."`
	Split            split.Cmd                    `cmd:"" help:"Split into multiple parquet files."`
	Transcode        transcode.Cmd                `cmd:"" help:"Convert Parquet file with different encoding/compression settings."`
	Version          version.Cmd                  `cmd:"" help:"Show build version."`
}

func newParser(cli *cli) *kong.Kong {
	parser := kong.Must(
		cli,
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{Compact: true}),
		kong.Description("A utility to inspect Parquet files, for full usage see https://github.com/hangxie/parquet-tools/blob/main/README.md"),
		kong.Vars{"writer_encryption_algorithms": strings.Join(pio.WriterEncryptionAlgorithms, ",")},
	)
	kongplete.Complete(parser, kongplete.WithPredictor("file", complete.PredictFiles("*")))
	return parser
}

func run(kongCtx *kong.Context, ctx context.Context) error {
	kongCtx.BindTo(ctx, (*context.Context)(nil))
	return kongCtx.Run()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	parser := newParser(new(cli))
	kongCtx, err := parser.Parse(os.Args[1:])
	parser.FatalIfErrorf(err)
	kongCtx.FatalIfErrorf(run(kongCtx, ctx))
}
