package main

import (
	"os"

	"github.com/alecthomas/kong"
	"github.com/posener/complete"
	"github.com/willabides/kongplete"

	"github.com/hangxie/parquet-browser/cmd"
)

var cli struct {
	TUI   cmd.TUICmd   `cmd:"" help:"Browse Parquet file with TUI."`
	Serve cmd.ServeCmd `cmd:"" help:"Start HTTP API server for Parquet file."`
}

func main() {
	parser := kong.Must(
		&cli,
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{Compact: true}),
		kong.Description("Yet another utility to inspect Parquet files, for full usage see https://github.com/hangxie/parquet-browser/blob/main/README.md"),
	)
	kongplete.Complete(parser, kongplete.WithPredictor("file", complete.PredictFiles("*")))

	ctx, err := parser.Parse(os.Args[1:])
	parser.FatalIfErrorf(err)
	ctx.FatalIfErrorf(ctx.Run())
}
