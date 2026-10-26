package cmd

import (
	"context"
	"fmt"

	pio "github.com/hangxie/parquet-tools/io"

	"github.com/hangxie/parquet-browser/service"
)

// ServeCmd is a kong command for serving HTTP API
type ServeCmd struct {
	URI  string `arg:"" predictor:"file" help:"URI of Parquet file."`
	Addr string `short:"a" default:":8080" help:"Address to listen on (default :8080)."`
	pio.ReadOption
}

// Run starts the HTTP API server
func (s ServeCmd) Run() error {
	// Cancelled on SIGINT/SIGTERM so opening and serving both shut down gracefully.
	ctx, stop := notifyContext()
	defer stop()
	return s.run(ctx)
}

// run is Run against ctx, so interrupt handling is testable without signal handlers.
func (s ServeCmd) run(ctx context.Context) error {
	svc, err := service.NewParquetService(ctx, s.URI, s.ReadOption)
	if err != nil {
		if ctx.Err() != nil {
			// Interrupted while opening: exit cleanly, like a graceful shutdown.
			return nil
		}
		return fmt.Errorf("failed to create service: %w", err)
	}
	defer func() { _ = svc.Close() }()

	return service.StartServer(ctx, svc, s.Addr)
}
