package cmd

import (
	"context"
	"fmt"
	"net"

	pio "github.com/hangxie/parquet-tools/io"

	"github.com/hangxie/parquet-browser/service"
)

// WebUICmd is a kong command for serving Web UI
type WebUICmd struct {
	URI  string `arg:"" predictor:"file" help:"URI of Parquet file."`
	Addr string `short:"a" default:"" help:"Address to listen on (default: random port)."`
	pio.ReadOption
}

// Run starts the Web UI server
func (w WebUICmd) Run() error {
	// Cancelled on SIGINT/SIGTERM so opening and serving both shut down gracefully.
	ctx, stop := notifyContext()
	defer stop()
	return w.run(ctx)
}

// run is Run against ctx, so interrupt handling is testable without signal handlers.
func (w WebUICmd) run(ctx context.Context) error {
	service.SetVersionGetter(GetVersion)

	svc, err := service.NewParquetService(ctx, w.URI, w.ReadOption)
	if err != nil {
		if ctx.Err() != nil {
			// Interrupted while opening the source: exit cleanly.
			return nil
		}
		return fmt.Errorf("failed to create service: %w", err)
	}
	defer func() { _ = svc.Close() }()

	// If no address specified, find a random available port
	addr := w.Addr
	if addr == "" {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return fmt.Errorf("failed to find available port: %w", err)
		}
		addr = listener.Addr().String()
		_ = listener.Close()
	}

	return service.StartWebUIServer(ctx, svc, addr)
}
