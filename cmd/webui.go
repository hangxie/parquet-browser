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
	// Application-lifetime context cancelled on SIGINT/SIGTERM so that opening
	// the source and the running server shut down gracefully on termination.
	ctx, stop := notifyContext()
	defer stop()
	return w.run(ctx)
}

// run performs the work of Run against ctx, split out so the interrupt-handling
// behavior is unit-testable without installing signal handlers.
func (w WebUICmd) run(ctx context.Context) error {
	// Set version getter for web UI
	service.SetVersionGetter(GetVersion)

	// Create the service
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

	// Start the web UI server with HTML interface
	return service.StartWebUIServer(ctx, svc, addr)
}
