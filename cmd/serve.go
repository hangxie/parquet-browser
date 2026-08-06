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
	// Application-lifetime context cancelled on SIGINT/SIGTERM so that opening
	// the source and the running server shut down gracefully on termination.
	ctx, stop := notifyContext()
	defer stop()
	return s.run(ctx)
}

// run performs the work of Run against ctx, split out so the interrupt-handling
// behavior is unit-testable without installing signal handlers.
func (s ServeCmd) run(ctx context.Context) error {
	// Create the service
	svc, err := service.NewParquetService(ctx, s.URI, s.ReadOption)
	if err != nil {
		if ctx.Err() != nil {
			// Interrupted while opening the source: exit cleanly, like the
			// graceful-shutdown path, rather than printing the cancellation.
			return nil
		}
		return fmt.Errorf("failed to create service: %w", err)
	}
	defer func() { _ = svc.Close() }()

	// Start the server
	return service.StartServer(ctx, svc, s.Addr)
}
