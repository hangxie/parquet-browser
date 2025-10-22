package cmd

import (
	"fmt"

	pio "github.com/hangxie/parquet-tools/io"

	"github.com/hangxie/parquet-browser/service"
)

// WebUICmd is a kong command for serving Web UI
type WebUICmd struct {
	URI  string `arg:"" predictor:"file" help:"URI of Parquet file."`
	Addr string `short:"a" default:":8080" help:"Address to listen on (default :8080)."`
	pio.ReadOption
}

// Run starts the Web UI server
func (w WebUICmd) Run() error {
	// Create the service
	svc, err := service.NewParquetService(w.URI, w.ReadOption)
	if err != nil {
		return fmt.Errorf("failed to create service: %w", err)
	}
	defer func() { _ = svc.Close() }()

	// Start the web UI server with HTML interface
	return service.StartWebUIServer(svc, w.Addr)
}
