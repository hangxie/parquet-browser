package cmd

import (
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
	// Create the service
	svc, err := service.NewParquetService(s.URI, s.ReadOption)
	if err != nil {
		return fmt.Errorf("failed to create service: %w", err)
	}
	defer func() { _ = svc.Close() }()

	// Start the server
	return service.StartServer(svc, s.Addr)
}
