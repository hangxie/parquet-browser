package cmd

import (
	"fmt"
	"net"

	pio "github.com/hangxie/parquet-tools/io"

	"github.com/hangxie/parquet-browser/service"
)

// WebUICmd is a kong command for serving Web UI
type WebUICmd struct {
	URI     string `arg:"" predictor:"file" help:"URI of Parquet file."`
	Addr    string `short:"a" default:"" help:"Address to listen on (default: random port)."`
	KeyFile string `name:"key-file" group:"Encryption" help:"path to a JSON file with {footer_key, aad_prefix, column_keys}. CLI flags override file values." default:""`
	pio.ReadOption
}

// Run starts the Web UI server
func (w WebUICmd) Run() error {
	if err := loadKeyFile(w.KeyFile, &w.ReadOption); err != nil {
		return err
	}
	// Set version getter for web UI
	service.SetVersionGetter(GetVersion)

	// Create the service
	svc, err := service.NewParquetService(w.URI, w.ReadOption)
	if err != nil {
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
	return service.StartWebUIServer(svc, addr)
}
