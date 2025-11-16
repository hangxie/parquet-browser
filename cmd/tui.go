package cmd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	pio "github.com/hangxie/parquet-tools/io"

	"github.com/hangxie/parquet-browser/service"
)

// TUICmd is a kong command for browse
type TUICmd struct {
	URI string `arg:"" predictor:"file" help:"URI of Parquet file."`
	pio.ReadOption
}

// serverResult contains the result of HTTP server startup
type serverResult struct {
	serverURL string
	server    *http.Server
	err       error
}

// startHTTPServer starts an embedded HTTP server for serving Parquet file data
// It runs in a goroutine and sends the result (server URL and instance, or error) to resultChan
func startHTTPServer(ctx context.Context, uri string, readOpt pio.ReadOption, resultChan chan<- serverResult) {
	// Create the service
	svc, err := service.NewParquetService(uri, readOpt)
	if err != nil {
		select {
		case <-ctx.Done():
			return
		case resultChan <- serverResult{err: err}:
		}
		return
	}

	// Find an available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		select {
		case <-ctx.Done():
			return
		case resultChan <- serverResult{err: fmt.Errorf("failed to find available port: %w", err)}:
		}
		return
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	// Create HTTP server in quiet mode (no logging)
	router := service.CreateRouter(svc, true) // quiet=true
	server := &http.Server{
		Addr:    addr,
		Handler: router,
	}

	// Start server in background
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Server error - ignore if already cancelled
			select {
			case <-ctx.Done():
			default:
				// Log error but don't send to channel as it may be full
			}
		}
	}()

	// Wait for server to be ready
	serverURL := fmt.Sprintf("http://%s", addr)
	for i := 0; i < 50; i++ {
		resp, err := http.Get(serverURL + "/info")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case <-ctx.Done():
		_ = server.Shutdown(context.Background())
		return
	case resultChan <- serverResult{serverURL: serverURL, server: server}:
	}
}

// Run does actual browse job
func (b TUICmd) Run() error {
	app := NewTUIApp()

	// Create a loading modal with cancellation instructions
	modal := tview.NewModal().
		SetText(fmt.Sprintf("Opening file...\n%s\n\nPlease wait...\n\nPress ESC or Ctrl+C to cancel", b.URI)).
		SetTextColor(tcell.ColorYellow)

	// Context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Track if loading was cancelled
	cancelled := false

	// Add input capture to handle cancellation
	modal.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape || event.Key() == tcell.KeyCtrlC {
			cancelled = true
			cancel()
			app.tviewApp.Stop()
			return nil
		}
		return event
	})

	app.pages.AddPage("loading", modal, true, true)
	app.tviewApp.SetRoot(app.pages, true)

	// Channel to receive the result of server startup
	resultChan := make(chan serverResult, 1)

	// Start embedded HTTP server in background
	go startHTTPServer(ctx, b.URI, b.ReadOption, resultChan)

	// Start the app and wait for server startup
	var httpServer *http.Server
	go func() {
		select {
		case <-ctx.Done():
			// User cancelled
			return
		case res := <-resultChan:
			app.tviewApp.QueueUpdateDraw(func() {
				if res.err != nil {
					// Show error modal
					errorModal := tview.NewModal().
						SetText(fmt.Sprintf("Error opening file:\n%v\n\nPress ESC to exit", res.err)).
						SetTextColor(tcell.ColorRed).
						AddButtons([]string{"Exit"}).
						SetDoneFunc(func(buttonIndex int, buttonLabel string) {
							app.tviewApp.Stop()
						})
					app.pages.AddPage("error", errorModal, true, true)
					app.pages.SwitchToPage("error")
					return
				}

				// Store server reference for cleanup
				httpServer = res.server

				// Create HTTP client and store in app
				app.httpClient = newParquetClient(res.serverURL)
				app.currentFile = b.URI

				// Remove loading modal and show main view
				app.pages.RemovePage("loading")
				app.showMainView()
				app.pages.AddPage("main", app.mainLayout, true, true)
				app.pages.SwitchToPage("main")
			})
		}
	}()

	// Run the app
	err := app.tviewApp.Run()

	// Clean up - shutdown HTTP server
	if httpServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}

	// If cancelled, return nil (successful cancellation)
	if cancelled {
		return nil
	}

	return err
}
