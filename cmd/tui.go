package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
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

// tuiShutdownGrace bounds the embedded server's graceful shutdown before
// connections are force-closed.
const tuiShutdownGrace = 5 * time.Second

// serverResult contains the result of HTTP server startup
type serverResult struct {
	serverURL string
	server    *http.Server
	serveErr  <-chan error            // carries ListenAndServe's result for shutdown
	svc       *service.ParquetService // owned by the caller; must be closed on teardown
	err       error
}

var (
	newTUIAppForRun       = NewTUIApp
	startHTTPServerForRun = startHTTPServer
)

// startHTTPServer starts an embedded HTTP server for serving Parquet file data
// It runs in a goroutine and sends the result (server URL and instance, or error) to resultChan
func startHTTPServer(ctx context.Context, uri string, readOpt pio.ReadOption, resultChan chan<- serverResult) {
	// Create the service
	svc, err := service.NewParquetService(ctx, uri, readOpt)
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
		_ = svc.Close() // release the reader we just opened
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

	// Start server in background, capturing ListenAndServe's result so the
	// shutdown path can wait for it (same lifecycle as serve/web-ui).
	serveErrCh := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErrCh <- err
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
		_ = service.ShutdownServer(server, serveErrCh, tuiShutdownGrace)
		_ = svc.Close()
		return
	case resultChan <- serverResult{serverURL: serverURL, server: server, serveErr: serveErrCh, svc: svc}:
	}
}

// Run does actual browse job
func (b TUICmd) Run() error {
	app := newTUIAppForRun()

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
	go startHTTPServerForRun(ctx, b.URI, b.ReadOption, resultChan)

	// Start the app and wait for server startup. The receiver records the result
	// under mu as soon as it arrives — before the UI callback, which may never run
	// after Stop — and recvDone lets cleanup wait for it to stop using resultChan.
	var (
		mu       sync.Mutex
		result   *serverResult
		recvDone = make(chan struct{})
	)
	go func() {
		defer close(recvDone)
		select {
		case <-ctx.Done():
			// User cancelled before startup completed.
			return
		case res := <-resultChan:
			mu.Lock()
			result = &res
			mu.Unlock()
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

	// Ensure the receiver goroutine has exited (so it no longer competes for
	// resultChan), then clean up whatever startup produced — regardless of
	// whether the UI callback ran.
	cancel()
	<-recvDone

	mu.Lock()
	res := result
	mu.Unlock()
	if res == nil {
		// The receiver exited via ctx.Done without consuming; a result may still
		// be buffered if the server came up just as we cancelled.
		select {
		case buffered := <-resultChan:
			res = &buffered
		default:
		}
	}
	if res != nil {
		// Shutdown the server (force-closing and waiting for its goroutine),
		// then release the reader's file handles.
		if res.server != nil {
			_ = service.ShutdownServer(res.server, res.serveErr, tuiShutdownGrace)
		}
		if res.svc != nil {
			_ = res.svc.Close()
		}
	}

	// If cancelled, return nil (successful cancellation)
	if cancelled {
		return nil
	}

	return err
}
