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

// tuiShutdownGrace bounds the embedded server's graceful shutdown before force-closing.
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

// startHTTPServer opens uri behind an embedded HTTP server and reports the outcome on resultChan
func startHTTPServer(ctx context.Context, uri string, readOpt pio.ReadOption, resultChan chan<- serverResult) {
	svc, err := service.NewParquetService(ctx, uri, readOpt)
	if err != nil {
		select {
		case <-ctx.Done():
			return
		case resultChan <- serverResult{err: err}:
		}
		return
	}

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

	router := service.CreateRouter(svc, true) // quiet: no request logging
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

	modal := tview.NewModal().
		SetText(fmt.Sprintf("Opening file...\n%s\n\nPlease wait...\n\nPress ESC or Ctrl+C to cancel", b.URI)).
		SetTextColor(tcell.ColorYellow)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancelled := false

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

	resultChan := make(chan serverResult, 1)

	go startHTTPServerForRun(ctx, b.URI, b.ReadOption, resultChan)

	// The receiver records the result under mu before the UI callback, which may never run after
	// Stop; recvDone lets cleanup wait until it stops reading resultChan.
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

				app.httpClient = newParquetClient(res.serverURL)
				app.currentFile = b.URI

				app.pages.RemovePage("loading")
				app.showMainView()
				app.pages.AddPage("main", app.mainLayout, true, true)
				app.pages.SwitchToPage("main")
			})
		}
	}()

	err := app.tviewApp.Run()

	// Wait for the receiver to stop reading resultChan, then clean up whatever startup produced.
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
		if res.server != nil {
			_ = service.ShutdownServer(res.server, res.serveErr, tuiShutdownGrace)
		}
		if res.svc != nil {
			_ = res.svc.Close()
		}
	}

	if cancelled {
		return nil
	}

	return err
}
