package cmd

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gdamore/tcell/v2"
	pio "github.com/hangxie/parquet-tools/io"
	"github.com/rivo/tview"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-browser/service"
)

func Test_startHTTPServer_InvalidFile(t *testing.T) {
	ctx := context.Background()
	resultChan := make(chan serverResult, 1)

	// Start server with invalid file
	go startHTTPServer(ctx, "nonexistent.parquet", pio.ReadOption{}, resultChan)

	// Wait for result
	select {
	case result := <-resultChan:
		// Should get an error
		require.Error(t, result.err)
		require.Empty(t, result.serverURL)
		require.Nil(t, result.server)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for server result")
	}
}

func Test_startHTTPServer_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	resultChan := make(chan serverResult, 1)

	// Cancel immediately
	cancel()

	// Start server with cancelled context
	go startHTTPServer(ctx, "test.parquet", pio.ReadOption{}, resultChan)

	// Should return quickly without sending to channel or send error
	select {
	case result := <-resultChan:
		// If we get a result, it should be an error
		require.Error(t, result.err)
	case <-time.After(500 * time.Millisecond):
		// No result is also acceptable for cancelled context
		// The function returns early without sending to channel
	}
}

func Test_startHTTPServer_RealFile(t *testing.T) {
	ctx := context.Background()
	resultChan := make(chan serverResult, 1)

	// Use the test file from parquet-tools repository
	testFileURL := "https://github.com/hangxie/parquet-tools/raw/refs/heads/main/testdata/good.parquet"

	// Start server with real file
	go startHTTPServer(ctx, testFileURL, pio.ReadOption{}, resultChan)

	// Wait for result
	select {
	case result := <-resultChan:
		// Should succeed
		require.NoError(t, result.err)
		require.NotEmpty(t, result.serverURL)
		require.NotNil(t, result.server)

		// Verify server is actually running by making a request
		resp, err := http.Get(result.serverURL + "/info")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		_ = resp.Body.Close()

		// Clean up - shutdown server
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		err = result.server.Shutdown(shutdownCtx)
		require.NoError(t, err)

	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for server to start")
	}
}

func Test_serverResult_Struct(t *testing.T) {
	// Test that serverResult struct has expected fields
	result := serverResult{
		serverURL: "http://localhost:8080",
		server:    nil,
		err:       nil,
	}

	require.Equal(t, "http://localhost:8080", result.serverURL)
	require.Nil(t, result.server)
	require.Nil(t, result.err)
}

func Test_TUICmd_FieldAccess(t *testing.T) {
	// Test that TUICmd fields can be accessed and set
	cmd := TUICmd{}

	// Set URI
	cmd.URI = "test.parquet"
	require.Equal(t, "test.parquet", cmd.URI)

	// Set ReadOption fields
	cmd.Anonymous = true
	require.True(t, cmd.Anonymous)

	cmd.ObjectVersion = strPtr("v1.0")
	require.Equal(t, "v1.0", *cmd.ObjectVersion)

	cmd.HTTPIgnoreTLSError = true
	require.True(t, cmd.HTTPIgnoreTLSError)
}

func Test_TUICmd_Run_ShowsMainView(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/info":
			_, _ = w.Write([]byte(`{
				"Version": 2,
				"NumRows": 1000,
				"NumRowGroups": 1,
				"NumLeafColumns": 2,
				"TotalCompressedSize": 512,
				"TotalUncompressedSize": 1024,
				"CompressionRatio": 2.0,
				"CreatedBy": "test-writer"
			}`))
		case "/rowgroups":
			_, _ = w.Write([]byte(`[
				{
					"Index": 0,
					"NumRows": 1000,
					"NumColumns": 2,
					"CompressedSize": 512,
					"UncompressedSize": 1024,
					"CompressionRatio": 2.0
				}
			]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	app := newTUIRunAppForTest(t)
	withTUIRunHooks(t, app, func(ctx context.Context, uri string, readOpt pio.ReadOption, resultChan chan<- serverResult) {
		select {
		case <-ctx.Done():
		case resultChan <- serverResult{serverURL: server.URL}:
		}
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- (TUICmd{URI: "/tmp/example.parquet"}).Run()
	}()

	waitForTUIPage(t, app, "main")

	var (
		currentFile string
		hasLoading  bool
		headerText  string
	)
	queueTUIUpdate(t, app, func() {
		currentFile = app.currentFile
		hasLoading = app.pages.HasPage("loading")
		headerText = app.headerView.GetText(false)
		app.tviewApp.Stop()
	})

	require.Equal(t, "/tmp/example.parquet", currentFile)
	require.False(t, hasLoading)
	require.Contains(t, headerText, "example.parquet")
	require.NoError(t, waitForTUIRun(t, errCh))
}

func Test_TUICmd_Run_ShowsStartupError(t *testing.T) {
	app := newTUIRunAppForTest(t)
	startErr := errors.New("startup failed")
	withTUIRunHooks(t, app, func(ctx context.Context, uri string, readOpt pio.ReadOption, resultChan chan<- serverResult) {
		select {
		case <-ctx.Done():
		case resultChan <- serverResult{err: startErr}:
		}
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- (TUICmd{URI: "/tmp/missing.parquet"}).Run()
	}()

	waitForTUIPage(t, app, "error")
	queueTUIUpdate(t, app, func() {
		app.tviewApp.Stop()
	})

	require.NoError(t, waitForTUIRun(t, errCh))
}

func Test_TUICmd_Run_CancelOnEscape(t *testing.T) {
	app := newTUIRunAppForTest(t)
	withTUIRunHooks(t, app, func(ctx context.Context, uri string, readOpt pio.ReadOption, resultChan chan<- serverResult) {
		<-ctx.Done()
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- (TUICmd{URI: "/tmp/example.parquet"}).Run()
	}()

	waitForTUIPage(t, app, "loading")
	app.tviewApp.QueueEvent(tcell.NewEventKey(tcell.KeyEscape, 0, tcell.ModNone))

	require.NoError(t, waitForTUIRun(t, errCh))
}

func newTUIRunAppForTest(t *testing.T) *TUIApp {
	t.Helper()

	app := NewTUIApp()
	screen := tcell.NewSimulationScreen("UTF-8")
	require.NoError(t, screen.Init())
	app.tviewApp.SetScreen(screen)
	return app
}

func withTUIRunHooks(
	t *testing.T,
	app *TUIApp,
	startServer func(context.Context, string, pio.ReadOption, chan<- serverResult),
) {
	t.Helper()

	previousNewTUIAppForRun := newTUIAppForRun
	previousStartHTTPServerForRun := startHTTPServerForRun
	newTUIAppForRun = func() *TUIApp {
		return app
	}
	startHTTPServerForRun = startServer
	t.Cleanup(func() {
		newTUIAppForRun = previousNewTUIAppForRun
		startHTTPServerForRun = previousStartHTTPServerForRun
	})
}

func waitForTUIRun(t *testing.T, errCh <-chan error) error {
	t.Helper()

	select {
	case err := <-errCh:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for TUICmd.Run to return")
		return nil
	}
}

func startTUIAppForTest(t *testing.T, app *TUIApp) func() {
	t.Helper()

	screen := tcell.NewSimulationScreen("UTF-8")
	require.NoError(t, screen.Init())
	app.tviewApp.SetScreen(screen)
	app.tviewApp.SetRoot(app.pages, true)

	errCh := make(chan error, 1)
	go func() {
		errCh <- app.tviewApp.Run()
	}()

	queueTUIUpdate(t, app, func() {})

	return func() {
		app.tviewApp.Stop()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for TUI application to stop")
		}
	}
}

func queueTUIUpdate(t *testing.T, app *TUIApp, fn func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		app.tviewApp.QueueUpdate(func() {
			defer close(done)
			fn()
		})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for TUI update")
	}
}

func waitForTUIPage(t *testing.T, app *TUIApp, name string) tview.Primitive {
	t.Helper()

	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for TUI page %q", name)
		case <-ticker.C:
			var primitive tview.Primitive
			queueTUIUpdate(t, app, func() {
				primitive = app.pages.GetPage(name)
			})
			if primitive != nil {
				return primitive
			}
		}
	}
}

// fixturePath returns a fixture downloaded by `make test`, failing the test when it is missing.
func fixturePath(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join("..", "build", "testdata", name)
	_, err := os.Stat(path)
	require.NoError(t, err, "missing fixture; run `make test` to download it")
	return path
}

// newFixtureTUIApp wires a TUIApp to a real embedded service backed by a fixture.
func newFixtureTUIApp(t *testing.T, name string) *TUIApp {
	t.Helper()

	svc, err := service.NewParquetService(context.Background(), fixturePath(t, name), pio.ReadOption{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	server := httptest.NewServer(service.CreateRouter(svc, true))
	t.Cleanup(server.Close)

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)
	return app
}

func Test_startHTTPServer_CorruptFooter(t *testing.T) {
	tests := []struct {
		file    string
		wantErr string
	}{
		{file: "bad-footer-magic.parquet", wantErr: `invalid footer magic "XXXX"`},
		{file: "negative-num-rows.parquet", wantErr: "num_rows -1 is negative"},
	}

	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			path := fixturePath(t, tt.file)
			resultChan := make(chan serverResult, 1)
			go startHTTPServer(context.Background(), path, pio.ReadOption{}, resultChan)

			select {
			case result := <-resultChan:
				require.Error(t, result.err)
				require.Contains(t, result.err.Error(), tt.wantErr)
				require.Nil(t, result.server)
				require.Nil(t, result.svc)
			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for server result")
			}
		})
	}
}
