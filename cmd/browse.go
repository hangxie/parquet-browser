package cmd

import (
	"context"
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/rivo/tview"

	pio "github.com/hangxie/parquet-tools/io"
)

// BrowseCmd is a kong command for browse
type BrowseCmd struct {
	URI string `arg:"" optional:"" predictor:"file" help:"URI of Parquet file (optional, will show file dialog if not provided)."`
	pio.ReadOption
}

// Run does actual browse job
func (b BrowseCmd) Run() error {
	app := NewBrowseApp()

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

	// Channel to receive the result of file opening
	type result struct {
		reader *reader.ParquetReader
		err    error
	}
	resultChan := make(chan result, 1)

	// Start file opening in background
	go func() {
		parquetReader, err := pio.NewParquetFileReader(b.URI, b.ReadOption)
		select {
		case <-ctx.Done():
			// Cancelled, clean up if needed
			if parquetReader != nil {
				_ = parquetReader.PFile.Close()
			}
			return
		case resultChan <- result{reader: parquetReader, err: err}:
		}
	}()

	// Start the app and wait for file opening to complete
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

				// Get metadata
				metadata := res.reader.Footer
				if metadata == nil {
					errorModal := tview.NewModal().
						SetText("Failed to get file metadata\n\nPress ESC to exit").
						SetTextColor(tcell.ColorRed).
						AddButtons([]string{"Exit"}).
						SetDoneFunc(func(buttonIndex int, buttonLabel string) {
							app.tviewApp.Stop()
						})
					app.pages.AddPage("error", errorModal, true, true)
					app.pages.SwitchToPage("error")
					return
				}

				// Store file info in app
				app.currentFile = b.URI
				app.parquetReader = res.reader
				app.metadata = metadata

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

	// Clean up
	if app.parquetReader != nil {
		_ = app.parquetReader.PFile.Close()
	}

	// If cancelled, return nil (successful cancellation)
	if cancelled {
		return nil
	}

	return err
}
