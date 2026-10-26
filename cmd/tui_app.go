package cmd

import (
	"github.com/rivo/tview"

	"github.com/hangxie/parquet-browser/model"
)

// TUIApp represents the TUI application for browsing Parquet files
type TUIApp struct {
	tviewApp       *tview.Application
	pages          *tview.Pages
	mainLayout     *tview.Flex
	headerView     *tview.TextView
	rowGroupList   *tview.Table
	statusLine     *tview.TextView
	currentFile    string
	httpClient     *parquetClient // HTTP client for data access
	lastRGIndex    int            // Track which row group we're positioned at (unused, kept for compatibility)
	lastRGPosition int64          // Track position within file (unused, kept for compatibility)
}

// NewTUIApp creates a new TUIApp instance
func NewTUIApp() *TUIApp {
	return &TUIApp{
		tviewApp:       tview.NewApplication(),
		pages:          tview.NewPages(),
		lastRGIndex:    -1, // Start with -1 to indicate no row group has been accessed yet
		lastRGPosition: 0,
	}
}

// readPageHeaders reads all page headers from a column chunk via HTTP API
func (app *TUIApp) readPageHeaders(rgIndex, colIndex int) ([]model.PageMetadata, error) {
	return app.httpClient.getAllPagesInfo(rgIndex, colIndex)
}

// readPageContent reads and decodes the content of a specific page via HTTP API
func (app *TUIApp) readPageContent(rgIndex, colIndex, pageIndex int) ([]string, error) {
	return app.httpClient.getPageContent(rgIndex, colIndex, pageIndex)
}

func (app *TUIApp) showSchema() {
	viewer := &schemaViewer{
		app:           app,
		schemaFormats: []string{"json", "raw", "go", "csv"},
		currentFormat: 0,
		isPretty:      true,
		textView: tview.NewTextView().
			SetDynamicColors(true).
			SetScrollable(true).
			SetWordWrap(false),
		titleBar: tview.NewTextView().
			SetDynamicColors(true).
			SetTextAlign(tview.AlignCenter),
		statusBar: tview.NewTextView().
			SetDynamicColors(true).
			SetTextAlign(tview.AlignCenter),
	}
	viewer.show()
}
