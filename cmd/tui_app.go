package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/hangxie/parquet-go/v2/parquet"
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
	// Use HTTP client to get page metadata
	return app.httpClient.getAllPagesInfo(rgIndex, colIndex)
}

// readPageContent reads and decodes the content of a specific page via HTTP API
func (app *TUIApp) readPageContent(rgIndex, colIndex, pageIndex int, allPages []model.PageMetadata, meta *parquet.ColumnMetaData) ([]string, error) {
	// Use HTTP client to get pre-formatted page content
	return app.httpClient.getPageContent(rgIndex, colIndex, pageIndex)
}

// buildColumnChunkInfoViewFromHTTP creates the info view for a column chunk using HTTP API data
func (app *TUIApp) buildColumnChunkInfoViewFromHTTP(colInfo model.ColumnChunkInfo, numPages int) *tview.TextView {
	infoView := tview.NewTextView().
		SetDynamicColors(true).
		SetWrap(true).
		SetWordWrap(true)

	var info strings.Builder

	// Line 1: Column path, type, logical type, converted type
	info.WriteString(fmt.Sprintf("[yellow]Column:[-] %s  ", colInfo.Name))
	info.WriteString(fmt.Sprintf("[yellow]Type:[-] %s  ", colInfo.PhysicalType))

	if colInfo.LogicalType != "-" {
		info.WriteString(fmt.Sprintf("[yellow]Logical:[-] %s  ", colInfo.LogicalType))
	}
	if colInfo.ConvertedType != "-" {
		info.WriteString(fmt.Sprintf("[yellow]Converted:[-] %s", colInfo.ConvertedType))
	}

	// Line 2: Values, codec, sizes
	info.WriteString(fmt.Sprintf("\n[yellow]Values:[-] %d  ", colInfo.NumValues))
	info.WriteString(fmt.Sprintf("[yellow]Codec:[-] %s  ", colInfo.Codec))

	if colInfo.CompressedSize > 0 && colInfo.UncompressedSize > 0 {
		info.WriteString(fmt.Sprintf("[yellow]Size:[-] %s → %s (%.2fx)",
			model.FormatBytes(colInfo.CompressedSize),
			model.FormatBytes(colInfo.UncompressedSize),
			colInfo.CompressionRatio))
	}

	// Line 3: Null count (if available) and number of pages
	if colInfo.NullCount != nil {
		info.WriteString(fmt.Sprintf("\n[yellow]Nulls:[-] %d  ", *colInfo.NullCount))
	} else {
		info.WriteString("\n")
	}

	if numPages > 0 {
		info.WriteString(fmt.Sprintf("[yellow]Pages:[-] %d", numPages))
	}

	// Line 4: Min/Max values (if available)
	if colInfo.MinValue != "" || colInfo.MaxValue != "" {
		info.WriteString("\n")
		if colInfo.MinValue != "" {
			info.WriteString(fmt.Sprintf("[yellow]Min:[-] %s", colInfo.MinValue))
		}
		if colInfo.MaxValue != "" {
			if colInfo.MinValue != "" {
				info.WriteString("  ")
			}
			info.WriteString(fmt.Sprintf("[yellow]Max:[-] %s", colInfo.MaxValue))
		}
	}

	infoView.SetText(info.String())
	infoView.SetBorder(true).SetTitle(" Column Chunk Info ")

	return infoView
}

// getColumnChunkInfoHeight calculates the height needed for column chunk info view
func getColumnChunkInfoHeight(infoView *tview.TextView) int {
	if infoView == nil {
		return 3
	}
	text := infoView.GetText(false)
	lines := strings.Count(text, "\n") + 1
	return lines + 2 // +2 for borders
}

func (app *TUIApp) showMainView() {
	app.mainLayout = tview.NewFlex().SetDirection(tview.FlexRow)

	// Create header view
	app.createHeaderView()

	// Create row group list
	app.createRowGroupList()

	// Create status line
	app.createStatusLine()

	// Assemble the layout with dynamic header height
	headerHeight := app.getHeaderHeight()
	app.mainLayout.
		AddItem(app.headerView, headerHeight, 0, false).
		AddItem(app.rowGroupList, 0, 1, true).
		AddItem(app.statusLine, 1, 0, false)

	// Add key bindings
	app.mainLayout.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEscape:
			app.tviewApp.Stop()
			return nil
		case tcell.KeyEnter:
			// Show column chunks view for currently selected row group
			row, _ := app.rowGroupList.GetSelection()
			if row > 0 { // Skip header row
				rgIndex := row - 1
				app.showColumnChunksView(rgIndex)
			}
			return nil
		case tcell.KeyRune:
			if event.Rune() == 's' {
				app.showSchema()
				return nil
			}
		}
		return event
	})
}

func (app *TUIApp) showSchema() {
	// Inline schema viewer creation
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

// parsePhysicalType converts a string to parquet.Type
func parsePhysicalType(typeStr string) parquet.Type {
	switch typeStr {
	case "BOOLEAN":
		return parquet.Type_BOOLEAN
	case "INT32":
		return parquet.Type_INT32
	case "INT64":
		return parquet.Type_INT64
	case "INT96":
		return parquet.Type_INT96
	case "FLOAT":
		return parquet.Type_FLOAT
	case "DOUBLE":
		return parquet.Type_DOUBLE
	case "BYTE_ARRAY":
		return parquet.Type_BYTE_ARRAY
	case "FIXED_LEN_BYTE_ARRAY":
		return parquet.Type_FIXED_LEN_BYTE_ARRAY
	default:
		return parquet.Type_BYTE_ARRAY // Default fallback
	}
}

// parseCompressionCodec converts a string to parquet.CompressionCodec
func parseCompressionCodec(codecStr string) parquet.CompressionCodec {
	switch codecStr {
	case "UNCOMPRESSED":
		return parquet.CompressionCodec_UNCOMPRESSED
	case "SNAPPY":
		return parquet.CompressionCodec_SNAPPY
	case "GZIP":
		return parquet.CompressionCodec_GZIP
	case "LZO":
		return parquet.CompressionCodec_LZO
	case "BROTLI":
		return parquet.CompressionCodec_BROTLI
	case "LZ4":
		return parquet.CompressionCodec_LZ4
	case "ZSTD":
		return parquet.CompressionCodec_ZSTD
	case "LZ4_RAW":
		return parquet.CompressionCodec_LZ4_RAW
	default:
		return parquet.CompressionCodec_UNCOMPRESSED // Default fallback
	}
}

func (app *TUIApp) showPageView(rgIndex, colIndex int) {
	// Page viewing via HTTP API
	// Show loading modal with cancellation instructions
	loadingModal := tview.NewModal().
		SetText("Loading Column Chunk Pages...\n\nPlease wait...\n\nPress ESC to cancel").
		SetTextColor(tcell.ColorYellow)

	// Context for cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Add input capture to handle cancellation
	loadingModal.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			cancel()
			app.pages.RemovePage("page-loading")
			return nil
		}
		return event
	})

	app.pages.AddPage("page-loading", loadingModal, true, true)

	// Build the page view in background
	go func() {
		defer cancel() // Ensure context is cancelled when done

		// Fetch column chunk info from HTTP API
		colInfo, err := app.httpClient.getColumnChunkInfo(rgIndex, colIndex)
		if err != nil {
			app.tviewApp.QueueUpdateDraw(func() {
				app.pages.RemovePage("page-loading")
				errorModal := tview.NewModal().
					SetText(fmt.Sprintf("Error loading column chunk:\n%v\n\nPress ESC to go back", err)).
					SetTextColor(tcell.ColorRed).
					AddButtons([]string{"OK"}).
					SetDoneFunc(func(buttonIndex int, buttonLabel string) {
						app.pages.RemovePage("page-error")
					})
				app.pages.AddPage("page-error", errorModal, true, true)
			})
			return
		}

		// Fetch page metadata from HTTP API
		pages, err := app.httpClient.getAllPagesInfo(rgIndex, colIndex)
		if err != nil {
			app.tviewApp.QueueUpdateDraw(func() {
				app.pages.RemovePage("page-loading")
				errorModal := tview.NewModal().
					SetText(fmt.Sprintf("Error loading pages:\n%v\n\nPress ESC to go back", err)).
					SetTextColor(tcell.ColorRed).
					AddButtons([]string{"OK"}).
					SetDoneFunc(func(buttonIndex int, buttonLabel string) {
						app.pages.RemovePage("page-error")
					})
				app.pages.AddPage("page-error", errorModal, true, true)
			})
			return
		}

		// Convert model.PageMetadata to model.PageMetadata for compatibility with existing UI code
		pageInfos := make([]model.PageMetadata, len(pages))
		for i, p := range pages {
			pageInfos[i] = model.PageMetadata{
				Offset:                    p.Offset,
				PageType:                  p.PageType,
				CompressedSize:            p.CompressedSize,
				UncompressedSize:          p.UncompressedSize,
				NumValues:                 p.NumValues,
				Encoding:                  p.Encoding,
				DefLevelEncoding:          p.DefLevelEncoding,
				RepLevelEncoding:          p.RepLevelEncoding,
				HasStatistics:             p.HasStatistics,
				HasCRC:                    p.HasCRC,
				MinValue:                  p.MinValue,
				MaxValue:                  p.MaxValue,
				NullCount:                 p.NullCount,
				CompressedSizeFormatted:   p.CompressedSizeFormatted,
				UncompressedSizeFormatted: p.UncompressedSizeFormatted,
				MinValueFormatted:         p.MinValueFormatted,
				MaxValueFormatted:         p.MaxValueFormatted,
			}
		}

		// Create column chunk info view with HTTP data
		infoView := app.buildColumnChunkInfoViewFromHTTP(colInfo, len(pageInfos))

		// Create a minimal ColumnMetaData from HTTP data for compatibility
		// This allows existing code to work without major refactoring
		physicalType := parsePhysicalType(colInfo.PhysicalType)
		codec := parseCompressionCodec(colInfo.Codec)
		meta := &parquet.ColumnMetaData{
			Type:                  physicalType,
			Codec:                 codec,
			NumValues:             colInfo.NumValues,
			TotalCompressedSize:   colInfo.CompressedSize,
			TotalUncompressedSize: colInfo.UncompressedSize,
			PathInSchema:          colInfo.PathInSchema,
		}

		// Create pages table with lazy loading
		pageTable := tview.NewTable().
			SetBorders(false).
			SetSeparator(tview.Borders.Vertical).
			SetSelectable(true, false).
			SetFixed(1, 0)

		pageTable.SetBorder(true).
			SetTitleAlign(tview.AlignLeft)

		// Create status text view for status line
		statusText := tview.NewTextView().
			SetDynamicColors(true).
			SetTextAlign(tview.AlignLeft)

		// Build table with lazy loading (use fetched pages)
		builder := &pageTableBuilder{
			app:            app,
			rgIndex:        rgIndex,
			colIndex:       colIndex,
			meta:           meta,
			table:          pageTable,
			pages:          pageInfos, // Use fetched pages
			batchSize:      50,        // Load 50 pages at a time
			loadedPages:    0,
			isLoading:      false,
			statusTextView: statusText,
		}

		builder.build()

		// Set status line text (keys only)
		status := " [yellow]Keys:[-] ESC=back, s=schema, ↑↓=scroll, Enter=see item details"
		statusText.SetText(status)

		// Check if cancelled before updating UI
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Update UI on main thread
		app.tviewApp.QueueUpdateDraw(func() {
			// Check one more time if cancelled
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Remove loading modal
			app.pages.RemovePage("page-loading")

			// Create flex layout - same structure as main view: header, table, status
			infoHeight := getColumnChunkInfoHeight(infoView)
			flex := tview.NewFlex().SetDirection(tview.FlexRow).
				AddItem(infoView, infoHeight, 0, false).
				AddItem(pageTable, 0, 1, true).
				AddItem(statusText, 1, 0, false)

			flex.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				switch event.Key() {
				case tcell.KeyEscape:
					app.pages.RemovePage("pageview")
					return nil
				case tcell.KeyRune:
					if event.Rune() == 's' {
						app.showSchema()
						return nil
					}
				}
				return event
			})

			app.pages.AddPage("pageview", flex, true, true)
		})
	}()
}

func (app *TUIApp) showPageContent(rgIndex, colIndex, pageIndex int, allPages []model.PageMetadata, meta *parquet.ColumnMetaData) {
	pageInfo := allPages[pageIndex]
	// Show loading message
	loadingModal := tview.NewModal().
		SetText(fmt.Sprintf("Loading page content...\n\nPage Type: %s\nValues: %d\n\nPlease wait...",
			pageInfo.PageType,
			pageInfo.NumValues)).
		SetTextColor(tcell.ColorYellow)

	app.pages.AddPage("page-content-loading", loadingModal, true, true)

	// Create context for cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Load content in background
	go func() {
		defer cancel()

		// Create header info view
		headerView := tview.NewTextView().
			SetDynamicColors(true).
			SetWrap(true).
			SetWordWrap(true)
		headerView.SetBorder(true).SetTitle(" Page Info ")

		// Create table for values
		contentTable := tview.NewTable().
			SetBorders(false).
			SetSeparator(tview.Borders.Vertical).
			SetSelectable(true, false).
			SetFixed(1, 0)

		contentTable.SetBorder(true).
			SetTitle(" Page Content (↑↓ to navigate) ").
			SetTitleAlign(tview.AlignLeft)

		// Create status text view for status line
		statusText := tview.NewTextView().
			SetDynamicColors(true).
			SetTextAlign(tview.AlignLeft)

		// Build table with lazy loading
		builder := &pageContentBuilder{
			app:            app,
			rgIndex:        rgIndex,
			colIndex:       colIndex,
			pageIndex:      pageIndex,
			pageInfo:       pageInfo,
			allPages:       allPages,
			meta:           meta,
			table:          contentTable,
			batchSize:      100, // Load 100 values at a time
			loadedValues:   0,
			isLoading:      false,
			statusTextView: statusText,
			headerView:     headerView,
			ctx:            ctx,
			cancel:         cancel,
		}

		table, err := builder.build()

		// Set status line text (keys only)
		status := " [yellow]Keys:[-] ESC=back, s=schema, ↑↓=scroll"
		statusText.SetText(status)

		app.tviewApp.QueueUpdateDraw(func() {
			app.pages.RemovePage("page-content-loading")

			if err != nil {
				// Show error in text view
				errorView := tview.NewTextView().
					SetDynamicColors(true).
					SetText(fmt.Sprintf("[red]Error reading page content:[-]\n%v", err))

				errorView.SetBorder(true).
					SetTitle(fmt.Sprintf(" Page Content - %s (ESC to close) ", pageInfo.PageType)).
					SetTitleAlign(tview.AlignLeft)

				errorView.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
					if event.Key() == tcell.KeyEscape {
						cancel()
						app.pages.RemovePage("page-content")
						return nil
					}
					return event
				})

				app.pages.AddPage("page-content", errorView, true, true)
				return
			}

			// Create flex layout - same structure as main view: header, table, status
			headerHeight := getPageContentHeaderHeight(headerView)
			flex := tview.NewFlex().SetDirection(tview.FlexRow).
				AddItem(headerView, headerHeight, 0, false).
				AddItem(table, 0, 1, true).
				AddItem(statusText, 1, 0, false)

			app.pages.AddPage("page-content", flex, true, true)

			// Set focus to the table to ensure it receives key events
			app.tviewApp.SetFocus(table)
		})
	}()
}

func (app *TUIApp) createHeaderView() {
	app.headerView = tview.NewTextView().
		SetDynamicColors(true).
		SetRegions(true).
		SetWrap(true). // Enable wrapping to handle long content
		SetWordWrap(true)

	app.headerView.SetBorder(true).
		SetTitle(" File Info ").
		SetTitleAlign(tview.AlignLeft)

	// Populate header with file summary information
	var header strings.Builder

	// Line 1: File name
	header.WriteString(fmt.Sprintf("[yellow]File:[-] %s", filepath.Base(app.currentFile)))

	// Line 2: Basic file info from HTTP API
	header.WriteString("\n")

	fileInfo, err := app.httpClient.getFileInfo()
	if err != nil {
		app.headerView.SetText(fmt.Sprintf("[red]Error loading file info: %v[-]", err))
		return
	}

	header.WriteString(fmt.Sprintf("[yellow]Version:[-] %d  ", fileInfo.Version))
	header.WriteString(fmt.Sprintf("[yellow]Row Groups:[-] %d  ", fileInfo.NumRowGroups))
	header.WriteString(fmt.Sprintf("[yellow]Rows:[-] %d  ", fileInfo.NumRows))
	header.WriteString(fmt.Sprintf("[yellow]Columns:[-] %d", fileInfo.NumLeafColumns))

	// Line 3: Total size (compressed → uncompressed) and creator info
	if fileInfo.TotalCompressedSize > 0 && fileInfo.TotalUncompressedSize > 0 {
		header.WriteString(fmt.Sprintf("\n[yellow]Total Size:[-] %s → %s (%.2fx)",
			model.FormatBytes(fileInfo.TotalCompressedSize),
			model.FormatBytes(fileInfo.TotalUncompressedSize),
			fileInfo.CompressionRatio))
	}
	if fileInfo.CreatedBy != "" {
		header.WriteString(fmt.Sprintf("  [yellow]Created By:[-] %s", fileInfo.CreatedBy))
	}

	app.headerView.SetText(header.String())
}

func (app *TUIApp) getHeaderHeight() int {
	if app.headerView == nil {
		return 3 // Default fallback
	}

	text := app.headerView.GetText(false)
	lines := strings.Count(text, "\n") + 1

	// Add 2 for top and bottom borders
	return lines + 2
}

func (app *TUIApp) createRowGroupList() {
	app.rowGroupList = tview.NewTable().
		SetBorders(false).
		SetSeparator(tview.Borders.Vertical).
		SetSelectable(true, false).
		SetFixed(1, 0)

	app.rowGroupList.SetBorder(true).
		SetTitle(" Row Groups (↑↓ to navigate) ").
		SetTitleAlign(tview.AlignLeft)

	// Get row groups from HTTP client
	rowGroups, err := app.httpClient.getAllRowGroupsInfo()
	if err != nil {
		// Show error in table
		cell := tview.NewTableCell(fmt.Sprintf("[red]Error loading row groups: %v[-]", err)).
			SetTextColor(tcell.ColorRed).
			SetAlign(tview.AlignLeft).
			SetExpansion(1)
		app.rowGroupList.SetCell(1, 0, cell)
		return
	}

	// Set header row (removed "Columns" as it's in the file header - all row groups have same columns)
	headers := []string{"#", "Rows", "Size"}
	for colIdx, header := range headers {
		cell := tview.NewTableCell(header).
			SetTextColor(tcell.ColorYellow).
			SetAlign(tview.AlignCenter).
			SetSelectable(false).
			SetExpansion(0)
		app.rowGroupList.SetCell(0, colIdx, cell)
	}

	// Populate rows
	for rowIdx, rg := range rowGroups {
		// Row group index
		cell := tview.NewTableCell(fmt.Sprintf("%d", rg.Index)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		app.rowGroupList.SetCell(rowIdx+1, 0, cell)

		// Rows
		cell = tview.NewTableCell(fmt.Sprintf("%d", rg.NumRows)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		app.rowGroupList.SetCell(rowIdx+1, 1, cell)

		// Size - show compressed → uncompressed (ratio)
		sizeStr := fmt.Sprintf("%s → %s",
			model.FormatBytes(rg.CompressedSize),
			model.FormatBytes(rg.UncompressedSize))
		cell = tview.NewTableCell(sizeStr).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		app.rowGroupList.SetCell(rowIdx+1, 2, cell)
	}

	// Selection handler removed - use keyboard shortcuts instead (d=data, c=columns)
}

func (app *TUIApp) createStatusLine() {
	app.statusLine = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft)

	status := " [yellow]Keys:[-] ESC=quit, s=schema, ↑↓=scroll, Enter=see item details"
	app.statusLine.SetText(status)
}

func (app *TUIApp) showColumnChunksView(rgIndex int) {
	// Get row group info from HTTP client
	rowGroup, err := app.httpClient.getRowGroupInfo(rgIndex)
	if err != nil {
		// Show error modal
		errorModal := tview.NewModal().
			SetText(fmt.Sprintf("Error loading row group:\n%v\n\nPress ESC to go back", err)).
			SetTextColor(tcell.ColorRed).
			AddButtons([]string{"OK"}).
			SetDoneFunc(func(buttonIndex int, buttonLabel string) {
				app.pages.RemovePage("error")
			})
		app.pages.AddPage("error", errorModal, true, true)
		return
	}

	// Get column chunks info to calculate totals
	columns, err := app.httpClient.getAllColumnChunksInfo(rgIndex)
	if err != nil {
		// Show error modal
		errorModal := tview.NewModal().
			SetText(fmt.Sprintf("Error loading columns:\n%v\n\nPress ESC to go back", err)).
			SetTextColor(tcell.ColorRed).
			AddButtons([]string{"OK"}).
			SetDoneFunc(func(buttonIndex int, buttonLabel string) {
				app.pages.RemovePage("error")
			})
		app.pages.AddPage("error", errorModal, true, true)
		return
	}

	// Calculate total values and nulls
	var totalValues int64
	var totalNulls int64
	for _, col := range columns {
		totalValues += col.NumValues
		if col.NullCount != nil {
			totalNulls += *col.NullCount
		}
	}

	// Create row group info header
	headerView := tview.NewTextView().
		SetDynamicColors(true).
		SetWrap(true).
		SetWordWrap(true)

	var info strings.Builder

	// Line 1: Row Group number, Rows, Columns
	info.WriteString(fmt.Sprintf("[yellow]Row Group:[-] %d  ", rowGroup.Index))
	info.WriteString(fmt.Sprintf("[yellow]Rows:[-] %d  ", rowGroup.NumRows))
	info.WriteString(fmt.Sprintf("[yellow]Columns:[-] %d", rowGroup.NumColumns))

	// Line 2: Total Values and Total Nulls
	info.WriteString("\n")
	info.WriteString(fmt.Sprintf("[yellow]Total Values:[-] %d  ", totalValues))
	info.WriteString(fmt.Sprintf("[yellow]Total Nulls:[-] %d", totalNulls))

	// Line 3: Size info
	info.WriteString("\n")
	info.WriteString(fmt.Sprintf("[yellow]Size:[-] %s → %s (%.2fx)",
		model.FormatBytes(rowGroup.CompressedSize),
		model.FormatBytes(rowGroup.UncompressedSize),
		rowGroup.CompressionRatio))

	headerView.SetText(info.String())
	headerView.SetBorder(true).SetTitle(" Row Group Info ")

	// Create column chunks list
	columnList := app.createColumnChunksList(rgIndex)

	columnList.SetBorder(true).
		SetTitle(" Column Chunks (↑↓ to navigate, Enter=view pages) ").
		SetTitleAlign(tview.AlignLeft)

	// Create status line (keys only)
	statusLine := tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft)

	status := " [yellow]Keys:[-] ESC=back, s=schema, ↑↓=scroll, Enter=view pages"
	statusLine.SetText(status)

	// Create flex layout - same structure as main view: header, table, status
	// Calculate dynamic height for header
	headerText := headerView.GetText(false)
	headerLines := strings.Count(headerText, "\n") + 1
	headerHeight := headerLines + 2 // +2 for borders

	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(headerView, headerHeight, 0, false).
		AddItem(columnList, 0, 1, true).
		AddItem(statusLine, 1, 0, false)

	flex.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEscape:
			app.pages.RemovePage("columnsview")
			return nil
		case tcell.KeyRune:
			if event.Rune() == 's' {
				app.showSchema()
				return nil
			}
		}
		return event
	})

	app.pages.AddPage("columnsview", flex, true, true)
}

func (app *TUIApp) createColumnChunksList(rgIndex int) *tview.Table {
	table := tview.NewTable().
		SetBorders(false).
		SetSeparator(tview.Borders.Vertical).
		SetSelectable(true, false).
		SetFixed(1, 0)

	table.SetBorder(false)

	// Get column chunks from HTTP client
	columns, err := app.httpClient.getAllColumnChunksInfo(rgIndex)
	if err != nil {
		// Show error in table
		cell := tview.NewTableCell(fmt.Sprintf("[red]Error loading columns: %v[-]", err)).
			SetTextColor(tcell.ColorRed).
			SetAlign(tview.AlignLeft).
			SetExpansion(1)
		table.SetCell(1, 0, cell)
		return table
	}

	// Set header row
	headers := []string{"#", "Name", "Type", "Codec", "Size", "Min", "Max"}
	for colIdx, header := range headers {
		cell := tview.NewTableCell(header).
			SetTextColor(tcell.ColorYellow).
			SetAlign(tview.AlignCenter).
			SetSelectable(false).
			SetExpansion(0)
		if colIdx == 1 { // Name column - limit width
			cell.SetMaxWidth(30)
		}
		table.SetCell(0, colIdx, cell)
	}

	// Populate rows
	for rowIdx, col := range columns {
		// Column index
		cell := tview.NewTableCell(fmt.Sprintf("%d", col.Index)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		table.SetCell(rowIdx+1, 0, cell)

		// Column name
		cell = tview.NewTableCell(col.Name).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft).
			SetMaxWidth(30)
		table.SetCell(rowIdx+1, 1, cell)

		// Type
		cell = tview.NewTableCell(col.PhysicalType).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		table.SetCell(rowIdx+1, 2, cell)

		// Codec
		cell = tview.NewTableCell(col.Codec).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		table.SetCell(rowIdx+1, 3, cell)

		// Size
		cell = tview.NewTableCell(model.FormatBytes(col.CompressedSize)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		table.SetCell(rowIdx+1, 4, cell)

		// Min value - base64 encoded
		minStr := "-"
		if len(col.MinValue) > 0 {
			minStr = string(col.MinValue)
			if len(minStr) > 20 {
				minStr = minStr[:20] + "..."
			}
		}
		cell = tview.NewTableCell(minStr).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		table.SetCell(rowIdx+1, 5, cell)

		// Max value - base64 encoded
		maxStr := "-"
		if len(col.MaxValue) > 0 {
			maxStr = string(col.MaxValue)
			if len(maxStr) > 20 {
				maxStr = maxStr[:20] + "..."
			}
		}
		cell = tview.NewTableCell(maxStr).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		table.SetCell(rowIdx+1, 6, cell)
	}

	// Add selection handler
	table.SetSelectedFunc(func(row, column int) {
		if row == 0 {
			return // Skip header
		}
		colIndex := row - 1
		app.showPageView(rgIndex, colIndex)
	})

	return table
}
