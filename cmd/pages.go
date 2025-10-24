package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/rivo/tview"

	"github.com/hangxie/parquet-browser/model"
)

// PageContent holds decoded page content
type PageContent struct {
	Values        []string
	Error         string
	TotalValues   int
	DisplayedVals int
}

// pageContentBuilder handles building page content view with lazy loading
type pageContentBuilder struct {
	app            *TUIApp
	rgIndex        int
	colIndex       int
	pageIndex      int
	pageInfo       model.PageMetadata
	allPages       []model.PageMetadata
	meta           *parquet.ColumnMetaData
	table          *tview.Table
	allValues      []string // Pre-formatted values from API/model layer
	loadedValues   int
	isLoading      bool
	batchSize      int
	statusTextView *tview.TextView
	headerView     *tview.TextView
	ctx            context.Context
	cancel         context.CancelFunc
}

// pageTableBuilder handles building page tables with lazy loading
type pageTableBuilder struct {
	app            *TUIApp
	rgIndex        int
	colIndex       int
	meta           *parquet.ColumnMetaData
	table          *tview.Table
	pages          []model.PageMetadata
	loadedPages    int
	isLoading      bool
	batchSize      int
	statusTextView *tview.TextView
	loadError      error
}

// readPageHeadersBatch reads page headers in batches for lazy loading
func (b *pageTableBuilder) readPageHeadersBatch(startIdx, count int) error {
	if b.loadError != nil {
		return b.loadError
	}

	// If we haven't started reading yet, read all headers at once
	// (page headers are lightweight, we just lazy-load the table display)
	if len(b.pages) == 0 {
		pages, err := b.app.readPageHeaders(b.rgIndex, b.colIndex)
		if err != nil {
			b.loadError = err
			return err
		}
		b.pages = pages
	}

	return nil
}

func (b *pageTableBuilder) build() *tview.Table {
	b.setupHeader()

	// Load all page headers (lightweight operation)
	err := b.readPageHeadersBatch(0, 0)
	if err != nil {
		// Show error message
		cell := tview.NewTableCell(fmt.Sprintf("[red]Error reading pages: %v[-]", err)).
			SetTextColor(tcell.ColorRed).
			SetAlign(tview.AlignLeft).
			SetExpansion(1)
		b.table.SetCell(1, 0, cell)
		return b.table
	}

	// Load ALL pages at once - they're lightweight metadata
	totalPages := len(b.pages)
	for pageIdx, page := range b.pages {
		tableRowIdx := pageIdx + 1 // +1 because row 0 is the header

		// Page number
		cell := tview.NewTableCell(fmt.Sprintf("%d", pageIdx)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 0, cell)

		// Page type
		cell = tview.NewTableCell(page.PageType).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft).
			SetMaxWidth(15)
		b.table.SetCell(tableRowIdx, 1, cell)

		// Offset
		cell = tview.NewTableCell(fmt.Sprintf("0x%X", page.Offset)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 2, cell)

		// Compressed size
		cell = tview.NewTableCell(page.CompressedSizeFormatted).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 3, cell)

		// Uncompressed size
		cell = tview.NewTableCell(page.UncompressedSizeFormatted).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 4, cell)

		// Num values
		cell = tview.NewTableCell(fmt.Sprintf("%d", page.NumValues)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 5, cell)

		// Encoding
		encoding := page.Encoding
		if encoding == "" {
			encoding = "-"
		}
		cell = tview.NewTableCell(encoding).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		b.table.SetCell(tableRowIdx, 6, cell)

		// Min value
		minStr := page.MinValue
		if minStr == "" {
			minStr = "-"
		}
		cell = tview.NewTableCell(minStr).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		b.table.SetCell(tableRowIdx, 7, cell)

		// Max value
		maxStr := page.MaxValue
		if maxStr == "" {
			maxStr = "-"
		}
		cell = tview.NewTableCell(maxStr).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		b.table.SetCell(tableRowIdx, 8, cell)
	}

	b.loadedPages = totalPages

	// Set simple title
	b.table.SetTitle(" Pages (↑↓ to navigate, Enter=view values) ")

	// Setup selection handler for viewing page content
	b.table.SetSelectedFunc(func(row, col int) {
		if row == 0 || len(b.pages) == 0 {
			return // Skip header
		}
		pageIndex := row - 1
		if pageIndex >= 0 && pageIndex < len(b.pages) {
			b.app.showPageContent(b.rgIndex, b.colIndex, pageIndex, b.pages, b.meta)
		}
	})

	return b.table
}

func (b *pageTableBuilder) setupHeader() {
	headers := []string{"#", "Page Type", "Offset", "Comp Size", "Uncomp Size", "Values", "Encoding", "Min", "Max"}
	for colIdx, header := range headers {
		cell := tview.NewTableCell(header).
			SetTextColor(tcell.ColorYellow).
			SetAlign(tview.AlignCenter).
			SetSelectable(false)
		if colIdx == 1 { // Page Type column - limit width
			cell.SetMaxWidth(15)
		}
		b.table.SetCell(0, colIdx, cell)
	}
}

func (b *pageContentBuilder) build() (*tview.Table, error) {
	// Read all values first
	values, err := b.app.readPageContent(b.rgIndex, b.colIndex, b.pageIndex, b.allPages, b.meta)
	if err != nil {
		return nil, err
	}

	b.allValues = values

	// Setup header
	b.setupHeader()

	// Load ALL values at once - they're already in memory
	totalValues := len(b.allValues)
	for i := 0; i < totalValues; i++ {
		tableRowIdx := i + 1 // +1 because row 0 is the header

		// Index column
		cell := tview.NewTableCell(fmt.Sprintf("%d", i+1)).
			SetTextColor(tcell.ColorDarkCyan).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 0, cell)

		// Value column - values are already formatted strings from the API/model layer
		cell = tview.NewTableCell(b.allValues[i]).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft).
			SetExpansion(1)
		b.table.SetCell(tableRowIdx, 1, cell)
	}

	b.loadedValues = totalValues

	// Update header info
	b.updateHeaderInfo()

	// Setup key handlers
	b.table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEscape:
			b.cancel()
			b.app.pages.RemovePage("page-content")
			return nil
		case tcell.KeyRune:
			if event.Rune() == 's' {
				b.app.showSchema()
				return nil
			}
		}
		return event
	})

	return b.table, nil
}

func (b *pageContentBuilder) setupHeader() {
	// Add header row with two columns: Index and Value
	headers := []string{"#", "Value"}
	for colIdx, header := range headers {
		cell := tview.NewTableCell(header).
			SetTextColor(tcell.ColorYellow).
			SetAlign(tview.AlignCenter).
			SetSelectable(false)
		if colIdx == 1 { // Value column should expand
			cell.SetExpansion(1)
		}
		b.table.SetCell(0, colIdx, cell)
	}
}

// updateHeaderInfo updates the header view with page information
func (b *pageContentBuilder) updateHeaderInfo() {
	var info strings.Builder

	// Line 1: Page type, offset, size
	info.WriteString(fmt.Sprintf("[yellow]Page Type:[-] %s  ", b.pageInfo.PageType))
	info.WriteString(fmt.Sprintf("[yellow]Offset:[-] 0x%X  ", b.pageInfo.Offset))
	info.WriteString(fmt.Sprintf("[yellow]Size:[-] %s → %s (%.2fx)",
		b.pageInfo.CompressedSizeFormatted,
		b.pageInfo.UncompressedSizeFormatted,
		float64(b.pageInfo.UncompressedSize)/float64(b.pageInfo.CompressedSize)))

	// Line 2: Values, nulls, encoding
	info.WriteString("\n")
	info.WriteString(fmt.Sprintf("[yellow]Values:[-] %d", b.pageInfo.NumValues))

	// Add null count if available
	if b.pageInfo.NullCount != nil {
		info.WriteString(fmt.Sprintf("  [yellow]Nulls:[-] %d", *b.pageInfo.NullCount))
	}

	if b.pageInfo.Encoding != "" {
		info.WriteString(fmt.Sprintf("  [yellow]Encoding:[-] %s", b.pageInfo.Encoding))
	}

	// Line 3: Min/Max (if available)
	if b.pageInfo.MinValue != "" || b.pageInfo.MaxValue != "" {
		info.WriteString("\n")
		if b.pageInfo.MinValue != "" {
			info.WriteString(fmt.Sprintf("[yellow]Min:[-] %s", b.pageInfo.MinValue))
		}
		if b.pageInfo.MaxValue != "" {
			if b.pageInfo.MinValue != "" {
				info.WriteString("  ")
			}
			info.WriteString(fmt.Sprintf("[yellow]Max:[-] %s", b.pageInfo.MaxValue))
		}
	}

	b.headerView.SetText(info.String())
}

// getPageContentHeaderHeight calculates the height needed for page content header
func getPageContentHeaderHeight(headerView *tview.TextView) int {
	if headerView == nil {
		return 3
	}
	text := headerView.GetText(false)
	lines := strings.Count(text, "\n") + 1
	return lines + 2 // +2 for borders
}
