package cmd

import (
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/rivo/tview"

	"github.com/hangxie/parquet-browser/model"
)

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
func (b *pageTableBuilder) readPageHeadersBatch() error {
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
	err := b.readPageHeadersBatch()
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
