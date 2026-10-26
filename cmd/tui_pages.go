package cmd

import (
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/rivo/tview"

	"github.com/hangxie/parquet-browser/model"
)

// pageTableBuilder builds the page list table for a column chunk
type pageTableBuilder struct {
	app            *TUIApp
	rgIndex        int
	colIndex       int
	meta           *parquet.ColumnMetaData
	table          *tview.Table
	pages          []model.PageMetadata
	statusTextView *tview.TextView
	loadError      error
}

// readPageHeadersBatch fetches the page headers unless they were supplied up front
func (b *pageTableBuilder) readPageHeadersBatch() error {
	if b.loadError != nil {
		return b.loadError
	}

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

	err := b.readPageHeadersBatch()
	if err != nil {
		cell := tview.NewTableCell(fmt.Sprintf("[red]Error reading pages: %v[-]", err)).
			SetTextColor(tcell.ColorRed).
			SetAlign(tview.AlignLeft).
			SetExpansion(1)
		b.table.SetCell(1, 0, cell)
		return b.table
	}

	for pageIdx, page := range b.pages {
		tableRowIdx := pageIdx + 1 // +1 because row 0 is the header

		cell := tview.NewTableCell(fmt.Sprintf("%d", pageIdx)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 0, cell)

		cell = tview.NewTableCell(page.PageType).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft).
			SetMaxWidth(15)
		b.table.SetCell(tableRowIdx, 1, cell)

		cell = tview.NewTableCell(fmt.Sprintf("0x%X", page.Offset)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 2, cell)

		cell = tview.NewTableCell(page.CompressedSizeFormatted).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 3, cell)

		cell = tview.NewTableCell(page.UncompressedSizeFormatted).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 4, cell)

		cell = tview.NewTableCell(fmt.Sprintf("%d", page.NumValues)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 5, cell)

		encoding := page.Encoding
		if encoding == "" {
			encoding = "-"
		}
		cell = tview.NewTableCell(encoding).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		b.table.SetCell(tableRowIdx, 6, cell)

		minStr := page.MinValue
		if minStr == "" {
			minStr = "-"
		}
		cell = tview.NewTableCell(minStr).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		b.table.SetCell(tableRowIdx, 7, cell)

		maxStr := page.MaxValue
		if maxStr == "" {
			maxStr = "-"
		}
		cell = tview.NewTableCell(maxStr).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		b.table.SetCell(tableRowIdx, 8, cell)
	}

	b.table.SetTitle(" Pages (↑↓ to navigate, Enter=view values) ")

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
