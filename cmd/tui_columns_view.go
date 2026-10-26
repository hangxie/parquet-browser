package cmd

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/hangxie/parquet-browser/model"
)

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
	_, _ = fmt.Fprintf(&info, "[yellow]Row Group:[-] %d  ", rowGroup.Index)
	_, _ = fmt.Fprintf(&info, "[yellow]Rows:[-] %d  ", rowGroup.NumRows)
	_, _ = fmt.Fprintf(&info, "[yellow]Columns:[-] %d", rowGroup.NumColumns)

	// Line 2: Total Values and Total Nulls
	info.WriteString("\n")
	_, _ = fmt.Fprintf(&info, "[yellow]Total Values:[-] %d  ", totalValues)
	_, _ = fmt.Fprintf(&info, "[yellow]Total Nulls:[-] %d", totalNulls)

	// Line 3: Size info
	info.WriteString("\n")
	_, _ = fmt.Fprintf(&info, "[yellow]Size:[-] %s → %s (%.2fx)",
		model.FormatBytes(rowGroup.CompressedSize),
		model.FormatBytes(rowGroup.UncompressedSize),
		rowGroup.CompressionRatio)

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
	if v := GetVersion(); v != "" {
		status += fmt.Sprintf("  [gray]%s[-]", v)
	}
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
