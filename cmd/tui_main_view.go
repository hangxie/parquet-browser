package cmd

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/hangxie/parquet-browser/model"
)

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
	_, _ = fmt.Fprintf(&header, "[yellow]File:[-] %s", filepath.Base(app.currentFile))

	// Line 2: Basic file info from HTTP API
	header.WriteString("\n")

	fileInfo, err := app.httpClient.getFileInfo()
	if err != nil {
		app.headerView.SetText(fmt.Sprintf("[red]Error loading file info: %v[-]", err))
		return
	}

	_, _ = fmt.Fprintf(&header, "[yellow]Version:[-] %d  ", fileInfo.Version)
	_, _ = fmt.Fprintf(&header, "[yellow]Row Groups:[-] %d  ", fileInfo.NumRowGroups)
	_, _ = fmt.Fprintf(&header, "[yellow]Rows:[-] %d  ", fileInfo.NumRows)
	_, _ = fmt.Fprintf(&header, "[yellow]Columns:[-] %d", fileInfo.NumLeafColumns)

	// Line 3: Total size (compressed → uncompressed) and creator info
	if fileInfo.TotalCompressedSize > 0 && fileInfo.TotalUncompressedSize > 0 {
		_, _ = fmt.Fprintf(&header, "\n[yellow]Total Size:[-] %s → %s (%.2fx)",
			model.FormatBytes(fileInfo.TotalCompressedSize),
			model.FormatBytes(fileInfo.TotalUncompressedSize),
			fileInfo.CompressionRatio)
	}
	if fileInfo.CreatedBy != "" {
		_, _ = fmt.Fprintf(&header, "  [yellow]Created By:[-] %s", fileInfo.CreatedBy)
	}
	if fileInfo.Encryption != "" {
		_, _ = fmt.Fprintf(&header, "  [yellow]Encryption:[-] %s", fileInfo.Encryption)
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
	if v := GetVersion(); v != "" {
		status += fmt.Sprintf("  [gray]%s[-]", v)
	}
	app.statusLine.SetText(status)
}
