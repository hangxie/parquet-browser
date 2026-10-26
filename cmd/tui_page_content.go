package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/hangxie/parquet-go/v3/parquet"
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

// pageContentBuilder builds the value table for a single page
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

func (app *TUIApp) showPageContent(rgIndex, colIndex, pageIndex int, allPages []model.PageMetadata, meta *parquet.ColumnMetaData) {
	pageInfo := allPages[pageIndex]
	loadingModal := tview.NewModal().
		SetText(fmt.Sprintf("Loading page content...\n\nPage Type: %s\nValues: %d\n\nPlease wait...",
			pageInfo.PageType,
			pageInfo.NumValues)).
		SetTextColor(tcell.ColorYellow)

	app.pages.AddPage("page-content-loading", loadingModal, true, true)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer cancel()

		headerView := tview.NewTextView().
			SetDynamicColors(true).
			SetWrap(true).
			SetWordWrap(true)
		headerView.SetBorder(true).SetTitle(" Page Info ")

		contentTable := tview.NewTable().
			SetBorders(false).
			SetSeparator(tview.Borders.Vertical).
			SetSelectable(true, false).
			SetFixed(1, 0)

		contentTable.SetBorder(true).
			SetTitle(" Page Content (↑↓ to navigate) ").
			SetTitleAlign(tview.AlignLeft)

		statusText := tview.NewTextView().
			SetDynamicColors(true).
			SetTextAlign(tview.AlignLeft)

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

		status := " [yellow]Keys:[-] ESC=back, s=schema, ↑↓=scroll"
		if v := GetVersion(); v != "" {
			status += fmt.Sprintf("  [gray]%s[-]", v)
		}
		statusText.SetText(status)

		app.tviewApp.QueueUpdateDraw(func() {
			app.pages.RemovePage("page-content-loading")

			if err != nil {
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

			// Same layout as the main view: header, table, status line.
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

func (b *pageContentBuilder) build() (*tview.Table, error) {
	values, err := b.app.readPageContent(b.rgIndex, b.colIndex, b.pageIndex)
	if err != nil {
		return nil, err
	}

	b.allValues = values

	b.setupHeader()

	// Values are already in memory, so fill the whole table at once.
	totalValues := len(b.allValues)
	for i := 0; i < totalValues; i++ {
		tableRowIdx := i + 1 // +1 because row 0 is the header

		cell := tview.NewTableCell(fmt.Sprintf("%d", i+1)).
			SetTextColor(tcell.ColorDarkCyan).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 0, cell)

		cell = tview.NewTableCell(b.allValues[i]).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft).
			SetExpansion(1)
		b.table.SetCell(tableRowIdx, 1, cell)
	}

	b.loadedValues = totalValues

	b.updateHeaderInfo()

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

	_, _ = fmt.Fprintf(&info, "[yellow]Page Type:[-] %s  ", b.pageInfo.PageType)
	_, _ = fmt.Fprintf(&info, "[yellow]Offset:[-] 0x%X  ", b.pageInfo.Offset)
	_, _ = fmt.Fprintf(&info, "[yellow]Size:[-] %s → %s (%.2fx)",
		b.pageInfo.CompressedSizeFormatted,
		b.pageInfo.UncompressedSizeFormatted,
		float64(b.pageInfo.UncompressedSize)/float64(b.pageInfo.CompressedSize))

	_, _ = info.WriteString("\n")
	_, _ = fmt.Fprintf(&info, "[yellow]Values:[-] %d", b.pageInfo.NumValues)

	if b.pageInfo.NullCount != nil {
		_, _ = fmt.Fprintf(&info, "  [yellow]Nulls:[-] %d", *b.pageInfo.NullCount)
	}

	if b.pageInfo.Encoding != "" {
		_, _ = fmt.Fprintf(&info, "  [yellow]Encoding:[-] %s", b.pageInfo.Encoding)
	}

	if b.pageInfo.MinValue != "" || b.pageInfo.MaxValue != "" {
		info.WriteString("\n")
		if b.pageInfo.MinValue != "" {
			_, _ = fmt.Fprintf(&info, "[yellow]Min:[-] %s", b.pageInfo.MinValue)
		}
		if b.pageInfo.MaxValue != "" {
			if b.pageInfo.MinValue != "" {
				info.WriteString("  ")
			}
			_, _ = fmt.Fprintf(&info, "[yellow]Max:[-] %s", b.pageInfo.MaxValue)
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
