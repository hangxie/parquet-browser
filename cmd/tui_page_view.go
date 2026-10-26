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

// buildColumnChunkInfoViewFromHTTP creates the info view for a column chunk using HTTP API data
func (app *TUIApp) buildColumnChunkInfoViewFromHTTP(colInfo model.ColumnChunkInfo, numPages int) *tview.TextView {
	infoView := tview.NewTextView().
		SetDynamicColors(true).
		SetWrap(true).
		SetWordWrap(true)

	var info strings.Builder

	// Line 1: Column path, type, logical type, converted type
	_, _ = fmt.Fprintf(&info, "[yellow]Column:[-] %s  ", colInfo.Name)
	_, _ = fmt.Fprintf(&info, "[yellow]Type:[-] %s  ", colInfo.PhysicalType)

	if colInfo.LogicalType != "-" {
		_, _ = fmt.Fprintf(&info, "[yellow]Logical:[-] %s  ", colInfo.LogicalType)
	}
	if colInfo.ConvertedType != "-" {
		_, _ = fmt.Fprintf(&info, "[yellow]Converted:[-] %s", colInfo.ConvertedType)
	}

	// Line 2: Values, codec, sizes
	_, _ = fmt.Fprintf(&info, "\n[yellow]Values:[-] %d  ", colInfo.NumValues)
	_, _ = fmt.Fprintf(&info, "[yellow]Codec:[-] %s  ", colInfo.Codec)

	if colInfo.CompressedSize > 0 && colInfo.UncompressedSize > 0 {
		_, _ = fmt.Fprintf(&info, "[yellow]Size:[-] %s → %s (%.2fx)",
			model.FormatBytes(colInfo.CompressedSize),
			model.FormatBytes(colInfo.UncompressedSize),
			colInfo.CompressionRatio)
	}

	// Line 3: Null count (if available) and number of pages
	if colInfo.NullCount != nil {
		_, _ = fmt.Fprintf(&info, "\n[yellow]Nulls:[-] %d  ", *colInfo.NullCount)
	} else {
		info.WriteString("\n")
	}

	if numPages > 0 {
		_, _ = fmt.Fprintf(&info, "[yellow]Pages:[-] %d", numPages)
	}

	// Line 4: Min/Max values (if available)
	if colInfo.MinValue != "" || colInfo.MaxValue != "" {
		info.WriteString("\n")
		if colInfo.MinValue != "" {
			_, _ = fmt.Fprintf(&info, "[yellow]Min:[-] %s", colInfo.MinValue)
		}
		if colInfo.MaxValue != "" {
			if colInfo.MinValue != "" {
				info.WriteString("  ")
			}
			_, _ = fmt.Fprintf(&info, "[yellow]Max:[-] %s", colInfo.MaxValue)
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
		if v := GetVersion(); v != "" {
			status += fmt.Sprintf("  [gray]%s[-]", v)
		}
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
