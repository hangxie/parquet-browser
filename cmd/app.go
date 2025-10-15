package cmd

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/gdamore/tcell/v2"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/rivo/tview"
)

// BrowseApp represents the TUI application for browsing Parquet files
type BrowseApp struct {
	tviewApp       *tview.Application
	pages          *tview.Pages
	mainLayout     *tview.Flex
	headerView     *tview.TextView
	rowGroupList   *tview.Table
	statusLine     *tview.TextView
	currentFile    string
	parquetReader  *reader.ParquetReader
	metadata       *parquet.FileMetaData // Parquet file metadata (footer)
	lastRGIndex    int                   // Track which row group we're positioned at
	lastRGPosition int64                 // Track position within file (total rows read)
}

// NewBrowseApp creates a new BrowseApp instance
func NewBrowseApp() *BrowseApp {
	return &BrowseApp{
		tviewApp:       tview.NewApplication(),
		pages:          tview.NewPages(),
		lastRGIndex:    -1, // Start with -1 to indicate no row group has been accessed yet
		lastRGPosition: 0,
	}
}

// Page reading methods

// getStartOffset determines where to start reading pages
func (app *BrowseApp) getStartOffset(meta *parquet.ColumnMetaData) int64 {
	if meta.DictionaryPageOffset != nil {
		return *meta.DictionaryPageOffset
	}
	return meta.DataPageOffset
}

// readSinglePageHeader reads a single page header and returns it with the header size
func (app *BrowseApp) readSinglePageHeader(pFile io.ReadSeeker, offset int64) (*parquet.PageHeader, int64, error) {
	// Seek to page header position
	_, err := pFile.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to seek to page: %w", err)
	}

	// Create a position-tracking transport
	trackingTransport := &positionTracker{r: pFile, pos: offset}
	proto := thrift.NewTCompactProtocolConf(trackingTransport, nil)

	pageHeader := parquet.NewPageHeader()
	if err := pageHeader.Read(context.Background(), proto); err != nil {
		return nil, 0, err
	}

	headerSize := trackingTransport.pos - offset

	// Seek to end of header
	_, err = pFile.Seek(trackingTransport.pos, io.SeekStart)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to seek after header: %w", err)
	}

	return pageHeader, headerSize, nil
}

// extractPageInfo creates a PageInfo from a page header
func (app *BrowseApp) extractPageInfo(pageHeader *parquet.PageHeader, offset int64) PageInfo {
	pageInfo := PageInfo{
		Offset:           offset,
		PageType:         pageHeader.Type.String(),
		CompressedSize:   pageHeader.CompressedPageSize,
		UncompressedSize: pageHeader.UncompressedPageSize,
		HasCRC:           pageHeader.IsSetCrc(),
	}

	// Get page-type specific details
	switch pageHeader.Type {
	case parquet.PageType_DATA_PAGE:
		app.populateDataPageInfo(&pageInfo, pageHeader.DataPageHeader)
	case parquet.PageType_DATA_PAGE_V2:
		app.populateDataPageV2Info(&pageInfo, pageHeader.DataPageHeaderV2)
	case parquet.PageType_DICTIONARY_PAGE:
		app.populateDictionaryPageInfo(&pageInfo, pageHeader.DictionaryPageHeader)
	case parquet.PageType_INDEX_PAGE:
		pageInfo.NumValues = 0 // Index pages don't have values
	}

	return pageInfo
}

// populateDataPageInfo populates info for DATA_PAGE type
func (app *BrowseApp) populateDataPageInfo(pageInfo *PageInfo, header *parquet.DataPageHeader) {
	if header == nil {
		return
	}
	pageInfo.NumValues = header.NumValues
	pageInfo.Encoding = header.Encoding.String()
	pageInfo.DefLevelEncoding = header.DefinitionLevelEncoding.String()
	pageInfo.RepLevelEncoding = header.RepetitionLevelEncoding.String()
	pageInfo.HasStatistics = header.IsSetStatistics()

	if pageInfo.HasStatistics && header.Statistics != nil {
		app.extractStatistics(pageInfo, header.Statistics)
	}
}

// populateDataPageV2Info populates info for DATA_PAGE_V2 type
func (app *BrowseApp) populateDataPageV2Info(pageInfo *PageInfo, header *parquet.DataPageHeaderV2) {
	if header == nil {
		return
	}
	pageInfo.NumValues = header.NumValues
	pageInfo.Encoding = header.Encoding.String()
	pageInfo.HasStatistics = header.IsSetStatistics()

	if pageInfo.HasStatistics && header.Statistics != nil {
		app.extractStatistics(pageInfo, header.Statistics)
	}
}

// populateDictionaryPageInfo populates info for DICTIONARY_PAGE type
func (app *BrowseApp) populateDictionaryPageInfo(pageInfo *PageInfo, header *parquet.DictionaryPageHeader) {
	if header == nil {
		return
	}
	pageInfo.NumValues = header.NumValues
	pageInfo.Encoding = header.Encoding.String()
}

// extractStatistics extracts min/max/null count statistics from page header
func (app *BrowseApp) extractStatistics(pageInfo *PageInfo, stats *parquet.Statistics) {
	// Prefer MinValue/MaxValue over deprecated Min/Max
	pageInfo.MinValue = stats.MinValue
	if len(pageInfo.MinValue) == 0 {
		pageInfo.MinValue = stats.Min
	}
	pageInfo.MaxValue = stats.MaxValue
	if len(pageInfo.MaxValue) == 0 {
		pageInfo.MaxValue = stats.Max
	}
	// Extract null count if available
	pageInfo.NullCount = stats.NullCount
}

// updateTotalValuesRead returns the number of values in this page (only for data pages)
func (app *BrowseApp) updateTotalValuesRead(pageInfo *PageInfo, pageHeader *parquet.PageHeader) int64 {
	// Only data pages count toward totalValuesRead
	if pageHeader.Type == parquet.PageType_DATA_PAGE || pageHeader.Type == parquet.PageType_DATA_PAGE_V2 {
		return int64(pageInfo.NumValues)
	}
	return 0
}

// shouldContinueReading checks if we should continue reading more pages
func (app *BrowseApp) shouldContinueReading(pages []PageInfo, currentOffset, endOffset, headerSize int64, pageHeader *parquet.PageHeader) bool {
	// Too many pages
	if len(pages) > 10000 {
		return false
	}

	// Reached end of column chunk
	if currentOffset >= endOffset {
		return false
	}

	// Check if offset is advancing (prevent infinite loop)
	lastOffset := currentOffset - headerSize - int64(pageHeader.CompressedPageSize)
	return currentOffset > lastOffset
}

// readPageHeaders reads all page headers from a column chunk
func (app *BrowseApp) readPageHeaders(rgIndex, colIndex int) ([]PageInfo, error) {
	rowGroup := app.metadata.RowGroups[rgIndex]
	column := rowGroup.Columns[colIndex]
	meta := column.MetaData

	var pages []PageInfo
	pFile := app.parquetReader.PFile

	// Calculate start and end offsets
	startOffset := app.getStartOffset(meta)
	endOffset := startOffset + meta.TotalCompressedSize

	// Read pages until we've read all values
	currentOffset := startOffset
	totalValuesRead := int64(0)

	for totalValuesRead < meta.NumValues {
		pageHeader, headerSize, err := app.readSinglePageHeader(pFile, currentOffset)
		if err != nil {
			break // Can't read more pages, we're done
		}

		// Extract page info
		pageInfo := app.extractPageInfo(pageHeader, currentOffset)

		// Count values for data pages
		valuesInPage := app.updateTotalValuesRead(&pageInfo, pageHeader)
		totalValuesRead += valuesInPage

		pages = append(pages, pageInfo)

		// Move to next page
		currentOffset = currentOffset + headerSize + int64(pageHeader.CompressedPageSize)

		// Safety checks
		if !app.shouldContinueReading(pages, currentOffset, endOffset, headerSize, pageHeader) {
			break
		}
	}

	return pages, nil
}

// findSchemaElement finds the schema element for a given path
func (app *BrowseApp) findSchemaElement(pathInSchema []string) *parquet.SchemaElement {
	if len(pathInSchema) == 0 {
		return nil
	}

	// The schema is a flat list in depth-first order
	// We need to find the leaf element that matches the full path
	for _, elem := range app.metadata.Schema {
		if elem.Name == pathInSchema[len(pathInSchema)-1] {
			// For simplicity, return the first match with the leaf name
			// In a more complex schema, we'd need to traverse the tree properly
			return elem
		}
	}

	return nil
}

// readPageContent reads and decodes the content of a specific page
func (app *BrowseApp) readPageContent(rgIndex, colIndex, pageIndex int, allPages []PageInfo, meta *parquet.ColumnMetaData) ([]interface{}, error) {
	if pageIndex < 0 || pageIndex >= len(allPages) {
		return nil, fmt.Errorf("invalid page index: %d", pageIndex)
	}

	pageInfo := allPages[pageIndex]

	// Dictionary pages don't contain row data, they contain dictionary values
	// Return an error or empty result for non-data pages
	if pageInfo.PageType != "DATA_PAGE" && pageInfo.PageType != "DATA_PAGE_V2" {
		return nil, fmt.Errorf("cannot read content of %s page - only DATA_PAGE and DATA_PAGE_V2 are supported", pageInfo.PageType)
	}

	// Strategy: For columns with nested/repeated fields, we cannot reliably skip by "NumValues"
	// because NumValues refers to physical values in the page, not logical rows.
	// Instead, we read ALL values from the entire column chunk and then slice to extract
	// just the values for the requested page.

	// Calculate which row in the file to start from
	// First, we need to skip all rows in previous row groups
	var rowsBeforeThisRG int64 = 0
	for i := 0; i < rgIndex; i++ {
		rowsBeforeThisRG += app.metadata.RowGroups[i].NumRows
	}

	// Create a NEW fresh reader for each page content read
	// This ensures we start from the beginning of the file and have a known state
	freshReader, err := reader.NewParquetColumnReader(app.parquetReader.PFile, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create fresh reader: %w", err)
	}
	defer func() {
		if stopErr := freshReader.ReadStopWithError(); stopErr != nil {
			// Log the error but don't fail the function - we're closing anyway
			// In a real application, you might want to log this
			_ = stopErr
		}
	}()

	// Skip to the beginning of the current row group if needed
	if rowsBeforeThisRG > 0 {
		err = freshReader.SkipRows(rowsBeforeThisRG)
		if err != nil {
			return nil, fmt.Errorf("failed to skip to row group %d (skipping %d rows): %w", rgIndex, rowsBeforeThisRG, err)
		}
	}

	// Read ALL values from this column chunk
	allValues, _, _, err := freshReader.ReadColumnByIndex(int64(colIndex), meta.NumValues)
	if err != nil {
		return nil, fmt.Errorf("failed to read all %d values from column: %w", meta.NumValues, err)
	}

	// Now slice the allValues array to extract only the values for the requested page
	// Calculate the start and end indices by summing up NumValues from previous pages
	var startIdx int64 = 0
	for i := 0; i < pageIndex; i++ {
		// Only count data pages (dictionary pages don't contain row values)
		if allPages[i].PageType == "DATA_PAGE" || allPages[i].PageType == "DATA_PAGE_V2" {
			startIdx += int64(allPages[i].NumValues)
		}
	}

	endIdx := startIdx + int64(pageInfo.NumValues)

	// Validate indices
	if startIdx >= int64(len(allValues)) {
		return []interface{}{}, nil // Page starts beyond available values
	}
	if endIdx > int64(len(allValues)) {
		endIdx = int64(len(allValues)) // Clamp to available values
	}

	// Extract the slice for this page
	pageValues := allValues[startIdx:endIdx]

	return pageValues, nil
}

// buildColumnChunkInfoView creates the info view for a column chunk
// If numPages is provided (> 0), it will be displayed in the header
func (app *BrowseApp) buildColumnChunkInfoView(meta *parquet.ColumnMetaData, schemaElem *parquet.SchemaElement, numPages int) *tview.TextView {
	infoView := tview.NewTextView().
		SetDynamicColors(true).
		SetWrap(true).
		SetWordWrap(true)

	var info strings.Builder

	// Line 1: Column path, type, logical type, converted type
	info.WriteString(fmt.Sprintf("[yellow]Column:[-] %s  ", strings.Join(meta.PathInSchema, ".")))
	info.WriteString(fmt.Sprintf("[yellow]Type:[-] %s  ", meta.Type.String()))

	// Add logical type and converted type if available
	if schemaElem != nil {
		logicalType := formatLogicalType(schemaElem.LogicalType)
		info.WriteString(fmt.Sprintf("[yellow]Logical:[-] %s  ", logicalType))

		convertedType := formatConvertedType(schemaElem.ConvertedType)
		info.WriteString(fmt.Sprintf("[yellow]Converted:[-] %s", convertedType))
	} else {
		info.WriteString("[yellow]Logical:[-] -  ")
		info.WriteString("[yellow]Converted:[-] -")
	}

	// Line 2: Codec, values, null count, size, pages
	info.WriteString("\n")
	info.WriteString(fmt.Sprintf("[yellow]Codec:[-] %s  ", meta.Codec.String()))
	info.WriteString(fmt.Sprintf("[yellow]Values:[-] %d  ", meta.NumValues))

	// Add page count if provided
	if numPages > 0 {
		info.WriteString(fmt.Sprintf("[yellow]Pages:[-] %d  ", numPages))
	}

	// Add null count if statistics are available
	if meta.Statistics != nil && meta.Statistics.NullCount != nil {
		info.WriteString(fmt.Sprintf("[yellow]Nulls:[-] %d  ", *meta.Statistics.NullCount))
	}

	info.WriteString(fmt.Sprintf("[yellow]Size:[-] %s → %s (%.2fx)",
		formatBytes(meta.TotalCompressedSize),
		formatBytes(meta.TotalUncompressedSize),
		float64(meta.TotalUncompressedSize)/float64(meta.TotalCompressedSize)))

	// Line 3: Min/Max statistics (if available)
	if meta.Statistics != nil {
		stats := meta.Statistics
		// Prefer MinValue/MaxValue over deprecated Min/Max
		minVal := stats.MinValue
		if minVal == nil {
			minVal = stats.Min
		}
		maxVal := stats.MaxValue
		if maxVal == nil {
			maxVal = stats.Max
		}

		if minVal != nil || maxVal != nil {
			info.WriteString("\n")
			if minVal != nil {
				info.WriteString(fmt.Sprintf("[yellow]Min:[-] %v", formatStatValueWithType(minVal, meta, schemaElem)))
			}
			if maxVal != nil {
				if minVal != nil {
					info.WriteString("  ")
				}
				info.WriteString(fmt.Sprintf("[yellow]Max:[-] %v", formatStatValueWithType(maxVal, meta, schemaElem)))
			}
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

func (app *BrowseApp) showMainView() {
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

func (app *BrowseApp) showSchema() {
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

func (app *BrowseApp) showPageView(rgIndex, colIndex int) {
	rowGroup := app.metadata.RowGroups[rgIndex]
	column := rowGroup.Columns[colIndex]
	meta := column.MetaData

	// Get schema element for this column to access logical/converted types
	schemaElem := app.findSchemaElement(meta.PathInSchema)

	// Show loading modal with cancellation instructions
	loadingModal := tview.NewModal().
		SetText(fmt.Sprintf("Loading Column Chunk Pages...\n\nColumn: %s\nType: %s\nValues: %d\n\nPlease wait...\n\nPress ESC to cancel",
			strings.Join(meta.PathInSchema, "."),
			meta.Type.String(),
			meta.NumValues)).
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

		// First, read page headers to get page count
		pages, err := app.readPageHeaders(rgIndex, colIndex)
		numPages := len(pages)
		if err != nil {
			numPages = 0 // If error, don't show page count
		}

		// Create column chunk info view with page count
		infoView := app.buildColumnChunkInfoView(meta, schemaElem, numPages)

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

		// Build table with lazy loading (use pre-read pages)
		builder := &pageTableBuilder{
			app:            app,
			rgIndex:        rgIndex,
			colIndex:       colIndex,
			meta:           meta,
			table:          pageTable,
			pages:          pages, // Use pre-read pages
			batchSize:      50,    // Load 50 pages at a time
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

func (app *BrowseApp) showPageContent(rgIndex, colIndex, pageIndex int, allPages []PageInfo, meta *parquet.ColumnMetaData) {
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

func (app *BrowseApp) createHeaderView() {
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

	// Line 2: Basic file info
	header.WriteString("\n")
	header.WriteString(fmt.Sprintf("[yellow]Version:[-] %d  ", app.metadata.Version))
	header.WriteString(fmt.Sprintf("[yellow]Row Groups:[-] %d  ", len(app.metadata.RowGroups)))
	header.WriteString(fmt.Sprintf("[yellow]Rows:[-] %d  ", app.metadata.NumRows))

	schema := app.metadata.Schema
	if len(schema) > 0 {
		// Count only leaf columns (columns with Type field)
		// This excludes group nodes like LIST, MAP, STRUCT
		leafColumns := countLeafColumns(schema)
		header.WriteString(fmt.Sprintf("[yellow]Columns:[-] %d", leafColumns))
	}

	// Line 3: Total size (compressed → uncompressed) and creator info
	// Calculate total compressed and uncompressed sizes across all row groups
	var totalCompressedSize int64
	var totalUncompressedSize int64
	for _, rg := range app.metadata.RowGroups {
		totalUncompressedSize += rg.TotalByteSize
		if rg.IsSetTotalCompressedSize() {
			totalCompressedSize += rg.GetTotalCompressedSize()
		} else {
			// Fallback: sum from columns
			totalCompressedSize += getTotalSize(rg)
		}
	}

	if totalCompressedSize > 0 && totalUncompressedSize > 0 {
		header.WriteString(fmt.Sprintf("\n[yellow]Total Size:[-] %s → %s (%.2fx)",
			formatBytes(totalCompressedSize),
			formatBytes(totalUncompressedSize),
			float64(totalUncompressedSize)/float64(totalCompressedSize)))
	}

	if app.metadata.CreatedBy != nil {
		header.WriteString(fmt.Sprintf("  [yellow]Created By:[-] %s", *app.metadata.CreatedBy))
	}

	app.headerView.SetText(header.String())
}

func (app *BrowseApp) getHeaderHeight() int {
	if app.headerView == nil {
		return 3 // Default fallback
	}

	text := app.headerView.GetText(false)
	lines := strings.Count(text, "\n") + 1

	// Add 2 for top and bottom borders
	return lines + 2
}

func (app *BrowseApp) createRowGroupList() {
	app.rowGroupList = tview.NewTable().
		SetBorders(false).
		SetSeparator(tview.Borders.Vertical).
		SetSelectable(true, false).
		SetFixed(1, 0)

	app.rowGroupList.SetBorder(true).
		SetTitle(" Row Groups (↑↓ to navigate) ").
		SetTitleAlign(tview.AlignLeft)

	// Populate with row groups
	rowGroups := app.metadata.RowGroups

	// Set header row
	headers := []string{"#", "Rows", "Columns", "Size"}
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
		numRows := rg.NumRows
		numColumns := len(rg.Columns)

		// Get compressed size - use field if available, otherwise calculate
		var compressedSize int64
		if rg.IsSetTotalCompressedSize() {
			compressedSize = rg.GetTotalCompressedSize()
		} else {
			compressedSize = getTotalSize(rg)
		}

		// Row group index
		cell := tview.NewTableCell(fmt.Sprintf("%d", rowIdx)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		app.rowGroupList.SetCell(rowIdx+1, 0, cell)

		// Rows
		cell = tview.NewTableCell(fmt.Sprintf("%d", numRows)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		app.rowGroupList.SetCell(rowIdx+1, 1, cell)

		// Columns
		cell = tview.NewTableCell(fmt.Sprintf("%d", numColumns)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		app.rowGroupList.SetCell(rowIdx+1, 2, cell)

		// Size - show compressed → uncompressed (ratio)
		sizeStr := fmt.Sprintf("%s → %s",
			formatBytes(compressedSize),
			formatBytes(rg.TotalByteSize))
		cell = tview.NewTableCell(sizeStr).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		app.rowGroupList.SetCell(rowIdx+1, 3, cell)
	}

	// Selection handler removed - use keyboard shortcuts instead (d=data, c=columns)
}

func (app *BrowseApp) createStatusLine() {
	app.statusLine = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft)

	status := " [yellow]Keys:[-] ESC=quit, s=schema, ↑↓=scroll, Enter=see item details"
	app.statusLine.SetText(status)
}

func (app *BrowseApp) showColumnChunksView(rgIndex int) {
	rowGroup := app.metadata.RowGroups[rgIndex]

	// Create row group info header
	headerView := tview.NewTextView().
		SetDynamicColors(true).
		SetWrap(true).
		SetWordWrap(true)

	var info strings.Builder

	// Line 1: Row Group number, Rows, Columns
	info.WriteString(fmt.Sprintf("[yellow]Row Group:[-] %d  ", rgIndex))
	info.WriteString(fmt.Sprintf("[yellow]Rows:[-] %d  ", rowGroup.NumRows))
	info.WriteString(fmt.Sprintf("[yellow]Columns:[-] %d", len(rowGroup.Columns)))

	// Line 2: Size and file offset info
	info.WriteString("\n")

	// Get uncompressed size (TotalByteSize is always present)
	uncompressedSize := rowGroup.TotalByteSize

	// Get compressed size - use field if available, otherwise calculate from columns
	var compressedSize int64
	if rowGroup.IsSetTotalCompressedSize() {
		compressedSize = rowGroup.GetTotalCompressedSize()
	} else {
		// Fallback: sum from individual columns
		compressedSize = getTotalSize(rowGroup)
	}

	// Display size info
	info.WriteString(fmt.Sprintf("[yellow]Size:[-] %s → %s (%.2fx)",
		formatBytes(compressedSize),
		formatBytes(uncompressedSize),
		float64(uncompressedSize)/float64(compressedSize)))

	// Add file offset if available
	if rowGroup.IsSetFileOffset() {
		info.WriteString(fmt.Sprintf("  [yellow]File Offset:[-] 0x%X", rowGroup.GetFileOffset()))
	}

	// Add ordinal if available
	if rowGroup.IsSetOrdinal() {
		info.WriteString(fmt.Sprintf("  [yellow]Ordinal:[-] %d", rowGroup.GetOrdinal()))
	}

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

	status := " [yellow]Keys:[-] ESC=back, s=schema, ↑↓=scroll, Enter=see item details"
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

func (app *BrowseApp) createColumnChunksList(rgIndex int) *tview.Table {
	rowGroup := app.metadata.RowGroups[rgIndex]

	table := tview.NewTable().
		SetBorders(false).
		SetSeparator(tview.Borders.Vertical).
		SetSelectable(true, false).
		SetFixed(1, 0)

	table.SetBorder(false)

	columns := rowGroup.Columns

	// Set header row
	headers := []string{"#", "Name", "Type", "Codec", "Size", "Min", "Max"}
	for colIdx, header := range headers {
		cell := tview.NewTableCell(header).
			SetTextColor(tcell.ColorYellow).
			SetAlign(tview.AlignCenter).
			SetSelectable(false).
			SetExpansion(0)
		if colIdx == 1 { // Name column should expand
			cell.SetExpansion(1)
		}
		table.SetCell(0, colIdx, cell)
	}

	// Populate rows
	for rowIdx, col := range columns {
		pathParts := col.MetaData.PathInSchema
		colName := strings.Join(pathParts, ".")

		// Column index
		cell := tview.NewTableCell(fmt.Sprintf("%d", rowIdx)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		table.SetCell(rowIdx+1, 0, cell)

		// Column name
		cell = tview.NewTableCell(colName).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft).
			SetExpansion(1)
		table.SetCell(rowIdx+1, 1, cell)

		// Type
		cell = tview.NewTableCell(col.MetaData.Type.String()).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		table.SetCell(rowIdx+1, 2, cell)

		// Codec
		cell = tview.NewTableCell(col.MetaData.Codec.String()).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		table.SetCell(rowIdx+1, 3, cell)

		// Size
		cell = tview.NewTableCell(formatBytes(col.MetaData.TotalCompressedSize)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		table.SetCell(rowIdx+1, 4, cell)

		// Get schema element for proper type interpretation
		schemaElem := findSchemaElement(app.metadata.Schema, pathParts)

		// Min value - prefer MinValue over deprecated Min
		minStr := "-"
		if col.MetaData.Statistics != nil {
			stats := col.MetaData.Statistics
			minVal := stats.MinValue
			if len(minVal) == 0 {
				minVal = stats.Min
			}
			if len(minVal) > 0 {
				minStr = formatStatValueWithType(minVal, col.MetaData, schemaElem)
			}
		}
		cell = tview.NewTableCell(minStr).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		table.SetCell(rowIdx+1, 5, cell)

		// Max value - prefer MaxValue over deprecated Max
		maxStr := "-"
		if col.MetaData.Statistics != nil {
			stats := col.MetaData.Statistics
			maxVal := stats.MaxValue
			if len(maxVal) == 0 {
				maxVal = stats.Max
			}
			if len(maxVal) > 0 {
				maxStr = formatStatValueWithType(maxVal, col.MetaData, schemaElem)
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
