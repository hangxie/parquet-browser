package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/rivo/tview"
)

// PageInfo holds information about a single page
type PageInfo struct {
	Offset           int64
	PageType         string
	CompressedSize   int32
	UncompressedSize int32
	NumValues        int32
	Encoding         string
	DefLevelEncoding string
	RepLevelEncoding string
	HasStatistics    bool
	HasCRC           bool
	MinValue         []byte
	MaxValue         []byte
	NullCount        *int64
}

// PageContent holds decoded page content
type PageContent struct {
	Values        []string
	Error         string
	TotalValues   int
	DisplayedVals int
}

// pageContentBuilder handles building page content view with lazy loading
type pageContentBuilder struct {
	app            *BrowseApp
	rgIndex        int
	colIndex       int
	pageIndex      int
	pageInfo       PageInfo
	allPages       []PageInfo
	meta           *parquet.ColumnMetaData
	table          *tview.Table
	allValues      []interface{}
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
	app            *BrowseApp
	rgIndex        int
	colIndex       int
	meta           *parquet.ColumnMetaData
	table          *tview.Table
	pages          []PageInfo
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

	// Get schema element for proper type interpretation of min/max values
	schemaElem := findSchemaElement(b.app.metadata.Schema, b.meta.PathInSchema)

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
			SetExpansion(1)
		b.table.SetCell(tableRowIdx, 1, cell)

		// Offset
		cell = tview.NewTableCell(fmt.Sprintf("0x%X", page.Offset)).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 2, cell)

		// Compressed size
		cell = tview.NewTableCell(formatBytes(int64(page.CompressedSize))).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignRight)
		b.table.SetCell(tableRowIdx, 3, cell)

		// Uncompressed size
		cell = tview.NewTableCell(formatBytes(int64(page.UncompressedSize))).
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

		// Min value - use formatStatValueWithType for proper type interpretation
		minStr := "-"
		if len(page.MinValue) > 0 {
			minStr = formatStatValueWithType(page.MinValue, b.meta, schemaElem)
		}
		cell = tview.NewTableCell(minStr).
			SetTextColor(tcell.ColorWhite).
			SetAlign(tview.AlignLeft)
		b.table.SetCell(tableRowIdx, 7, cell)

		// Max value - use formatStatValueWithType for proper type interpretation
		maxStr := "-"
		if len(page.MaxValue) > 0 {
			maxStr = formatStatValueWithType(page.MaxValue, b.meta, schemaElem)
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
		if colIdx == 1 { // Page Type column should expand
			cell.SetExpansion(1)
		}
		b.table.SetCell(0, colIdx, cell)
	}
}

// formatLogicalType formats the logical type for display
func formatLogicalType(logicalType *parquet.LogicalType) string {
	if logicalType == nil {
		return "-"
	}

	// LogicalType is a union type, check which field is set
	if logicalType.IsSetSTRING() {
		return "STRING"
	}
	if logicalType.IsSetMAP() {
		return "MAP"
	}
	if logicalType.IsSetLIST() {
		return "LIST"
	}
	if logicalType.IsSetENUM() {
		return "ENUM"
	}
	if logicalType.IsSetDECIMAL() {
		decimal := logicalType.DECIMAL
		return fmt.Sprintf("DECIMAL(%d,%d)", decimal.Precision, decimal.Scale)
	}
	if logicalType.IsSetDATE() {
		return "DATE"
	}
	if logicalType.IsSetTIME() {
		time := logicalType.TIME
		return fmt.Sprintf("TIME(%v,%t)", time.Unit, time.IsAdjustedToUTC)
	}
	if logicalType.IsSetTIMESTAMP() {
		ts := logicalType.TIMESTAMP
		return fmt.Sprintf("TIMESTAMP(%v,%t)", ts.Unit, ts.IsAdjustedToUTC)
	}
	if logicalType.IsSetINTEGER() {
		integer := logicalType.INTEGER
		sign := "signed"
		if !integer.IsSigned {
			sign = "unsigned"
		}
		return fmt.Sprintf("INTEGER(%d,%s)", integer.BitWidth, sign)
	}
	if logicalType.IsSetUNKNOWN() {
		return "UNKNOWN"
	}
	if logicalType.IsSetJSON() {
		return "JSON"
	}
	if logicalType.IsSetBSON() {
		return "BSON"
	}
	if logicalType.IsSetUUID() {
		return "UUID"
	}
	if logicalType.IsSetFLOAT16() {
		return "FLOAT16"
	}

	return "-"
}

// formatConvertedType formats the converted type for display
func formatConvertedType(convertedType *parquet.ConvertedType) string {
	if convertedType == nil {
		return "-"
	}
	return convertedType.String()
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

		// Value column
		valueStr := b.formatValue(b.allValues[i])
		cell = tview.NewTableCell(valueStr).
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

// formatValue formats a value for display in the table
func (b *pageContentBuilder) formatValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}

	// Get schema element for type-aware formatting
	schemaElem := findSchemaElement(b.app.metadata.Schema, b.meta.PathInSchema)

	// Apply logical type conversions if needed
	formattedVal := b.formatValueWithLogicalType(val, schemaElem)

	// Convert to string for display
	str := fmt.Sprintf("%v", formattedVal)
	if len(str) > 200 {
		return str[:200] + "..."
	}
	return str
}

// formatValueWithLogicalType applies logical type formatting to a value
//
//nolint:gocognit // Complex type conversion logic with many Parquet types - inherent complexity
func (b *pageContentBuilder) formatValueWithLogicalType(val interface{}, schemaElem *parquet.SchemaElement) interface{} {
	if val == nil || schemaElem == nil {
		return val
	}

	// Handle byte arrays with logical/converted types
	if bytes, ok := val.([]byte); ok {
		// If it has a logical or converted type, apply the conversion
		if schemaElem.LogicalType != nil || schemaElem.ConvertedType != nil {
			// Convert the value using the same logic as min/max values
			decoded := decodeStatValue(string(bytes), b.meta.Type, schemaElem)
			if decoded != string(bytes) {
				// If decoding changed the value, use the decoded version
				return decoded
			}
		}

		// No logical type or conversion didn't change it - treat as string/binary
		str := string(bytes)
		if isValidUTF8(str) {
			return str
		}
		// If not valid UTF-8, show as hex for small values
		if len(bytes) <= 32 {
			return fmt.Sprintf("0x%X", bytes)
		}
		return fmt.Sprintf("<binary:%d bytes>", len(bytes))
	}

	// IMPORTANT: Handle types that come as strings but contain binary data
	// The parquet-go reader returns FIXED_LEN_BYTE_ARRAY and BYTE_ARRAY as strings,
	// but for certain logical types they contain raw binary bytes that need conversion
	if schemaElem.ConvertedType != nil {
		switch *schemaElem.ConvertedType {
		case parquet.ConvertedType_INTERVAL:
			// INTERVAL: FIXED_LEN_BYTE_ARRAY[12] containing raw interval bytes
			if strVal, ok := val.(string); ok {
				decoded := decodeStatValue(strVal, b.meta.Type, schemaElem)
				return decoded
			}
		case parquet.ConvertedType_DECIMAL:
			// DECIMAL can be stored as BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY
			// When it's one of these, the reader returns raw binary bytes as a string
			if b.meta.Type == parquet.Type_BYTE_ARRAY || b.meta.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
				if strVal, ok := val.(string); ok {
					// Raw binary data - convert to decimal
					decoded := decodeStatValue(strVal, b.meta.Type, schemaElem)
					return decoded
				}
			}
		}
	}

	// For non-byte-array types, check if we need to apply logical type formatting
	// The parquet-go reader usually handles this, but we can add special formatting here if needed
	if schemaElem.LogicalType != nil {
		// Handle special display formatting for certain logical types
		switch {
		case schemaElem.LogicalType.IsSetDECIMAL():
			// If it's already a numeric type, format with proper decimal precision
			if schemaElem.Scale != nil && *schemaElem.Scale > 0 {
				switch v := val.(type) {
				case int32:
					decoded := decodeStatValue(v, b.meta.Type, schemaElem)
					return decoded
				case int64:
					decoded := decodeStatValue(v, b.meta.Type, schemaElem)
					return decoded
				case float32, float64:
					// Already a float, format with precision
					return val
				}
			}
		case schemaElem.LogicalType.IsSetDATE():
			// DATE is stored as int32 (days since Unix epoch)
			if i32Val, ok := val.(int32); ok {
				decoded := decodeStatValue(i32Val, b.meta.Type, schemaElem)
				return decoded
			}
		case schemaElem.LogicalType.IsSetTIMESTAMP():
			// TIMESTAMP is stored as int64 (time unit since Unix epoch)
			if i64Val, ok := val.(int64); ok {
				decoded := decodeStatValue(i64Val, b.meta.Type, schemaElem)
				return decoded
			}
		case schemaElem.LogicalType.IsSetTIME():
			// TIME is stored as int32 or int64
			if i32Val, ok := val.(int32); ok {
				decoded := decodeStatValue(i32Val, b.meta.Type, schemaElem)
				return decoded
			}
			if i64Val, ok := val.(int64); ok {
				decoded := decodeStatValue(i64Val, b.meta.Type, schemaElem)
				return decoded
			}
		case schemaElem.LogicalType.IsSetUUID():
			// UUID is stored as FIXED_LEN_BYTE_ARRAY[16]
			if strVal, ok := val.(string); ok {
				decoded := decodeStatValue(strVal, b.meta.Type, schemaElem)
				return decoded
			}
		}
	}

	// Handle converted types (backward compatibility)
	if schemaElem.ConvertedType != nil {
		switch *schemaElem.ConvertedType {
		case parquet.ConvertedType_DECIMAL:
			// Apply decimal conversion for int32/int64 values
			switch v := val.(type) {
			case int32:
				decoded := decodeStatValue(v, b.meta.Type, schemaElem)
				return decoded
			case int64:
				decoded := decodeStatValue(v, b.meta.Type, schemaElem)
				return decoded
			}
		case parquet.ConvertedType_DATE:
			if i32Val, ok := val.(int32); ok {
				decoded := decodeStatValue(i32Val, b.meta.Type, schemaElem)
				return decoded
			}
		case parquet.ConvertedType_TIMESTAMP_MILLIS, parquet.ConvertedType_TIMESTAMP_MICROS:
			if i64Val, ok := val.(int64); ok {
				decoded := decodeStatValue(i64Val, b.meta.Type, schemaElem)
				return decoded
			}
		case parquet.ConvertedType_TIME_MILLIS:
			if i32Val, ok := val.(int32); ok {
				decoded := decodeStatValue(i32Val, b.meta.Type, schemaElem)
				return decoded
			}
		case parquet.ConvertedType_TIME_MICROS:
			if i64Val, ok := val.(int64); ok {
				decoded := decodeStatValue(i64Val, b.meta.Type, schemaElem)
				return decoded
			}
		}
	}

	// Return value as-is if no special formatting needed
	return val
}

// updateHeaderInfo updates the header view with page information
func (b *pageContentBuilder) updateHeaderInfo() {
	var info strings.Builder

	// Line 1: Page type, offset, size
	info.WriteString(fmt.Sprintf("[yellow]Page Type:[-] %s  ", b.pageInfo.PageType))
	info.WriteString(fmt.Sprintf("[yellow]Offset:[-] 0x%X  ", b.pageInfo.Offset))
	info.WriteString(fmt.Sprintf("[yellow]Size:[-] %s → %s (%.2fx)",
		formatBytes(int64(b.pageInfo.CompressedSize)),
		formatBytes(int64(b.pageInfo.UncompressedSize)),
		float64(b.pageInfo.UncompressedSize)/float64(b.pageInfo.CompressedSize)))

	// Line 2: Values, nulls, encoding
	info.WriteString("\n")
	info.WriteString(fmt.Sprintf("[yellow]Values:[-] %d/%d", b.loadedValues, len(b.allValues)))

	// Add null count if available
	if b.pageInfo.NullCount != nil {
		info.WriteString(fmt.Sprintf("  [yellow]Nulls:[-] %d", *b.pageInfo.NullCount))
	}

	if b.pageInfo.Encoding != "" {
		info.WriteString(fmt.Sprintf("  [yellow]Encoding:[-] %s", b.pageInfo.Encoding))
	}

	// Line 3: Min/Max (if available)
	if len(b.pageInfo.MinValue) > 0 || len(b.pageInfo.MaxValue) > 0 {
		info.WriteString("\n")
		// Get schema element for proper type interpretation
		schemaElem := findSchemaElement(b.app.metadata.Schema, b.meta.PathInSchema)

		if len(b.pageInfo.MinValue) > 0 {
			info.WriteString(fmt.Sprintf("[yellow]Min:[-] %s", formatStatValueWithType(b.pageInfo.MinValue, b.meta, schemaElem)))
		}
		if len(b.pageInfo.MaxValue) > 0 {
			if len(b.pageInfo.MinValue) > 0 {
				info.WriteString("  ")
			}
			info.WriteString(fmt.Sprintf("[yellow]Max:[-] %s", formatStatValueWithType(b.pageInfo.MaxValue, b.meta, schemaElem)))
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
