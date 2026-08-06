package model

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
)

// This file holds the page-level read methods of ParquetReader (metadata,
// content, and dictionary decoding). The exported methods acquire the reader
// lock and delegate to non-locking cores; see reader_lock.go.

func convertPageHeaderInfoToMetadata(headerInfo reader.PageHeaderInfo, columnMeta *parquet.ColumnMetaData, schemaElem *parquet.SchemaElement) PageMetadata {
	pageInfo := PageMetadata{
		Index:            headerInfo.Index,
		Offset:           headerInfo.Offset,
		PageType:         headerInfo.PageType.String(),
		CompressedSize:   headerInfo.CompressedSize,
		UncompressedSize: headerInfo.UncompressedSize,
		NumValues:        headerInfo.NumValues,
		HasCRC:           headerInfo.HasCRC,
	}

	// Set encoding information based on page type
	switch headerInfo.PageType {
	case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
		pageInfo.Encoding = headerInfo.Encoding.String()
		pageInfo.DefLevelEncoding = headerInfo.DefLevelEncoding.String()
		pageInfo.RepLevelEncoding = headerInfo.RepLevelEncoding.String()
		pageInfo.HasStatistics = headerInfo.HasStatistics

		// Extract statistics if available
		if headerInfo.HasStatistics && headerInfo.Statistics != nil {
			stats := headerInfo.Statistics
			pageInfo.NullCount = stats.NullCount

			// Prefer MinValue/MaxValue over deprecated Min/Max
			minValueBytes := stats.MinValue
			if len(minValueBytes) == 0 {
				minValueBytes = stats.Min
			}
			maxValueBytes := stats.MaxValue
			if len(maxValueBytes) == 0 {
				maxValueBytes = stats.Max
			}

			// Format the values for display
			if len(minValueBytes) > 0 && columnMeta != nil {
				pageInfo.MinValue = FormatStatValue(minValueBytes, columnMeta, schemaElem)
			}
			if len(maxValueBytes) > 0 && columnMeta != nil {
				pageInfo.MaxValue = FormatStatValue(maxValueBytes, columnMeta, schemaElem)
			}
		}
	case parquet.PageType_DICTIONARY_PAGE:
		pageInfo.Encoding = headerInfo.Encoding.String()
	}

	// Format sizes for display
	pageInfo.CompressedSizeFormatted = FormatBytes(int64(pageInfo.CompressedSize))
	pageInfo.UncompressedSizeFormatted = FormatBytes(int64(pageInfo.UncompressedSize))

	// Keep the formatted fields for backward compatibility
	pageInfo.MinValueFormatted = pageInfo.MinValue
	pageInfo.MaxValueFormatted = pageInfo.MaxValue

	return pageInfo
}

// GetPageMetadataList returns metadata for all pages in a column chunk
func (pr *ParquetReader) GetPageMetadataList(ctx context.Context, rgIndex, colIndex int) ([]PageMetadata, error) {
	if pr == nil {
		return nil, ErrInvalidRowGroupIndex
	}
	if err := pr.acquire(ctx); err != nil {
		return nil, err
	}
	defer pr.release()
	return pr.pageMetadataList(ctx, rgIndex, colIndex)
}

// pageMetadataList is the non-locking core of GetPageMetadataList; callers must
// already hold the reader lock (see acquire in reader_lock.go).
func (pr *ParquetReader) pageMetadataList(ctx context.Context, rgIndex, colIndex int) ([]PageMetadata, error) {
	if pr == nil || pr.metadata == nil {
		return nil, ErrInvalidRowGroupIndex
	}

	numRowGroups := len(pr.metadata.RowGroups)
	if rgIndex < 0 || rgIndex >= numRowGroups {
		return nil, fmt.Errorf("row group index %d out of range [0, %d): %w",
			rgIndex, numRowGroups, ErrInvalidRowGroupIndex)
	}

	rg := pr.metadata.RowGroups[rgIndex]
	numColumns := len(rg.Columns)
	if colIndex < 0 || colIndex >= numColumns {
		return nil, fmt.Errorf("column index %d out of range [0, %d): %w",
			colIndex, numColumns, ErrInvalidColumnIndex)
	}

	meta := rg.Columns[colIndex].MetaData

	// Get schema element for formatting
	schemaElem := findSchemaElement(pr.metadata.Schema, meta.PathInSchema)

	pageHeaders, err := pr.Reader.GetAllPageHeadersWithContext(ctx, rgIndex, colIndex)
	if err != nil {
		return nil, err
	}

	// Convert PageHeaderInfo to PageMetadata
	pages := make([]PageMetadata, len(pageHeaders))
	for i, headerInfo := range pageHeaders {
		pages[i] = convertPageHeaderInfoToMetadata(headerInfo, meta, schemaElem)
	}

	return pages, nil
}

// GetPageMetadata returns metadata for a specific page
func (pr *ParquetReader) GetPageMetadata(ctx context.Context, rgIndex, colIndex, pageIndex int) (PageMetadata, error) {
	if pr == nil {
		return PageMetadata{}, ErrInvalidRowGroupIndex
	}
	if err := pr.acquire(ctx); err != nil {
		return PageMetadata{}, err
	}
	defer pr.release()

	pages, err := pr.pageMetadataList(ctx, rgIndex, colIndex)
	if err != nil {
		return PageMetadata{}, err
	}

	numPages := len(pages)
	if pageIndex < 0 || pageIndex >= numPages {
		return PageMetadata{}, fmt.Errorf("page index %d out of range [0, %d): %w",
			pageIndex, numPages, ErrInvalidPageIndex)
	}

	return pages[pageIndex], nil
}

// GetPageContent reads and decodes the values from a specific page
func (pr *ParquetReader) GetPageContent(ctx context.Context, rgIndex, colIndex, pageIndex int) ([]interface{}, error) {
	if pr == nil {
		return nil, ErrInvalidRowGroupIndex
	}
	if err := pr.acquire(ctx); err != nil {
		return nil, err
	}
	defer pr.release()
	return pr.pageContent(ctx, rgIndex, colIndex, pageIndex)
}

// pageContent is the non-locking core of GetPageContent; callers must already
// hold the reader lock (see acquire in reader_lock.go).
func (pr *ParquetReader) pageContent(ctx context.Context, rgIndex, colIndex, pageIndex int) ([]interface{}, error) {
	if pr == nil || pr.metadata == nil {
		return nil, ErrInvalidRowGroupIndex
	}

	numRowGroups := len(pr.metadata.RowGroups)
	if rgIndex < 0 || rgIndex >= numRowGroups {
		return nil, fmt.Errorf("row group index %d out of range [0, %d): %w",
			rgIndex, numRowGroups, ErrInvalidRowGroupIndex)
	}

	rg := pr.metadata.RowGroups[rgIndex]
	numColumns := len(rg.Columns)
	if colIndex < 0 || colIndex >= numColumns {
		return nil, fmt.Errorf("column index %d out of range [0, %d): %w",
			colIndex, numColumns, ErrInvalidColumnIndex)
	}

	meta := rg.Columns[colIndex].MetaData

	// Get all page metadata to understand page boundaries
	pages, err := pr.pageMetadataList(ctx, rgIndex, colIndex)
	if err != nil {
		return nil, err
	}

	numPages := len(pages)
	if pageIndex < 0 || pageIndex >= numPages {
		return nil, fmt.Errorf("page index %d out of range [0, %d): %w",
			pageIndex, numPages, ErrInvalidPageIndex)
	}

	pageInfo := pages[pageIndex]

	// Handle different page types
	switch pageInfo.PageType {
	case "DATA_PAGE", "DATA_PAGE_V2":
		// Continue with normal data page reading
	case "DICTIONARY_PAGE":
		// For dictionary pages, we need to read and decode the dictionary
		return pr.readDictionaryPageContent(ctx, rgIndex, colIndex, pageIndex, pages)
	default:
		// For other page types (INDEX_PAGE, etc.), return empty
		// These pages don't contain user data
		return []interface{}{}, nil
	}

	// Calculate rows before this row group
	var rowsBeforeThisRG int64 = 0
	for i := 0; i < rgIndex; i++ {
		rowsBeforeThisRG += pr.metadata.RowGroups[i].NumRows
	}

	// Create a fresh column reader
	freshReader, err := reader.NewParquetColumnReaderWithContext(ctx, pr.Reader.PFile, reader.WithNP(4))
	if err != nil {
		return nil, err
	}
	defer func() { _ = freshReader.ReadStopWithContext(ctx) }()

	// Skip to the beginning of the current row group
	if rowsBeforeThisRG > 0 {
		err = freshReader.SkipRowsWithContext(ctx, rowsBeforeThisRG)
		if err != nil {
			return nil, err
		}
	}

	// Read ALL values from this column chunk
	allValues, _, _, err := freshReader.ReadColumnByIndexWithContext(ctx, int64(colIndex), meta.NumValues)
	if err != nil {
		return nil, err
	}

	// Calculate the start index for this page
	var startIdx int64 = 0
	for i := 0; i < pageIndex; i++ {
		if pages[i].PageType == "DATA_PAGE" || pages[i].PageType == "DATA_PAGE_V2" {
			startIdx += int64(pages[i].NumValues)
		}
	}

	// Extract values for just this page
	endIdx := startIdx + int64(pageInfo.NumValues)
	if endIdx > int64(len(allValues)) {
		endIdx = int64(len(allValues))
	}

	return allValues[startIdx:endIdx], nil
}

// readDictionaryPageContent reads and decodes dictionary page values
func (pr *ParquetReader) readDictionaryPageContent(ctx context.Context, rgIndex, colIndex, pageIndex int, pages []PageMetadata) ([]interface{}, error) {
	rg := pr.metadata.RowGroups[rgIndex]
	meta := rg.Columns[colIndex].MetaData
	pageInfo := pages[pageIndex]

	values, err := pr.Reader.ReadDictionaryPageValuesWithContext(ctx, pageInfo.Offset, meta.Codec, meta.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to read dictionary page: %w", err)
	}

	return values, nil
}

// GetPageContentFormatted returns pre-formatted string values for display
// This is the preferred method for frontends to use
func (pr *ParquetReader) GetPageContentFormatted(ctx context.Context, rgIndex, colIndex, pageIndex int) ([]string, error) {
	if pr == nil {
		return nil, ErrInvalidRowGroupIndex
	}
	if err := pr.acquire(ctx); err != nil {
		return nil, err
	}
	defer pr.release()

	// Get raw values
	rawValues, err := pr.pageContent(ctx, rgIndex, colIndex, pageIndex)
	if err != nil {
		return nil, err
	}

	// Get column metadata and schema element for formatting
	rg := pr.metadata.RowGroups[rgIndex]
	meta := rg.Columns[colIndex].MetaData
	schemaElem := findSchemaElement(pr.metadata.Schema, meta.PathInSchema)

	// Format each value
	formattedValues := make([]string, len(rawValues))
	for i, rawVal := range rawValues {
		// Handle special case: nil values for STRING logical type should be treated as empty strings
		// This is because parquet readers may return nil for zero-length BYTE_ARRAY values
		if rawVal == nil && schemaElem != nil && schemaElem.LogicalType != nil && schemaElem.LogicalType.IsSetSTRING() {
			formattedValues[i] = ""
			continue
		}
		formattedValues[i] = FormatValue(rawVal, meta.Type, schemaElem)
	}

	return formattedValues, nil
}
