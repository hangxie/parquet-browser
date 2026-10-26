package model

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/reader"
)

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

// pageContent is GetPageContent without locking; callers hold the reader lock.
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

	switch pageInfo.PageType {
	case "DATA_PAGE", "DATA_PAGE_V2":
		// Decoded below.
	case "DICTIONARY_PAGE":
		return pr.readDictionaryPageContent(ctx, rgIndex, colIndex, pageIndex, pages)
	default:
		// Other page types (INDEX_PAGE, ...) carry no user data.
		return []interface{}{}, nil
	}

	var rowsBeforeThisRG int64 = 0
	for i := 0; i < rgIndex; i++ {
		rowsBeforeThisRG += pr.metadata.RowGroups[i].NumRows
	}

	freshReader, err := reader.NewParquetColumnReaderWithContext(ctx, pr.Reader.PFile, reader.WithNP(4))
	if err != nil {
		return nil, err
	}
	defer func() { _ = freshReader.ReadStopWithContext(ctx) }()

	if rowsBeforeThisRG > 0 {
		err = freshReader.SkipRowsWithContext(ctx, rowsBeforeThisRG)
		if err != nil {
			return nil, err
		}
	}

	// Values are read per column chunk, then sliced down to this page.
	allValues, _, _, err := freshReader.ReadColumnByIndexWithContext(ctx, int64(colIndex), meta.NumValues)
	if err != nil {
		return nil, err
	}

	var startIdx int64 = 0
	for i := 0; i < pageIndex; i++ {
		if pages[i].PageType == "DATA_PAGE" || pages[i].PageType == "DATA_PAGE_V2" {
			startIdx += int64(pages[i].NumValues)
		}
	}

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

// GetPageContentFormatted returns the page's values formatted for display
func (pr *ParquetReader) GetPageContentFormatted(ctx context.Context, rgIndex, colIndex, pageIndex int) ([]string, error) {
	if pr == nil {
		return nil, ErrInvalidRowGroupIndex
	}
	if err := pr.acquire(ctx); err != nil {
		return nil, err
	}
	defer pr.release()

	rawValues, err := pr.pageContent(ctx, rgIndex, colIndex, pageIndex)
	if err != nil {
		return nil, err
	}

	rg := pr.metadata.RowGroups[rgIndex]
	meta := rg.Columns[colIndex].MetaData
	schemaElem := findSchemaElement(pr.metadata.Schema, meta.PathInSchema)

	formattedValues := make([]string, len(rawValues))
	for i, rawVal := range rawValues {
		// Readers may return nil for zero-length BYTE_ARRAY values; show STRING ones as empty.
		if rawVal == nil && schemaElem != nil && schemaElem.LogicalType != nil && schemaElem.LogicalType.IsSetSTRING() {
			formattedValues[i] = ""
			continue
		}
		formattedValues[i] = FormatValue(rawVal, meta.Type, schemaElem)
	}

	return formattedValues, nil
}
