package model

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
)

// PageMetadata contains metadata about a page
type PageMetadata struct {
	Index            int
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
	MinValue         string // Formatted for display
	MaxValue         string // Formatted for display
	NullCount        *int64
	// Pre-formatted copies kept for existing API clients.
	CompressedSizeFormatted   string `json:"compressedSizeFormatted,omitempty"`
	UncompressedSizeFormatted string `json:"uncompressedSizeFormatted,omitempty"`
	MinValueFormatted         string `json:"minValueFormatted,omitempty"`
	MaxValueFormatted         string `json:"maxValueFormatted,omitempty"`
}

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

	switch headerInfo.PageType {
	case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
		pageInfo.Encoding = headerInfo.Encoding.String()
		pageInfo.DefLevelEncoding = headerInfo.DefLevelEncoding.String()
		pageInfo.RepLevelEncoding = headerInfo.RepLevelEncoding.String()
		pageInfo.HasStatistics = headerInfo.HasStatistics

		if headerInfo.HasStatistics && headerInfo.Statistics != nil {
			stats := headerInfo.Statistics
			pageInfo.NullCount = stats.NullCount

			// Prefer MinValue/MaxValue over the deprecated Min/Max.
			minValueBytes := stats.MinValue
			if len(minValueBytes) == 0 {
				minValueBytes = stats.Min
			}
			maxValueBytes := stats.MaxValue
			if len(maxValueBytes) == 0 {
				maxValueBytes = stats.Max
			}

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

	pageInfo.CompressedSizeFormatted = FormatBytes(int64(pageInfo.CompressedSize))
	pageInfo.UncompressedSizeFormatted = FormatBytes(int64(pageInfo.UncompressedSize))

	// Pre-formatted copies kept for existing API clients.
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

// pageMetadataList is GetPageMetadataList without locking; callers hold the reader lock.
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

	schemaElem := findSchemaElement(pr.metadata.Schema, meta.PathInSchema)

	pageHeaders, err := pr.Reader.GetAllPageHeadersWithContext(ctx, rgIndex, colIndex)
	if err != nil {
		return nil, err
	}

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
