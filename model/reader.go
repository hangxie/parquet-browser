package model

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
)

// FileInfo contains metadata about a Parquet file
type FileInfo struct {
	Version               int32
	NumRowGroups          int
	NumRows               int64
	NumLeafColumns        int
	TotalCompressedSize   int64
	TotalUncompressedSize int64
	CompressionRatio      float64
	CreatedBy             string
}

// RowGroupInfo contains metadata about a row group
type RowGroupInfo struct {
	Index            int
	NumRows          int64
	NumColumns       int
	CompressedSize   int64
	UncompressedSize int64
	CompressionRatio float64
}

// ColumnChunkInfo contains metadata about a column chunk
type ColumnChunkInfo struct {
	Index            int
	PathInSchema     []string
	Name             string
	PhysicalType     string
	LogicalType      string
	ConvertedType    string
	Codec            string
	NumValues        int64
	NullCount        *int64
	CompressedSize   int64
	UncompressedSize int64
	CompressionRatio float64
	MinValue         string // Formatted for display
	MaxValue         string // Formatted for display
	// Formatted fields for display (kept for backward compatibility)
	CompressedSizeFormatted   string `json:"compressedSizeFormatted,omitempty"`
	UncompressedSizeFormatted string `json:"uncompressedSizeFormatted,omitempty"`
	MinValueFormatted         string `json:"minValueFormatted,omitempty"`
	MaxValueFormatted         string `json:"maxValueFormatted,omitempty"`
}

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
	// Formatted fields for display (kept for backward compatibility)
	CompressedSizeFormatted   string `json:"compressedSizeFormatted,omitempty"`
	UncompressedSizeFormatted string `json:"uncompressedSizeFormatted,omitempty"`
	MinValueFormatted         string `json:"minValueFormatted,omitempty"`
	MaxValueFormatted         string `json:"maxValueFormatted,omitempty"`
}

// ParquetReader wraps the parquet-go reader with utility methods
type ParquetReader struct {
	Reader   *reader.ParquetReader
	metadata *parquet.FileMetaData
}

// NewParquetReader creates a new ParquetReader
func NewParquetReader(r *reader.ParquetReader) *ParquetReader {
	return &ParquetReader{
		Reader:   r,
		metadata: r.Footer,
	}
}

// GetFileInfo extracts file-level information
func (pr *ParquetReader) GetFileInfo() FileInfo {
	info := FileInfo{
		Version:      pr.metadata.Version,
		NumRowGroups: len(pr.metadata.RowGroups),
		NumRows:      pr.metadata.NumRows,
	}

	// Count leaf columns
	info.NumLeafColumns = countLeafColumns(pr.metadata.Schema)

	// Calculate total sizes
	for _, rg := range pr.metadata.RowGroups {
		info.TotalUncompressedSize += rg.TotalByteSize
		if rg.IsSetTotalCompressedSize() {
			info.TotalCompressedSize += rg.GetTotalCompressedSize()
			continue
		}

		// Sum up compressed sizes from all columns
		var total int64
		for _, col := range rg.Columns {
			total += col.MetaData.TotalCompressedSize
		}
		info.TotalCompressedSize += total
	}

	// Calculate compression ratio
	if info.TotalCompressedSize > 0 {
		info.CompressionRatio = float64(info.TotalUncompressedSize) / float64(info.TotalCompressedSize)
	}

	// Get created by
	if pr.metadata.CreatedBy != nil {
		info.CreatedBy = *pr.metadata.CreatedBy
	}

	return info
}

// GetRowGroupInfo extracts row group information
func (pr *ParquetReader) GetRowGroupInfo(rgIndex int) (RowGroupInfo, error) {
	if pr == nil || pr.metadata == nil {
		return RowGroupInfo{}, ErrInvalidRowGroupIndex
	}

	numRowGroups := len(pr.metadata.RowGroups)
	if rgIndex < 0 || rgIndex >= numRowGroups {
		return RowGroupInfo{}, fmt.Errorf("row group index %d out of range [0, %d): %w",
			rgIndex, numRowGroups, ErrInvalidRowGroupIndex)
	}

	rg := pr.metadata.RowGroups[rgIndex]

	info := RowGroupInfo{
		Index:            rgIndex,
		NumRows:          rg.NumRows,
		NumColumns:       len(rg.Columns),
		UncompressedSize: rg.TotalByteSize,
	}

	// Get compressed size
	if rg.IsSetTotalCompressedSize() {
		info.CompressedSize = rg.GetTotalCompressedSize()
	} else {
		// Sum up compressed sizes from all columns
		for _, col := range rg.Columns {
			info.CompressedSize += col.MetaData.TotalCompressedSize
		}
	}

	// Calculate compression ratio
	if info.CompressedSize > 0 {
		info.CompressionRatio = float64(info.UncompressedSize) / float64(info.CompressedSize)
	}

	return info, nil
}

// GetAllRowGroupsInfo returns info for all row groups
func (pr *ParquetReader) GetAllRowGroupsInfo() []RowGroupInfo {
	infos := make([]RowGroupInfo, len(pr.metadata.RowGroups))
	for i := range pr.metadata.RowGroups {
		info, _ := pr.GetRowGroupInfo(i)
		infos[i] = info
	}
	return infos
}

// GetColumnChunkInfo extracts column chunk information
func (pr *ParquetReader) GetColumnChunkInfo(rgIndex, colIndex int) (ColumnChunkInfo, error) {
	if pr == nil || pr.metadata == nil {
		return ColumnChunkInfo{}, ErrInvalidRowGroupIndex
	}

	numRowGroups := len(pr.metadata.RowGroups)
	if rgIndex < 0 || rgIndex >= numRowGroups {
		return ColumnChunkInfo{}, fmt.Errorf("row group index %d out of range [0, %d): %w",
			rgIndex, numRowGroups, ErrInvalidRowGroupIndex)
	}

	rg := pr.metadata.RowGroups[rgIndex]
	numColumns := len(rg.Columns)
	if colIndex < 0 || colIndex >= numColumns {
		return ColumnChunkInfo{}, fmt.Errorf("column index %d out of range [0, %d): %w",
			colIndex, numColumns, ErrInvalidColumnIndex)
	}

	col := rg.Columns[colIndex]
	meta := col.MetaData

	info := ColumnChunkInfo{
		Index:            colIndex,
		PathInSchema:     meta.PathInSchema,
		Name:             formatColumnName(meta.PathInSchema),
		PhysicalType:     meta.Type.String(),
		Codec:            meta.Codec.String(),
		NumValues:        meta.NumValues,
		CompressedSize:   meta.TotalCompressedSize,
		UncompressedSize: meta.TotalUncompressedSize,
	}

	// Calculate compression ratio
	if info.CompressedSize > 0 {
		info.CompressionRatio = float64(info.UncompressedSize) / float64(info.CompressedSize)
	}

	// Get schema element for logical/converted types
	schemaElem := findSchemaElement(pr.metadata.Schema, meta.PathInSchema)
	if schemaElem != nil {
		info.LogicalType = formatLogicalType(schemaElem.LogicalType)
		info.ConvertedType = "-"
		if schemaElem.ConvertedType != nil {
			info.ConvertedType = schemaElem.ConvertedType.String()
		}
	}

	// Get statistics if available
	if meta.Statistics != nil {
		stats := meta.Statistics

		// Null count
		info.NullCount = stats.NullCount

		// Min/Max values - prefer MinValue/MaxValue over deprecated Min/Max
		minValueBytes := stats.MinValue
		if len(minValueBytes) == 0 {
			minValueBytes = stats.Min
		}
		maxValueBytes := stats.MaxValue
		if len(maxValueBytes) == 0 {
			maxValueBytes = stats.Max
		}

		// Format min/max values for display
		info.MinValue = FormatStatValue(minValueBytes, meta, schemaElem)
		info.MaxValue = FormatStatValue(maxValueBytes, meta, schemaElem)
		// Keep the formatted fields for backward compatibility
		info.MinValueFormatted = info.MinValue
		info.MaxValueFormatted = info.MaxValue
	}

	// Format sizes for display
	info.CompressedSizeFormatted = FormatBytes(info.CompressedSize)
	info.UncompressedSizeFormatted = FormatBytes(info.UncompressedSize)

	return info, nil
}

// GetAllColumnChunksInfo returns info for all columns in a row group
func (pr *ParquetReader) GetAllColumnChunksInfo(rgIndex int) ([]ColumnChunkInfo, error) {
	if pr == nil || pr.metadata == nil {
		return nil, ErrInvalidRowGroupIndex
	}

	numRowGroups := len(pr.metadata.RowGroups)
	if rgIndex < 0 || rgIndex >= numRowGroups {
		return nil, fmt.Errorf("row group index %d out of range [0, %d): %w",
			rgIndex, numRowGroups, ErrInvalidRowGroupIndex)
	}

	rg := pr.metadata.RowGroups[rgIndex]
	infos := make([]ColumnChunkInfo, len(rg.Columns))

	for i := range rg.Columns {
		info, _ := pr.GetColumnChunkInfo(rgIndex, i)
		infos[i] = info
	}

	return infos, nil
}

// formatColumnName creates a display name from path in schema
func formatColumnName(pathInSchema []string) string {
	return strings.Join(pathInSchema, ".")
}

// convertPageHeaderInfoToMetadata converts reader.PageHeaderInfo to PageMetadata
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
	if headerInfo.PageType == parquet.PageType_DATA_PAGE || headerInfo.PageType == parquet.PageType_DATA_PAGE_V2 {
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
	} else if headerInfo.PageType == parquet.PageType_DICTIONARY_PAGE {
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
func (pr *ParquetReader) GetPageMetadataList(rgIndex, colIndex int) ([]PageMetadata, error) {
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

	pageHeaders, err := pr.Reader.GetAllPageHeaders(rgIndex, colIndex)
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
func (pr *ParquetReader) GetPageMetadata(rgIndex, colIndex, pageIndex int) (PageMetadata, error) {
	pages, err := pr.GetPageMetadataList(rgIndex, colIndex)
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
func (pr *ParquetReader) GetPageContent(rgIndex, colIndex, pageIndex int) ([]interface{}, error) {
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
	pages, err := pr.GetPageMetadataList(rgIndex, colIndex)
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
		return pr.readDictionaryPageContent(rgIndex, colIndex, pageIndex, pages)
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
	freshReader, err := reader.NewParquetColumnReader(pr.Reader.PFile, 4)
	if err != nil {
		return nil, err
	}
	defer func() { _ = freshReader.ReadStopWithError() }()

	// Skip to the beginning of the current row group
	if rowsBeforeThisRG > 0 {
		err = freshReader.SkipRows(rowsBeforeThisRG)
		if err != nil {
			return nil, err
		}
	}

	// Read ALL values from this column chunk
	allValues, _, _, err := freshReader.ReadColumnByIndex(int64(colIndex), meta.NumValues)
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
func (pr *ParquetReader) readDictionaryPageContent(rgIndex, colIndex, pageIndex int, pages []PageMetadata) ([]interface{}, error) {
	rg := pr.metadata.RowGroups[rgIndex]
	meta := rg.Columns[colIndex].MetaData
	pageInfo := pages[pageIndex]

	values, err := pr.Reader.ReadDictionaryPageValues(pageInfo.Offset, meta.Codec, meta.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to read dictionary page: %w", err)
	}

	return values, nil
}

// GetPageContentFormatted returns pre-formatted string values for display
// This is the preferred method for frontends to use
func (pr *ParquetReader) GetPageContentFormatted(rgIndex, colIndex, pageIndex int) ([]string, error) {
	// Get raw values
	rawValues, err := pr.GetPageContent(rgIndex, colIndex, pageIndex)
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
