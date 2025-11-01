package model

import (
	"bytes"
	"fmt"

	"github.com/hangxie/parquet-go/v2/compress"
	"github.com/hangxie/parquet-go/v2/encoding"
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
		} else {
			info.TotalCompressedSize += getTotalSize(rg)
		}
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
	if rgIndex < 0 || rgIndex >= len(pr.metadata.RowGroups) {
		return RowGroupInfo{}, ErrInvalidRowGroupIndex
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
		info.CompressedSize = getTotalSize(rg)
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
	if rgIndex < 0 || rgIndex >= len(pr.metadata.RowGroups) {
		return ColumnChunkInfo{}, ErrInvalidRowGroupIndex
	}

	rg := pr.metadata.RowGroups[rgIndex]
	if colIndex < 0 || colIndex >= len(rg.Columns) {
		return ColumnChunkInfo{}, ErrInvalidColumnIndex
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
		info.ConvertedType = formatConvertedType(schemaElem.ConvertedType)
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
	if rgIndex < 0 || rgIndex >= len(pr.metadata.RowGroups) {
		return nil, ErrInvalidRowGroupIndex
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
	return formatPathInSchema(pathInSchema)
}

// GetPageMetadataList returns metadata for all pages in a column chunk
func (pr *ParquetReader) GetPageMetadataList(rgIndex, colIndex int) ([]PageMetadata, error) {
	if rgIndex < 0 || rgIndex >= len(pr.metadata.RowGroups) {
		return nil, ErrInvalidRowGroupIndex
	}

	rg := pr.metadata.RowGroups[rgIndex]
	if colIndex < 0 || colIndex >= len(rg.Columns) {
		return nil, ErrInvalidColumnIndex
	}

	column := rg.Columns[colIndex]
	meta := column.MetaData

	// Get schema element for formatting
	schemaElem := findSchemaElement(pr.metadata.Schema, meta.PathInSchema)

	var pages []PageMetadata
	pFile := pr.Reader.PFile

	// Calculate start offset
	startOffset := getColumnStartOffset(meta)

	// Read pages until we've read all values
	currentOffset := startOffset
	totalValuesRead := int64(0)
	pageIndex := 0

	for totalValuesRead < meta.NumValues {
		pageHeader, headerSize, err := readSinglePageHeader(pFile, currentOffset)
		if err != nil {
			break // Can't read more pages, we're done
		}

		// Extract page info
		pageInfo := extractPageMetadata(pageHeader, currentOffset, pageIndex, meta, schemaElem)
		pageIndex++

		// Count values for data pages
		valuesInPage := countPageValues(pageHeader)
		totalValuesRead += valuesInPage

		pages = append(pages, pageInfo)

		// Move to next page
		currentOffset = currentOffset + headerSize + int64(pageHeader.CompressedPageSize)

		// Safety check: Don't read beyond reasonable limits
		if len(pages) > 10000 || currentOffset > startOffset+meta.TotalCompressedSize+1024*1024 {
			break
		}
	}

	return pages, nil
}

// GetPageMetadata returns metadata for a specific page
func (pr *ParquetReader) GetPageMetadata(rgIndex, colIndex, pageIndex int) (PageMetadata, error) {
	pages, err := pr.GetPageMetadataList(rgIndex, colIndex)
	if err != nil {
		return PageMetadata{}, err
	}

	if pageIndex < 0 || pageIndex >= len(pages) {
		return PageMetadata{}, ErrInvalidPageIndex
	}

	return pages[pageIndex], nil
}

// GetPageContent reads and decodes the values from a specific page
func (pr *ParquetReader) GetPageContent(rgIndex, colIndex, pageIndex int) ([]interface{}, error) {
	if rgIndex < 0 || rgIndex >= len(pr.metadata.RowGroups) {
		return nil, ErrInvalidRowGroupIndex
	}

	rg := pr.metadata.RowGroups[rgIndex]
	if colIndex < 0 || colIndex >= len(rg.Columns) {
		return nil, ErrInvalidColumnIndex
	}

	meta := rg.Columns[colIndex].MetaData

	// Get all page metadata to understand page boundaries
	pages, err := pr.GetPageMetadataList(rgIndex, colIndex)
	if err != nil {
		return nil, err
	}

	if pageIndex < 0 || pageIndex >= len(pages) {
		return nil, ErrInvalidPageIndex
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

	// Read the raw page data
	pFile := pr.Reader.PFile

	// We need to re-read the header to get the exact header size
	pageHeader, headerSize, err := readSinglePageHeader(pFile, pageInfo.Offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page header: %w", err)
	}

	// Move to the actual page data (after header)
	dataOffset := pageInfo.Offset + headerSize
	_, err = pFile.Seek(dataOffset, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to page data: %w", err)
	}

	// Read compressed page data
	compressedData := make([]byte, pageHeader.CompressedPageSize)
	_, err = pFile.Read(compressedData)
	if err != nil {
		return nil, fmt.Errorf("failed to read compressed page data: %w", err)
	}

	// Decompress the data
	uncompressedData, err := compress.Uncompress(compressedData, meta.Codec)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress page data: %w", err)
	}

	// Decode the dictionary values based on the physical type and encoding
	dictHeader := pageHeader.DictionaryPageHeader
	if dictHeader == nil {
		return nil, fmt.Errorf("missing dictionary page header")
	}

	// Dictionary pages typically use PLAIN or PLAIN_DICTIONARY encoding
	if dictHeader.Encoding != parquet.Encoding_PLAIN && dictHeader.Encoding != parquet.Encoding_PLAIN_DICTIONARY {
		return nil, fmt.Errorf("unsupported encoding for dictionary: %v", dictHeader.Encoding)
	}

	numValues := dictHeader.NumValues
	bytesReader := bytes.NewReader(uncompressedData)
	values, err := encoding.ReadPlain(bytesReader, meta.Type, uint64(numValues), 0)
	if err != nil {
		return nil, fmt.Errorf("failed to decode dictionary values: %w", err)
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
