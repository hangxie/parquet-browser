package model

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"
	"unsafe"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/golang/snappy"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// getTotalSize gets the total compressed size of a row group
func getTotalSize(rg *parquet.RowGroup) int64 {
	var total int64
	for _, col := range rg.Columns {
		total += col.MetaData.TotalCompressedSize
	}
	return total
}

// countLeafColumns counts only leaf columns (columns with Type field) in the schema
func countLeafColumns(schema []*parquet.SchemaElement) int {
	count := 0
	for _, elem := range schema {
		// Only count elements with Type field set (leaf columns)
		if elem.IsSetType() {
			count++
		}
	}
	return count
}

// formatPathInSchema formats a path in schema for display
func formatPathInSchema(pathInSchema []string) string {
	return strings.Join(pathInSchema, ".")
}

// countLeafColumns counts only leaf columns (columns with Type field) in the schema
// This excludes group nodes like LIST, MAP, and STRUCT which don't have actual data

// formatPathInSchema formats a path in schema for display

// findSchemaElement finds the schema element for a given path
//
//nolint:gocognit // Complex path matching with stack-based tree traversal - inherent complexity
func findSchemaElement(schema []*parquet.SchemaElement, pathInSchema []string) *parquet.SchemaElement {
	if len(pathInSchema) == 0 || len(schema) == 0 {
		return nil
	}

	// The schema is stored as a flat list in depth-first pre-order traversal
	// We need to reconstruct paths to find the correct element

	// Build a stack-based traversal to match the full path
	type stackEntry struct {
		path       []string
		childCount int
	}

	var stack []stackEntry
	var candidates []*parquet.SchemaElement

	for _, elem := range schema {
		// Skip root element
		if elem.Name == "Parquet_go_root" || elem.Name == "" {
			continue
		}

		// Pop completed parent nodes from stack
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			if top.childCount > 0 {
				top.childCount--
				break
			}
			stack = stack[:len(stack)-1]
		}

		// Build current path
		currentPath := make([]string, 0, len(stack)+1)
		for _, entry := range stack {
			currentPath = append(currentPath, entry.path[len(entry.path)-1])
		}
		currentPath = append(currentPath, elem.Name)

		// Check if this matches our target path
		if len(currentPath) == len(pathInSchema) {
			match := true
			for i := range pathInSchema {
				// Case-insensitive match to handle Key_value vs key_value
				if !strings.EqualFold(pathInSchema[i], currentPath[i]) {
					match = false
					break
				}
			}
			if match {
				candidates = append(candidates, elem)
			}
		}

		// Push current element to stack if it has children
		childCount := 0
		if elem.NumChildren != nil {
			childCount = int(*elem.NumChildren)
		}
		if childCount > 0 {
			stack = append(stack, stackEntry{
				path:       currentPath,
				childCount: childCount,
			})
		}
	}

	// Return the first matching candidate
	if len(candidates) > 0 {
		return candidates[0]
	}

	// Fallback: match just the leaf name (for backward compatibility with simple schemas)
	leafName := pathInSchema[len(pathInSchema)-1]
	for _, elem := range schema {
		if strings.EqualFold(elem.Name, leafName) {
			return elem
		}
	}

	return nil
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

// positionTracker wraps a reader and tracks read position
type positionTracker struct {
	r   io.Reader
	pos int64
}

func (p *positionTracker) Read(buf []byte) (n int, err error) {
	n, err = p.r.Read(buf)
	p.pos += int64(n)
	return n, err
}

func (p *positionTracker) Write(buf []byte) (int, error) {
	return 0, fmt.Errorf("write not supported")
}

func (p *positionTracker) Close() error {
	return nil
}

func (p *positionTracker) Flush(ctx context.Context) error {
	return nil
}

func (p *positionTracker) RemainingBytes() uint64 {
	return ^uint64(0) // Unknown
}

func (p *positionTracker) IsOpen() bool {
	return true
}

func (p *positionTracker) Open() error {
	return nil
}

// getColumnStartOffset returns the starting offset for a column's pages
func getColumnStartOffset(meta *parquet.ColumnMetaData) int64 {
	if meta.DictionaryPageOffset != nil {
		return *meta.DictionaryPageOffset
	}
	return meta.DataPageOffset
}

// readSinglePageHeader reads a page header from the given offset
func readSinglePageHeader(pFile io.ReadSeeker, offset int64) (*parquet.PageHeader, int64, error) {
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

// extractPageMetadata creates PageMetadata from a page header
func extractPageMetadata(pageHeader *parquet.PageHeader, offset int64, index int, columnMeta *parquet.ColumnMetaData, schemaElem *parquet.SchemaElement) PageMetadata {
	pageInfo := PageMetadata{
		Index:            index,
		Offset:           offset,
		PageType:         pageHeader.Type.String(),
		CompressedSize:   pageHeader.CompressedPageSize,
		UncompressedSize: pageHeader.UncompressedPageSize,
		HasCRC:           pageHeader.IsSetCrc(),
	}

	// Get page-type specific details
	switch pageHeader.Type {
	case parquet.PageType_DATA_PAGE:
		populateDataPageMetadata(&pageInfo, pageHeader.DataPageHeader, columnMeta, schemaElem)
	case parquet.PageType_DATA_PAGE_V2:
		populateDataPageV2Metadata(&pageInfo, pageHeader.DataPageHeaderV2, columnMeta, schemaElem)
	case parquet.PageType_DICTIONARY_PAGE:
		populateDictionaryPageMetadata(&pageInfo, pageHeader.DictionaryPageHeader)
	case parquet.PageType_INDEX_PAGE:
		pageInfo.NumValues = 0 // Index pages don't have values
	}

	// Format sizes for display
	pageInfo.CompressedSizeFormatted = FormatBytes(int64(pageInfo.CompressedSize))
	pageInfo.UncompressedSizeFormatted = FormatBytes(int64(pageInfo.UncompressedSize))

	// MinValue and MaxValue are already formatted strings at this point
	// Keep the formatted fields for backward compatibility
	pageInfo.MinValueFormatted = pageInfo.MinValue
	pageInfo.MaxValueFormatted = pageInfo.MaxValue

	return pageInfo
}

// populateDataPageMetadata populates metadata for DATA_PAGE type
func populateDataPageMetadata(pageInfo *PageMetadata, header *parquet.DataPageHeader, columnMeta *parquet.ColumnMetaData, schemaElem *parquet.SchemaElement) {
	if header == nil {
		return
	}
	pageInfo.NumValues = header.NumValues
	pageInfo.Encoding = header.Encoding.String()
	pageInfo.DefLevelEncoding = header.DefinitionLevelEncoding.String()
	pageInfo.RepLevelEncoding = header.RepetitionLevelEncoding.String()
	pageInfo.HasStatistics = header.IsSetStatistics()

	if pageInfo.HasStatistics && header.Statistics != nil {
		extractPageStatistics(pageInfo, header.Statistics, columnMeta, schemaElem)
	}
}

// populateDataPageV2Metadata populates metadata for DATA_PAGE_V2 type
func populateDataPageV2Metadata(pageInfo *PageMetadata, header *parquet.DataPageHeaderV2, columnMeta *parquet.ColumnMetaData, schemaElem *parquet.SchemaElement) {
	if header == nil {
		return
	}
	pageInfo.NumValues = header.NumValues
	pageInfo.Encoding = header.Encoding.String()
	pageInfo.HasStatistics = header.IsSetStatistics()

	if pageInfo.HasStatistics && header.Statistics != nil {
		extractPageStatistics(pageInfo, header.Statistics, columnMeta, schemaElem)
	}
}

// populateDictionaryPageMetadata populates metadata for DICTIONARY_PAGE type
func populateDictionaryPageMetadata(pageInfo *PageMetadata, header *parquet.DictionaryPageHeader) {
	if header == nil {
		return
	}
	pageInfo.NumValues = header.NumValues
	pageInfo.Encoding = header.Encoding.String()
}

// extractPageStatistics extracts statistics from page header
func extractPageStatistics(pageInfo *PageMetadata, stats *parquet.Statistics, columnMeta *parquet.ColumnMetaData, schemaElem *parquet.SchemaElement) {
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

	// Extract null count if available
	pageInfo.NullCount = stats.NullCount
}

// countPageValues returns the number of values in a page (only for data pages)
func countPageValues(pageHeader *parquet.PageHeader) int64 {
	// Only data pages count toward total values
	if pageHeader.Type == parquet.PageType_DATA_PAGE && pageHeader.DataPageHeader != nil {
		return int64(pageHeader.DataPageHeader.NumValues)
	}
	if pageHeader.Type == parquet.PageType_DATA_PAGE_V2 && pageHeader.DataPageHeaderV2 != nil {
		return int64(pageHeader.DataPageHeaderV2.NumValues)
	}
	return 0
}

// decompressPageData decompresses page data based on the codec
func decompressPageData(compressedData []byte, codec parquet.CompressionCodec, uncompressedSize int32) ([]byte, error) {
	switch codec {
	case parquet.CompressionCodec_UNCOMPRESSED:
		return compressedData, nil
	case parquet.CompressionCodec_SNAPPY:
		return decompressSnappy(compressedData)
	case parquet.CompressionCodec_GZIP:
		return decompressGzip(compressedData)
	case parquet.CompressionCodec_LZO:
		return nil, fmt.Errorf("LZO compression not supported")
	case parquet.CompressionCodec_BROTLI:
		return decompressBrotli(compressedData)
	case parquet.CompressionCodec_LZ4:
		return decompressLZ4(compressedData, int(uncompressedSize))
	case parquet.CompressionCodec_ZSTD:
		return decompressZstd(compressedData)
	default:
		return nil, fmt.Errorf("unsupported compression codec: %v", codec)
	}
}

// decodeDictionaryValues decodes dictionary values based on physical type and encoding
func decodeDictionaryValues(data []byte, physicalType parquet.Type, encoding parquet.Encoding, numValues int) ([]interface{}, error) {
	// Dictionary pages typically use PLAIN or PLAIN_DICTIONARY encoding
	switch encoding {
	case parquet.Encoding_PLAIN, parquet.Encoding_PLAIN_DICTIONARY:
		return decodePlainValues(data, physicalType, numValues)
	case parquet.Encoding_RLE_DICTIONARY:
		// RLE_DICTIONARY is only for data pages, not dictionary pages
		return nil, fmt.Errorf("RLE_DICTIONARY encoding not valid for dictionary pages")
	default:
		return nil, fmt.Errorf("unsupported encoding for dictionary: %v", encoding)
	}
}

// decodePlainValues decodes PLAIN encoded values
func decodePlainValues(data []byte, physicalType parquet.Type, numValues int) ([]interface{}, error) {
	values := make([]interface{}, 0, numValues)
	offset := 0

	for i := 0; i < numValues && offset < len(data); i++ {
		var value interface{}
		var bytesRead int

		switch physicalType {
		case parquet.Type_BOOLEAN:
			if offset < len(data) {
				value = data[offset]&0x01 != 0
				bytesRead = 1
			}

		case parquet.Type_INT32:
			if offset+4 <= len(data) {
				value = int32(data[offset]) | int32(data[offset+1])<<8 |
					int32(data[offset+2])<<16 | int32(data[offset+3])<<24
				bytesRead = 4
			}

		case parquet.Type_INT64:
			if offset+8 <= len(data) {
				value = int64(data[offset]) | int64(data[offset+1])<<8 |
					int64(data[offset+2])<<16 | int64(data[offset+3])<<24 |
					int64(data[offset+4])<<32 | int64(data[offset+5])<<40 |
					int64(data[offset+6])<<48 | int64(data[offset+7])<<56
				bytesRead = 8
			}

		case parquet.Type_FLOAT:
			if offset+4 <= len(data) {
				bits := uint32(data[offset]) | uint32(data[offset+1])<<8 |
					uint32(data[offset+2])<<16 | uint32(data[offset+3])<<24
				value = *(*float32)(unsafe.Pointer(&bits))
				bytesRead = 4
			}

		case parquet.Type_DOUBLE:
			if offset+8 <= len(data) {
				bits := uint64(data[offset]) | uint64(data[offset+1])<<8 |
					uint64(data[offset+2])<<16 | uint64(data[offset+3])<<24 |
					uint64(data[offset+4])<<32 | uint64(data[offset+5])<<40 |
					uint64(data[offset+6])<<48 | uint64(data[offset+7])<<56
				value = *(*float64)(unsafe.Pointer(&bits))
				bytesRead = 8
			}

		case parquet.Type_BYTE_ARRAY:
			// BYTE_ARRAY: 4-byte length + data
			if offset+4 <= len(data) {
				length := int32(data[offset]) | int32(data[offset+1])<<8 |
					int32(data[offset+2])<<16 | int32(data[offset+3])<<24
				if offset+4+int(length) <= len(data) {
					value = string(data[offset+4 : offset+4+int(length)])
					bytesRead = 4 + int(length)
				}
			}

		case parquet.Type_FIXED_LEN_BYTE_ARRAY:
			// Need type length from schema - for now, treat as raw bytes
			// This would require passing schema element info
			return nil, fmt.Errorf("FIXED_LEN_BYTE_ARRAY decoding requires schema information")

		default:
			return nil, fmt.Errorf("unsupported physical type: %v", physicalType)
		}

		if bytesRead == 0 {
			break // Not enough data
		}

		values = append(values, value)
		offset += bytesRead
	}

	return values, nil
}

// Decompression helper functions

func decompressSnappy(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

func decompressGzip(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	return io.ReadAll(reader)
}

func decompressLZ4(data []byte, uncompressedSize int) ([]byte, error) {
	reader := lz4.NewReader(bytes.NewReader(data))
	result := make([]byte, uncompressedSize)
	_, err := io.ReadFull(reader, result)
	return result, err
}

func decompressZstd(data []byte) ([]byte, error) {
	reader, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func decompressBrotli(data []byte) ([]byte, error) {
	// Brotli support requires additional dependency
	return nil, fmt.Errorf("brotli decompression not implemented")
}
