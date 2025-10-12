package cmd

import (
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
	MinValue         []byte
	MaxValue         []byte
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
	MinValue         []byte
	MaxValue         []byte
	NullCount        *int64
}

// ParquetReader wraps the parquet-go reader with utility methods
type ParquetReader struct {
	reader   *reader.ParquetReader
	metadata *parquet.FileMetaData
}

// NewParquetReader creates a new ParquetReader
func NewParquetReader(r *reader.ParquetReader) *ParquetReader {
	return &ParquetReader{
		reader:   r,
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
		info.MinValue = stats.MinValue
		if len(info.MinValue) == 0 {
			info.MinValue = stats.Min
		}
		info.MaxValue = stats.MaxValue
		if len(info.MaxValue) == 0 {
			info.MaxValue = stats.Max
		}
	}

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
