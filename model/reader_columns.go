package model

import (
	"fmt"
	"strings"
)

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
	// Pre-formatted copies kept for existing API clients.
	CompressedSizeFormatted   string `json:"compressedSizeFormatted,omitempty"`
	UncompressedSizeFormatted string `json:"uncompressedSizeFormatted,omitempty"`
	MinValueFormatted         string `json:"minValueFormatted,omitempty"`
	MaxValueFormatted         string `json:"maxValueFormatted,omitempty"`
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

	if info.CompressedSize > 0 {
		info.CompressionRatio = float64(info.UncompressedSize) / float64(info.CompressedSize)
	}

	schemaElem := findSchemaElement(pr.metadata.Schema, meta.PathInSchema)
	if schemaElem != nil {
		info.LogicalType = formatLogicalType(schemaElem.LogicalType)
		info.ConvertedType = "-"
		if schemaElem.ConvertedType != nil {
			info.ConvertedType = schemaElem.ConvertedType.String()
		}
	}

	if meta.Statistics != nil {
		stats := meta.Statistics

		info.NullCount = stats.NullCount

		// Prefer MinValue/MaxValue over the deprecated Min/Max.
		minValueBytes := stats.MinValue
		if len(minValueBytes) == 0 {
			minValueBytes = stats.Min
		}
		maxValueBytes := stats.MaxValue
		if len(maxValueBytes) == 0 {
			maxValueBytes = stats.Max
		}

		info.MinValue = FormatStatValue(minValueBytes, meta, schemaElem)
		info.MaxValue = FormatStatValue(maxValueBytes, meta, schemaElem)
		// Pre-formatted copies kept for existing API clients.
		info.MinValueFormatted = info.MinValue
		info.MaxValueFormatted = info.MaxValue
	}

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

// formatColumnName joins a schema path into a dotted display name
func formatColumnName(pathInSchema []string) string {
	return strings.Join(pathInSchema, ".")
}
