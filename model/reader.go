package model

import (
	"fmt"
	"sync"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
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
	// "" when unencrypted, otherwise "FOOTER_KEY", "COLUMN_KEY", or "MIXED".
	Encryption string
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

// ParquetReader wraps the parquet-go reader; sem serializes every use of it, metadata reads need no lock.
type ParquetReader struct {
	Reader    *reader.ParquetReader
	metadata  *parquet.FileMetaData
	sem       chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// NewParquetReader creates a new ParquetReader
func NewParquetReader(r *reader.ParquetReader) *ParquetReader {
	return &ParquetReader{
		Reader:   r,
		metadata: r.Footer,
		sem:      make(chan struct{}, 1),
	}
}

// GetFileInfo extracts file-level information
func (pr *ParquetReader) GetFileInfo() FileInfo {
	info := FileInfo{
		Version:      pr.metadata.Version,
		NumRowGroups: len(pr.metadata.RowGroups),
		NumRows:      pr.metadata.NumRows,
	}

	info.NumLeafColumns = countLeafColumns(pr.metadata.Schema)

	for _, rg := range pr.metadata.RowGroups {
		info.TotalUncompressedSize += rg.TotalByteSize
		if rg.IsSetTotalCompressedSize() {
			info.TotalCompressedSize += rg.GetTotalCompressedSize()
			continue
		}

		// total_compressed_size is optional; fall back to the column sums.
		var total int64
		for _, col := range rg.Columns {
			total += col.MetaData.TotalCompressedSize
		}
		info.TotalCompressedSize += total
	}

	if info.TotalCompressedSize > 0 {
		info.CompressionRatio = float64(info.TotalUncompressedSize) / float64(info.TotalCompressedSize)
	}

	if pr.metadata.CreatedBy != nil {
		info.CreatedBy = *pr.metadata.CreatedBy
	}

	info.Encryption = pr.detectEncryptionMode()

	return info
}

// detectEncryptionMode classifies the file's encryption from its file and column crypto metadata.
func (pr *ParquetReader) detectEncryptionMode() string {
	encrypted := (pr.Reader != nil && pr.Reader.FileCrypto != nil) ||
		(pr.metadata != nil && pr.metadata.IsSetEncryptionAlgorithm())

	sawFooter, sawColumn := false, false
	for _, rg := range pr.metadata.RowGroups {
		for _, col := range rg.Columns {
			cm := col.GetCryptoMetadata()
			if cm == nil {
				continue
			}
			if cm.ENCRYPTION_WITH_FOOTER_KEY != nil {
				sawFooter = true
			}
			if cm.ENCRYPTION_WITH_COLUMN_KEY != nil {
				sawColumn = true
			}
		}
	}

	switch {
	case sawFooter && sawColumn:
		return "MIXED"
	case sawColumn:
		return "COLUMN_KEY"
	case sawFooter:
		return "FOOTER_KEY"
	case encrypted:
		// File-level encryption without column metadata means uniform footer-key encryption.
		return "FOOTER_KEY"
	}
	return ""
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

	if rg.IsSetTotalCompressedSize() {
		info.CompressedSize = rg.GetTotalCompressedSize()
	} else {
		// total_compressed_size is optional; fall back to the column sums.
		for _, col := range rg.Columns {
			info.CompressedSize += col.MetaData.TotalCompressedSize
		}
	}

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
