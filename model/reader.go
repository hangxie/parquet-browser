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
	// Encryption is "" when the file is not encrypted, otherwise one of
	// "FOOTER_KEY", "COLUMN_KEY", or "MIXED" (some columns use the footer
	// key, some use per-column keys).
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

// ParquetReader wraps the parquet-go reader with utility methods. That reader is
// not goroutine-safe, so sem (a 1-slot semaphore, see reader_lock.go) serializes
// every access to it; metadata-only accessors need no lock.
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

// The reader-lock methods (acquire, release, WithLock) and Close live in
// reader_lock.go.

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

	info.Encryption = pr.detectEncryptionMode()

	return info
}

// detectEncryptionMode inspects file-level crypto metadata and per-column
// crypto metadata to classify the file's encryption mode after parquet-go has
// opened the footer. Plaintext-footer files with encrypted columns can be
// classified without column keys, but encrypted-footer files require the footer
// key before NewParquetReader is constructed.
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
		// File-level encryption present but no per-column metadata: this is
		// uniform footer-key encryption.
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
