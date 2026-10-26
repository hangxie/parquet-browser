package model

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

func Test_NewParquetReader(t *testing.T) {
	t.Run("Opens and reads parquet file", func(t *testing.T) {
		// Open a test parquet file using parquet-tools helper
		pr, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithContext(context.Background()) }()

		// Wrap it in our ParquetReader
		parquetReader := NewParquetReader(pr)
		require.NotNil(t, parquetReader)
		require.NotNil(t, parquetReader.Reader)
		require.NotNil(t, parquetReader.metadata)

		// Test GetFileInfo
		fileInfo := parquetReader.GetFileInfo()
		require.Greater(t, fileInfo.NumRows, int64(0))
		require.Greater(t, fileInfo.NumLeafColumns, 0)
		require.Greater(t, fileInfo.NumRowGroups, 0)
	})

	t.Run("Gets row group info", func(t *testing.T) {
		pr, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithContext(context.Background()) }()

		parquetReader := NewParquetReader(pr)

		// Test GetRowGroupInfo
		rgInfo, err := parquetReader.GetRowGroupInfo(0)
		require.NoError(t, err)
		require.Equal(t, 0, rgInfo.Index)
		require.Greater(t, rgInfo.NumRows, int64(0))
		require.Greater(t, rgInfo.NumColumns, 0)
	})

	t.Run("Gets column chunk info", func(t *testing.T) {
		pr, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithContext(context.Background()) }()

		parquetReader := NewParquetReader(pr)

		// Test GetColumnChunkInfo
		colInfo, err := parquetReader.GetColumnChunkInfo(0, 0)
		require.NoError(t, err)
		require.Equal(t, 0, colInfo.Index)
		require.NotEmpty(t, colInfo.Name)
		require.NotEmpty(t, colInfo.PhysicalType)
	})

	t.Run("Gets page metadata", func(t *testing.T) {
		pr, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithContext(context.Background()) }()

		parquetReader := NewParquetReader(pr)

		// Test GetPageMetadataList
		pages, err := parquetReader.GetPageMetadataList(context.Background(), 0, 0)
		require.NoError(t, err)
		require.Greater(t, len(pages), 0)

		// Verify page metadata structure
		firstPage := pages[0]
		require.GreaterOrEqual(t, firstPage.Index, 0)
		require.NotEmpty(t, firstPage.PageType)
	})
}

// fixturePath returns a fixture downloaded by `make test`, failing the test when it is missing.
func fixturePath(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join("..", "build", "testdata", name)
	_, err := os.Stat(path)
	require.NoError(t, err, "missing fixture; run `make test` to download it")
	return path
}

// Test NewParquetReader with real parquet file
func Test_NewParquetReader_WithRealFile(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	require.NotNil(t, pr)
	require.NotNil(t, pr.Reader)
	require.NotNil(t, pr.metadata)

	// Verify metadata is properly set
	require.Equal(t, parquetReader.Footer, pr.metadata)
}

// Test GetFileInfo with real parquet file
func Test_GetFileInfo_WithRealFile(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)
	info := pr.GetFileInfo()

	// Verify basic fields are populated
	require.Greater(t, info.NumRowGroups, 0)
	require.Greater(t, info.NumRows, int64(0))
	require.Greater(t, info.NumLeafColumns, 0)
	require.Greater(t, info.TotalCompressedSize, int64(0))
	require.Greater(t, info.TotalUncompressedSize, int64(0))
	require.Greater(t, info.CompressionRatio, 0.0)
	require.NotEqual(t, "", info.CreatedBy)
	// The bundled fixture is not encrypted.
	require.Empty(t, info.Encryption)
}

// Test GetRowGroupInfo with valid indices
func Test_GetRowGroupInfo_ValidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	// Test first row group
	info, err := pr.GetRowGroupInfo(0)
	require.NoError(t, err)

	require.Equal(t, 0, info.Index)
	require.Greater(t, info.NumRows, int64(0))
	require.Greater(t, info.NumColumns, 0)
	require.Greater(t, info.CompressedSize, int64(0))
	require.Greater(t, info.UncompressedSize, int64(0))
	require.Greater(t, info.CompressionRatio, 0.0)
}

// Test GetRowGroupInfo with invalid indices
func Test_GetRowGroupInfo_InvalidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	tests := []struct {
		name  string
		index int
	}{
		{"Negative index", -1},
		{"Out of bounds", 9999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pr.GetRowGroupInfo(tt.index)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidRowGroupIndex)
		})
	}
}

// Test GetAllRowGroupsInfo
func Test_GetAllRowGroupsInfo(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)
	infos := pr.GetAllRowGroupsInfo()

	require.NotEmpty(t, infos)

	// Verify each row group info is populated
	for i, info := range infos {
		require.Equal(t, i, info.Index)
		require.Greater(t, info.NumRows, int64(0))
		require.Greater(t, info.NumColumns, 0)
	}
}

// Test GetPageMetadataList

// openFixture opens a fixture and closes it when the test ends.
func openFixture(t *testing.T, name string) *ParquetReader {
	t.Helper()
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, name), pio.ReadOption{})
	require.NoError(t, err)

	pr := NewParquetReader(parquetReader)
	t.Cleanup(func() { _ = pr.Close() })
	return pr
}

// Files with a corrupt footer used to open and expose garbage metadata; they are
// now rejected while the footer is read.
func Test_NewParquetFileReader_CorruptFooter(t *testing.T) {
	tests := []struct {
		file    string
		wantErr string
	}{
		{file: "bad-footer-magic.parquet", wantErr: `invalid footer magic "XXXX"`},
		{file: "negative-num-rows.parquet", wantErr: "num_rows -1 is negative"},
	}

	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, tt.file), pio.ReadOption{})
			require.Error(t, err)
			require.Nil(t, parquetReader)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// A schema without leaf columns still has rows; it must open and report an
// empty column list rather than failing.
func Test_ZeroColumnFile(t *testing.T) {
	pr := openFixture(t, "zero-column.parquet")

	info := pr.GetFileInfo()
	require.Equal(t, 1, info.NumRowGroups)
	require.Equal(t, int64(3), info.NumRows)
	require.Zero(t, info.NumLeafColumns)
	require.Zero(t, info.TotalCompressedSize)
	require.Zero(t, info.TotalUncompressedSize)
	require.Zero(t, info.CompressionRatio)

	rowGroups := pr.GetAllRowGroupsInfo()
	require.Len(t, rowGroups, 1)
	require.Equal(t, int64(3), rowGroups[0].NumRows)
	require.Zero(t, rowGroups[0].NumColumns)
	require.Zero(t, rowGroups[0].CompressionRatio)

	columns, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err)
	require.Empty(t, columns)

	_, err = pr.GetColumnChunkInfo(0, 0)
	require.ErrorIs(t, err, ErrInvalidColumnIndex)

	_, err = pr.GetPageMetadataList(context.Background(), 0, 0)
	require.ErrorIs(t, err, ErrInvalidColumnIndex)

	_, err = pr.GetPageContentFormatted(context.Background(), 0, 0, 0)
	require.ErrorIs(t, err, ErrInvalidColumnIndex)
}
