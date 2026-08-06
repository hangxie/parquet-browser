package model

import (
	"context"
	"path/filepath"
	"testing"

	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

func Test_NewParquetReader(t *testing.T) {
	t.Run("Opens and reads parquet file", func(t *testing.T) {
		// Open a test parquet file using parquet-tools helper
		pr, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
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
		pr, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
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
		pr, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
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
		pr, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
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

func Test_FormatColumnName(t *testing.T) {
	tests := []struct {
		name     string
		path     []string
		expected string
	}{
		{
			name:     "Simple path",
			path:     []string{"column1"},
			expected: "column1",
		},
		{
			name:     "Nested path",
			path:     []string{"parent", "child"},
			expected: "parent.child",
		},
		{
			name:     "Deep nesting",
			path:     []string{"a", "b", "c", "d"},
			expected: "a.b.c.d",
		},
		{
			name:     "Empty path",
			path:     []string{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatColumnName(tt.path)
			require.Equal(t, tt.expected, result)
		})
	}
}

func getTestParquetFilePath() string {
	return filepath.Join("..", "build", "testdata", "all-types.parquet")
}

// Test NewParquetReader with real parquet file
func Test_NewParquetReader_WithRealFile(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
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
	parquetReader, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
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
	parquetReader, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
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
	parquetReader, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
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
	parquetReader, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
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

// Test GetColumnChunkInfo with valid indices
func Test_GetColumnChunkInfo_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	// Test first column of first row group
	info, err := pr.GetColumnChunkInfo(0, 0)
	require.NoError(t, err)

	require.Equal(t, 0, info.Index)
	require.NotEmpty(t, info.PathInSchema)
	require.NotEmpty(t, info.Name)
	require.NotEmpty(t, info.PhysicalType)
	require.NotEmpty(t, info.Codec)
	require.GreaterOrEqual(t, info.NumValues, int64(0))
}

// Test GetColumnChunkInfo with invalid indices
func Test_GetColumnChunkInfo_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	tests := []struct {
		name     string
		rgIndex  int
		colIndex int
		wantErr  error
	}{
		{"Invalid row group", -1, 0, ErrInvalidRowGroupIndex},
		{"Row group out of bounds", 9999, 0, ErrInvalidRowGroupIndex},
		{"Invalid column", 0, -1, ErrInvalidColumnIndex},
		{"Column out of bounds", 0, 9999, ErrInvalidColumnIndex},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pr.GetColumnChunkInfo(tt.rgIndex, tt.colIndex)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// Test GetAllColumnChunksInfo
func Test_GetAllColumnChunksInfo_ValidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	infos, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err)
	require.NotEmpty(t, infos)

	// Verify each column info is populated
	for i, info := range infos {
		require.Equal(t, i, info.Index)
		require.NotEmpty(t, info.Name)
	}
}

// Test GetAllColumnChunksInfo with invalid index
func Test_GetAllColumnChunksInfo_InvalidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	tests := []struct {
		name    string
		rgIndex int
	}{
		{"Negative index", -1},
		{"Out of bounds", 9999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pr.GetAllColumnChunksInfo(tt.rgIndex)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidRowGroupIndex)
		})
	}
}

// Test GetPageMetadataList
