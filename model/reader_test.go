package model

import (
	"path/filepath"
	"testing"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

func Test_NewParquetReader(t *testing.T) {
	t.Run("Opens and reads parquet file", func(t *testing.T) {
		// Open a test parquet file using parquet-tools helper
		pr, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

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
		pr, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		parquetReader := NewParquetReader(pr)

		// Test GetRowGroupInfo
		rgInfo, err := parquetReader.GetRowGroupInfo(0)
		require.NoError(t, err)
		require.Equal(t, 0, rgInfo.Index)
		require.Greater(t, rgInfo.NumRows, int64(0))
		require.Greater(t, rgInfo.NumColumns, 0)
	})

	t.Run("Gets column chunk info", func(t *testing.T) {
		pr, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		parquetReader := NewParquetReader(pr)

		// Test GetColumnChunkInfo
		colInfo, err := parquetReader.GetColumnChunkInfo(0, 0)
		require.NoError(t, err)
		require.Equal(t, 0, colInfo.Index)
		require.NotEmpty(t, colInfo.Name)
		require.NotEmpty(t, colInfo.PhysicalType)
	})

	t.Run("Gets page metadata", func(t *testing.T) {
		pr, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		parquetReader := NewParquetReader(pr)

		// Test GetPageMetadataList
		pages, err := parquetReader.GetPageMetadataList(0, 0)
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

func Test_ConvertPageHeaderInfoToMetadata(t *testing.T) {
	t.Run("DATA_PAGE", func(t *testing.T) {
		headerInfo := reader.PageHeaderInfo{
			Index:            0,
			Offset:           1000,
			PageType:         parquet.PageType_DATA_PAGE,
			CompressedSize:   1024,
			UncompressedSize: 2048,
			NumValues:        100,
			Encoding:         parquet.Encoding_PLAIN,
			DefLevelEncoding: parquet.Encoding_RLE,
			RepLevelEncoding: parquet.Encoding_RLE,
		}

		result := convertPageHeaderInfoToMetadata(headerInfo, nil, nil)

		require.Equal(t, 0, result.Index)
		require.Equal(t, int64(1000), result.Offset)
		require.Equal(t, "DATA_PAGE", result.PageType)
		require.Equal(t, int32(1024), result.CompressedSize)
		require.Equal(t, int32(2048), result.UncompressedSize)
		require.Equal(t, int32(100), result.NumValues)
		require.Equal(t, "PLAIN", result.Encoding)
	})

	t.Run("DATA_PAGE_V2", func(t *testing.T) {
		headerInfo := reader.PageHeaderInfo{
			Index:            1,
			Offset:           2000,
			PageType:         parquet.PageType_DATA_PAGE_V2,
			CompressedSize:   1024,
			UncompressedSize: 2048,
			NumValues:        200,
			Encoding:         parquet.Encoding_DELTA_BINARY_PACKED,
		}

		result := convertPageHeaderInfoToMetadata(headerInfo, nil, nil)

		require.Equal(t, 1, result.Index)
		require.Equal(t, "DATA_PAGE_V2", result.PageType)
		require.Equal(t, int32(200), result.NumValues)
		require.Equal(t, "DELTA_BINARY_PACKED", result.Encoding)
	})

	t.Run("DICTIONARY_PAGE", func(t *testing.T) {
		headerInfo := reader.PageHeaderInfo{
			Index:            1,
			Offset:           500,
			PageType:         parquet.PageType_DICTIONARY_PAGE,
			CompressedSize:   512,
			UncompressedSize: 512,
			NumValues:        50,
			Encoding:         parquet.Encoding_PLAIN_DICTIONARY,
		}

		result := convertPageHeaderInfoToMetadata(headerInfo, nil, nil)

		require.Equal(t, "DICTIONARY_PAGE", result.PageType)
		require.Equal(t, int32(50), result.NumValues)
		require.Equal(t, "PLAIN_DICTIONARY", result.Encoding)
	})

	t.Run("INDEX_PAGE", func(t *testing.T) {
		headerInfo := reader.PageHeaderInfo{
			Index:            2,
			Offset:           3000,
			PageType:         parquet.PageType_INDEX_PAGE,
			CompressedSize:   256,
			UncompressedSize: 256,
		}

		result := convertPageHeaderInfoToMetadata(headerInfo, nil, nil)

		require.Equal(t, 2, result.Index)
		require.Equal(t, "INDEX_PAGE", result.PageType)
	})
}

func Test_ConvertPageHeaderInfoToMetadataWithStatistics(t *testing.T) {
	t.Run("With statistics", func(t *testing.T) {
		nullCount := int64(10)
		stats := &parquet.Statistics{
			MinValue:  []byte{0x01, 0x00, 0x00, 0x00}, // int32: 1
			MaxValue:  []byte{0x64, 0x00, 0x00, 0x00}, // int32: 100
			NullCount: &nullCount,
		}

		headerInfo := reader.PageHeaderInfo{
			Index:            0,
			Offset:           1000,
			PageType:         parquet.PageType_DATA_PAGE,
			CompressedSize:   1024,
			UncompressedSize: 2048,
			NumValues:        100,
			Encoding:         parquet.Encoding_PLAIN,
			DefLevelEncoding: parquet.Encoding_RLE,
			RepLevelEncoding: parquet.Encoding_RLE,
			HasStatistics:    true,
			Statistics:       stats,
		}

		columnMeta := &parquet.ColumnMetaData{
			Type: parquet.Type_INT32,
		}

		result := convertPageHeaderInfoToMetadata(headerInfo, columnMeta, nil)

		require.Equal(t, int32(100), result.NumValues)
		require.True(t, result.HasStatistics)
		require.NotEmpty(t, result.MinValue)
		require.NotEmpty(t, result.MaxValue)
		require.Equal(t, &nullCount, result.NullCount)
	})

	t.Run("With null count", func(t *testing.T) {
		nullCount := int64(10)
		stats := &parquet.Statistics{
			NullCount: &nullCount,
		}

		headerInfo := reader.PageHeaderInfo{
			PageType:      parquet.PageType_DATA_PAGE,
			Encoding:      parquet.Encoding_PLAIN,
			HasStatistics: true,
			Statistics:    stats,
		}

		result := convertPageHeaderInfoToMetadata(headerInfo, nil, nil)

		require.NotNil(t, result.NullCount)
		require.Equal(t, int64(10), *result.NullCount)
	})
}

// Helper to get the path to test parquet file
func getTestParquetFilePath() string {
	return filepath.Join("..", "build", "testdata", "all-types.parquet")
}

// Test NewParquetReader with real parquet file
func Test_NewParquetReader_WithRealFile(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	require.NotNil(t, pr)
	require.NotNil(t, pr.Reader)
	require.NotNil(t, pr.metadata)

	// Verify metadata is properly set
	require.Equal(t, parquetReader.Footer, pr.metadata)
}

// Test GetFileInfo with real parquet file
func Test_GetFileInfo_WithRealFile(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

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
}

// Test GetRowGroupInfo with valid indices
func Test_GetRowGroupInfo_ValidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

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
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

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
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

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
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

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
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

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
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	infos, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err)
	require.NotEmpty(t, infos)

	// Verify each column info is populated
	for i, info := range infos {
		require.Equal(t, i, info.Index, "Column %d: Index should match", i)
		require.NotEmpty(t, info.Name, "Column %d: Name should not be empty", i)
	}
}

// Test GetAllColumnChunksInfo with invalid index
func Test_GetAllColumnChunksInfo_InvalidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

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
func Test_GetPageMetadataList_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	pages, err := pr.GetPageMetadataList(0, 0)
	require.NoError(t, err)
	require.NotEmpty(t, pages)

	// Verify page metadata
	for i, page := range pages {
		require.Equal(t, i, page.Index)
		require.NotEmpty(t, page.PageType)
		require.True(t, page.CompressedSize > 0 || page.UncompressedSize > 0)
	}
}

// Test GetPageMetadataList with invalid indices
func Test_GetPageMetadataList_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	tests := []struct {
		name     string
		rgIndex  int
		colIndex int
		wantErr  error
	}{
		{"Invalid row group", -1, 0, ErrInvalidRowGroupIndex},
		{"Invalid column", 0, -1, ErrInvalidColumnIndex},
		{"Row group out of bounds", 9999, 0, ErrInvalidRowGroupIndex},
		{"Column out of bounds", 0, 9999, ErrInvalidColumnIndex},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pr.GetPageMetadataList(tt.rgIndex, tt.colIndex)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// Test GetPageMetadata
func Test_GetPageMetadata_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	page, err := pr.GetPageMetadata(0, 0, 0)
	require.NoError(t, err)

	require.Equal(t, 0, page.Index)
	require.NotEmpty(t, page.PageType)
}

// Test GetPageMetadata with invalid indices
func Test_GetPageMetadata_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	tests := []struct {
		name      string
		rgIndex   int
		colIndex  int
		pageIndex int
	}{
		{"Invalid row group", -1, 0, 0},
		{"Invalid column", 0, -1, 0},
		{"Invalid page", 0, 0, -1},
		{"Page out of bounds", 0, 0, 9999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pr.GetPageMetadata(tt.rgIndex, tt.colIndex, tt.pageIndex)
			require.Error(t, err)
		})
	}
}

// Test GetPageContent
func Test_GetPageContent_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	// First, check if page 0 is a data page
	pages, err := pr.GetPageMetadataList(0, 0)
	require.NoError(t, err)
	require.NotEqual(t, 0, len(pages))

	values, err := pr.GetPageContent(0, 0, 1)
	require.NoError(t, err)
	require.NotEqual(t, 0, len(values))
}

// Test GetPageContent with dictionary page
func Test_GetPageContent_DictionaryPage(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	// Find a column with a dictionary page
	allColumns, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err)
	require.Equal(t, 57, len(allColumns))

	pages, err := pr.GetPageMetadataList(0, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(pages))

	values, err := pr.GetPageContent(0, 0, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(values))

	pages, err = pr.GetPageMetadataList(0, 16)
	require.NoError(t, err)
	require.Equal(t, 4, len(pages))

	values, err = pr.GetPageContent(0, 1, 0)
	require.NoError(t, err)
	require.Equal(t, 5, len(values))
}

// Test GetPageContent with invalid indices
func Test_GetPageContent_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	tests := []struct {
		name      string
		rgIndex   int
		colIndex  int
		pageIndex int
	}{
		{"Invalid row group", -1, 0, 0},
		{"Invalid column", 0, -1, 0},
		{"Invalid page", 0, 0, -1},
		{"Page out of bounds", 0, 0, 9999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pr.GetPageContent(tt.rgIndex, tt.colIndex, tt.pageIndex)
			require.Error(t, err)
		})
	}
}

// Test GetPageContentFormatted
func Test_GetPageContentFormatted_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	// Find the first data page
	pages, err := pr.GetPageMetadataList(0, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(pages))

	values, err := pr.GetPageContentFormatted(0, 0, 1)
	require.NoError(t, err)
	require.Equal(t, 2, len(values))
}

// Test GetPageContentFormatted with invalid indices
func Test_GetPageContentFormatted_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	tests := []struct {
		name      string
		rgIndex   int
		colIndex  int
		pageIndex int
	}{
		{"Invalid row group", -1, 0, 0},
		{"Invalid column", 0, -1, 0},
		{"Invalid page", 0, 0, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pr.GetPageContentFormatted(tt.rgIndex, tt.colIndex, tt.pageIndex)
			require.Error(t, err)
		})
	}
}

// Test readDictionaryPageContent (indirectly through GetPageContent)
func Test_ReadDictionaryPageContent_Coverage(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	// Scan all columns looking for dictionary pages
	allColumns, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err)
	require.Equal(t, 57, len(allColumns))

	values, err := pr.GetPageContent(0, 0, 1)
	require.NoError(t, err)
	require.Equal(t, 2, len(values))
}
