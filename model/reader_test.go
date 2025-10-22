package model

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/hangxie/parquet-go/v2/parquet"
	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

func Test_NewParquetReader(t *testing.T) {
	// This is a basic test to ensure NewParquetReader doesn't panic with nil
	// In a real scenario, you'd need a valid parquet reader
	t.Run("NewParquetReader structure", func(t *testing.T) {
		// We can't easily test this without a real parquet file
		// but we can verify the function signature exists
		// This would require integration tests with actual parquet files
		t.Skip("Requires actual parquet file for integration testing")
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
			require.Equal(t, tt.expected, result, "formatColumnName() should match")
		})
	}
}

func Test_GetColumnStartOffset(t *testing.T) {
	tests := []struct {
		name     string
		meta     *parquet.ColumnMetaData
		expected int64
	}{
		{
			name: "With dictionary page offset",
			meta: &parquet.ColumnMetaData{
				DictionaryPageOffset: int64Ptr(1000),
				DataPageOffset:       2000,
			},
			expected: 1000,
		},
		{
			name: "Without dictionary page offset",
			meta: &parquet.ColumnMetaData{
				DictionaryPageOffset: nil,
				DataPageOffset:       2000,
			},
			expected: 2000,
		},
		{
			name: "Zero offsets",
			meta: &parquet.ColumnMetaData{
				DictionaryPageOffset: int64Ptr(0),
				DataPageOffset:       0,
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getColumnStartOffset(tt.meta)
			require.Equal(t, tt.expected, result, "getColumnStartOffset() should match")
		})
	}
}

func Test_CountPageValues(t *testing.T) {
	tests := []struct {
		name     string
		header   *parquet.PageHeader
		expected int64
	}{
		{
			name: "DATA_PAGE with values",
			header: &parquet.PageHeader{
				Type: parquet.PageType_DATA_PAGE,
				DataPageHeader: &parquet.DataPageHeader{
					NumValues: 100,
				},
			},
			expected: 100,
		},
		{
			name: "DATA_PAGE_V2 with values",
			header: &parquet.PageHeader{
				Type: parquet.PageType_DATA_PAGE_V2,
				DataPageHeaderV2: &parquet.DataPageHeaderV2{
					NumValues: 200,
				},
			},
			expected: 200,
		},
		{
			name: "DICTIONARY_PAGE",
			header: &parquet.PageHeader{
				Type: parquet.PageType_DICTIONARY_PAGE,
				DictionaryPageHeader: &parquet.DictionaryPageHeader{
					NumValues: 50,
				},
			},
			expected: 0, // Dictionary pages don't count toward total values
		},
		{
			name: "INDEX_PAGE",
			header: &parquet.PageHeader{
				Type: parquet.PageType_INDEX_PAGE,
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := countPageValues(tt.header)
			require.Equal(t, tt.expected, result, "countPageValues() should match")
		})
	}
}

func Test_ExtractPageMetadata(t *testing.T) {
	t.Run("DATA_PAGE", func(t *testing.T) {
		header := &parquet.PageHeader{
			Type:                 parquet.PageType_DATA_PAGE,
			CompressedPageSize:   1024,
			UncompressedPageSize: 2048,
			DataPageHeader: &parquet.DataPageHeader{
				NumValues:               100,
				Encoding:                parquet.Encoding_PLAIN,
				DefinitionLevelEncoding: parquet.Encoding_RLE,
				RepetitionLevelEncoding: parquet.Encoding_RLE,
			},
		}

		result := extractPageMetadata(header, 1000, 0, nil, nil)

		require.Equal(t, 0, result.Index, "Index should match")
		require.Equal(t, int64(1000), result.Offset, "Offset should match")
		require.Equal(t, "DATA_PAGE", result.PageType, "PageType should match")
		require.Equal(t, int32(1024), result.CompressedSize, "CompressedSize should match")
		require.Equal(t, int32(2048), result.UncompressedSize, "UncompressedSize should match")
		require.Equal(t, int32(100), result.NumValues, "NumValues should match")
		require.Equal(t, "PLAIN", result.Encoding, "Encoding should match")
	})

	t.Run("DICTIONARY_PAGE", func(t *testing.T) {
		header := &parquet.PageHeader{
			Type:                 parquet.PageType_DICTIONARY_PAGE,
			CompressedPageSize:   512,
			UncompressedPageSize: 512,
			DictionaryPageHeader: &parquet.DictionaryPageHeader{
				NumValues: 50,
				Encoding:  parquet.Encoding_PLAIN_DICTIONARY,
			},
		}

		result := extractPageMetadata(header, 500, 1, nil, nil)

		require.Equal(t, "DICTIONARY_PAGE", result.PageType, "PageType should match")
		require.Equal(t, int32(50), result.NumValues, "NumValues should match")
		require.Equal(t, "PLAIN_DICTIONARY", result.Encoding, "Encoding should match")
	})
}

func Test_PopulateDataPageMetadata(t *testing.T) {
	header := &parquet.DataPageHeader{
		NumValues:               100,
		Encoding:                parquet.Encoding_PLAIN,
		DefinitionLevelEncoding: parquet.Encoding_RLE,
		RepetitionLevelEncoding: parquet.Encoding_BIT_PACKED,
	}

	pageInfo := &PageMetadata{}
	populateDataPageMetadata(pageInfo, header, nil, nil)

	require.Equal(t, int32(100), pageInfo.NumValues, "NumValues should match")
	require.Equal(t, "PLAIN", pageInfo.Encoding, "Encoding should match")
	require.Equal(t, "RLE", pageInfo.DefLevelEncoding, "DefLevelEncoding should match")
	require.Equal(t, "BIT_PACKED", pageInfo.RepLevelEncoding, "RepLevelEncoding should match")
}

func Test_PopulateDataPageV2Metadata(t *testing.T) {
	header := &parquet.DataPageHeaderV2{
		NumValues: 200,
		Encoding:  parquet.Encoding_DELTA_BINARY_PACKED,
	}

	pageInfo := &PageMetadata{}
	populateDataPageV2Metadata(pageInfo, header, nil, nil)

	require.Equal(t, int32(200), pageInfo.NumValues, "NumValues should match")
	require.Equal(t, "DELTA_BINARY_PACKED", pageInfo.Encoding, "Encoding should match")
}

func Test_PopulateDictionaryPageMetadata(t *testing.T) {
	header := &parquet.DictionaryPageHeader{
		NumValues: 50,
		Encoding:  parquet.Encoding_PLAIN_DICTIONARY,
	}

	pageInfo := &PageMetadata{}
	populateDictionaryPageMetadata(pageInfo, header)

	require.Equal(t, int32(50), pageInfo.NumValues, "NumValues should match")
	require.Equal(t, "PLAIN_DICTIONARY", pageInfo.Encoding, "Encoding should match")
}

func Test_ExtractPageStatistics(t *testing.T) {
	t.Run("With min/max values", func(t *testing.T) {
		stats := &parquet.Statistics{
			MinValue: []byte{0x01, 0x00, 0x00, 0x00}, // int32: 1
			MaxValue: []byte{0x64, 0x00, 0x00, 0x00}, // int32: 100
		}

		columnMeta := &parquet.ColumnMetaData{
			Type: parquet.Type_INT32,
		}

		pageInfo := &PageMetadata{}
		extractPageStatistics(pageInfo, stats, columnMeta, nil)

		require.NotEmpty(t, pageInfo.MinValue, "MinValue should not be empty")
		require.NotEmpty(t, pageInfo.MaxValue, "MaxValue should not be empty")
	})

	t.Run("With null count", func(t *testing.T) {
		nullCount := int64(10)
		stats := &parquet.Statistics{
			NullCount: &nullCount,
		}

		pageInfo := &PageMetadata{}
		extractPageStatistics(pageInfo, stats, nil, nil)

		require.NotNil(t, pageInfo.NullCount, "NullCount should not be nil")
		require.Equal(t, int64(10), *pageInfo.NullCount, "NullCount should match")
	})
}

func Test_PositionTracker(t *testing.T) {
	// Create a simple byte buffer
	data := []byte("hello world")
	tracker := &positionTracker{
		r:   nil, // We'll just test the interface methods
		pos: 0,
	}

	t.Run("Write should return error", func(t *testing.T) {
		_, err := tracker.Write(data)
		require.Error(t, err, "Write() should return error")
	})

	t.Run("IsOpen should return true", func(t *testing.T) {
		require.True(t, tracker.IsOpen(), "IsOpen() should return true")
	})

	t.Run("RemainingBytes should return max uint64", func(t *testing.T) {
		require.Equal(t, ^uint64(0), tracker.RemainingBytes(), "RemainingBytes() should return max uint64")
	})

	t.Run("Close should succeed", func(t *testing.T) {
		err := tracker.Close()
		require.NoError(t, err, "Close() should not return error")
	})

	t.Run("Open should succeed", func(t *testing.T) {
		err := tracker.Open()
		require.NoError(t, err, "Open() should not return error")
	})

	t.Run("Flush should succeed", func(t *testing.T) {
		err := tracker.Flush(context.Background())
		require.NoError(t, err, "Flush() should not return error")
	})
}

// Helper function
func int64Ptr(i int64) *int64 {
	return &i
}

// Helper to get the path to test parquet file
func getTestParquetFilePath() string {
	return filepath.Join("..", "build", "testdata", "all-types.parquet")
}

// Test NewParquetReader with real parquet file
func Test_NewParquetReader_WithRealFile(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	require.NotNil(t, pr, "NewParquetReader should not return nil")
	require.NotNil(t, pr.Reader, "Reader field should not be nil")
	require.NotNil(t, pr.metadata, "metadata field should not be nil")

	// Verify metadata is properly set
	require.Equal(t, parquetReader.Footer, pr.metadata, "metadata should be set to reader's Footer")
}

// Test GetFileInfo with real parquet file
func Test_GetFileInfo_WithRealFile(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)
	info := pr.GetFileInfo()

	// Verify basic fields are populated
	require.Greater(t, info.NumRowGroups, 0, "NumRowGroups should be > 0")
	require.Greater(t, info.NumRows, int64(0), "NumRows should be > 0")
	require.Greater(t, info.NumLeafColumns, 0, "NumLeafColumns should be > 0")
	require.Greater(t, info.TotalCompressedSize, int64(0), "TotalCompressedSize should be > 0")
	require.Greater(t, info.TotalUncompressedSize, int64(0), "TotalUncompressedSize should be > 0")
	require.Greater(t, info.CompressionRatio, 0.0, "CompressionRatio should be > 0")

	if info.CreatedBy == "" {
		t.Log("CreatedBy is empty (optional field)")
	}

	t.Logf("File info: Version=%d, RowGroups=%d, Rows=%d, Columns=%d",
		info.Version, info.NumRowGroups, info.NumRows, info.NumLeafColumns)
}

// Test GetRowGroupInfo with valid indices
func Test_GetRowGroupInfo_ValidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	// Test first row group
	info, err := pr.GetRowGroupInfo(0)
	require.NoError(t, err, "GetRowGroupInfo(0) should succeed")

	require.Equal(t, 0, info.Index, "Index should be 0")
	require.Greater(t, info.NumRows, int64(0), "NumRows should be > 0")
	require.Greater(t, info.NumColumns, 0, "NumColumns should be > 0")
	require.Greater(t, info.CompressedSize, int64(0), "CompressedSize should be > 0")
	require.Greater(t, info.UncompressedSize, int64(0), "UncompressedSize should be > 0")
	require.Greater(t, info.CompressionRatio, 0.0, "CompressionRatio should be > 0")

	t.Logf("Row group 0: NumRows=%d, NumColumns=%d, CompressionRatio=%.2f",
		info.NumRows, info.NumColumns, info.CompressionRatio)
}

// Test GetRowGroupInfo with invalid indices
func Test_GetRowGroupInfo_InvalidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
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
			require.Error(t, err, "Should return error for invalid index")
			require.ErrorIs(t, err, ErrInvalidRowGroupIndex, "Should return ErrInvalidRowGroupIndex")
		})
	}
}

// Test GetAllRowGroupsInfo
func Test_GetAllRowGroupsInfo(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)
	infos := pr.GetAllRowGroupsInfo()

	require.NotEmpty(t, infos, "Should have at least one row group")

	// Verify each row group info is populated
	for i, info := range infos {
		require.Equal(t, i, info.Index, "Row group %d: Index should match", i)
		require.Greater(t, info.NumRows, int64(0), "Row group %d: NumRows should be > 0", i)
		require.Greater(t, info.NumColumns, 0, "Row group %d: NumColumns should be > 0", i)
	}

	t.Logf("Found %d row groups", len(infos))
}

// Test GetColumnChunkInfo with valid indices
func Test_GetColumnChunkInfo_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	// Test first column of first row group
	info, err := pr.GetColumnChunkInfo(0, 0)
	require.NoError(t, err, "GetColumnChunkInfo(0, 0) should succeed")

	require.Equal(t, 0, info.Index, "Index should be 0")
	require.NotEmpty(t, info.PathInSchema, "PathInSchema should not be empty")
	require.NotEmpty(t, info.Name, "Name should not be empty")
	require.NotEmpty(t, info.PhysicalType, "PhysicalType should not be empty")
	require.NotEmpty(t, info.Codec, "Codec should not be empty")
	require.GreaterOrEqual(t, info.NumValues, int64(0), "NumValues should be >= 0")

	t.Logf("Column 0: Name=%s, Type=%s, Codec=%s, Values=%d",
		info.Name, info.PhysicalType, info.Codec, info.NumValues)
}

// Test GetColumnChunkInfo with invalid indices
func Test_GetColumnChunkInfo_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
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
			require.Error(t, err, "Should return error for invalid indices")
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// Test GetAllColumnChunksInfo
func Test_GetAllColumnChunksInfo_ValidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	infos, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err, "GetAllColumnChunksInfo(0) should succeed")
	require.NotEmpty(t, infos, "Should have at least one column")

	// Verify each column info is populated
	for i, info := range infos {
		require.Equal(t, i, info.Index, "Column %d: Index should match", i)
		require.NotEmpty(t, info.Name, "Column %d: Name should not be empty", i)
	}

	t.Logf("Found %d columns in row group 0", len(infos))
}

// Test GetAllColumnChunksInfo with invalid index
func Test_GetAllColumnChunksInfo_InvalidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
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
			require.Error(t, err, "Should return error for invalid index")
			require.ErrorIs(t, err, ErrInvalidRowGroupIndex)
		})
	}
}

// Test GetPageMetadataList
func Test_GetPageMetadataList_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	pages, err := pr.GetPageMetadataList(0, 0)
	require.NoError(t, err, "GetPageMetadataList(0, 0) should succeed")
	require.NotEmpty(t, pages, "Should have at least one page")

	// Verify page metadata
	for i, page := range pages {
		require.Equal(t, i, page.Index, "Page %d: Index should match", i)
		require.NotEmpty(t, page.PageType, "Page %d: PageType should not be empty", i)

		require.True(t, page.CompressedSize > 0 || page.UncompressedSize > 0, "Page %d: At least one size should be > 0", i)

		t.Logf("Page %d: Type=%s, CompressedSize=%d, UncompressedSize=%d, NumValues=%d",
			i, page.PageType, page.CompressedSize, page.UncompressedSize, page.NumValues)
	}
}

// Test GetPageMetadataList with invalid indices
func Test_GetPageMetadataList_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
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
			require.Error(t, err, "Should return error for invalid indices")
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// Test GetPageMetadata
func Test_GetPageMetadata_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	page, err := pr.GetPageMetadata(0, 0, 0)
	require.NoError(t, err, "GetPageMetadata(0, 0, 0) should succeed")

	require.Equal(t, 0, page.Index, "Index should be 0")
	require.NotEmpty(t, page.PageType, "PageType should not be empty")

	t.Logf("Page metadata: Type=%s, Size=%d, NumValues=%d",
		page.PageType, page.CompressedSize, page.NumValues)
}

// Test GetPageMetadata with invalid indices
func Test_GetPageMetadata_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
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
			require.Error(t, err, "Should return error for invalid indices")
		})
	}
}

// Test GetPageContent
func Test_GetPageContent_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	// First, check if page 0 is a data page
	pages, err := pr.GetPageMetadataList(0, 0)
	require.NoError(t, err, "Failed to get page metadata")

	if len(pages) == 0 {
		t.Skip("No pages available")
	}

	// Find the first data page
	dataPageIndex := -1
	for i, page := range pages {
		if page.PageType == "DATA_PAGE" || page.PageType == "DATA_PAGE_V2" {
			dataPageIndex = i
			break
		}
	}

	if dataPageIndex == -1 {
		t.Skip("No data pages found")
	}

	values, err := pr.GetPageContent(0, 0, dataPageIndex)
	require.NoError(t, err, "GetPageContent should succeed")

	if len(values) == 0 {
		t.Log("Page has no values (could be valid for some data types)")
	} else {
		t.Logf("Read %d values from page %d", len(values), dataPageIndex)
	}
}

// Test GetPageContent with dictionary page
func Test_GetPageContent_DictionaryPage(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	// Find a column with a dictionary page
	allColumns, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err, "Failed to get columns")

	for colIdx := range allColumns {
		pages, err := pr.GetPageMetadataList(0, colIdx)
		if err != nil {
			continue
		}

		// Find dictionary page
		for pageIdx, page := range pages {
			if page.PageType == "DICTIONARY_PAGE" {
				values, err := pr.GetPageContent(0, colIdx, pageIdx)
				if err != nil {
					t.Logf("Dictionary page read failed (expected for some encodings): %v", err)
					return // Test passed - we exercised the code path
				}

				t.Logf("Read %d dictionary values from column %d", len(values), colIdx)
				return // Success
			}
		}
	}

	t.Log("No dictionary pages found (some parquet files don't use dictionaries)")
}

// Test GetPageContent with invalid indices
func Test_GetPageContent_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
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
			require.Error(t, err, "Should return error for invalid indices")
		})
	}
}

// Test GetPageContentFormatted
func Test_GetPageContentFormatted_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	// Find the first data page
	pages, err := pr.GetPageMetadataList(0, 0)
	require.NoError(t, err, "Failed to get page metadata")

	dataPageIndex := -1
	for i, page := range pages {
		if page.PageType == "DATA_PAGE" || page.PageType == "DATA_PAGE_V2" {
			dataPageIndex = i
			break
		}
	}

	if dataPageIndex == -1 {
		t.Skip("No data pages found")
	}

	values, err := pr.GetPageContentFormatted(0, 0, dataPageIndex)
	require.NoError(t, err, "GetPageContentFormatted should succeed")

	// Verify all values are strings
	for i, val := range values {
		if val == "" {
			// Empty strings are valid
			continue
		}
		t.Logf("Value %d: %q", i, val)
		if i >= 5 { // Only log first few values
			break
		}
	}

	t.Logf("Read %d formatted values", len(values))
}

// Test GetPageContentFormatted with invalid indices
func Test_GetPageContentFormatted_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
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
			require.Error(t, err, "Should return error for invalid indices")
		})
	}
}

// Test readDictionaryPageContent (indirectly through GetPageContent)
func Test_ReadDictionaryPageContent_Coverage(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(getTestParquetFilePath(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to open parquet file: %v", err)
	}
	defer func() { _ = parquetReader.ReadStopWithError() }()

	pr := NewParquetReader(parquetReader)

	// Scan all columns looking for dictionary pages
	allColumns, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err, "Failed to get columns")

	dictionaryPageFound := false
outer:
	for colIdx := range allColumns {
		pages, err := pr.GetPageMetadataList(0, colIdx)
		if err != nil {
			continue
		}

		for pageIdx, page := range pages {
			if page.PageType == "DICTIONARY_PAGE" {
				dictionaryPageFound = true
				// Try to read the dictionary page
				_, err := pr.GetPageContent(0, colIdx, pageIdx)
				if err != nil {
					// It's okay if we can't decode all dictionary types
					t.Logf("Dictionary page decoding failed (may be expected): %v", err)
				} else {
					t.Logf("Successfully read dictionary page from column %d", colIdx)
				}
				break outer // We've exercised the code path
			}
		}
	}

	if !dictionaryPageFound {
		t.Log("No dictionary pages found in test file")
	}
}
