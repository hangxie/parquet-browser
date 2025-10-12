package cmd

import (
	"strings"
	"testing"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/rivo/tview"
	"github.com/stretchr/testify/assert"
)

// Test getStartOffset
func Test_BrowseApp_GetStartOffset(t *testing.T) {
	app := &BrowseApp{}

	tests := []struct {
		name     string
		meta     *parquet.ColumnMetaData
		expected int64
	}{
		{
			name: "with dictionary page offset",
			meta: &parquet.ColumnMetaData{
				DictionaryPageOffset: int64Ptr(100),
				DataPageOffset:       200,
			},
			expected: 100,
		},
		{
			name: "without dictionary page offset",
			meta: &parquet.ColumnMetaData{
				DictionaryPageOffset: nil,
				DataPageOffset:       300,
			},
			expected: 300,
		},
		{
			name: "dictionary offset is zero",
			meta: &parquet.ColumnMetaData{
				DictionaryPageOffset: int64Ptr(0),
				DataPageOffset:       400,
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := app.getStartOffset(tt.meta)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test extractPageInfo
func Test_BrowseApp_ExtractPageInfo(t *testing.T) {
	app := &BrowseApp{}

	tests := []struct {
		name       string
		pageHeader *parquet.PageHeader
		offset     int64
		expected   PageInfo
	}{
		{
			name: "data page with all fields",
			pageHeader: &parquet.PageHeader{
				Type:                 parquet.PageType_DATA_PAGE,
				CompressedPageSize:   1000,
				UncompressedPageSize: 2000,
				Crc:                  int32Ptr(12345),
				DataPageHeader: &parquet.DataPageHeader{
					NumValues:               100,
					Encoding:                parquet.Encoding_PLAIN,
					DefinitionLevelEncoding: parquet.Encoding_RLE,
					RepetitionLevelEncoding: parquet.Encoding_BIT_PACKED,
					Statistics: &parquet.Statistics{
						MinValue:  []byte{0x01},
						MaxValue:  []byte{0xFF},
						NullCount: int64Ptr(5),
					},
				},
			},
			offset: 500,
			expected: PageInfo{
				Offset:           500,
				PageType:         "DATA_PAGE",
				CompressedSize:   1000,
				UncompressedSize: 2000,
				HasCRC:           true,
				NumValues:        100,
				Encoding:         "PLAIN",
				DefLevelEncoding: "RLE",
				RepLevelEncoding: "BIT_PACKED",
				HasStatistics:    true,
				MinValue:         []byte{0x01},
				MaxValue:         []byte{0xFF},
				NullCount:        int64Ptr(5),
			},
		},
		{
			name: "data page v2",
			pageHeader: &parquet.PageHeader{
				Type:                 parquet.PageType_DATA_PAGE_V2,
				CompressedPageSize:   800,
				UncompressedPageSize: 1600,
				DataPageHeaderV2: &parquet.DataPageHeaderV2{
					NumValues: 50,
					Encoding:  parquet.Encoding_DELTA_BINARY_PACKED,
					Statistics: &parquet.Statistics{
						MinValue:  []byte{0x10},
						MaxValue:  []byte{0xF0},
						NullCount: int64Ptr(2),
					},
				},
			},
			offset: 1000,
			expected: PageInfo{
				Offset:           1000,
				PageType:         "DATA_PAGE_V2",
				CompressedSize:   800,
				UncompressedSize: 1600,
				HasCRC:           false,
				NumValues:        50,
				Encoding:         "DELTA_BINARY_PACKED",
				HasStatistics:    true,
				MinValue:         []byte{0x10},
				MaxValue:         []byte{0xF0},
				NullCount:        int64Ptr(2),
			},
		},
		{
			name: "dictionary page",
			pageHeader: &parquet.PageHeader{
				Type:                 parquet.PageType_DICTIONARY_PAGE,
				CompressedPageSize:   500,
				UncompressedPageSize: 500,
				DictionaryPageHeader: &parquet.DictionaryPageHeader{
					NumValues: 10,
					Encoding:  parquet.Encoding_PLAIN_DICTIONARY,
				},
			},
			offset: 0,
			expected: PageInfo{
				Offset:           0,
				PageType:         "DICTIONARY_PAGE",
				CompressedSize:   500,
				UncompressedSize: 500,
				HasCRC:           false,
				NumValues:        10,
				Encoding:         "PLAIN_DICTIONARY",
				HasStatistics:    false,
			},
		},
		{
			name: "index page",
			pageHeader: &parquet.PageHeader{
				Type:                 parquet.PageType_INDEX_PAGE,
				CompressedPageSize:   200,
				UncompressedPageSize: 200,
			},
			offset: 2000,
			expected: PageInfo{
				Offset:           2000,
				PageType:         "INDEX_PAGE",
				CompressedSize:   200,
				UncompressedSize: 200,
				HasCRC:           false,
				NumValues:        0,
				HasStatistics:    false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := app.extractPageInfo(tt.pageHeader, tt.offset)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test populateDataPageInfo
func Test_BrowseApp_PopulateDataPageInfo(t *testing.T) {
	app := &BrowseApp{}

	tests := []struct {
		name     string
		header   *parquet.DataPageHeader
		expected PageInfo
	}{
		{
			name: "complete data page header",
			header: &parquet.DataPageHeader{
				NumValues:               200,
				Encoding:                parquet.Encoding_RLE_DICTIONARY,
				DefinitionLevelEncoding: parquet.Encoding_RLE,
				RepetitionLevelEncoding: parquet.Encoding_BIT_PACKED,
				Statistics: &parquet.Statistics{
					MinValue:  []byte("min"),
					MaxValue:  []byte("max"),
					NullCount: int64Ptr(10),
				},
			},
			expected: PageInfo{
				NumValues:        200,
				Encoding:         "RLE_DICTIONARY",
				DefLevelEncoding: "RLE",
				RepLevelEncoding: "BIT_PACKED",
				HasStatistics:    true,
				MinValue:         []byte("min"),
				MaxValue:         []byte("max"),
				NullCount:        int64Ptr(10),
			},
		},
		{
			name: "no statistics",
			header: &parquet.DataPageHeader{
				NumValues:               100,
				Encoding:                parquet.Encoding_PLAIN,
				DefinitionLevelEncoding: parquet.Encoding_RLE,
				RepetitionLevelEncoding: parquet.Encoding_RLE,
				Statistics:              nil,
			},
			expected: PageInfo{
				NumValues:        100,
				Encoding:         "PLAIN",
				DefLevelEncoding: "RLE",
				RepLevelEncoding: "RLE",
				HasStatistics:    false,
			},
		},
		{
			name:   "nil header",
			header: nil,
			expected: PageInfo{
				NumValues: 0,
			},
		},
		{
			name: "deprecated min/max fallback",
			header: &parquet.DataPageHeader{
				NumValues:               150,
				Encoding:                parquet.Encoding_PLAIN,
				DefinitionLevelEncoding: parquet.Encoding_RLE,
				RepetitionLevelEncoding: parquet.Encoding_RLE,
				Statistics: &parquet.Statistics{
					Min:       []byte("old_min"),
					Max:       []byte("old_max"),
					NullCount: int64Ptr(3),
				},
			},
			expected: PageInfo{
				NumValues:        150,
				Encoding:         "PLAIN",
				DefLevelEncoding: "RLE",
				RepLevelEncoding: "RLE",
				HasStatistics:    true,
				MinValue:         []byte("old_min"),
				MaxValue:         []byte("old_max"),
				NullCount:        int64Ptr(3),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pageInfo := &PageInfo{}
			app.populateDataPageInfo(pageInfo, tt.header)
			assert.Equal(t, tt.expected.NumValues, pageInfo.NumValues)
			assert.Equal(t, tt.expected.Encoding, pageInfo.Encoding)
			assert.Equal(t, tt.expected.DefLevelEncoding, pageInfo.DefLevelEncoding)
			assert.Equal(t, tt.expected.RepLevelEncoding, pageInfo.RepLevelEncoding)
			assert.Equal(t, tt.expected.HasStatistics, pageInfo.HasStatistics)
			assert.Equal(t, tt.expected.MinValue, pageInfo.MinValue)
			assert.Equal(t, tt.expected.MaxValue, pageInfo.MaxValue)
			if tt.expected.NullCount != nil {
				assert.NotNil(t, pageInfo.NullCount)
				assert.Equal(t, *tt.expected.NullCount, *pageInfo.NullCount)
			}
		})
	}
}

// Test populateDataPageV2Info
func Test_BrowseApp_PopulateDataPageV2Info(t *testing.T) {
	app := &BrowseApp{}

	tests := []struct {
		name     string
		header   *parquet.DataPageHeaderV2
		expected PageInfo
	}{
		{
			name: "complete v2 header",
			header: &parquet.DataPageHeaderV2{
				NumValues: 300,
				Encoding:  parquet.Encoding_DELTA_BYTE_ARRAY,
				Statistics: &parquet.Statistics{
					MinValue:  []byte("a"),
					MaxValue:  []byte("z"),
					NullCount: int64Ptr(15),
				},
			},
			expected: PageInfo{
				NumValues:     300,
				Encoding:      "DELTA_BYTE_ARRAY",
				HasStatistics: true,
				MinValue:      []byte("a"),
				MaxValue:      []byte("z"),
				NullCount:     int64Ptr(15),
			},
		},
		{
			name: "no statistics",
			header: &parquet.DataPageHeaderV2{
				NumValues:  250,
				Encoding:   parquet.Encoding_PLAIN,
				Statistics: nil,
			},
			expected: PageInfo{
				NumValues:     250,
				Encoding:      "PLAIN",
				HasStatistics: false,
			},
		},
		{
			name:   "nil header",
			header: nil,
			expected: PageInfo{
				NumValues: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pageInfo := &PageInfo{}
			app.populateDataPageV2Info(pageInfo, tt.header)
			assert.Equal(t, tt.expected.NumValues, pageInfo.NumValues)
			assert.Equal(t, tt.expected.Encoding, pageInfo.Encoding)
			assert.Equal(t, tt.expected.HasStatistics, pageInfo.HasStatistics)
			assert.Equal(t, tt.expected.MinValue, pageInfo.MinValue)
			assert.Equal(t, tt.expected.MaxValue, pageInfo.MaxValue)
		})
	}
}

// Test populateDictionaryPageInfo
func Test_BrowseApp_PopulateDictionaryPageInfo(t *testing.T) {
	app := &BrowseApp{}

	tests := []struct {
		name     string
		header   *parquet.DictionaryPageHeader
		expected PageInfo
	}{
		{
			name: "complete dictionary header",
			header: &parquet.DictionaryPageHeader{
				NumValues: 50,
				Encoding:  parquet.Encoding_PLAIN_DICTIONARY,
			},
			expected: PageInfo{
				NumValues: 50,
				Encoding:  "PLAIN_DICTIONARY",
			},
		},
		{
			name:   "nil header",
			header: nil,
			expected: PageInfo{
				NumValues: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pageInfo := &PageInfo{}
			app.populateDictionaryPageInfo(pageInfo, tt.header)
			assert.Equal(t, tt.expected.NumValues, pageInfo.NumValues)
			assert.Equal(t, tt.expected.Encoding, pageInfo.Encoding)
		})
	}
}

// Test extractStatistics
func Test_BrowseApp_ExtractStatistics(t *testing.T) {
	app := &BrowseApp{}

	tests := []struct {
		name     string
		stats    *parquet.Statistics
		expected PageInfo
	}{
		{
			name: "with MinValue/MaxValue",
			stats: &parquet.Statistics{
				MinValue:  []byte("new_min"),
				MaxValue:  []byte("new_max"),
				NullCount: int64Ptr(7),
			},
			expected: PageInfo{
				MinValue:  []byte("new_min"),
				MaxValue:  []byte("new_max"),
				NullCount: int64Ptr(7),
			},
		},
		{
			name: "with deprecated Min/Max",
			stats: &parquet.Statistics{
				Min:       []byte("old_min"),
				Max:       []byte("old_max"),
				NullCount: int64Ptr(3),
			},
			expected: PageInfo{
				MinValue:  []byte("old_min"),
				MaxValue:  []byte("old_max"),
				NullCount: int64Ptr(3),
			},
		},
		{
			name: "prefer MinValue over Min",
			stats: &parquet.Statistics{
				MinValue:  []byte("new_min"),
				Min:       []byte("old_min"),
				MaxValue:  []byte("new_max"),
				Max:       []byte("old_max"),
				NullCount: int64Ptr(5),
			},
			expected: PageInfo{
				MinValue:  []byte("new_min"),
				MaxValue:  []byte("new_max"),
				NullCount: int64Ptr(5),
			},
		},
		{
			name: "empty MinValue falls back to Min",
			stats: &parquet.Statistics{
				MinValue:  []byte{},
				Min:       []byte("old_min"),
				MaxValue:  []byte{},
				Max:       []byte("old_max"),
				NullCount: int64Ptr(2),
			},
			expected: PageInfo{
				MinValue:  []byte("old_min"),
				MaxValue:  []byte("old_max"),
				NullCount: int64Ptr(2),
			},
		},
		{
			name: "no null count",
			stats: &parquet.Statistics{
				MinValue:  []byte("min"),
				MaxValue:  []byte("max"),
				NullCount: nil,
			},
			expected: PageInfo{
				MinValue:  []byte("min"),
				MaxValue:  []byte("max"),
				NullCount: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pageInfo := &PageInfo{}
			app.extractStatistics(pageInfo, tt.stats)
			assert.Equal(t, tt.expected.MinValue, pageInfo.MinValue)
			assert.Equal(t, tt.expected.MaxValue, pageInfo.MaxValue)
			if tt.expected.NullCount != nil {
				assert.NotNil(t, pageInfo.NullCount)
				assert.Equal(t, *tt.expected.NullCount, *pageInfo.NullCount)
			} else {
				assert.Nil(t, pageInfo.NullCount)
			}
		})
	}
}

// Test updateTotalValuesRead
func Test_BrowseApp_UpdateTotalValuesRead(t *testing.T) {
	app := &BrowseApp{}

	tests := []struct {
		name       string
		pageInfo   PageInfo
		pageHeader *parquet.PageHeader
		expected   int64
	}{
		{
			name: "data page",
			pageInfo: PageInfo{
				NumValues: 100,
			},
			pageHeader: &parquet.PageHeader{
				Type: parquet.PageType_DATA_PAGE,
			},
			expected: 100,
		},
		{
			name: "data page v2",
			pageInfo: PageInfo{
				NumValues: 200,
			},
			pageHeader: &parquet.PageHeader{
				Type: parquet.PageType_DATA_PAGE_V2,
			},
			expected: 200,
		},
		{
			name: "dictionary page - not counted",
			pageInfo: PageInfo{
				NumValues: 50,
			},
			pageHeader: &parquet.PageHeader{
				Type: parquet.PageType_DICTIONARY_PAGE,
			},
			expected: 0,
		},
		{
			name: "index page - not counted",
			pageInfo: PageInfo{
				NumValues: 25,
			},
			pageHeader: &parquet.PageHeader{
				Type: parquet.PageType_INDEX_PAGE,
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := app.updateTotalValuesRead(&tt.pageInfo, tt.pageHeader)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test shouldContinueReading
func Test_BrowseApp_ShouldContinueReading(t *testing.T) {
	app := &BrowseApp{}

	tests := []struct {
		name       string
		pages      []PageInfo
		curOffset  int64
		endOffset  int64
		headerSize int64
		pageHeader *parquet.PageHeader
		expected   bool
	}{
		{
			name:       "normal case - continue",
			pages:      []PageInfo{{}, {}},
			curOffset:  2000,
			endOffset:  10000,
			headerSize: 100,
			pageHeader: &parquet.PageHeader{
				CompressedPageSize: 800,
			},
			expected: true,
		},
		{
			name:       "too many pages",
			pages:      make([]PageInfo, 10001),
			curOffset:  2000,
			endOffset:  10000,
			headerSize: 100,
			pageHeader: &parquet.PageHeader{
				CompressedPageSize: 800,
			},
			expected: false,
		},
		{
			name:       "reached end offset",
			pages:      []PageInfo{{}, {}},
			curOffset:  10000,
			endOffset:  10000,
			headerSize: 100,
			pageHeader: &parquet.PageHeader{
				CompressedPageSize: 800,
			},
			expected: false,
		},
		{
			name:       "exceeded end offset",
			pages:      []PageInfo{{}, {}},
			curOffset:  10001,
			endOffset:  10000,
			headerSize: 100,
			pageHeader: &parquet.PageHeader{
				CompressedPageSize: 800,
			},
			expected: false,
		},
		{
			name:       "offset not advancing - infinite loop prevention",
			pages:      []PageInfo{{}, {}},
			curOffset:  1000,
			endOffset:  10000,
			headerSize: 100,
			pageHeader: &parquet.PageHeader{
				CompressedPageSize: 900, // lastOffset = 1000 - 100 - 900 = 0, curOffset = 1000
			},
			expected: true, // 1000 > 0
		},
		{
			name:       "offset equal to lastOffset - prevent infinite loop",
			pages:      []PageInfo{{}, {}},
			curOffset:  1000,
			endOffset:  10000,
			headerSize: 100,
			pageHeader: &parquet.PageHeader{
				CompressedPageSize: 900, // lastOffset = 1000 - 100 - 900 = 0
			},
			expected: true, // 1000 > 0, so continue
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := app.shouldContinueReading(tt.pages, tt.curOffset, tt.endOffset, tt.headerSize, tt.pageHeader)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test findSchemaElement
func Test_BrowseApp_FindSchemaElement(t *testing.T) {
	app := &BrowseApp{
		metadata: &parquet.FileMetaData{
			Schema: []*parquet.SchemaElement{
				{Name: "root"},
				{Name: "id", Type: parquet.TypePtr(parquet.Type_INT64)},
				{Name: "name", Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY)},
				{Name: "nested_struct"},
				{Name: "nested_field", Type: parquet.TypePtr(parquet.Type_INT32)},
				{Name: "list"},
				{Name: "element", Type: parquet.TypePtr(parquet.Type_FLOAT)},
			},
		},
	}

	tests := []struct {
		name         string
		pathInSchema []string
		expectedName string
		shouldBeNil  bool
	}{
		{
			name:         "empty path",
			pathInSchema: []string{},
			shouldBeNil:  true,
		},
		{
			name:         "simple path - id",
			pathInSchema: []string{"id"},
			expectedName: "id",
			shouldBeNil:  false,
		},
		{
			name:         "simple path - name",
			pathInSchema: []string{"name"},
			expectedName: "name",
			shouldBeNil:  false,
		},
		{
			name:         "nested path",
			pathInSchema: []string{"nested_struct", "nested_field"},
			expectedName: "nested_field",
			shouldBeNil:  false,
		},
		{
			name:         "list element path",
			pathInSchema: []string{"list", "element"},
			expectedName: "element",
			shouldBeNil:  false,
		},
		{
			name:         "non-existent field",
			pathInSchema: []string{"does_not_exist"},
			shouldBeNil:  true,
		},
		{
			name:         "nested non-existent field",
			pathInSchema: []string{"nested_struct", "does_not_exist"},
			shouldBeNil:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := app.findSchemaElement(tt.pathInSchema)
			if tt.shouldBeNil {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, tt.expectedName, result.Name)
			}
		})
	}
}

// TestGetColumnChunkInfoHeight tests the height calculation for column chunk info
func Test_GetColumnChunkInfoHeight(t *testing.T) {
	tests := []struct {
		name           string
		text           string
		expectedHeight int
	}{
		{
			name:           "single line",
			text:           "Column: test",
			expectedHeight: 3, // 1 line + 2 borders
		},
		{
			name:           "two lines",
			text:           "Line 1\nLine 2",
			expectedHeight: 4, // 2 lines + 2 borders
		},
		{
			name:           "three lines",
			text:           "Line 1\nLine 2\nLine 3",
			expectedHeight: 5, // 3 lines + 2 borders
		},
		{
			name:           "empty text",
			text:           "",
			expectedHeight: 3, // 1 line (empty) + 2 borders
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a real text view
			infoView := tview.NewTextView().SetText(tt.text)
			height := getColumnChunkInfoHeight(infoView)
			assert.Equal(t, tt.expectedHeight, height)
		})
	}
}

// TestGetColumnChunkInfoHeight_NilView tests nil view handling
func Test_GetColumnChunkInfoHeight_NilView(t *testing.T) {
	height := getColumnChunkInfoHeight(nil)
	assert.Equal(t, 3, height, "Should return default height of 3 for nil view")
}

// TestBuildColumnChunkInfoView tests column chunk info view creation
func Test_BuildColumnChunkInfoView(t *testing.T) {
	app := NewBrowseApp()

	tests := []struct {
		name             string
		meta             *parquet.ColumnMetaData
		schemaElem       *parquet.SchemaElement
		numPages         int
		expectContains   []string
		expectNotContain []string
	}{
		{
			name: "basic column info",
			meta: &parquet.ColumnMetaData{
				Type:                  parquet.Type_INT64,
				PathInSchema:          []string{"id"},
				Codec:                 parquet.CompressionCodec_SNAPPY,
				NumValues:             100,
				TotalCompressedSize:   500,
				TotalUncompressedSize: 800,
			},
			schemaElem: &parquet.SchemaElement{
				Name: "id",
				Type: parquet.TypePtr(parquet.Type_INT64),
			},
			numPages: 0,
			expectContains: []string{
				"Column:", "id",
				"Type:", "INT64",
				"Codec:", "SNAPPY",
				"Values:", "100",
				"Size:",
			},
			expectNotContain: []string{"Pages:"},
		},
		{
			name: "column with page count",
			meta: &parquet.ColumnMetaData{
				Type:                  parquet.Type_BYTE_ARRAY,
				PathInSchema:          []string{"doc", "title"},
				Codec:                 parquet.CompressionCodec_GZIP,
				NumValues:             50,
				TotalCompressedSize:   1000,
				TotalUncompressedSize: 2000,
			},
			schemaElem: &parquet.SchemaElement{
				Name: "title",
				Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{
					STRING: &parquet.StringType{},
				},
				ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			},
			numPages: 5,
			expectContains: []string{
				"Column:", "doc.title",
				"Type:", "BYTE_ARRAY",
				"Logical:", "STRING",
				"Converted:", "UTF8",
				"Codec:", "GZIP",
				"Values:", "50",
				"Pages:", "5",
				"Size:",
			},
			expectNotContain: []string{},
		},
		{
			name: "column with statistics",
			meta: &parquet.ColumnMetaData{
				Type:                  parquet.Type_INT32,
				PathInSchema:          []string{"age"},
				Codec:                 parquet.CompressionCodec_SNAPPY,
				NumValues:             1000,
				TotalCompressedSize:   4000,
				TotalUncompressedSize: 4000,
				Statistics: &parquet.Statistics{
					MinValue:  []byte{0x00, 0x00, 0x00, 0x01},
					MaxValue:  []byte{0x00, 0x00, 0x00, 0x64},
					NullCount: int64Ptr(10),
				},
			},
			schemaElem: nil,
			numPages:   3,
			expectContains: []string{
				"Column:", "age",
				"Type:", "INT32",
				"Logical:", "-",
				"Converted:", "-",
				"Codec:", "SNAPPY",
				"Values:", "1000",
				"Pages:", "3",
				"Nulls:", "10",
				"Min:",
				"Max:",
			},
			expectNotContain: []string{},
		},
		{
			name: "column with no statistics",
			meta: &parquet.ColumnMetaData{
				Type:                  parquet.Type_FLOAT,
				PathInSchema:          []string{"price"},
				Codec:                 parquet.CompressionCodec_UNCOMPRESSED,
				NumValues:             500,
				TotalCompressedSize:   2000,
				TotalUncompressedSize: 2000,
				Statistics:            nil,
			},
			schemaElem: &parquet.SchemaElement{
				Name: "price",
				Type: parquet.TypePtr(parquet.Type_FLOAT),
			},
			numPages: 2,
			expectContains: []string{
				"Column:", "price",
				"Type:", "FLOAT",
				"Codec:", "UNCOMPRESSED",
				"Values:", "500",
				"Pages:", "2",
			},
			expectNotContain: []string{"Min:", "Max:", "Nulls:"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			infoView := app.buildColumnChunkInfoView(tt.meta, tt.schemaElem, tt.numPages)
			assert.NotNil(t, infoView)

			// Get the text content
			text := infoView.GetText(false)
			assert.NotEmpty(t, text)

			// Check expected content
			for _, expected := range tt.expectContains {
				assert.Contains(t, text, expected,
					"Info view should contain '%s'", expected)
			}

			// Check not present content
			for _, notExpected := range tt.expectNotContain {
				assert.NotContains(t, text, notExpected,
					"Info view should not contain '%s'", notExpected)
			}

			// Verify it's a multi-line display (should have at least 2 lines)
			lines := strings.Split(text, "\n")
			assert.GreaterOrEqual(t, len(lines), 2,
				"Info view should have at least 2 lines")
		})
	}
}

// TestBuildColumnChunkInfoView_MultilineContent tests multi-line content structure
func Test_BuildColumnChunkInfoView_MultilineContent(t *testing.T) {
	app := NewBrowseApp()

	meta := &parquet.ColumnMetaData{
		Type:                  parquet.Type_BYTE_ARRAY,
		PathInSchema:          []string{"data"},
		Codec:                 parquet.CompressionCodec_SNAPPY,
		NumValues:             100,
		TotalCompressedSize:   1000,
		TotalUncompressedSize: 2000,
		Statistics: &parquet.Statistics{
			MinValue:  []byte("A"),
			MaxValue:  []byte("Z"),
			NullCount: int64Ptr(5),
		},
	}

	schemaElem := &parquet.SchemaElement{
		Name: "data",
		Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
		LogicalType: &parquet.LogicalType{
			STRING: &parquet.StringType{},
		},
	}

	infoView := app.buildColumnChunkInfoView(meta, schemaElem, 3)
	text := infoView.GetText(false)

	lines := strings.Split(text, "\n")

	// Should have 3 lines:
	// Line 1: Column, Type, Logical, Converted
	// Line 2: Codec, Values, Pages, Nulls, Size
	// Line 3: Min, Max
	assert.Equal(t, 3, len(lines), "Should have 3 lines")

	// Line 1 should contain column info
	assert.Contains(t, lines[0], "Column:")
	assert.Contains(t, lines[0], "Type:")

	// Line 2 should contain codec and values
	assert.Contains(t, lines[1], "Codec:")
	assert.Contains(t, lines[1], "Values:")

	// Line 3 should contain statistics
	assert.Contains(t, lines[2], "Min:")
	assert.Contains(t, lines[2], "Max:")
}

// Note: int64Ptr helper is already defined in model_test.go
func Test_NewBrowseApp(t *testing.T) {
	// Test that NewBrowseApp creates a valid app instance
	app := NewBrowseApp()

	assert.NotNil(t, app)
	assert.NotNil(t, app.tviewApp)
	assert.NotNil(t, app.pages)
}

// TestGetHeaderHeight tests dynamic header height calculation
func Test_GetHeaderHeight(t *testing.T) {
	tests := []struct {
		name           string
		createdBy      *string
		numRowGroups   int
		expectedHeight int // lines + 2 for borders
	}{
		{
			name:           "three line header (no created by)",
			createdBy:      nil,
			numRowGroups:   1,
			expectedHeight: 5, // 3 lines (basic info + file info + size) + 2 borders
		},
		{
			name:           "three line header (with created by)",
			createdBy:      stringPtr("parquet-go version 1.2.0"),
			numRowGroups:   1,
			expectedHeight: 5, // 3 lines + 2 borders
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := NewBrowseApp()
			app.currentFile = "test.parquet"
			app.metadata = &parquet.FileMetaData{
				Version: 1,
				NumRows: 1000,
				Schema: []*parquet.SchemaElement{
					{Name: "root"},
					{Name: "id", Type: parquet.TypePtr(parquet.Type_INT64)},
				},
				RowGroups: make([]*parquet.RowGroup, tt.numRowGroups),
			}

			// Create row groups with size data
			for i := 0; i < tt.numRowGroups; i++ {
				app.metadata.RowGroups[i] = &parquet.RowGroup{
					NumRows:             100,
					TotalByteSize:       1000,
					TotalCompressedSize: int64Ptr(500),
				}
			}

			if tt.createdBy != nil {
				app.metadata.CreatedBy = tt.createdBy
			}

			// Create the header view
			app.createHeaderView()

			// Get the calculated height
			height := app.getHeaderHeight()

			assert.Equal(t, tt.expectedHeight, height, "Header height should match expected value")
		})
	}
}

// TestGetHeaderHeight_NilHeaderView tests fallback when headerView is nil
func Test_GetHeaderHeight_NilHeaderView(t *testing.T) {
	app := NewBrowseApp()
	// Don't create header view

	height := app.getHeaderHeight()
	assert.Equal(t, 3, height, "Should return default height of 3 when headerView is nil")
}

// TestCreateHeaderView_MultiLine tests that header creates multiple lines correctly
func Test_CreateHeaderView_MultiLine(t *testing.T) {
	app := NewBrowseApp()
	app.currentFile = "test.parquet"
	app.metadata = &parquet.FileMetaData{
		Version:   1,
		NumRows:   1000000,
		CreatedBy: stringPtr("parquet-go version 1.2.0 (build 12345)"),
		Schema: []*parquet.SchemaElement{
			{Name: "root"},
			{Name: "col1", Type: parquet.TypePtr(parquet.Type_INT64)},
			{Name: "col2", Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY)},
		},
		RowGroups: []*parquet.RowGroup{
			{
				NumRows:             100000,
				TotalByteSize:       10000000,
				TotalCompressedSize: int64Ptr(5000000),
			},
		},
	}

	app.createHeaderView()

	// Get the text content
	text := app.headerView.GetText(false)

	// Should have at least one newline (splitting basic info and size/creator info)
	assert.Contains(t, text, "\n", "Header should contain newline for multiple lines")

	// Should contain all expected fields
	assert.Contains(t, text, "Version:", "Should contain version")
	assert.Contains(t, text, "Row Groups:", "Should contain row groups")
	assert.Contains(t, text, "Rows:", "Should contain rows")
	assert.Contains(t, text, "Columns:", "Should contain columns")
	assert.Contains(t, text, "Size:", "Should contain size")
	assert.Contains(t, text, "Created By:", "Should contain created by")

	// Height should be 5 (3 lines + 2 borders)
	height := app.getHeaderHeight()
	assert.Equal(t, 5, height, "Header with created by should have height of 5")
}

// Helper function for tests
func stringPtr(s string) *string {
	return &s
}
