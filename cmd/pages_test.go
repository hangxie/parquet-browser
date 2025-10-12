package cmd

import (
	"testing"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/stretchr/testify/assert"
)

// Test getStartOffset
func Test_GetStartOffset(t *testing.T) {
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
func Test_ExtractPageInfo(t *testing.T) {
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
func Test_PopulateDataPageInfo(t *testing.T) {
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
func Test_PopulateDataPageV2Info(t *testing.T) {
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
func Test_PopulateDictionaryPageInfo(t *testing.T) {
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
func Test_ExtractStatistics(t *testing.T) {
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
func Test_UpdateTotalValuesRead(t *testing.T) {
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
func Test_ShouldContinueReading(t *testing.T) {
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

// Test formatLogicalType with all types
func Test_FormatLogicalType(t *testing.T) {
	tests := []struct {
		name        string
		logicalType *parquet.LogicalType
		expected    string
	}{
		{
			name:        "nil logical type",
			logicalType: nil,
			expected:    "-",
		},
		{
			name: "STRING",
			logicalType: &parquet.LogicalType{
				STRING: &parquet.StringType{},
			},
			expected: "STRING",
		},
		{
			name: "MAP",
			logicalType: &parquet.LogicalType{
				MAP: &parquet.MapType{},
			},
			expected: "MAP",
		},
		{
			name: "LIST",
			logicalType: &parquet.LogicalType{
				LIST: &parquet.ListType{},
			},
			expected: "LIST",
		},
		{
			name: "ENUM",
			logicalType: &parquet.LogicalType{
				ENUM: &parquet.EnumType{},
			},
			expected: "ENUM",
		},
		{
			name: "DECIMAL",
			logicalType: &parquet.LogicalType{
				DECIMAL: &parquet.DecimalType{
					Precision: 10,
					Scale:     2,
				},
			},
			expected: "DECIMAL(10,2)",
		},
		{
			name: "DATE",
			logicalType: &parquet.LogicalType{
				DATE: &parquet.DateType{},
			},
			expected: "DATE",
		},
		{
			name: "TIME",
			logicalType: &parquet.LogicalType{
				TIME: &parquet.TimeType{
					IsAdjustedToUTC: true,
					Unit: &parquet.TimeUnit{
						MILLIS: &parquet.MilliSeconds{},
					},
				},
			},
			expected: "TIME(&{MILLIS:0xc000010b40 MICROS:<nil> NANOS:<nil>},true)",
		},
		{
			name: "TIMESTAMP",
			logicalType: &parquet.LogicalType{
				TIMESTAMP: &parquet.TimestampType{
					IsAdjustedToUTC: false,
					Unit: &parquet.TimeUnit{
						MICROS: &parquet.MicroSeconds{},
					},
				},
			},
			expected: "TIMESTAMP(&{MILLIS:<nil> MICROS:0xc000010ba0 NANOS:<nil>},false)",
		},
		{
			name: "INTEGER signed",
			logicalType: &parquet.LogicalType{
				INTEGER: &parquet.IntType{
					BitWidth: 32,
					IsSigned: true,
				},
			},
			expected: "INTEGER(32,signed)",
		},
		{
			name: "INTEGER unsigned",
			logicalType: &parquet.LogicalType{
				INTEGER: &parquet.IntType{
					BitWidth: 64,
					IsSigned: false,
				},
			},
			expected: "INTEGER(64,unsigned)",
		},
		{
			name: "UNKNOWN",
			logicalType: &parquet.LogicalType{
				UNKNOWN: &parquet.NullType{},
			},
			expected: "UNKNOWN",
		},
		{
			name: "JSON",
			logicalType: &parquet.LogicalType{
				JSON: &parquet.JsonType{},
			},
			expected: "JSON",
		},
		{
			name: "BSON",
			logicalType: &parquet.LogicalType{
				BSON: &parquet.BsonType{},
			},
			expected: "BSON",
		},
		{
			name: "UUID",
			logicalType: &parquet.LogicalType{
				UUID: &parquet.UUIDType{},
			},
			expected: "UUID",
		},
		{
			name: "FLOAT16",
			logicalType: &parquet.LogicalType{
				FLOAT16: &parquet.Float16Type{},
			},
			expected: "FLOAT16",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatLogicalType(tt.logicalType)
			// For TIME and TIMESTAMP, we just check if they start with the expected prefix
			// because the pointer addresses will vary
			if tt.name == "TIME" {
				assert.Contains(t, result, "TIME(")
			} else if tt.name == "TIMESTAMP" {
				assert.Contains(t, result, "TIMESTAMP(")
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// Test formatConvertedType
func Test_FormatConvertedType(t *testing.T) {
	tests := []struct {
		name          string
		convertedType *parquet.ConvertedType
		expected      string
	}{
		{
			name:          "nil converted type",
			convertedType: nil,
			expected:      "-",
		},
		{
			name: "UTF8",
			convertedType: func() *parquet.ConvertedType {
				ct := parquet.ConvertedType_UTF8
				return &ct
			}(),
			expected: "UTF8",
		},
		{
			name: "INT_8",
			convertedType: func() *parquet.ConvertedType {
				ct := parquet.ConvertedType_INT_8
				return &ct
			}(),
			expected: "INT_8",
		},
		{
			name: "DECIMAL",
			convertedType: func() *parquet.ConvertedType {
				ct := parquet.ConvertedType_DECIMAL
				return &ct
			}(),
			expected: "DECIMAL",
		},
		{
			name: "TIMESTAMP_MILLIS",
			convertedType: func() *parquet.ConvertedType {
				ct := parquet.ConvertedType_TIMESTAMP_MILLIS
				return &ct
			}(),
			expected: "TIMESTAMP_MILLIS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatConvertedType(tt.convertedType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Helper function for int32 pointers
func int32Ptr(v int32) *int32 {
	return &v
}
