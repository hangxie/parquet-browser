package model

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/golang/snappy"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func Test_GetTotalSize(t *testing.T) {
	tests := []struct {
		name     string
		rowGroup *parquet.RowGroup
		expected int64
	}{
		{
			name: "Empty row group",
			rowGroup: &parquet.RowGroup{
				Columns: []*parquet.ColumnChunk{},
			},
			expected: 0,
		},
		{
			name: "Single column",
			rowGroup: &parquet.RowGroup{
				Columns: []*parquet.ColumnChunk{
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 1024}},
				},
			},
			expected: 1024,
		},
		{
			name: "Multiple columns",
			rowGroup: &parquet.RowGroup{
				Columns: []*parquet.ColumnChunk{
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 1024}},
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 2048}},
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 512}},
				},
			},
			expected: 3584,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getTotalSize(tt.rowGroup)
			require.Equal(t, tt.expected, result, "getTotalSize() should match")
		})
	}
}

func Test_CountLeafColumns(t *testing.T) {
	tests := []struct {
		name     string
		schema   []*parquet.SchemaElement
		expected int
	}{
		{
			name:     "Empty schema",
			schema:   []*parquet.SchemaElement{},
			expected: 0,
		},
		{
			name: "Only leaf columns",
			schema: []*parquet.SchemaElement{
				{Name: "root", NumChildren: intPtr(2)},
				{Name: "col1", Type: parquetTypePtr(parquet.Type_INT32)},
				{Name: "col2", Type: parquetTypePtr(parquet.Type_BYTE_ARRAY)},
			},
			expected: 2,
		},
		{
			name: "Mixed leaf and group nodes",
			schema: []*parquet.SchemaElement{
				{Name: "root", NumChildren: intPtr(3)},
				{Name: "col1", Type: parquetTypePtr(parquet.Type_INT32)},
				{Name: "group", NumChildren: intPtr(2)},
				{Name: "col2", Type: parquetTypePtr(parquet.Type_INT64)},
				{Name: "col3", Type: parquetTypePtr(parquet.Type_BOOLEAN)},
			},
			expected: 3, // Only col1, col2, col3 are leaf columns
		},
		{
			name: "Only group nodes",
			schema: []*parquet.SchemaElement{
				{Name: "root", NumChildren: intPtr(2)},
				{Name: "group1", NumChildren: intPtr(1)},
				{Name: "group2", NumChildren: intPtr(1)},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := countLeafColumns(tt.schema)
			require.Equal(t, tt.expected, result, "countLeafColumns() should match")
		})
	}
}

func Test_FormatPathInSchema(t *testing.T) {
	tests := []struct {
		name     string
		path     []string
		expected string
	}{
		{
			name:     "Empty path",
			path:     []string{},
			expected: "",
		},
		{
			name:     "Single element",
			path:     []string{"field1"},
			expected: "field1",
		},
		{
			name:     "Multiple elements",
			path:     []string{"parent", "child", "grandchild"},
			expected: "parent.child.grandchild",
		},
		{
			name:     "With special characters",
			path:     []string{"my_field", "sub_field"},
			expected: "my_field.sub_field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatPathInSchema(tt.path)
			require.Equal(t, tt.expected, result, "formatPathInSchema() should match")
		})
	}
}

func Test_FormatLogicalType(t *testing.T) {
	tests := []struct {
		name        string
		logicalType *parquet.LogicalType
		expected    string
	}{
		{
			name:        "Nil logical type",
			logicalType: nil,
			expected:    "-",
		},
		{
			name:        "STRING type",
			logicalType: &parquet.LogicalType{STRING: &parquet.StringType{}},
			expected:    "STRING",
		},
		{
			name:        "MAP type",
			logicalType: &parquet.LogicalType{MAP: &parquet.MapType{}},
			expected:    "MAP",
		},
		{
			name:        "LIST type",
			logicalType: &parquet.LogicalType{LIST: &parquet.ListType{}},
			expected:    "LIST",
		},
		{
			name:        "ENUM type",
			logicalType: &parquet.LogicalType{ENUM: &parquet.EnumType{}},
			expected:    "ENUM",
		},
		{
			name: "DECIMAL type",
			logicalType: &parquet.LogicalType{
				DECIMAL: &parquet.DecimalType{Precision: 10, Scale: 2},
			},
			expected: "DECIMAL(10,2)",
		},
		{
			name:        "DATE type",
			logicalType: &parquet.LogicalType{DATE: &parquet.DateType{}},
			expected:    "DATE",
		},
		{
			name:        "JSON type",
			logicalType: &parquet.LogicalType{JSON: &parquet.JsonType{}},
			expected:    "JSON",
		},
		{
			name:        "BSON type",
			logicalType: &parquet.LogicalType{BSON: &parquet.BsonType{}},
			expected:    "BSON",
		},
		{
			name:        "UUID type",
			logicalType: &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			expected:    "UUID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatLogicalType(tt.logicalType)
			require.Equal(t, tt.expected, result, "formatLogicalType() should match")
		})
	}
}

func Test_FormatConvertedType(t *testing.T) {
	tests := []struct {
		name          string
		convertedType *parquet.ConvertedType
		expected      string
	}{
		{
			name:          "Nil converted type",
			convertedType: nil,
			expected:      "-",
		},
		{
			name:          "UTF8 type",
			convertedType: convertedTypePtr(parquet.ConvertedType_UTF8),
			expected:      "UTF8",
		},
		{
			name:          "MAP type",
			convertedType: convertedTypePtr(parquet.ConvertedType_MAP),
			expected:      "MAP",
		},
		{
			name:          "LIST type",
			convertedType: convertedTypePtr(parquet.ConvertedType_LIST),
			expected:      "LIST",
		},
		{
			name:          "DECIMAL type",
			convertedType: convertedTypePtr(parquet.ConvertedType_DECIMAL),
			expected:      "DECIMAL",
		},
		{
			name:          "DATE type",
			convertedType: convertedTypePtr(parquet.ConvertedType_DATE),
			expected:      "DATE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatConvertedType(tt.convertedType)
			require.Equal(t, tt.expected, result, "formatConvertedType() should match")
		})
	}
}

func Test_DecompressSnappy(t *testing.T) {
	// Test with simple data - note: we can't easily create valid Snappy data without the encoder
	// So this is a basic test that the function exists and handles errors
	t.Run("Invalid snappy data", func(t *testing.T) {
		_, err := decompressSnappy([]byte("not valid snappy"))
		require.Error(t, err, "decompressSnappy() should fail with invalid data")
	})
}

func Test_DecompressGzip(t *testing.T) {
	t.Run("Invalid gzip data", func(t *testing.T) {
		_, err := decompressGzip([]byte("not valid gzip"))
		require.Error(t, err, "decompressGzip() should fail with invalid data")
	})
}

func Test_DecompressPageData(t *testing.T) {
	tests := []struct {
		name      string
		codec     parquet.CompressionCodec
		expectErr bool
	}{
		{
			name:      "UNCOMPRESSED",
			codec:     parquet.CompressionCodec_UNCOMPRESSED,
			expectErr: false,
		},
		{
			name:      "LZO not supported",
			codec:     parquet.CompressionCodec_LZO,
			expectErr: true,
		},
		{
			name:      "BROTLI not implemented",
			codec:     parquet.CompressionCodec_BROTLI,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte("test data")
			_, err := decompressPageData(data, tt.codec, 1024)
			if tt.expectErr {
				require.Error(t, err, "decompressPageData() should return error for unsupported codec")
			} else {
				require.NoError(t, err, "decompressPageData() should not return error")
			}
		})
	}
}

func Test_DecodePlainValues_BOOLEAN(t *testing.T) {
	data := []byte{0x01, 0x00, 0x01}
	values, err := decodePlainValues(data, parquet.Type_BOOLEAN, 3)
	require.NoError(t, err, "decodePlainValues() should not return error")
	require.Len(t, values, 3, "decodePlainValues() should return 3 values")
	expected := []bool{true, false, true}
	for i, v := range values {
		b, ok := v.(bool)
		require.True(t, ok, "decodePlainValues()[%d] should be bool", i)
		require.Equal(t, expected[i], b, "decodePlainValues()[%d] should match expected value", i)
	}
}

func Test_DecodePlainValues_INT32(t *testing.T) {
	// Little-endian representation of [1, 2, 3]
	data := []byte{
		0x01, 0x00, 0x00, 0x00, // 1
		0x02, 0x00, 0x00, 0x00, // 2
		0x03, 0x00, 0x00, 0x00, // 3
	}
	values, err := decodePlainValues(data, parquet.Type_INT32, 3)
	require.NoError(t, err, "decodePlainValues() should not return error")
	require.Len(t, values, 3, "decodePlainValues() should return 3 values")
	expected := []int32{1, 2, 3}
	for i, v := range values {
		i32, ok := v.(int32)
		require.True(t, ok, "decodePlainValues()[%d] should be int32", i)
		require.Equal(t, expected[i], i32, "decodePlainValues()[%d] should match expected value", i)
	}
}

func Test_DecodePlainValues_BYTE_ARRAY(t *testing.T) {
	// BYTE_ARRAY: 4-byte length followed by data
	// "hi" = length 2, then 'h', 'i'
	data := []byte{
		0x02, 0x00, 0x00, 0x00, 'h', 'i',
	}
	values, err := decodePlainValues(data, parquet.Type_BYTE_ARRAY, 1)
	require.NoError(t, err, "decodePlainValues() should not return error")
	require.Len(t, values, 1, "decodePlainValues() should return 1 value")
	s, ok := values[0].(string)
	require.True(t, ok, "decodePlainValues()[0] should be string")
	require.Equal(t, "hi", s, "decodePlainValues()[0] should match expected value")
}

func Test_DecodeDictionaryValues(t *testing.T) {
	t.Run("PLAIN encoding", func(t *testing.T) {
		data := []byte{0x01, 0x00, 0x00, 0x00} // INT32 value: 1
		values, err := decodeDictionaryValues(data, parquet.Type_INT32, parquet.Encoding_PLAIN, 1)
		require.NoError(t, err, "decodeDictionaryValues() should not return error")
		require.Len(t, values, 1, "decodeDictionaryValues() should return 1 value")
	})

	t.Run("RLE_DICTIONARY encoding (should error)", func(t *testing.T) {
		data := []byte{0x01}
		_, err := decodeDictionaryValues(data, parquet.Type_INT32, parquet.Encoding_RLE_DICTIONARY, 1)
		require.Error(t, err, "decodeDictionaryValues() should error for RLE_DICTIONARY encoding")
	})

	t.Run("Unsupported encoding", func(t *testing.T) {
		data := []byte{0x01}
		_, err := decodeDictionaryValues(data, parquet.Type_INT32, parquet.Encoding_DELTA_BINARY_PACKED, 1)
		require.Error(t, err, "decodeDictionaryValues() should error for unsupported encoding")
	})
}

func Test_DecompressLZ4(t *testing.T) {
	t.Run("Valid LZ4 compressed data", func(t *testing.T) {
		// Create test data
		originalData := []byte("Hello, World! This is a test string for LZ4 compression.")

		// Compress using LZ4
		var compressed bytes.Buffer
		writer := lz4.NewWriter(&compressed)
		_, err := writer.Write(originalData)
		require.NoError(t, err, "LZ4 write should not error")
		err = writer.Close()
		require.NoError(t, err, "LZ4 writer close should not error")

		// Decompress
		decompressed, err := decompressLZ4(compressed.Bytes(), len(originalData))
		require.NoError(t, err, "decompressLZ4() should not return error")
		require.Equal(t, originalData, decompressed, "Decompressed data should match original")
	})

	t.Run("Invalid LZ4 data", func(t *testing.T) {
		// Try to decompress invalid data
		invalidData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
		_, err := decompressLZ4(invalidData, 10)
		require.Error(t, err, "decompressLZ4() should return error for invalid data")
	})

	t.Run("Empty data", func(t *testing.T) {
		// Compress empty data
		var compressed bytes.Buffer
		writer := lz4.NewWriter(&compressed)
		err := writer.Close()
		require.NoError(t, err, "LZ4 writer close should not error")

		decompressed, err := decompressLZ4(compressed.Bytes(), 0)
		require.NoError(t, err, "decompressLZ4() should not return error for empty data")
		require.Empty(t, decompressed, "Decompressed data should be empty")
	})
}

func Test_DecompressZstd(t *testing.T) {
	t.Run("Valid Zstd compressed data", func(t *testing.T) {
		// Create test data
		originalData := []byte("Hello, World! This is a test string for Zstandard compression.")

		// Compress using Zstd
		var compressed bytes.Buffer
		writer, err := zstd.NewWriter(&compressed)
		require.NoError(t, err, "Zstd writer creation should not error")
		_, err = writer.Write(originalData)
		require.NoError(t, err, "Zstd write should not error")
		err = writer.Close()
		require.NoError(t, err, "Zstd writer close should not error")

		// Decompress
		decompressed, err := decompressZstd(compressed.Bytes())
		require.NoError(t, err, "decompressZstd() should not return error")
		require.Equal(t, originalData, decompressed, "Decompressed data should match original")
	})

	t.Run("Invalid Zstd data", func(t *testing.T) {
		// Try to decompress invalid data
		invalidData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
		_, err := decompressZstd(invalidData)
		require.Error(t, err, "decompressZstd() should return error for invalid data")
	})

	t.Run("Empty valid Zstd stream", func(t *testing.T) {
		// Compress empty data
		var compressed bytes.Buffer
		writer, err := zstd.NewWriter(&compressed)
		require.NoError(t, err, "Zstd writer creation should not error")
		err = writer.Close()
		require.NoError(t, err, "Zstd writer close should not error")

		decompressed, err := decompressZstd(compressed.Bytes())
		require.NoError(t, err, "decompressZstd() should not return error for empty data")
		require.Empty(t, decompressed, "Decompressed data should be empty")
	})
}

// Helper function
func convertedTypePtr(ct parquet.ConvertedType) *parquet.ConvertedType {
	return &ct
}

// Test decompressGzip success path
func Test_DecompressGzip_Success(t *testing.T) {
	t.Run("Valid gzip compressed data", func(t *testing.T) {
		// Create test data
		originalData := []byte("Hello, World! This is a test string for gzip compression.")

		// Compress using gzip
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		_, err := writer.Write(originalData)
		require.NoError(t, err, "gzip write should not error")
		err = writer.Close()
		require.NoError(t, err, "gzip writer close should not error")

		// Decompress
		decompressed, err := decompressGzip(compressed.Bytes())
		require.NoError(t, err, "decompressGzip() should not return error")
		require.Equal(t, originalData, decompressed, "Decompressed data should match original")
	})

	t.Run("Empty gzip stream", func(t *testing.T) {
		// Compress empty data
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		err := writer.Close()
		require.NoError(t, err, "gzip writer close should not error")

		decompressed, err := decompressGzip(compressed.Bytes())
		require.NoError(t, err, "decompressGzip() should not return error for empty data")
		require.Empty(t, decompressed, "Decompressed data should be empty")
	})
}

// Test decodePlainValues for all types
func Test_DecodePlainValues_AllTypes(t *testing.T) {
	t.Run("INT64", func(t *testing.T) {
		// Little-endian representation of [100, 200]
		data := []byte{
			0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 100
			0xC8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 200
		}
		values, err := decodePlainValues(data, parquet.Type_INT64, 2)
		require.NoError(t, err)
		require.Len(t, values, 2)
		require.Equal(t, int64(100), values[0])
		require.Equal(t, int64(200), values[1])
	})

	t.Run("FLOAT", func(t *testing.T) {
		// IEEE 754 little-endian representation of 1.5
		data := []byte{0x00, 0x00, 0xC0, 0x3F}
		values, err := decodePlainValues(data, parquet.Type_FLOAT, 1)
		require.NoError(t, err)
		require.Len(t, values, 1)
		f, ok := values[0].(float32)
		require.True(t, ok)
		require.InDelta(t, 1.5, f, 0.001)
	})

	t.Run("DOUBLE", func(t *testing.T) {
		// IEEE 754 little-endian representation of 2.5
		data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x40}
		values, err := decodePlainValues(data, parquet.Type_DOUBLE, 1)
		require.NoError(t, err)
		require.Len(t, values, 1)
		d, ok := values[0].(float64)
		require.True(t, ok)
		require.InDelta(t, 2.5, d, 0.001)
	})

	t.Run("FIXED_LEN_BYTE_ARRAY", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03, 0x04}
		values, err := decodePlainValues(data, parquet.Type_FIXED_LEN_BYTE_ARRAY, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "FIXED_LEN_BYTE_ARRAY decoding requires schema information")
		require.Nil(t, values)
	})

	t.Run("INT96", func(t *testing.T) {
		// 12 bytes for INT96
		data := make([]byte, 12)
		values, err := decodePlainValues(data, parquet.Type_INT96, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported physical type: INT96")
		require.Nil(t, values)
	})

	t.Run("Unsupported type", func(t *testing.T) {
		// Use an invalid type value
		data := []byte{0x01}
		_, err := decodePlainValues(data, parquet.Type(999), 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported physical type")
	})

	t.Run("Insufficient data for INT32", func(t *testing.T) {
		// Only 2 bytes instead of 4
		data := []byte{0x01, 0x02}
		values, err := decodePlainValues(data, parquet.Type_INT32, 1)
		// When there's insufficient data, it returns empty slice without error
		require.NoError(t, err)
		require.Equal(t, 0, len(values))
	})

	t.Run("Insufficient data for BYTE_ARRAY", func(t *testing.T) {
		// Length says 10 bytes, but only provide 2
		data := []byte{0x0A, 0x00, 0x00, 0x00, 0x01, 0x02}
		values, err := decodePlainValues(data, parquet.Type_BYTE_ARRAY, 1)
		// When there's insufficient data, it returns empty slice without error
		require.NoError(t, err)
		require.Equal(t, 0, len(values))
	})
}

// Test findSchemaElement with various paths
func Test_FindSchemaElement(t *testing.T) {
	schema := []*parquet.SchemaElement{
		{Name: "Parquet_go_root", NumChildren: intPtr(3)},
		{Name: "field1", Type: parquetTypePtr(parquet.Type_INT32)},
		{Name: "group1", NumChildren: intPtr(2)},
		{Name: "nested1", Type: parquetTypePtr(parquet.Type_INT64)},
		{Name: "nested2", Type: parquetTypePtr(parquet.Type_BOOLEAN)},
		{Name: "field2", Type: parquetTypePtr(parquet.Type_BYTE_ARRAY)},
	}

	tests := []struct {
		name     string
		path     []string
		expected string // expected element name or empty if nil
	}{
		{
			name:     "Empty path",
			path:     []string{},
			expected: "",
		},
		{
			name:     "Root level field",
			path:     []string{"field1"},
			expected: "field1",
		},
		{
			name:     "Nested field",
			path:     []string{"group1", "nested1"},
			expected: "nested1",
		},
		{
			name:     "Case insensitive match",
			path:     []string{"FIELD1"},
			expected: "field1",
		},
		{
			name:     "Non-existent path",
			path:     []string{"does_not_exist"},
			expected: "",
		},
		{
			name:     "Partial match only",
			path:     []string{"group1"},
			expected: "group1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findSchemaElement(schema, tt.path)
			if tt.expected == "" {
				require.Nil(t, result)
			} else {
				require.NotNil(t, result)
				require.Equal(t, tt.expected, result.Name)
			}
		})
	}
}

// Test formatLogicalType with time types
func Test_FormatLogicalType_TimeTypes(t *testing.T) {
	tests := []struct {
		name        string
		logicalType *parquet.LogicalType
		expected    string
	}{
		{
			name: "TIME with millis",
			logicalType: &parquet.LogicalType{
				TIME: &parquet.TimeType{
					IsAdjustedToUTC: true,
					Unit:            &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
				},
			},
			expected: "TIME(TimeUnit({MILLIS:MilliSeconds({}) MICROS:<nil> NANOS:<nil>}),true)",
		},
		{
			name: "TIME with micros",
			logicalType: &parquet.LogicalType{
				TIME: &parquet.TimeType{
					IsAdjustedToUTC: false,
					Unit:            &parquet.TimeUnit{MICROS: &parquet.MicroSeconds{}},
				},
			},
			expected: "TIME(TimeUnit({MILLIS:<nil> MICROS:MicroSeconds({}) NANOS:<nil>}),false)",
		},
		{
			name: "TIME with nanos",
			logicalType: &parquet.LogicalType{
				TIME: &parquet.TimeType{
					IsAdjustedToUTC: true,
					Unit:            &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
				},
			},
			expected: "TIME(TimeUnit({MILLIS:<nil> MICROS:<nil> NANOS:NanoSeconds({})}),true)",
		},
		{
			name: "TIMESTAMP with millis",
			logicalType: &parquet.LogicalType{
				TIMESTAMP: &parquet.TimestampType{
					IsAdjustedToUTC: true,
					Unit:            &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
				},
			},
			expected: "TIMESTAMP(TimeUnit({MILLIS:MilliSeconds({}) MICROS:<nil> NANOS:<nil>}),true)",
		},
		{
			name: "TIMESTAMP with micros",
			logicalType: &parquet.LogicalType{
				TIMESTAMP: &parquet.TimestampType{
					IsAdjustedToUTC: false,
					Unit:            &parquet.TimeUnit{MICROS: &parquet.MicroSeconds{}},
				},
			},
			expected: "TIMESTAMP(TimeUnit({MILLIS:<nil> MICROS:MicroSeconds({}) NANOS:<nil>}),false)",
		},
		{
			name: "TIMESTAMP with nanos",
			logicalType: &parquet.LogicalType{
				TIMESTAMP: &parquet.TimestampType{
					IsAdjustedToUTC: true,
					Unit:            &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
				},
			},
			expected: "TIMESTAMP(TimeUnit({MILLIS:<nil> MICROS:<nil> NANOS:NanoSeconds({})}),true)",
		},
		{
			name: "INTEGER with bit width",
			logicalType: &parquet.LogicalType{
				INTEGER: &parquet.IntType{
					BitWidth: 8,
					IsSigned: true,
				},
			},
			expected: "INTEGER(8,signed)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatLogicalType(tt.logicalType)
			require.Equal(t, tt.expected, result)
		})
	}
}

// Test formatLogicalType with additional types
func Test_FormatLogicalType_AdditionalTypes(t *testing.T) {
	tests := []struct {
		name        string
		logicalType *parquet.LogicalType
		expected    string
	}{
		{
			name:        "UNKNOWN type",
			logicalType: &parquet.LogicalType{UNKNOWN: &parquet.NullType{}},
			expected:    "UNKNOWN",
		},
		{
			name:        "JSON type",
			logicalType: &parquet.LogicalType{JSON: &parquet.JsonType{}},
			expected:    "JSON",
		},
		{
			name:        "BSON type",
			logicalType: &parquet.LogicalType{BSON: &parquet.BsonType{}},
			expected:    "BSON",
		},
		{
			name:        "UUID type",
			logicalType: &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			expected:    "UUID",
		},
		{
			name:        "FLOAT16 type",
			logicalType: &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			expected:    "FLOAT16",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatLogicalType(tt.logicalType)
			require.Equal(t, tt.expected, result)
		})
	}
}

// Test decompressPageData with all codec paths
func Test_DecompressPageData_AllCodecs(t *testing.T) {
	t.Run("SNAPPY - success", func(t *testing.T) {
		originalData := []byte("test data for snappy")
		compressed := snappy.Encode(nil, originalData)

		decompressed, err := decompressPageData(compressed, parquet.CompressionCodec_SNAPPY, int32(len(originalData)))
		require.NoError(t, err)
		require.Equal(t, originalData, decompressed)
	})

	t.Run("GZIP - success", func(t *testing.T) {
		originalData := []byte("test data for gzip")
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		_, _ = writer.Write(originalData)
		_ = writer.Close()

		decompressed, err := decompressPageData(compressed.Bytes(), parquet.CompressionCodec_GZIP, int32(len(originalData)))
		require.NoError(t, err)
		require.Equal(t, originalData, decompressed)
	})

	t.Run("LZ4 - success", func(t *testing.T) {
		originalData := []byte("test data for LZ4 compression")
		var compressed bytes.Buffer
		writer := lz4.NewWriter(&compressed)
		_, _ = writer.Write(originalData)
		_ = writer.Close()

		decompressed, err := decompressPageData(compressed.Bytes(), parquet.CompressionCodec_LZ4, int32(len(originalData)))
		require.NoError(t, err)
		require.Equal(t, originalData, decompressed)
	})

	t.Run("ZSTD - success", func(t *testing.T) {
		originalData := []byte("test data for zstd compression")
		var compressed bytes.Buffer
		writer, _ := zstd.NewWriter(&compressed)
		_, _ = writer.Write(originalData)
		_ = writer.Close()

		decompressed, err := decompressPageData(compressed.Bytes(), parquet.CompressionCodec_ZSTD, int32(len(originalData)))
		require.NoError(t, err)
		require.Equal(t, originalData, decompressed)
	})

	t.Run("SNAPPY - error on invalid data", func(t *testing.T) {
		invalidData := []byte{0xFF, 0xFF, 0xFF}
		_, err := decompressPageData(invalidData, parquet.CompressionCodec_SNAPPY, 10)
		require.Error(t, err)
	})

	t.Run("GZIP - error on invalid data", func(t *testing.T) {
		invalidData := []byte{0xFF, 0xFF, 0xFF}
		_, err := decompressPageData(invalidData, parquet.CompressionCodec_GZIP, 10)
		require.Error(t, err)
	})

	t.Run("LZ4 - error on invalid data", func(t *testing.T) {
		invalidData := []byte{0xFF, 0xFF, 0xFF}
		_, err := decompressPageData(invalidData, parquet.CompressionCodec_LZ4, 10)
		require.Error(t, err)
	})

	t.Run("ZSTD - error on invalid data", func(t *testing.T) {
		invalidData := []byte{0xFF, 0xFF, 0xFF}
		_, err := decompressPageData(invalidData, parquet.CompressionCodec_ZSTD, 10)
		require.Error(t, err)
	})

	t.Run("UNCOMPRESSED - success", func(t *testing.T) {
		data := []byte("uncompressed data")
		result, err := decompressPageData(data, parquet.CompressionCodec_UNCOMPRESSED, int32(len(data)))
		require.NoError(t, err)
		require.Equal(t, data, result)
	})

	t.Run("LZO - unsupported", func(t *testing.T) {
		data := []byte("test data")
		_, err := decompressPageData(data, parquet.CompressionCodec_LZO, 10)
		require.Error(t, err)
		require.Contains(t, err.Error(), "LZO compression not supported")
	})

	t.Run("BROTLI - not implemented", func(t *testing.T) {
		data := []byte("test data")
		_, err := decompressPageData(data, parquet.CompressionCodec_BROTLI, 10)
		require.Error(t, err)
		require.Contains(t, err.Error(), "brotli decompression not implemented")
	})

	t.Run("Unknown codec - error", func(t *testing.T) {
		data := []byte("test data")
		_, err := decompressPageData(data, parquet.CompressionCodec(999), 10)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported compression codec")
	})
}
