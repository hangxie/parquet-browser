package cmd

import (
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/stretchr/testify/assert"
)

func Test_parsePhysicalType(t *testing.T) {
	tests := []struct {
		name     string
		typeStr  string
		expected parquet.Type
	}{
		{
			name:     "BOOLEAN type",
			typeStr:  "BOOLEAN",
			expected: parquet.Type_BOOLEAN,
		},
		{
			name:     "INT32 type",
			typeStr:  "INT32",
			expected: parquet.Type_INT32,
		},
		{
			name:     "INT64 type",
			typeStr:  "INT64",
			expected: parquet.Type_INT64,
		},
		{
			name:     "INT96 type",
			typeStr:  "INT96",
			expected: parquet.Type_INT96,
		},
		{
			name:     "FLOAT type",
			typeStr:  "FLOAT",
			expected: parquet.Type_FLOAT,
		},
		{
			name:     "DOUBLE type",
			typeStr:  "DOUBLE",
			expected: parquet.Type_DOUBLE,
		},
		{
			name:     "BYTE_ARRAY type",
			typeStr:  "BYTE_ARRAY",
			expected: parquet.Type_BYTE_ARRAY,
		},
		{
			name:     "FIXED_LEN_BYTE_ARRAY type",
			typeStr:  "FIXED_LEN_BYTE_ARRAY",
			expected: parquet.Type_FIXED_LEN_BYTE_ARRAY,
		},
		{
			name:     "Unknown type defaults to BYTE_ARRAY",
			typeStr:  "UNKNOWN",
			expected: parquet.Type_BYTE_ARRAY,
		},
		{
			name:     "Empty string defaults to BYTE_ARRAY",
			typeStr:  "",
			expected: parquet.Type_BYTE_ARRAY,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parsePhysicalType(tt.typeStr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func Test_parseCompressionCodec(t *testing.T) {
	tests := []struct {
		name     string
		codecStr string
		expected parquet.CompressionCodec
	}{
		{
			name:     "UNCOMPRESSED codec",
			codecStr: "UNCOMPRESSED",
			expected: parquet.CompressionCodec_UNCOMPRESSED,
		},
		{
			name:     "SNAPPY codec",
			codecStr: "SNAPPY",
			expected: parquet.CompressionCodec_SNAPPY,
		},
		{
			name:     "GZIP codec",
			codecStr: "GZIP",
			expected: parquet.CompressionCodec_GZIP,
		},
		{
			name:     "LZO codec",
			codecStr: "LZO",
			expected: parquet.CompressionCodec_LZO,
		},
		{
			name:     "BROTLI codec",
			codecStr: "BROTLI",
			expected: parquet.CompressionCodec_BROTLI,
		},
		{
			name:     "LZ4 codec",
			codecStr: "LZ4",
			expected: parquet.CompressionCodec_LZ4,
		},
		{
			name:     "ZSTD codec",
			codecStr: "ZSTD",
			expected: parquet.CompressionCodec_ZSTD,
		},
		{
			name:     "LZ4_RAW codec",
			codecStr: "LZ4_RAW",
			expected: parquet.CompressionCodec_LZ4_RAW,
		},
		{
			name:     "Unknown codec defaults to UNCOMPRESSED",
			codecStr: "UNKNOWN",
			expected: parquet.CompressionCodec_UNCOMPRESSED,
		},
		{
			name:     "Empty string defaults to UNCOMPRESSED",
			codecStr: "",
			expected: parquet.CompressionCodec_UNCOMPRESSED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseCompressionCodec(tt.codecStr)
			assert.Equal(t, tt.expected, result)
		})
	}
}
