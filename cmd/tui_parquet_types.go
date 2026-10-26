package cmd

import (
	"github.com/hangxie/parquet-go/v3/parquet"
)

// parsePhysicalType converts a string to parquet.Type
func parsePhysicalType(typeStr string) parquet.Type {
	switch typeStr {
	case "BOOLEAN":
		return parquet.Type_BOOLEAN
	case "INT32":
		return parquet.Type_INT32
	case "INT64":
		return parquet.Type_INT64
	case "INT96":
		return parquet.Type_INT96
	case "FLOAT":
		return parquet.Type_FLOAT
	case "DOUBLE":
		return parquet.Type_DOUBLE
	case "BYTE_ARRAY":
		return parquet.Type_BYTE_ARRAY
	case "FIXED_LEN_BYTE_ARRAY":
		return parquet.Type_FIXED_LEN_BYTE_ARRAY
	default:
		return parquet.Type_BYTE_ARRAY // Default fallback
	}
}

// parseCompressionCodec converts a string to parquet.CompressionCodec
func parseCompressionCodec(codecStr string) parquet.CompressionCodec {
	switch codecStr {
	case "UNCOMPRESSED":
		return parquet.CompressionCodec_UNCOMPRESSED
	case "SNAPPY":
		return parquet.CompressionCodec_SNAPPY
	case "GZIP":
		return parquet.CompressionCodec_GZIP
	case "LZO":
		return parquet.CompressionCodec_LZO
	case "BROTLI":
		return parquet.CompressionCodec_BROTLI
	case "LZ4":
		return parquet.CompressionCodec_LZ4
	case "ZSTD":
		return parquet.CompressionCodec_ZSTD
	case "LZ4_RAW":
		return parquet.CompressionCodec_LZ4_RAW
	default:
		return parquet.CompressionCodec_UNCOMPRESSED // Default fallback
	}
}
