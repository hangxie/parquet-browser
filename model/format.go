package model

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"unicode"
	"unicode/utf8"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/types"
)

var geospatialOpt = types.WithGeospatialConfig(types.NewGeospatialConfig(
	types.WithGeometryJSONMode(types.GeospatialModeGeoJSON),
	types.WithGeographyJSONMode(types.GeospatialModeGeoJSON),
))

// FormatBytes formats bytes as human readable size
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// FormatStatValue formats statistics values (min/max) based on column type information
// This uses parquet-go's types.ParquetTypeToJSONTypeWithLogical function
func FormatStatValue(value []byte, columnMeta *parquet.ColumnMetaData, schemaElem *parquet.SchemaElement) string {
	if len(value) == 0 {
		return "-"
	}

	// Check if this is a type where min/max don't apply (geospatial or interval types)
	// These types don't have meaningful min/max values in their statistics
	if schemaElem != nil {
		// Check for geospatial types (GEOMETRY or GEOGRAPHY)
		if schemaElem.LogicalType != nil {
			if schemaElem.LogicalType.IsSetGEOMETRY() || schemaElem.LogicalType.IsSetGEOGRAPHY() {
				return "-"
			}
		}
		// Check for interval type (deprecated converted type)
		if schemaElem.ConvertedType != nil && *schemaElem.ConvertedType == parquet.ConvertedType_INTERVAL {
			return "-"
		}
	}

	// Retrieve the raw value from bytes
	rawValue := retrieveStatValue(value, columnMeta.Type)
	if rawValue == nil {
		return "-"
	}

	// Build a SchemaElement for type conversion
	se := schemaElem
	if se == nil {
		se = &parquet.SchemaElement{Type: &columnMeta.Type}
	}

	jsonValue := types.ConvertToJSONType(rawValue, se, geospatialOpt)

	// Format for display
	// For complex types (maps, slices), use JSON encoding for proper formatting
	switch jsonValue.(type) {
	case map[string]any, []any, []map[string]any:
		if jsonBytes, err := json.Marshal(jsonValue); err == nil {
			str := string(jsonBytes)
			if len(str) > 50 {
				return str[:50] + "..."
			}
			return str
		}
	}

	// For simple types, use standard formatting
	str := fmt.Sprintf("%v", jsonValue)
	if len(str) > 50 {
		return str[:50] + "..."
	}
	return str
}

// retrieveStatValue converts raw statistic bytes to Go type based on Parquet physical type
// This is similar to parquet-tools' retrieveValue function
func retrieveStatValue(value []byte, parquetType parquet.Type) any {
	if value == nil {
		return nil
	}

	switch parquetType {
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return string(value)
	case parquet.Type_BOOLEAN:
		if len(value) < 1 {
			return fmt.Sprintf("failed to read data as %s: insufficient bytes", parquetType.String())
		}
		return value[0]&1 != 0
	case parquet.Type_INT32:
		if len(value) < 4 {
			return fmt.Sprintf("failed to read data as %s: insufficient bytes", parquetType.String())
		}
		return int32(binary.LittleEndian.Uint32(value[:4]))
	case parquet.Type_INT64:
		if len(value) < 8 {
			return fmt.Sprintf("failed to read data as %s: insufficient bytes", parquetType.String())
		}
		return int64(binary.LittleEndian.Uint64(value[:8]))
	case parquet.Type_INT96:
		if len(value) < 12 {
			return fmt.Sprintf("failed to read data as %s: insufficient bytes", parquetType.String())
		}
		return string(value[:12])
	case parquet.Type_FLOAT:
		if len(value) < 4 {
			return fmt.Sprintf("failed to read data as %s: insufficient bytes", parquetType.String())
		}
		return math.Float32frombits(binary.LittleEndian.Uint32(value[:4]))
	case parquet.Type_DOUBLE:
		if len(value) < 8 {
			return fmt.Sprintf("failed to read data as %s: insufficient bytes", parquetType.String())
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(value[:8]))
	default:
		return fmt.Sprintf("unsupported type: %s", parquetType.String())
	}
}

// IsValidUTF8 checks if a string contains valid and mostly printable UTF-8
func IsValidUTF8(s string) bool {
	// Check if valid UTF-8
	if !utf8.ValidString(s) {
		return false
	}

	// Count printable characters
	printable := 0
	total := 0
	for _, r := range s {
		total++
		if unicode.IsPrint(r) || unicode.IsSpace(r) {
			printable++
		}
	}

	// Require at least 80% printable characters
	return total > 0 && (printable*100/total >= 80)
}

// FormatValue formats a value for display, applying logical type conversions
// This is the main entry point for formatting page content values
func FormatValue(val interface{}, parquetType parquet.Type, schemaElem *parquet.SchemaElement) string {
	if val == nil {
		return "NULL"
	}

	// Check for empty string - should display as empty, not NULL
	if str, ok := val.(string); ok && str == "" {
		return ""
	}

	// Build a SchemaElement for type conversion
	se := schemaElem
	if se == nil {
		se = &parquet.SchemaElement{Type: &parquetType}
	}

	// Use parquet-go's type conversion function
	formattedVal := types.ConvertToJSONType(val, se, geospatialOpt)

	// Convert to string for display
	// For complex types (maps, slices), use JSON encoding for proper formatting
	switch formattedVal.(type) {
	case map[string]any, []any, []map[string]any:
		if jsonBytes, err := json.Marshal(formattedVal); err == nil {
			str := string(jsonBytes)
			if len(str) > 200 {
				return str[:200] + "..."
			}
			return str
		}
	}

	// For simple types, use standard formatting
	str := fmt.Sprintf("%v", formattedVal)
	if len(str) > 200 {
		return str[:200] + "..."
	}
	return str
}
