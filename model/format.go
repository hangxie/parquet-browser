package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/hangxie/parquet-go/v2/encoding"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/types"
)

func init() {
	// Configure geospatial rendering to use GeoJSON for both GEOMETRY and GEOGRAPHY types
	// This provides human-readable coordinate data instead of hex-encoded WKB
	types.SetGeometryJSONMode(types.GeospatialModeGeoJSON)
	types.SetGeographyJSONMode(types.GeospatialModeGeoJSON)
}

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

	// Get precision and scale for numeric types
	var precision, scale int
	if schemaElem != nil {
		precision = int(schemaElem.GetPrecision())
		scale = int(schemaElem.GetScale())
	}

	// Convert to JSON type with logical type support
	var convertedType *parquet.ConvertedType
	var logicalType *parquet.LogicalType
	if schemaElem != nil {
		convertedType = schemaElem.ConvertedType
		logicalType = schemaElem.LogicalType
	}

	jsonValue := types.ParquetTypeToJSONTypeWithLogical(
		rawValue,
		&columnMeta.Type,
		convertedType,
		logicalType,
		precision,
		scale,
	)

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

	// Handle byte array types specially
	if parquetType == parquet.Type_BYTE_ARRAY || parquetType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		return string(value)
	}

	// Use encoding.ReadPlain for other types
	buf := bytes.NewReader(value)
	vals, err := encoding.ReadPlain(buf, parquetType, 1, 0)
	if err != nil {
		return fmt.Sprintf("failed to read data as %s: %v", parquetType.String(), err)
	}
	if len(vals) == 0 {
		return nil
	}
	return vals[0]
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

	// Get precision and scale for numeric types
	var precision, scale int
	var convertedType *parquet.ConvertedType
	var logicalType *parquet.LogicalType

	if schemaElem != nil {
		precision = int(schemaElem.GetPrecision())
		scale = int(schemaElem.GetScale())
		convertedType = schemaElem.ConvertedType
		logicalType = schemaElem.LogicalType
	}

	// Use parquet-go's type conversion function
	formattedVal := types.ParquetTypeToJSONTypeWithLogical(
		val,
		&parquetType,
		convertedType,
		logicalType,
		precision,
		scale,
	)

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
