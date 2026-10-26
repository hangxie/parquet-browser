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

// FormatStatValue formats a min/max statistic for display based on the column's types
func FormatStatValue(value []byte, columnMeta *parquet.ColumnMetaData, schemaElem *parquet.SchemaElement) string {
	if len(value) == 0 {
		return "-"
	}

	// Min/max are meaningless for geospatial and interval types.
	if schemaElem != nil {
		if schemaElem.LogicalType != nil {
			if schemaElem.LogicalType.IsSetGEOMETRY() || schemaElem.LogicalType.IsSetGEOGRAPHY() {
				return "-"
			}
		}
		if schemaElem.ConvertedType != nil && *schemaElem.ConvertedType == parquet.ConvertedType_INTERVAL {
			return "-"
		}
	}

	rawValue := retrieveStatValue(value, columnMeta.Type)
	if rawValue == nil {
		return "-"
	}

	se := schemaElem
	if se == nil {
		se = &parquet.SchemaElement{Type: &columnMeta.Type}
	}

	// An unrenderable value still comes back with a fallback rendering, which is what we show.
	jsonValue, _ := types.ConvertValue(rawValue, se, geospatialOpt)

	// JSON-encode maps and slices so they render readably.
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

	str := fmt.Sprintf("%v", jsonValue)
	if len(str) > 50 {
		return str[:50] + "..."
	}
	return str
}

// retrieveStatValue decodes raw statistic bytes into a Go value for the physical type
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
	if !utf8.ValidString(s) {
		return false
	}

	printable := 0
	total := 0
	for _, r := range s {
		total++
		if unicode.IsPrint(r) || unicode.IsSpace(r) {
			printable++
		}
	}

	return total > 0 && (printable*100/total >= 80)
}

// FormatValue formats a page value for display, applying logical type conversions
func FormatValue(val interface{}, parquetType parquet.Type, schemaElem *parquet.SchemaElement) string {
	if val == nil {
		return "NULL"
	}

	// An empty string is a value, not NULL.
	if str, ok := val.(string); ok && str == "" {
		return ""
	}

	se := schemaElem
	if se == nil {
		se = &parquet.SchemaElement{Type: &parquetType}
	}

	// An unrenderable value still comes back with a fallback rendering, which is what we show.
	formattedVal, _ := types.ConvertValue(val, se, geospatialOpt)

	// JSON-encode maps and slices so they render readably.
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

	str := fmt.Sprintf("%v", formattedVal)
	if len(str) > 200 {
		return str[:200] + "..."
	}
	return str
}
