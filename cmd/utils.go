package cmd

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/types"
)

// formatStatValue formats statistics values (min/max) for display
// This is a simple version for backward compatibility - use formatStatValueWithType for proper type interpretation
func formatStatValue(value []byte) string {
	if len(value) == 0 {
		return "-"
	}

	// Try to interpret as UTF-8 string
	str := string(value)

	// Check if it's valid UTF-8 and printable
	if !isValidUTF8(str) {
		// If not valid UTF-8, show as hex for small values, or just indicate binary
		if len(value) <= 8 {
			return fmt.Sprintf("0x%X", value)
		}
		return fmt.Sprintf("<binary:%d bytes>", len(value))
	}

	// Limit to 50 chars for display
	if len(str) > 50 {
		return str[:50] + "..."
	}
	return str
}

// formatStatValueWithType formats statistics values (min/max) based on column type information
// This mimics how parquet-tools interprets min/max values
func formatStatValueWithType(value []byte, columnMeta *parquet.ColumnMetaData, schemaElem *parquet.SchemaElement) string {
	if len(value) == 0 {
		return "-"
	}

	// First, retrieve the raw value based on physical type
	rawValue := retrieveRawValue(value, columnMeta.Type)
	if rawValue == nil {
		return "-"
	}

	// Then decode it based on logical/converted type
	decodedValue := decodeStatValue(rawValue, columnMeta.Type, schemaElem)

	// Format for display
	return formatDecodedValue(decodedValue)
}

// retrieveRawValue converts raw bytes to Go type based on Parquet physical type
func retrieveRawValue(value []byte, parquetType parquet.Type) any {
	if value == nil {
		return nil
	}

	buf := bytes.NewReader(value)
	switch parquetType {
	case parquet.Type_BOOLEAN:
		var b bool
		if err := binary.Read(buf, binary.LittleEndian, &b); err != nil {
			return fmt.Sprintf("error: %v", err)
		}
		return b
	case parquet.Type_INT32:
		var i32 int32
		if err := binary.Read(buf, binary.LittleEndian, &i32); err != nil {
			return fmt.Sprintf("error: %v", err)
		}
		return i32
	case parquet.Type_INT64:
		var i64 int64
		if err := binary.Read(buf, binary.LittleEndian, &i64); err != nil {
			return fmt.Sprintf("error: %v", err)
		}
		return i64
	case parquet.Type_FLOAT:
		var f32 float32
		if err := binary.Read(buf, binary.LittleEndian, &f32); err != nil {
			return fmt.Sprintf("error: %v", err)
		}
		return f32
	case parquet.Type_DOUBLE:
		var f64 float64
		if err := binary.Read(buf, binary.LittleEndian, &f64); err != nil {
			return fmt.Sprintf("error: %v", err)
		}
		return f64
	}
	return string(value)
}

// decodeStatValue decodes a value based on logical/converted type
//
//nolint:gocognit // Complex type conversion logic with many Parquet types - inherent complexity
func decodeStatValue(value any, parquetType parquet.Type, schemaElem *parquet.SchemaElement) any {
	if value == nil || schemaElem == nil {
		return value
	}

	// Handle INT96 (deprecated timestamp type)
	if parquetType == parquet.Type_INT96 {
		if strVal, ok := value.(string); ok {
			return types.INT96ToTime(strVal)
		}
		return value
	}

	// Handle BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY without logical/converted type
	if (parquetType == parquet.Type_BYTE_ARRAY || parquetType == parquet.Type_FIXED_LEN_BYTE_ARRAY) &&
		schemaElem.ConvertedType == nil && schemaElem.LogicalType == nil {
		if strVal, ok := value.(string); ok {
			return base64.StdEncoding.EncodeToString([]byte(strVal))
		}
		return value
	}

	// Handle converted types (backward compatibility)
	if schemaElem.ConvertedType != nil {
		switch *schemaElem.ConvertedType {
		case parquet.ConvertedType_DECIMAL:
			precision := int32(10) // default
			scale := int32(0)      // default
			if schemaElem.Precision != nil {
				precision = *schemaElem.Precision
			}
			if schemaElem.Scale != nil {
				scale = *schemaElem.Scale
			}
			return types.ConvertDecimalValue(value, &parquetType, int(precision), int(scale))
		case parquet.ConvertedType_DATE:
			return types.ConvertDateLogicalValue(value)
		case parquet.ConvertedType_TIME_MICROS, parquet.ConvertedType_TIME_MILLIS:
			if schemaElem.LogicalType != nil && schemaElem.LogicalType.TIME != nil {
				return types.ConvertTimeLogicalValue(value, schemaElem.LogicalType.GetTIME())
			}
			return value
		case parquet.ConvertedType_TIMESTAMP_MICROS, parquet.ConvertedType_TIMESTAMP_MILLIS:
			return types.ConvertTimestampValue(value, *schemaElem.ConvertedType)
		case parquet.ConvertedType_INTERVAL:
			if strVal, ok := value.(string); ok {
				return types.IntervalToString([]byte(strVal))
			}
			return value
		case parquet.ConvertedType_BSON:
			return types.ConvertBSONLogicalValue(value)
		}
	}

	// Handle logical types
	if schemaElem.LogicalType != nil {
		switch {
		case schemaElem.LogicalType.IsSetDECIMAL():
			precision := int32(10) // default
			scale := int32(0)      // default
			if schemaElem.Precision != nil {
				precision = *schemaElem.Precision
			}
			if schemaElem.Scale != nil {
				scale = *schemaElem.Scale
			}
			return types.ConvertDecimalValue(value, &parquetType, int(precision), int(scale))
		case schemaElem.LogicalType.IsSetDATE():
			return types.ConvertDateLogicalValue(value)
		case schemaElem.LogicalType.IsSetTIME():
			return types.ConvertTimeLogicalValue(value, schemaElem.LogicalType.GetTIME())
		case schemaElem.LogicalType.IsSetTIMESTAMP():
			if i64Val, ok := value.(int64); ok {
				if schemaElem.LogicalType.TIMESTAMP.Unit.IsSetMILLIS() {
					return types.TIMESTAMP_MILLISToISO8601(i64Val, false)
				}
				if schemaElem.LogicalType.TIMESTAMP.Unit.IsSetMICROS() {
					return types.TIMESTAMP_MICROSToISO8601(i64Val, false)
				}
				return types.TIMESTAMP_NANOSToISO8601(i64Val, false)
			}
			return value
		case schemaElem.LogicalType.IsSetUUID():
			return types.ConvertUUIDValue(value)
		case schemaElem.LogicalType.IsSetBSON():
			return types.ConvertBSONLogicalValue(value)
		case schemaElem.LogicalType.IsSetFLOAT16():
			return types.ConvertFloat16LogicalValue(value)
		}
	}

	return value
}

// formatDecodedValue formats a decoded value for display
func formatDecodedValue(value any) string {
	if value == nil {
		return "-"
	}

	// Handle different types
	switch v := value.(type) {
	case string:
		// Limit string length for display
		if len(v) > 50 {
			return v[:50] + "..."
		}
		return v
	case int, int32, int64, uint, uint32, uint64:
		return fmt.Sprintf("%v", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case bool:
		return fmt.Sprintf("%v", v)
	default:
		// For complex types, use JSON marshaling
		str := fmt.Sprintf("%v", v)
		if len(str) > 50 {
			return str[:50] + "..."
		}
		return str
	}
}

// isValidUTF8 checks if a string contains valid and mostly printable UTF-8
func isValidUTF8(s string) bool {
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

func formatBytes(bytes int64) string {
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

func getTotalSize(rg *parquet.RowGroup) int64 {
	total := int64(0)
	for _, col := range rg.Columns {
		total += col.MetaData.TotalCompressedSize
	}
	return total
}

// countLeafColumns counts only leaf columns (columns with Type field) in the schema
// This excludes group nodes like LIST, MAP, and STRUCT which don't have actual data
func countLeafColumns(schema []*parquet.SchemaElement) int {
	count := 0
	for _, elem := range schema {
		// Leaf columns have a Type field set
		if elem.Type != nil {
			count++
		}
	}
	return count
}

// formatPathInSchema formats a path in schema for display
func formatPathInSchema(pathInSchema []string) string {
	return strings.Join(pathInSchema, ".")
}

// findSchemaElement finds the schema element for a given path
//
//nolint:gocognit // Complex path matching with stack-based tree traversal - inherent complexity
func findSchemaElement(schema []*parquet.SchemaElement, pathInSchema []string) *parquet.SchemaElement {
	if len(pathInSchema) == 0 || len(schema) == 0 {
		return nil
	}

	// The schema is stored as a flat list in depth-first pre-order traversal
	// We need to reconstruct paths to find the correct element

	// Build a stack-based traversal to match the full path
	type stackEntry struct {
		path       []string
		childCount int
	}

	var stack []stackEntry
	var candidates []*parquet.SchemaElement

	for _, elem := range schema {
		// Skip root element
		if elem.Name == "Parquet_go_root" || elem.Name == "" {
			continue
		}

		// Pop completed parent nodes from stack
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			if top.childCount > 0 {
				top.childCount--
				break
			}
			stack = stack[:len(stack)-1]
		}

		// Build current path
		currentPath := make([]string, 0, len(stack)+1)
		for _, entry := range stack {
			currentPath = append(currentPath, entry.path[len(entry.path)-1])
		}
		currentPath = append(currentPath, elem.Name)

		// Check if this matches our target path
		if len(currentPath) == len(pathInSchema) {
			match := true
			for i := range pathInSchema {
				// Case-insensitive match to handle Key_value vs key_value
				if !strings.EqualFold(pathInSchema[i], currentPath[i]) {
					match = false
					break
				}
			}
			if match {
				candidates = append(candidates, elem)
			}
		}

		// Push current element to stack if it has children
		childCount := 0
		if elem.NumChildren != nil {
			childCount = int(*elem.NumChildren)
		}
		if childCount > 0 {
			stack = append(stack, stackEntry{
				path:       currentPath,
				childCount: childCount,
			})
		}
	}

	// Return the first matching candidate
	if len(candidates) > 0 {
		return candidates[0]
	}

	// Fallback: match just the leaf name (for backward compatibility with simple schemas)
	leafName := pathInSchema[len(pathInSchema)-1]
	for _, elem := range schema {
		if strings.EqualFold(elem.Name, leafName) {
			return elem
		}
	}

	return nil
}
