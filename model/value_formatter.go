package model

import (
	"encoding/base64"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/types"
)

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

	// Apply logical type conversions if schema element is available
	if schemaElem != nil {
		formattedVal := formatValueWithLogicalType(val, parquetType, schemaElem)
		// Convert to string for display
		str := fmt.Sprintf("%v", formattedVal)
		if len(str) > 200 {
			return str[:200] + "..."
		}
		return str
	}

	// No schema information - format as-is
	str := fmt.Sprintf("%v", val)
	if len(str) > 200 {
		return str[:200] + "..."
	}
	return str
}

// formatValueWithLogicalType applies logical type formatting to a value
//
//nolint:gocognit // Complex type conversion logic with many Parquet types - inherent complexity
func formatValueWithLogicalType(val interface{}, parquetType parquet.Type, schemaElem *parquet.SchemaElement) interface{} {
	if val == nil || schemaElem == nil {
		return val
	}

	// Handle INT96 (deprecated timestamp type) - must come before byte array handling
	// INT96 values come through as strings from the parquet reader
	if parquetType == parquet.Type_INT96 {
		if strVal, ok := val.(string); ok {
			return decodeValue(strVal, parquetType, schemaElem)
		}
	}

	// Handle string values with logical/converted types (from parquet reader)
	// This handles BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY types that come through as strings
	if strVal, ok := val.(string); ok {
		if schemaElem.LogicalType != nil || schemaElem.ConvertedType != nil {
			// Apply logical type conversion
			decoded := decodeValue(strVal, parquetType, schemaElem)
			if decoded != strVal {
				// If decoding changed the value, use the decoded version
				return decoded
			}
		}
	}

	// Handle byte arrays with logical/converted types
	if bytes, ok := val.([]byte); ok {
		// If it has a logical or converted type, apply the conversion
		if schemaElem.LogicalType != nil || schemaElem.ConvertedType != nil {
			// Convert the value using the same logic as min/max values
			decoded := decodeValue(string(bytes), parquetType, schemaElem)
			if decoded != string(bytes) {
				// If decoding changed the value, use the decoded version
				return decoded
			}
		}

		// No logical type or conversion didn't change it - treat as string/binary
		str := string(bytes)
		if isValidUTF8String(str) {
			return str
		}
		// If not valid UTF-8, show as hex for small values
		if len(bytes) <= 32 {
			return fmt.Sprintf("0x%X", bytes)
		}
		return fmt.Sprintf("<binary:%d bytes>", len(bytes))
	}

	// For non-byte-array types, check if we need to apply logical type formatting
	if schemaElem.LogicalType != nil {
		// Handle special display formatting for certain logical types
		switch {
		case schemaElem.LogicalType.IsSetDECIMAL():
			// If it's already a numeric type, format with proper decimal precision
			if schemaElem.Scale != nil && *schemaElem.Scale > 0 {
				switch v := val.(type) {
				case int32:
					return decodeValue(v, parquetType, schemaElem)
				case int64:
					return decodeValue(v, parquetType, schemaElem)
				case float32, float64:
					// Already a float, format with precision
					return val
				}
			}
		case schemaElem.LogicalType.IsSetDATE():
			if i32Val, ok := val.(int32); ok {
				return decodeValue(i32Val, parquetType, schemaElem)
			}
		case schemaElem.LogicalType.IsSetTIMESTAMP():
			if i64Val, ok := val.(int64); ok {
				return decodeValue(i64Val, parquetType, schemaElem)
			}
		case schemaElem.LogicalType.IsSetTIME():
			if i32Val, ok := val.(int32); ok {
				return decodeValue(i32Val, parquetType, schemaElem)
			}
			if i64Val, ok := val.(int64); ok {
				return decodeValue(i64Val, parquetType, schemaElem)
			}
		case schemaElem.LogicalType.IsSetUUID():
			if strVal, ok := val.(string); ok {
				return decodeValue(strVal, parquetType, schemaElem)
			}
		}
	}

	// Handle converted types (backward compatibility)
	if schemaElem.ConvertedType != nil {
		switch *schemaElem.ConvertedType {
		case parquet.ConvertedType_INTERVAL:
			if strVal, ok := val.(string); ok {
				return decodeValue(strVal, parquetType, schemaElem)
			}
		case parquet.ConvertedType_DECIMAL:
			// DECIMAL can be stored as BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY
			if parquetType == parquet.Type_BYTE_ARRAY || parquetType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
				if strVal, ok := val.(string); ok {
					return decodeValue(strVal, parquetType, schemaElem)
				}
			}
			// Apply decimal conversion for int32/int64 values
			switch v := val.(type) {
			case int32:
				return decodeValue(v, parquetType, schemaElem)
			case int64:
				return decodeValue(v, parquetType, schemaElem)
			}
		case parquet.ConvertedType_DATE:
			if i32Val, ok := val.(int32); ok {
				return decodeValue(i32Val, parquetType, schemaElem)
			}
		case parquet.ConvertedType_TIMESTAMP_MILLIS, parquet.ConvertedType_TIMESTAMP_MICROS:
			if i64Val, ok := val.(int64); ok {
				return decodeValue(i64Val, parquetType, schemaElem)
			}
		case parquet.ConvertedType_TIME_MILLIS:
			if i32Val, ok := val.(int32); ok {
				return decodeValue(i32Val, parquetType, schemaElem)
			}
		case parquet.ConvertedType_TIME_MICROS:
			if i64Val, ok := val.(int64); ok {
				return decodeValue(i64Val, parquetType, schemaElem)
			}
		}
	}

	// Return value as-is if no special formatting needed
	return val
}

// decodeValue decodes a value based on logical/converted type
//
//nolint:gocognit // Complex type conversion logic with many Parquet types - inherent complexity
func decodeValue(value any, parquetType parquet.Type, schemaElem *parquet.SchemaElement) any {
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

// isValidUTF8String checks if a string contains valid and mostly printable UTF-8
func isValidUTF8String(s string) bool {
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

// findSchemaElementByPath finds the schema element for a given path
//
//nolint:gocognit // Complex schema traversal logic with many edge cases - inherent complexity
func findSchemaElementByPath(schema []*parquet.SchemaElement, pathInSchema []string) *parquet.SchemaElement {
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
