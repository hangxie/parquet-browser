package model

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v2/parquet"
)

// countLeafColumns counts only leaf columns (columns with Type field) in the schema
func countLeafColumns(schema []*parquet.SchemaElement) int {
	count := 0
	for _, elem := range schema {
		// Only count elements with Type field set (leaf columns)
		if elem.IsSetType() {
			count++
		}
	}
	return count
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

// formatLogicalType formats the logical type for display
func formatLogicalType(logicalType *parquet.LogicalType) string {
	if logicalType == nil {
		return "-"
	}

	// LogicalType is a union type, check which field is set
	if logicalType.IsSetSTRING() {
		return "STRING"
	}
	if logicalType.IsSetMAP() {
		return "MAP"
	}
	if logicalType.IsSetLIST() {
		return "LIST"
	}
	if logicalType.IsSetENUM() {
		return "ENUM"
	}
	if logicalType.IsSetDECIMAL() {
		decimal := logicalType.DECIMAL
		return fmt.Sprintf("DECIMAL(%d,%d)", decimal.Precision, decimal.Scale)
	}
	if logicalType.IsSetDATE() {
		return "DATE"
	}
	if logicalType.IsSetTIME() {
		time := logicalType.TIME
		unit := formatTimeUnit(time.Unit)
		adjusted := "UTC"
		if !time.IsAdjustedToUTC {
			adjusted = "local"
		}
		return fmt.Sprintf("TIME(%s,%s)", unit, adjusted)
	}
	if logicalType.IsSetTIMESTAMP() {
		ts := logicalType.TIMESTAMP
		unit := formatTimeUnit(ts.Unit)
		adjusted := "UTC"
		if !ts.IsAdjustedToUTC {
			adjusted = "local"
		}
		return fmt.Sprintf("TIMESTAMP(%s,%s)", unit, adjusted)
	}
	if logicalType.IsSetINTEGER() {
		integer := logicalType.INTEGER
		sign := "signed"
		if !integer.IsSigned {
			sign = "unsigned"
		}
		return fmt.Sprintf("INTEGER(%d,%s)", integer.BitWidth, sign)
	}
	if logicalType.IsSetUNKNOWN() {
		return "UNKNOWN"
	}
	if logicalType.IsSetJSON() {
		return "JSON"
	}
	if logicalType.IsSetBSON() {
		return "BSON"
	}
	if logicalType.IsSetUUID() {
		return "UUID"
	}
	if logicalType.IsSetFLOAT16() {
		return "FLOAT16"
	}
	if logicalType.IsSetVARIANT() {
		return "VARIANT"
	}
	if logicalType.IsSetGEOMETRY() {
		return "GEOMETRY"
	}
	if logicalType.IsSetGEOGRAPHY() {
		return "GEOGRAPHY"
	}

	return "-"
}

// formatTimeUnit formats a TimeUnit to a clean string representation
func formatTimeUnit(unit *parquet.TimeUnit) string {
	if unit == nil {
		return "unknown"
	}
	if unit.IsSetMILLIS() {
		return "MILLIS"
	}
	if unit.IsSetMICROS() {
		return "MICROS"
	}
	if unit.IsSetNANOS() {
		return "NANOS"
	}
	return "unknown"
}
