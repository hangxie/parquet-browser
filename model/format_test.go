package model

import (
	"fmt"
	"testing"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/stretchr/testify/require"
)

func Test_FormatBytes(t *testing.T) {
	tests := []struct {
		name     string
		bytes    int64
		expected string
	}{
		{"Zero bytes", 0, "0 B"},
		{"Less than 1KB", 512, "512 B"},
		{"Exactly 1KB", 1024, "1.0 KB"},
		{"1.5KB", 1536, "1.5 KB"},
		{"Exactly 1MB", 1024 * 1024, "1.0 MB"},
		{"1.2MB", 1228800, "1.2 MB"},
		{"Exactly 1GB", 1024 * 1024 * 1024, "1.0 GB"},
		{"1.5GB", 1610612736, "1.5 GB"},
		{"Large value in TB", 1024 * 1024 * 1024 * 1024, "1.0 TB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatBytes(tt.bytes)
			require.Equal(t, tt.expected, result, "FormatBytes(%d) should match", tt.bytes)
		})
	}
}

func Test_FormatStatValue(t *testing.T) {
	tests := []struct {
		name        string
		value       []byte
		parquetType parquet.Type
		expected    string
	}{
		{
			name:        "Empty value",
			value:       []byte{},
			parquetType: parquet.Type_INT32,
			expected:    "-",
		},
		{
			name:        "Nil value",
			value:       nil,
			parquetType: parquet.Type_INT32,
			expected:    "-",
		},
		{
			name:        "INT32 value",
			value:       []byte{0x01, 0x00, 0x00, 0x00}, // 1 in little-endian
			parquetType: parquet.Type_INT32,
			expected:    "1",
		},
		{
			name:        "INT64 value",
			value:       []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, // -1 in little-endian
			parquetType: parquet.Type_INT64,
			expected:    "-1",
		},
		{
			name:        "BOOLEAN true",
			value:       []byte{0x01},
			parquetType: parquet.Type_BOOLEAN,
			expected:    "true",
		},
		{
			name:        "BOOLEAN false",
			value:       []byte{0x00},
			parquetType: parquet.Type_BOOLEAN,
			expected:    "false",
		},
		{
			name:        "BYTE_ARRAY as base64",
			value:       []byte("test"),
			parquetType: parquet.Type_BYTE_ARRAY,
			expected:    "dGVzdA==", // Base64 encoded when no logical type
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatStatValue(tt.value, &parquet.ColumnMetaData{Type: tt.parquetType}, nil)
			require.Equal(t, tt.expected, result, "FormatStatValue() should match")
		})
	}
}

func Test_RetrieveRawValue(t *testing.T) {
	tests := []struct {
		name        string
		value       []byte
		parquetType parquet.Type
		expected    interface{}
	}{
		{
			name:        "Nil value",
			value:       nil,
			parquetType: parquet.Type_INT32,
			expected:    nil,
		},
		{
			name:        "BOOLEAN true",
			value:       []byte{0x01},
			parquetType: parquet.Type_BOOLEAN,
			expected:    true,
		},
		{
			name:        "BOOLEAN false",
			value:       []byte{0x00},
			parquetType: parquet.Type_BOOLEAN,
			expected:    false,
		},
		{
			name:        "INT32 positive",
			value:       []byte{0x2A, 0x00, 0x00, 0x00}, // 42
			parquetType: parquet.Type_INT32,
			expected:    int32(42),
		},
		{
			name:        "INT32 negative",
			value:       []byte{0xFF, 0xFF, 0xFF, 0xFF}, // -1
			parquetType: parquet.Type_INT32,
			expected:    int32(-1),
		},
		{
			name:        "INT64",
			value:       []byte{0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // 42
			parquetType: parquet.Type_INT64,
			expected:    int64(42),
		},
		{
			name:        "FLOAT",
			value:       []byte{0x00, 0x00, 0x80, 0x3F}, // 1.0 in IEEE 754 little-endian
			parquetType: parquet.Type_FLOAT,
			expected:    float32(1.0),
		},
		{
			name:        "DOUBLE",
			value:       []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F}, // 1.0 in IEEE 754 little-endian
			parquetType: parquet.Type_DOUBLE,
			expected:    float64(1.0),
		},
		{
			name:        "BYTE_ARRAY",
			value:       []byte("hello"),
			parquetType: parquet.Type_BYTE_ARRAY,
			expected:    "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := retrieveStatValue(tt.value, tt.parquetType)
			require.Equal(t, tt.expected, result, "retrieveStatValue() should match")
		})
	}
}

func Test_IsValidUTF8(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "Valid ASCII",
			input:    "hello world",
			expected: true,
		},
		{
			name:     "Valid UTF-8 with special chars",
			input:    "Hello, 世界!",
			expected: true,
		},
		{
			name:     "Valid UTF-8 with emoji",
			input:    "Hello 👋",
			expected: true,
		},
		{
			name:     "Empty string",
			input:    "",
			expected: false, // total is 0
		},
		{
			name:     "Only printable chars",
			input:    "ABCabc123!@#",
			expected: true,
		},
		{
			name:     "With tabs and newlines",
			input:    "line1\n\tline2",
			expected: true,
		},
		{
			name:     "Invalid UTF-8",
			input:    string([]byte{0xFF, 0xFE}),
			expected: false,
		},
		{
			name:     "Mostly non-printable",
			input:    string([]byte{0x00, 0x01, 0x02, 0x03, 0x04}),
			expected: false, // less than 80% printable
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsValidUTF8(tt.input)
			require.Equal(t, tt.expected, result, "IsValidUTF8(%q) should match", tt.input)
		})
	}
}

func Test_FormatValue(t *testing.T) {
	tests := []struct {
		name        string
		val         interface{}
		parquetType parquet.Type
		schemaElem  *parquet.SchemaElement
		expected    string
	}{
		{
			name:        "Nil value",
			val:         nil,
			parquetType: parquet.Type_INT32,
			schemaElem:  nil,
			expected:    "NULL",
		},
		{
			name:        "Empty string",
			val:         "",
			parquetType: parquet.Type_BYTE_ARRAY,
			schemaElem:  nil,
			expected:    "",
		},
		{
			name:        "Simple string (base64 encoded without schema)",
			val:         "hello",
			parquetType: parquet.Type_BYTE_ARRAY,
			schemaElem:  nil,
			expected:    "aGVsbG8=", // Base64 encoded by parquet-go's type converter
		},
		{
			name:        "Integer value",
			val:         42,
			parquetType: parquet.Type_INT32,
			schemaElem:  nil,
			expected:    "42",
		},
		{
			name:        "Boolean true",
			val:         true,
			parquetType: parquet.Type_BOOLEAN,
			schemaElem:  nil,
			expected:    "true",
		},
		{
			name:        "Boolean false",
			val:         false,
			parquetType: parquet.Type_BOOLEAN,
			schemaElem:  nil,
			expected:    "false",
		},
		{
			name:        "Float value",
			val:         3.14,
			parquetType: parquet.Type_DOUBLE,
			schemaElem:  nil,
			expected:    "3.14",
		},
		// Note: byte arrays are formatted as arrays when passed directly
		// The actual parquet reader converts them appropriately

	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatValue(tt.val, tt.parquetType, tt.schemaElem)
			require.Equal(t, tt.expected, result, "FormatValue() should match")
		})
	}
}

func Test_FormatValue_Truncation(t *testing.T) {
	// Test that very long values are truncated
	longString := make([]byte, 300)
	for i := range longString {
		longString[i] = 'a'
	}

	result := FormatValue(string(longString), parquet.Type_BYTE_ARRAY, nil)
	require.Equal(t, 203, len(result), "FormatValue() length should be 203 (200 chars + '...')")
	require.Equal(t, "...", result[200:], "FormatValue() should end with '...'")
}

func Test_FormatValue_WithLogicalType(t *testing.T) {
	// Test STRING logical type
	stringLogicalType := &parquet.LogicalType{STRING: &parquet.StringType{}}
	schema := &parquet.SchemaElement{
		Name:        "test_string",
		LogicalType: stringLogicalType,
	}

	result := FormatValue("hello", parquet.Type_BYTE_ARRAY, schema)
	require.Equal(t, "hello", result, "FormatValue() with STRING logical type should match")
}

func Test_FindSchemaElementByPath(t *testing.T) {
	// Create a simple schema for testing
	schema := []*parquet.SchemaElement{
		{Name: "Parquet_go_root", NumChildren: intPtr(2)},
		{Name: "field1", Type: parquetTypePtr(parquet.Type_INT32)},
		{Name: "field2", Type: parquetTypePtr(parquet.Type_BYTE_ARRAY)},
	}

	tests := []struct {
		name         string
		schema       []*parquet.SchemaElement
		path         []string
		expectNil    bool
		expectedName string
	}{
		{
			name:         "Find existing field",
			schema:       schema,
			path:         []string{"field1"},
			expectNil:    false,
			expectedName: "field1",
		},
		{
			name:         "Find second field",
			schema:       schema,
			path:         []string{"field2"},
			expectNil:    false,
			expectedName: "field2",
		},
		{
			name:      "Empty path",
			schema:    schema,
			path:      []string{},
			expectNil: true,
		},
		{
			name:      "Non-existent field",
			schema:    schema,
			path:      []string{"nonexistent"},
			expectNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findSchemaElement(tt.schema, tt.path)
			if tt.expectNil {
				require.Nil(t, result, "findSchemaElement() should return nil")
			} else {
				require.NotNil(t, result, "findSchemaElement() should return non-nil")
				require.Equal(t, tt.expectedName, result.Name, "findSchemaElement() name should match")
			}
		})
	}
}

func Test_FindSchemaElementByPath_CaseInsensitive(t *testing.T) {
	schema := []*parquet.SchemaElement{
		{Name: "Parquet_go_root", NumChildren: intPtr(1)},
		{Name: "MyField", Type: parquetTypePtr(parquet.Type_INT32)},
	}

	// Should match case-insensitively
	result := findSchemaElement(schema, []string{"myfield"})
	require.NotNil(t, result, "findSchemaElement() with different case should find element")
	require.Equal(t, "MyField", result.Name, "findSchemaElement() name should match")
}

// Test retrieveStatValue error paths - when binary.Read fails
func Test_RetrieveRawValue_ErrorPaths(t *testing.T) {
	tests := []struct {
		name        string
		value       []byte
		parquetType parquet.Type
		expectError bool
	}{
		{
			name:        "BOOLEAN with insufficient data",
			value:       []byte{}, // Empty byte slice
			parquetType: parquet.Type_BOOLEAN,
			expectError: true,
		},
		{
			name:        "INT32 with insufficient data",
			value:       []byte{0x01}, // Only 1 byte, need 4
			parquetType: parquet.Type_INT32,
			expectError: true,
		},
		{
			name:        "INT32 with partial data",
			value:       []byte{0x01, 0x02}, // Only 2 bytes, need 4
			parquetType: parquet.Type_INT32,
			expectError: true,
		},
		{
			name:        "INT64 with insufficient data",
			value:       []byte{0x01, 0x02, 0x03}, // Only 3 bytes, need 8
			parquetType: parquet.Type_INT64,
			expectError: true,
		},
		{
			name:        "INT64 with partial data",
			value:       []byte{0x01, 0x02, 0x03, 0x04}, // Only 4 bytes, need 8
			parquetType: parquet.Type_INT64,
			expectError: true,
		},
		{
			name:        "FLOAT with insufficient data",
			value:       []byte{0x01, 0x02}, // Only 2 bytes, need 4
			parquetType: parquet.Type_FLOAT,
			expectError: true,
		},
		{
			name:        "DOUBLE with insufficient data",
			value:       []byte{0x01, 0x02, 0x03, 0x04}, // Only 4 bytes, need 8
			parquetType: parquet.Type_DOUBLE,
			expectError: true,
		},
		{
			name:        "DOUBLE with partial data",
			value:       []byte{0x01, 0x02, 0x03, 0x04, 0x05}, // Only 5 bytes, need 8
			parquetType: parquet.Type_DOUBLE,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := retrieveStatValue(tt.value, tt.parquetType)
			if tt.expectError {
				// Should return an error string
				resultStr, ok := result.(string)
				require.True(t, ok, "Result should be a string for error case")
				require.Contains(t, resultStr, "failed to read data as", "Error string should contain 'failed to read data as'")
			} else {
				require.NotContains(t, fmt.Sprintf("%v", result), "failed to read data as", "Result should not contain error")
			}
		})
	}
}

// Test retrieveStatValue with edge cases
func Test_RetrieveRawValue_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		value       []byte
		parquetType parquet.Type
		expected    interface{}
	}{
		{
			name:        "INT32 with exact size but zero bytes",
			value:       []byte{0x00, 0x00, 0x00, 0x00},
			parquetType: parquet.Type_INT32,
			expected:    int32(0),
		},
		{
			name:        "INT64 with exact size but zero bytes",
			value:       []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			parquetType: parquet.Type_INT64,
			expected:    int64(0),
		},
		{
			name:        "FLOAT with exact size but zero bytes",
			value:       []byte{0x00, 0x00, 0x00, 0x00},
			parquetType: parquet.Type_FLOAT,
			expected:    float32(0),
		},
		{
			name:        "DOUBLE with exact size but zero bytes",
			value:       []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			parquetType: parquet.Type_DOUBLE,
			expected:    float64(0),
		},
		{
			name:        "Empty BYTE_ARRAY",
			value:       []byte{},
			parquetType: parquet.Type_BYTE_ARRAY,
			expected:    "",
		},
		{
			name:        "FIXED_LEN_BYTE_ARRAY falls through to default",
			value:       []byte("fixed"),
			parquetType: parquet.Type_FIXED_LEN_BYTE_ARRAY,
			expected:    "fixed",
		},
		{
			name:        "INT96 falls through to default",
			value:       []byte("int96value12"),
			parquetType: parquet.Type_INT96,
			expected:    "int96value12",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := retrieveStatValue(tt.value, tt.parquetType)
			require.Equal(t, tt.expected, result, "retrieveStatValue() should match")
		})
	}
}

// Test retrieveStatValue with maximum and minimum values
func Test_RetrieveRawValue_MinMaxValues(t *testing.T) {
	tests := []struct {
		name        string
		value       []byte
		parquetType parquet.Type
		expected    interface{}
	}{
		{
			name:        "INT32 max value",
			value:       []byte{0xFF, 0xFF, 0xFF, 0x7F}, // 2147483647
			parquetType: parquet.Type_INT32,
			expected:    int32(2147483647),
		},
		{
			name:        "INT32 min value",
			value:       []byte{0x00, 0x00, 0x00, 0x80}, // -2147483648
			parquetType: parquet.Type_INT32,
			expected:    int32(-2147483648),
		},
		{
			name:        "INT64 max value",
			value:       []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}, // 9223372036854775807
			parquetType: parquet.Type_INT64,
			expected:    int64(9223372036854775807),
		},
		{
			name:        "INT64 min value",
			value:       []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80}, // -9223372036854775808
			parquetType: parquet.Type_INT64,
			expected:    int64(-9223372036854775808),
		},
		{
			name:        "FLOAT negative value",
			value:       []byte{0x00, 0x00, 0x80, 0xBF}, // -1.0
			parquetType: parquet.Type_FLOAT,
			expected:    float32(-1.0),
		},
		{
			name:        "DOUBLE negative value",
			value:       []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xBF}, // -1.0
			parquetType: parquet.Type_DOUBLE,
			expected:    float64(-1.0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := retrieveStatValue(tt.value, tt.parquetType)
			require.Equal(t, tt.expected, result, "retrieveStatValue() should match")
		})
	}
}

// Helper functions for tests
func intPtr(i int32) *int32 {
	return &i
}

func parquetTypePtr(t parquet.Type) *parquet.Type {
	return &t
}
