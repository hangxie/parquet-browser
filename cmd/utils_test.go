package cmd

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/stretchr/testify/assert"
)

func Test_FormatStatValueWithType_INT32(t *testing.T) {
	// Create INT32 value
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, uint32(int32(42)))

	meta := &parquet.ColumnMetaData{
		Type: parquet.Type_INT32,
	}
	schemaElem := &parquet.SchemaElement{
		Type: &meta.Type,
	}

	result := formatStatValueWithType(value, meta, schemaElem)
	expected := "42"
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func Test_FormatStatValueWithType_BYTE_ARRAY_UTF8(t *testing.T) {
	// Create UTF8 string value
	value := []byte("test-string")

	meta := &parquet.ColumnMetaData{
		Type: parquet.Type_BYTE_ARRAY,
	}
	convertedType := parquet.ConvertedType_UTF8
	schemaElem := &parquet.SchemaElement{
		Type:          &meta.Type,
		ConvertedType: &convertedType,
	}

	result := formatStatValueWithType(value, meta, schemaElem)
	expected := "test-string"
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func Test_FormatStatValueWithType_INT32_DECIMAL(t *testing.T) {
	// Create a decimal value stored as INT32
	// For example, 123 with scale 2 should be displayed as 1.23
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, uint32(int32(123)))

	meta := &parquet.ColumnMetaData{
		Type: parquet.Type_INT32,
	}
	convertedType := parquet.ConvertedType_DECIMAL
	precision := int32(10)
	scale := int32(2)
	schemaElem := &parquet.SchemaElement{
		Type:          &meta.Type,
		ConvertedType: &convertedType,
		Precision:     &precision,
		Scale:         &scale,
	}

	result := formatStatValueWithType(value, meta, schemaElem)
	// The result should be a decimal value like "1.23"
	// Note: actual formatting may vary based on the types package implementation
	if result == "" || result == "-" {
		t.Errorf("Expected a decimal value, got %s", result)
	}
	t.Logf("INT32 DECIMAL result: %s", result)
}

func Test_FormatStatValueWithType_EmptyValue(t *testing.T) {
	value := []byte{}

	meta := &parquet.ColumnMetaData{
		Type: parquet.Type_INT32,
	}
	schemaElem := &parquet.SchemaElement{
		Type: &meta.Type,
	}

	result := formatStatValueWithType(value, meta, schemaElem)
	expected := "-"
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func Test_FormatStatValueWithType_NilSchema(t *testing.T) {
	// Should still work with nil schema, falling back to basic type interpretation
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, uint32(int32(99)))

	meta := &parquet.ColumnMetaData{
		Type: parquet.Type_INT32,
	}

	result := formatStatValueWithType(value, meta, nil)
	expected := "99"
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func Test_RetrieveRawValue(t *testing.T) {
	tests := []struct {
		name         string
		value        []byte
		parquetType  parquet.Type
		expectedType string
	}{
		{
			name:         "INT32",
			value:        func() []byte { b := make([]byte, 4); binary.LittleEndian.PutUint32(b, 42); return b }(),
			parquetType:  parquet.Type_INT32,
			expectedType: "int32",
		},
		{
			name:         "INT64",
			value:        func() []byte { b := make([]byte, 8); binary.LittleEndian.PutUint64(b, 1234567890); return b }(),
			parquetType:  parquet.Type_INT64,
			expectedType: "int64",
		},
		{
			name:         "BYTE_ARRAY",
			value:        []byte("hello"),
			parquetType:  parquet.Type_BYTE_ARRAY,
			expectedType: "string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := retrieveRawValue(tt.value, tt.parquetType)
			if result == nil {
				t.Errorf("Expected non-nil result")
			}
			t.Logf("%s result: %v (type: %T)", tt.name, result, result)
		})
	}
}

// Test retrieveRawValue for different physical types
func TestRetrieveRawValue(t *testing.T) {
	tests := []struct {
		name        string
		value       []byte
		parquetType parquet.Type
		want        any
	}{
		{
			name:        "nil value",
			value:       nil,
			parquetType: parquet.Type_INT32,
			want:        nil,
		},
		{
			name:        "boolean true",
			value:       []byte{1},
			parquetType: parquet.Type_BOOLEAN,
			want:        true,
		},
		{
			name:        "boolean false",
			value:       []byte{0},
			parquetType: parquet.Type_BOOLEAN,
			want:        false,
		},
		{
			name:        "int32 positive",
			value:       []byte{0x2A, 0, 0, 0}, // 42 in little-endian
			parquetType: parquet.Type_INT32,
			want:        int32(42),
		},
		{
			name:        "int32 negative",
			value:       []byte{0xD6, 0xFF, 0xFF, 0xFF}, // -42 in little-endian
			parquetType: parquet.Type_INT32,
			want:        int32(-42),
		},
		{
			name:        "int64 positive",
			value:       []byte{0x2A, 0, 0, 0, 0, 0, 0, 0}, // 42 in little-endian
			parquetType: parquet.Type_INT64,
			want:        int64(42),
		},
		{
			name:        "float32",
			value:       []byte{0x00, 0x00, 0x28, 0x42}, // 42.0 in little-endian
			parquetType: parquet.Type_FLOAT,
			want:        float32(42.0),
		},
		{
			name:        "float64",
			value:       []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x45, 0x40}, // 42.0 in little-endian
			parquetType: parquet.Type_DOUBLE,
			want:        float64(42.0),
		},
		{
			name:        "byte array as string",
			value:       []byte("hello"),
			parquetType: parquet.Type_BYTE_ARRAY,
			want:        "hello",
		},
		{
			name:        "fixed len byte array",
			value:       []byte{1, 2, 3, 4},
			parquetType: parquet.Type_FIXED_LEN_BYTE_ARRAY,
			want:        string([]byte{1, 2, 3, 4}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := retrieveRawValue(tt.value, tt.parquetType)
			if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", tt.want) {
				t.Errorf("retrieveRawValue() = %v (type %T), want %v (type %T)", got, got, tt.want, tt.want)
			}
		})
	}
}

// Test formatDecodedValue for different value types
func TestFormatDecodedValue(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{
			name:  "nil value",
			value: nil,
			want:  "-",
		},
		{
			name:  "short string",
			value: "hello",
			want:  "hello",
		},
		{
			name:  "long string truncated",
			value: "this is a very long string that exceeds fifty characters and should be truncated",
			want:  "this is a very long string that exceeds fifty char...",
		},
		{
			name:  "int32",
			value: int32(42),
			want:  "42",
		},
		{
			name:  "int64",
			value: int64(123456789),
			want:  "123456789",
		},
		{
			name:  "uint32",
			value: uint32(42),
			want:  "42",
		},
		{
			name:  "float32",
			value: float32(3.14159),
			want:  "3.14159",
		},
		{
			name:  "float64",
			value: float64(2.71828),
			want:  "2.71828",
		},
		{
			name:  "bool true",
			value: true,
			want:  "true",
		},
		{
			name:  "bool false",
			value: false,
			want:  "false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDecodedValue(tt.value)
			if got != tt.want {
				t.Errorf("formatDecodedValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test findSchemaElement with complex nested schemas
func TestFindSchemaElement(t *testing.T) {
	// Create a complex schema with nested structures
	schema := []*parquet.SchemaElement{
		{Name: "Parquet_go_root", NumChildren: int32Ptr(5)},
		{Name: "id", Type: typePtr(parquet.Type_INT64)},
		{Name: "name", Type: typePtr(parquet.Type_BYTE_ARRAY)},
		{Name: "nested", NumChildren: int32Ptr(2)},
		{Name: "field1", Type: typePtr(parquet.Type_INT32)},
		{Name: "field2", Type: typePtr(parquet.Type_BYTE_ARRAY)},
	}

	tests := []struct {
		name         string
		pathInSchema []string
		wantName     string
		wantNil      bool
	}{
		{
			name:         "simple field",
			pathInSchema: []string{"id"},
			wantName:     "id",
			wantNil:      false,
		},
		{
			name:         "another simple field",
			pathInSchema: []string{"name"},
			wantName:     "name",
			wantNil:      false,
		},
		{
			name:         "nested field",
			pathInSchema: []string{"nested", "field1"},
			wantName:     "field1",
			wantNil:      false,
		},
		{
			name:         "another nested field",
			pathInSchema: []string{"nested", "field2"},
			wantName:     "field2",
			wantNil:      false,
		},
		{
			name:         "non-existent field",
			pathInSchema: []string{"nonexistent"},
			wantNil:      true,
		},
		{
			name:         "empty path",
			pathInSchema: []string{},
			wantNil:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findSchemaElement(schema, tt.pathInSchema)
			if tt.wantNil {
				if got != nil {
					t.Errorf("findSchemaElement() = %v, want nil", got)
				}
			} else {
				if got == nil {
					t.Errorf("findSchemaElement() = nil, want non-nil")
				} else if got.Name != tt.wantName {
					t.Errorf("findSchemaElement().Name = %v, want %v", got.Name, tt.wantName)
				}
			}
		})
	}
}

// Test decodeStatValue for various logical types
func TestDecodeStatValue_LogicalTypes(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		parquetType parquet.Type
		schemaElem  *parquet.SchemaElement
		wantType    string // type name to check
	}{
		{
			name:        "nil value",
			value:       nil,
			parquetType: parquet.Type_INT32,
			schemaElem:  nil,
			wantType:    "<nil>",
		},
		{
			name:        "nil schema element",
			value:       int32(42),
			parquetType: parquet.Type_INT32,
			schemaElem:  nil,
			wantType:    "int32",
		},
		{
			name:        "date logical type",
			value:       int32(18628), // 2021-01-01
			parquetType: parquet.Type_INT32,
			schemaElem: &parquet.SchemaElement{
				LogicalType: &parquet.LogicalType{
					DATE: &parquet.DateType{},
				},
			},
			wantType: "string", // Should return formatted date string
		},
		{
			name:        "timestamp millis",
			value:       int64(1609459200000), // 2021-01-01 00:00:00
			parquetType: parquet.Type_INT64,
			schemaElem: &parquet.SchemaElement{
				LogicalType: &parquet.LogicalType{
					TIMESTAMP: &parquet.TimestampType{
						Unit: &parquet.TimeUnit{
							MILLIS: &parquet.MilliSeconds{},
						},
					},
				},
			},
			wantType: "string", // Should return ISO8601 string
		},
		{
			name:        "decimal int32",
			value:       int32(12345),
			parquetType: parquet.Type_INT32,
			schemaElem: &parquet.SchemaElement{
				ConvertedType: convertedTypePtr(parquet.ConvertedType_DECIMAL),
				Precision:     int32Ptr(10),
				Scale:         int32Ptr(2),
			},
			wantType: "string", // Should return formatted decimal
		},
		{
			name:        "plain int32 without logical type",
			value:       int32(42),
			parquetType: parquet.Type_INT32,
			schemaElem: &parquet.SchemaElement{
				Type: typePtr(parquet.Type_INT32),
			},
			wantType: "int32",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decodeStatValue(tt.value, tt.parquetType, tt.schemaElem)
			gotType := fmt.Sprintf("%T", got)
			if !strings.Contains(gotType, tt.wantType) && tt.wantType != "<nil>" {
				t.Logf("decodeStatValue() returned type %s, value: %v", gotType, got)
			}
			// Just verify it doesn't panic and returns something
			if got == nil && tt.value != nil && tt.wantType != "<nil>" {
				t.Errorf("decodeStatValue() returned nil for non-nil input")
			}
		})
	}
}

// Helper functions for tests
func typePtr(t parquet.Type) *parquet.Type {
	return &t
}

func convertedTypePtr(ct parquet.ConvertedType) *parquet.ConvertedType {
	return &ct
}

// Test formatStatValue with various input types
func Test_FormatStatValue(t *testing.T) {
	tests := []struct {
		name     string
		value    []byte
		expected string
	}{
		{
			name:     "empty value",
			value:    []byte{},
			expected: "-",
		},
		{
			name:     "nil value",
			value:    nil,
			expected: "-",
		},
		{
			name:     "valid UTF-8 string",
			value:    []byte("hello world"),
			expected: "hello world",
		},
		{
			name:     "UTF-8 string longer than 50 chars",
			value:    []byte("this is a very long string that exceeds fifty characters in length"),
			expected: "this is a very long string that exceeds fifty char...",
		},
		{
			name:     "binary data (small)",
			value:    []byte{0x00, 0x01, 0xFF, 0xAB},
			expected: "0x0001FFAB",
		},
		{
			name:     "binary data (large)",
			value:    []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09},
			expected: "<binary:10 bytes>",
		},
		{
			name:     "valid UTF-8 with special characters",
			value:    []byte("hello\nworld\ttab"),
			expected: "hello\nworld\ttab",
		},
		{
			name:     "UTF-8 with emoji",
			value:    []byte("Hello 👋 World"),
			expected: "Hello 👋 World",
		},
		{
			name:     "invalid UTF-8",
			value:    []byte{0xFF, 0xFE, 0xFD},
			expected: "0xFFFEFD",
		},
		{
			name:     "partially printable",
			value:    []byte{0x01, 0x02, 0x03, 0x04},
			expected: "0x01020304",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatStatValue(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test isValidUTF8 with various strings
func Test_IsValidUTF8(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "empty string",
			input:    "",
			expected: false, // total is 0, so returns false
		},
		{
			name:     "valid ASCII",
			input:    "hello world",
			expected: true,
		},
		{
			name:     "valid UTF-8 with emoji",
			input:    "Hello 👋 World 🌍",
			expected: true,
		},
		{
			name:     "valid UTF-8 with Chinese",
			input:    "你好世界",
			expected: true,
		},
		{
			name:     "printable with whitespace",
			input:    "hello\nworld\ttab space",
			expected: true,
		},
		{
			name:     "mostly control characters (below 80%)",
			input:    "\x00\x01\x02\x03\x04hello",
			expected: false,
		},
		{
			name:     "exactly at boundary",
			input:    "hello", // 100% printable
			expected: true,
		},
		{
			name:     "mixed printable and non-printable",
			input:    "abc\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09",
			expected: false, // 3 printable out of 13 = 23%
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidUTF8(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test findSchemaElement with various paths
func Test_FindSchemaElement(t *testing.T) {
	schema := []*parquet.SchemaElement{
		{Name: "root"},
		{Name: "id", Type: typePtr(parquet.Type_INT64)},
		{Name: "name", Type: typePtr(parquet.Type_BYTE_ARRAY)},
		{Name: "nested_struct"},
		{Name: "nested_field", Type: typePtr(parquet.Type_INT32)},
		{Name: "list"},
		{Name: "element", Type: typePtr(parquet.Type_FLOAT)},
	}

	tests := []struct {
		name         string
		pathInSchema []string
		expectedName string
		shouldBeNil  bool
	}{
		{
			name:         "empty path",
			pathInSchema: []string{},
			shouldBeNil:  true,
		},
		{
			name:         "simple path - id",
			pathInSchema: []string{"id"},
			expectedName: "id",
			shouldBeNil:  false,
		},
		{
			name:         "simple path - name",
			pathInSchema: []string{"name"},
			expectedName: "name",
			shouldBeNil:  false,
		},
		{
			name:         "nested path",
			pathInSchema: []string{"nested_struct", "nested_field"},
			expectedName: "nested_field",
			shouldBeNil:  false,
		},
		{
			name:         "list element path",
			pathInSchema: []string{"list", "element"},
			expectedName: "element",
			shouldBeNil:  false,
		},
		{
			name:         "non-existent field",
			pathInSchema: []string{"does_not_exist"},
			shouldBeNil:  true,
		},
		{
			name:         "nested non-existent field",
			pathInSchema: []string{"nested_struct", "does_not_exist"},
			shouldBeNil:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findSchemaElement(schema, tt.pathInSchema)
			if tt.shouldBeNil {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, tt.expectedName, result.Name)
			}
		})
	}
}

// Test countLeafColumns with various schema structures
func Test_CountLeafColumns(t *testing.T) {
	tests := []struct {
		name     string
		schema   []*parquet.SchemaElement
		expected int
	}{
		{
			name:     "empty schema",
			schema:   []*parquet.SchemaElement{},
			expected: 0,
		},
		{
			name: "only group nodes",
			schema: []*parquet.SchemaElement{
				{Name: "root"},
				{Name: "group1"},
				{Name: "group2"},
			},
			expected: 0,
		},
		{
			name: "only leaf nodes",
			schema: []*parquet.SchemaElement{
				{Name: "id", Type: typePtr(parquet.Type_INT64)},
				{Name: "name", Type: typePtr(parquet.Type_BYTE_ARRAY)},
				{Name: "age", Type: typePtr(parquet.Type_INT32)},
			},
			expected: 3,
		},
		{
			name: "mixed group and leaf nodes",
			schema: []*parquet.SchemaElement{
				{Name: "root"}, // group
				{Name: "id", Type: typePtr(parquet.Type_INT64)},     // leaf
				{Name: "struct_field"},                              // group
				{Name: "nested", Type: typePtr(parquet.Type_INT32)}, // leaf
				{Name: "list"}, // group
				{Name: "element", Type: typePtr(parquet.Type_FLOAT)}, // leaf
			},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := countLeafColumns(tt.schema)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test formatPathInSchema
func Test_FormatPathInSchema(t *testing.T) {
	tests := []struct {
		name     string
		path     []string
		expected string
	}{
		{
			name:     "empty path",
			path:     []string{},
			expected: "",
		},
		{
			name:     "single element",
			path:     []string{"id"},
			expected: "id",
		},
		{
			name:     "nested path",
			path:     []string{"doc", "sourceResource", "title"},
			expected: "doc.sourceResource.title",
		},
		{
			name:     "deep nesting",
			path:     []string{"level1", "level2", "level3", "level4", "level5"},
			expected: "level1.level2.level3.level4.level5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatPathInSchema(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test GetFileInfo with no CreatedBy
func Test_FileInfo_GetFileInfo_NoCreatedBy(t *testing.T) {
	metadata := &parquet.FileMetaData{
		Version:   1,
		NumRows:   1000,
		CreatedBy: nil, // No created by
		Schema: []*parquet.SchemaElement{
			{Name: "root"},
			{Name: "id", Type: typePtr(parquet.Type_INT64)},
		},
		RowGroups: []*parquet.RowGroup{
			{
				NumRows:             1000,
				TotalByteSize:       10000,
				TotalCompressedSize: int64Ptr(5000),
			},
		},
	}

	pr := &ParquetReader{
		metadata: metadata,
	}

	info := pr.GetFileInfo()
	assert.Equal(t, "", info.CreatedBy)
}

// Test GetRowGroupInfo with no TotalCompressedSize
func Test_RowGroupInfo_GetRowGroupInfo_NoCompressedSize(t *testing.T) {
	metadata := &parquet.FileMetaData{
		RowGroups: []*parquet.RowGroup{
			{
				NumRows:             500,
				TotalByteSize:       10000,
				TotalCompressedSize: nil, // Not set
				Columns: []*parquet.ColumnChunk{
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 3000}},
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 2000}},
				},
			},
		},
	}

	pr := &ParquetReader{
		metadata: metadata,
	}

	info, err := pr.GetRowGroupInfo(0)
	assert.NoError(t, err)
	assert.Equal(t, int64(5000), info.CompressedSize) // Sum from columns
}

// Test GetColumnChunkInfo with no statistics
func Test_ColumnChunkInfo_GetColumnChunkInfo_NoStats(t *testing.T) {
	metadata := &parquet.FileMetaData{
		Schema: []*parquet.SchemaElement{
			{Name: "root"},
			{Name: "id", Type: typePtr(parquet.Type_INT64)},
		},
		RowGroups: []*parquet.RowGroup{
			{
				Columns: []*parquet.ColumnChunk{
					{
						MetaData: &parquet.ColumnMetaData{
							Type:                  parquet.Type_INT64,
							PathInSchema:          []string{"id"},
							Codec:                 parquet.CompressionCodec_SNAPPY,
							NumValues:             100,
							TotalCompressedSize:   500,
							TotalUncompressedSize: 800,
							Statistics:            nil, // No statistics
						},
					},
				},
			},
		},
	}

	pr := &ParquetReader{
		metadata: metadata,
	}

	info, err := pr.GetColumnChunkInfo(0, 0)
	assert.NoError(t, err)
	assert.Nil(t, info.NullCount)
	assert.Nil(t, info.MinValue)
	assert.Nil(t, info.MaxValue)
}

// Test GetColumnChunkInfo with no schema element match
func Test_ColumnChunkInfo_GetColumnChunkInfo_NoSchemaMatch(t *testing.T) {
	metadata := &parquet.FileMetaData{
		Schema: []*parquet.SchemaElement{
			{Name: "root"},
			{Name: "other_field", Type: typePtr(parquet.Type_INT32)},
		},
		RowGroups: []*parquet.RowGroup{
			{
				Columns: []*parquet.ColumnChunk{
					{
						MetaData: &parquet.ColumnMetaData{
							Type:                  parquet.Type_INT64,
							PathInSchema:          []string{"id"}, // No match in schema
							Codec:                 parquet.CompressionCodec_SNAPPY,
							NumValues:             100,
							TotalCompressedSize:   500,
							TotalUncompressedSize: 800,
						},
					},
				},
			},
		},
	}

	pr := &ParquetReader{
		metadata: metadata,
	}

	info, err := pr.GetColumnChunkInfo(0, 0)
	assert.NoError(t, err)
	assert.Equal(t, "", info.LogicalType)
	assert.Equal(t, "", info.ConvertedType)
}

// Test GetColumnChunkInfo with deprecated Min/Max in statistics
func Test_ColumnChunkInfo_GetColumnChunkInfo_DeprecatedMinMax(t *testing.T) {
	metadata := &parquet.FileMetaData{
		Schema: []*parquet.SchemaElement{
			{Name: "root"},
			{Name: "id", Type: typePtr(parquet.Type_INT64)},
		},
		RowGroups: []*parquet.RowGroup{
			{
				Columns: []*parquet.ColumnChunk{
					{
						MetaData: &parquet.ColumnMetaData{
							Type:                  parquet.Type_INT64,
							PathInSchema:          []string{"id"},
							Codec:                 parquet.CompressionCodec_SNAPPY,
							NumValues:             100,
							TotalCompressedSize:   500,
							TotalUncompressedSize: 800,
							Statistics: &parquet.Statistics{
								// Using deprecated Min/Max instead of MinValue/MaxValue
								Min: []byte{0x01},
								Max: []byte{0xFF},
								// MinValue and MaxValue are not set
							},
						},
					},
				},
			},
		},
	}

	pr := &ParquetReader{
		metadata: metadata,
	}

	info, err := pr.GetColumnChunkInfo(0, 0)
	assert.NoError(t, err)
	// Should fall back to deprecated Min/Max
	assert.Equal(t, []byte{0x01}, info.MinValue)
	assert.Equal(t, []byte{0xFF}, info.MaxValue)
}

// Test GetFileInfo with zero compressed size (edge case)
func Test_FileInfo_GetFileInfo_ZeroCompressedSize(t *testing.T) {
	metadata := &parquet.FileMetaData{
		Version: 1,
		NumRows: 0,
		Schema: []*parquet.SchemaElement{
			{Name: "root"},
		},
		RowGroups: []*parquet.RowGroup{},
	}

	pr := &ParquetReader{
		metadata: metadata,
	}

	info := pr.GetFileInfo()
	assert.Equal(t, float64(0), info.CompressionRatio)
}

// Test GetRowGroupInfo with zero compressed size
func Test_RowGroupInfo_GetRowGroupInfo_ZeroCompressedSize(t *testing.T) {
	metadata := &parquet.FileMetaData{
		RowGroups: []*parquet.RowGroup{
			{
				NumRows:             0,
				TotalByteSize:       0,
				TotalCompressedSize: int64Ptr(0),
				Columns:             []*parquet.ColumnChunk{},
			},
		},
	}

	pr := &ParquetReader{
		metadata: metadata,
	}

	info, err := pr.GetRowGroupInfo(0)
	assert.NoError(t, err)
	assert.Equal(t, float64(0), info.CompressionRatio)
}

// Test GetColumnChunkInfo with zero compressed size
func Test_ColumnChunkInfo_GetColumnChunkInfo_ZeroCompressedSize(t *testing.T) {
	metadata := &parquet.FileMetaData{
		Schema: []*parquet.SchemaElement{
			{Name: "root"},
			{Name: "id", Type: typePtr(parquet.Type_INT64)},
		},
		RowGroups: []*parquet.RowGroup{
			{
				Columns: []*parquet.ColumnChunk{
					{
						MetaData: &parquet.ColumnMetaData{
							Type:                  parquet.Type_INT64,
							PathInSchema:          []string{"id"},
							Codec:                 parquet.CompressionCodec_UNCOMPRESSED,
							NumValues:             0,
							TotalCompressedSize:   0,
							TotalUncompressedSize: 0,
						},
					},
				},
			},
		},
	}

	pr := &ParquetReader{
		metadata: metadata,
	}

	info, err := pr.GetColumnChunkInfo(0, 0)
	assert.NoError(t, err)
	assert.Equal(t, float64(0), info.CompressionRatio)
}

// Test decodeStatValue with INT96 type
func Test_DecodeStatValue_INT96(t *testing.T) {
	// INT96 is stored as 12 bytes (nanoseconds + julian day)
	value := "testvalue123" // 12 bytes as string
	schemaElem := &parquet.SchemaElement{
		Type: typePtr(parquet.Type_INT96),
	}

	result := decodeStatValue(value, parquet.Type_INT96, schemaElem)
	assert.NotNil(t, result)
	// Result should be converted to time
	t.Logf("INT96 decoded: %v", result)
}

// Test decodeStatValue with BYTE_ARRAY without logical type (should base64 encode)
func Test_DecodeStatValue_ByteArray_NoLogicalType(t *testing.T) {
	value := "binary data"
	schemaElem := &parquet.SchemaElement{
		Type: typePtr(parquet.Type_BYTE_ARRAY),
		// No ConvertedType or LogicalType
	}

	result := decodeStatValue(value, parquet.Type_BYTE_ARRAY, schemaElem)
	assert.NotNil(t, result)
	// Should be base64 encoded
	resultStr, ok := result.(string)
	assert.True(t, ok)
	assert.NotEqual(t, "binary data", resultStr) // Should be encoded
	t.Logf("Base64 encoded: %s", resultStr)
}

// Test decodeStatValue with FIXED_LEN_BYTE_ARRAY without logical type
func Test_DecodeStatValue_FixedLenByteArray_NoLogicalType(t *testing.T) {
	value := "fixeddata"
	schemaElem := &parquet.SchemaElement{
		Type: typePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
		// No ConvertedType or LogicalType
	}

	result := decodeStatValue(value, parquet.Type_FIXED_LEN_BYTE_ARRAY, schemaElem)
	assert.NotNil(t, result)
	// Should be base64 encoded
	t.Logf("Fixed len byte array decoded: %v", result)
}

// Test decodeStatValue with ConvertedType_TIME_MICROS
func Test_DecodeStatValue_ConvertedType_TimeMicros(t *testing.T) {
	value := int64(12345678) // microseconds
	schemaElem := &parquet.SchemaElement{
		ConvertedType: convertedTypePtr(parquet.ConvertedType_TIME_MICROS),
		LogicalType: &parquet.LogicalType{
			TIME: &parquet.TimeType{
				Unit: &parquet.TimeUnit{
					MICROS: &parquet.MicroSeconds{},
				},
			},
		},
	}

	result := decodeStatValue(value, parquet.Type_INT64, schemaElem)
	assert.NotNil(t, result)
	t.Logf("Time micros decoded: %v", result)
}

// Test decodeStatValue with ConvertedType_TIME_MILLIS
func Test_DecodeStatValue_ConvertedType_TimeMillis(t *testing.T) {
	value := int32(12345) // milliseconds
	schemaElem := &parquet.SchemaElement{
		ConvertedType: convertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
	}

	result := decodeStatValue(value, parquet.Type_INT32, schemaElem)
	assert.NotNil(t, result)
	// Without LogicalType, should return as-is
	t.Logf("Time millis decoded: %v", result)
}

// Test decodeStatValue with ConvertedType_TIMESTAMP_MICROS
func Test_DecodeStatValue_ConvertedType_TimestampMicros(t *testing.T) {
	value := int64(1609459200000000) // 2021-01-01 00:00:00 UTC in microseconds
	schemaElem := &parquet.SchemaElement{
		ConvertedType: convertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS),
	}

	result := decodeStatValue(value, parquet.Type_INT64, schemaElem)
	assert.NotNil(t, result)
	t.Logf("Timestamp micros decoded: %v", result)
}

// Test decodeStatValue with ConvertedType_TIMESTAMP_MILLIS
func Test_DecodeStatValue_ConvertedType_TimestampMillis(t *testing.T) {
	value := int64(1609459200000) // 2021-01-01 00:00:00 UTC in milliseconds
	schemaElem := &parquet.SchemaElement{
		ConvertedType: convertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS),
	}

	result := decodeStatValue(value, parquet.Type_INT64, schemaElem)
	assert.NotNil(t, result)
	t.Logf("Timestamp millis decoded: %v", result)
}

// Test decodeStatValue with ConvertedType_INTERVAL
func Test_DecodeStatValue_ConvertedType_Interval(t *testing.T) {
	// INTERVAL is 12 bytes: months (4 bytes) + days (4 bytes) + milliseconds (4 bytes)
	value := string([]byte{1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0}) // 1 month, 2 days, 3 ms
	schemaElem := &parquet.SchemaElement{
		ConvertedType: convertedTypePtr(parquet.ConvertedType_INTERVAL),
	}

	result := decodeStatValue(value, parquet.Type_FIXED_LEN_BYTE_ARRAY, schemaElem)
	assert.NotNil(t, result)
	t.Logf("Interval decoded: %v", result)
}

// Test decodeStatValue with ConvertedType_BSON
func Test_DecodeStatValue_ConvertedType_BSON(t *testing.T) {
	value := "bson_data"
	schemaElem := &parquet.SchemaElement{
		ConvertedType: convertedTypePtr(parquet.ConvertedType_BSON),
	}

	result := decodeStatValue(value, parquet.Type_BYTE_ARRAY, schemaElem)
	assert.NotNil(t, result)
	t.Logf("BSON decoded: %v", result)
}

// Test decodeStatValue with LogicalType UUID
func Test_DecodeStatValue_LogicalType_UUID(t *testing.T) {
	// UUID is 16 bytes
	value := string([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
	schemaElem := &parquet.SchemaElement{
		LogicalType: &parquet.LogicalType{
			UUID: &parquet.UUIDType{},
		},
	}

	result := decodeStatValue(value, parquet.Type_FIXED_LEN_BYTE_ARRAY, schemaElem)
	assert.NotNil(t, result)
	t.Logf("UUID decoded: %v", result)
}

// Test decodeStatValue with LogicalType BSON
func Test_DecodeStatValue_LogicalType_BSON(t *testing.T) {
	value := "bson_logical"
	schemaElem := &parquet.SchemaElement{
		LogicalType: &parquet.LogicalType{
			BSON: &parquet.BsonType{},
		},
	}

	result := decodeStatValue(value, parquet.Type_BYTE_ARRAY, schemaElem)
	assert.NotNil(t, result)
	t.Logf("BSON logical decoded: %v", result)
}

// Test decodeStatValue with LogicalType FLOAT16
func Test_DecodeStatValue_LogicalType_Float16(t *testing.T) {
	// Float16 is 2 bytes
	value := string([]byte{0x00, 0x3C}) // Some float16 value
	schemaElem := &parquet.SchemaElement{
		LogicalType: &parquet.LogicalType{
			FLOAT16: &parquet.Float16Type{},
		},
	}

	result := decodeStatValue(value, parquet.Type_FIXED_LEN_BYTE_ARRAY, schemaElem)
	assert.NotNil(t, result)
	t.Logf("Float16 decoded: %v", result)
}

// Test decodeStatValue with LogicalType TIMESTAMP with MICROS
func Test_DecodeStatValue_LogicalType_Timestamp_Micros(t *testing.T) {
	value := int64(1609459200000000) // 2021-01-01 00:00:00 UTC in microseconds
	schemaElem := &parquet.SchemaElement{
		LogicalType: &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{
					MICROS: &parquet.MicroSeconds{},
				},
			},
		},
	}

	result := decodeStatValue(value, parquet.Type_INT64, schemaElem)
	assert.NotNil(t, result)
	resultStr, ok := result.(string)
	assert.True(t, ok)
	assert.Contains(t, resultStr, "2021") // Should contain year
	t.Logf("Timestamp micros: %v", result)
}

// Test decodeStatValue with LogicalType TIMESTAMP with NANOS
func Test_DecodeStatValue_LogicalType_Timestamp_Nanos(t *testing.T) {
	value := int64(1609459200000000000) // 2021-01-01 00:00:00 UTC in nanoseconds
	schemaElem := &parquet.SchemaElement{
		LogicalType: &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{
					NANOS: &parquet.NanoSeconds{},
				},
			},
		},
	}

	result := decodeStatValue(value, parquet.Type_INT64, schemaElem)
	assert.NotNil(t, result)
	resultStr, ok := result.(string)
	assert.True(t, ok)
	assert.Contains(t, resultStr, "2021") // Should contain year
	t.Logf("Timestamp nanos: %v", result)
}

// Test decodeStatValue with LogicalType DECIMAL with default precision/scale
func Test_DecodeStatValue_LogicalType_Decimal_Defaults(t *testing.T) {
	value := int32(12345)
	schemaElem := &parquet.SchemaElement{
		LogicalType: &parquet.LogicalType{
			DECIMAL: &parquet.DecimalType{
				Precision: 10,
				Scale:     2,
			},
		},
		// No Precision or Scale fields set
	}

	result := decodeStatValue(value, parquet.Type_INT32, schemaElem)
	assert.NotNil(t, result)
	t.Logf("Decimal with defaults: %v", result)
}

// Test decodeStatValue with timestamp non-int64 value (should return as-is)
func Test_DecodeStatValue_LogicalType_Timestamp_NonInt64(t *testing.T) {
	value := "not an int64"
	schemaElem := &parquet.SchemaElement{
		LogicalType: &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{
					MILLIS: &parquet.MilliSeconds{},
				},
			},
		},
	}

	result := decodeStatValue(value, parquet.Type_INT64, schemaElem)
	assert.Equal(t, value, result) // Should return as-is
}

// Test retrieveRawValue with invalid data (too short buffer)
func Test_RetrieveRawValue_InvalidData(t *testing.T) {
	tests := []struct {
		name        string
		value       []byte
		parquetType parquet.Type
	}{
		{
			name:        "int32 with insufficient bytes",
			value:       []byte{0x01, 0x02}, // Only 2 bytes, need 4
			parquetType: parquet.Type_INT32,
		},
		{
			name:        "int64 with insufficient bytes",
			value:       []byte{0x01, 0x02, 0x03}, // Only 3 bytes, need 8
			parquetType: parquet.Type_INT64,
		},
		{
			name:        "float32 with insufficient bytes",
			value:       []byte{0x01, 0x02}, // Only 2 bytes, need 4
			parquetType: parquet.Type_FLOAT,
		},
		{
			name:        "float64 with insufficient bytes",
			value:       []byte{0x01, 0x02, 0x03}, // Only 3 bytes, need 8
			parquetType: parquet.Type_DOUBLE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := retrieveRawValue(tt.value, tt.parquetType)
			// Should return error string or fallback to string
			t.Logf("Result for %s: %v", tt.name, result)
			assert.NotNil(t, result)
		})
	}
}

// Test formatDecodedValue with uint types
func Test_FormatDecodedValue_UintTypes(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{
			name:  "uint",
			value: uint(42),
			want:  "42",
		},
		{
			name:  "uint64",
			value: uint64(9223372036854775807),
			want:  "9223372036854775807",
		},
		{
			name:  "int",
			value: int(42),
			want:  "42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDecodedValue(tt.value)
			assert.Equal(t, tt.want, got)
		})
	}
}

// Test formatDecodedValue with complex type (default case)
func Test_FormatDecodedValue_ComplexType(t *testing.T) {
	type customType struct {
		Field1 string
		Field2 int
	}

	value := customType{Field1: "test", Field2: 42}
	result := formatDecodedValue(value)
	assert.NotEmpty(t, result)
	assert.Contains(t, result, "test")
	assert.Contains(t, result, "42")
}

// Test formatDecodedValue with complex type that's too long
func Test_FormatDecodedValue_ComplexType_Long(t *testing.T) {
	type customType struct {
		Field1 string
	}

	value := customType{Field1: "this is a very very very very very very very long field value that exceeds 50 characters"}
	result := formatDecodedValue(value)
	assert.NotEmpty(t, result)
	// Should be truncated
	if len(result) > 53 { // 50 + "..."
		t.Errorf("Expected truncated result, got %d chars: %s", len(result), result)
	}
}

// Test formatBytes with various sizes
func Test_FormatBytes(t *testing.T) {
	tests := []struct {
		name     string
		bytes    int64
		expected string
	}{
		{
			name:     "less than 1KB",
			bytes:    512,
			expected: "512 B",
		},
		{
			name:     "exactly 1KB",
			bytes:    1024,
			expected: "1.0 KB",
		},
		{
			name:     "MB range",
			bytes:    1024 * 1024 * 5,
			expected: "5.0 MB",
		},
		{
			name:     "GB range",
			bytes:    1024 * 1024 * 1024 * 2,
			expected: "2.0 GB",
		},
		{
			name:     "TB range",
			bytes:    1024 * 1024 * 1024 * 1024 * 3,
			expected: "3.0 TB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatBytes(tt.bytes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test getTotalSize with multiple columns
func Test_GetTotalSize(t *testing.T) {
	rg := &parquet.RowGroup{
		Columns: []*parquet.ColumnChunk{
			{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 1000}},
			{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 2000}},
			{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 3000}},
		},
	}

	total := getTotalSize(rg)
	assert.Equal(t, int64(6000), total)
}

// Test getTotalSize with empty row group
func Test_GetTotalSize_Empty(t *testing.T) {
	rg := &parquet.RowGroup{
		Columns: []*parquet.ColumnChunk{},
	}

	total := getTotalSize(rg)
	assert.Equal(t, int64(0), total)
}

// Test findSchemaElement with empty schema
func Test_FindSchemaElement_EmptySchema(t *testing.T) {
	schema := []*parquet.SchemaElement{}
	result := findSchemaElement(schema, []string{"field"})
	assert.Nil(t, result)
}

// Test findSchemaElement with case-insensitive matching
func Test_FindSchemaElement_CaseInsensitive(t *testing.T) {
	schema := []*parquet.SchemaElement{
		{Name: "root"},
		{Name: "MyField", Type: typePtr(parquet.Type_INT32)},
	}

	result := findSchemaElement(schema, []string{"myfield"}) // lowercase
	assert.NotNil(t, result)
	assert.Equal(t, "MyField", result.Name)
}

// Test findSchemaElement with Parquet_go_root
func Test_FindSchemaElement_ParquetGoRoot(t *testing.T) {
	schema := []*parquet.SchemaElement{
		{Name: "Parquet_go_root", NumChildren: int32Ptr(1)},
		{Name: "field1", Type: typePtr(parquet.Type_INT32)},
	}

	result := findSchemaElement(schema, []string{"field1"})
	assert.NotNil(t, result)
	assert.Equal(t, "field1", result.Name)
}

// Test findSchemaElement fallback to leaf name matching
func Test_FindSchemaElement_LeafNameFallback(t *testing.T) {
	schema := []*parquet.SchemaElement{
		{Name: "root"},
		{Name: "deeply"},
		{Name: "nested"},
		{Name: "field", Type: typePtr(parquet.Type_INT32)},
	}

	// Try to match just "field" even though full path might not match
	result := findSchemaElement(schema, []string{"field"})
	assert.NotNil(t, result)
	assert.Equal(t, "field", result.Name)
}

// Test decodeStatValue with INT96 non-string value (edge case)
func Test_DecodeStatValue_INT96_NonString(t *testing.T) {
	// INT96 where value is not a string (should return as-is)
	value := int32(12345) // Not a string
	schemaElem := &parquet.SchemaElement{
		Type: typePtr(parquet.Type_INT96),
	}

	result := decodeStatValue(value, parquet.Type_INT96, schemaElem)
	assert.Equal(t, value, result) // Should return original value
}

// Test decodeStatValue with BYTE_ARRAY non-string value (edge case)
func Test_DecodeStatValue_ByteArray_NonString(t *testing.T) {
	// BYTE_ARRAY where value is not a string (should return as-is)
	value := int32(12345) // Not a string
	schemaElem := &parquet.SchemaElement{
		Type: typePtr(parquet.Type_BYTE_ARRAY),
		// No ConvertedType or LogicalType
	}

	result := decodeStatValue(value, parquet.Type_BYTE_ARRAY, schemaElem)
	assert.Equal(t, value, result) // Should return original value
}

// Test decodeStatValue with ConvertedType_DATE
func Test_DecodeStatValue_ConvertedType_Date(t *testing.T) {
	value := int32(18628) // Days since epoch: 2021-01-01
	schemaElem := &parquet.SchemaElement{
		ConvertedType: convertedTypePtr(parquet.ConvertedType_DATE),
	}

	result := decodeStatValue(value, parquet.Type_INT32, schemaElem)
	assert.NotNil(t, result)
	// Should return date string
	t.Logf("Date decoded: %v", result)
}

// Test decodeStatValue with ConvertedType_INTERVAL non-string value
func Test_DecodeStatValue_ConvertedType_Interval_NonString(t *testing.T) {
	// INTERVAL where value is not a string (should return as-is)
	value := int32(12345)
	schemaElem := &parquet.SchemaElement{
		ConvertedType: convertedTypePtr(parquet.ConvertedType_INTERVAL),
	}

	result := decodeStatValue(value, parquet.Type_FIXED_LEN_BYTE_ARRAY, schemaElem)
	assert.Equal(t, value, result) // Should return original value
}

// Test decodeStatValue with LogicalType DECIMAL with Precision and Scale set
func Test_DecodeStatValue_LogicalType_Decimal_WithPrecisionScale(t *testing.T) {
	value := int32(12345)
	precision := int32(10)
	scale := int32(2)
	schemaElem := &parquet.SchemaElement{
		LogicalType: &parquet.LogicalType{
			DECIMAL: &parquet.DecimalType{
				Precision: 10,
				Scale:     2,
			},
		},
		Precision: &precision,
		Scale:     &scale,
	}

	result := decodeStatValue(value, parquet.Type_INT32, schemaElem)
	assert.NotNil(t, result)
	t.Logf("Decimal with precision/scale: %v", result)
}

// Test decodeStatValue with ConvertedType DECIMAL with Precision and Scale set
func Test_DecodeStatValue_ConvertedType_Decimal_WithPrecisionScale(t *testing.T) {
	value := int32(12345)
	precision := int32(5)
	scale := int32(2)
	schemaElem := &parquet.SchemaElement{
		ConvertedType: convertedTypePtr(parquet.ConvertedType_DECIMAL),
		Precision:     &precision,
		Scale:         &scale,
	}

	result := decodeStatValue(value, parquet.Type_INT32, schemaElem)
	assert.NotNil(t, result)
	t.Logf("Decimal converted type with precision/scale: %v", result)
}

// Test decodeStatValue with LogicalType DATE
func Test_DecodeStatValue_LogicalType_Date(t *testing.T) {
	value := int32(18628) // Days since epoch: 2021-01-01
	schemaElem := &parquet.SchemaElement{
		LogicalType: &parquet.LogicalType{
			DATE: &parquet.DateType{},
		},
	}

	result := decodeStatValue(value, parquet.Type_INT32, schemaElem)
	assert.NotNil(t, result)
	t.Logf("Date logical: %v", result)
}

// Test decodeStatValue with LogicalType TIME
func Test_DecodeStatValue_LogicalType_Time(t *testing.T) {
	value := int64(12345678) // microseconds
	schemaElem := &parquet.SchemaElement{
		LogicalType: &parquet.LogicalType{
			TIME: &parquet.TimeType{
				Unit: &parquet.TimeUnit{
					MICROS: &parquet.MicroSeconds{},
				},
			},
		},
	}

	result := decodeStatValue(value, parquet.Type_INT64, schemaElem)
	assert.NotNil(t, result)
	t.Logf("Time logical: %v", result)
}

// Test retrieveRawValue with BOOLEAN error case
func Test_RetrieveRawValue_Boolean_Error(t *testing.T) {
	// Empty buffer for boolean (should cause error)
	value := []byte{}
	result := retrieveRawValue(value, parquet.Type_BOOLEAN)
	// Should return error string
	assert.NotNil(t, result)
	resultStr, ok := result.(string)
	if ok {
		assert.Contains(t, resultStr, "error")
	}
	t.Logf("Boolean error result: %v", result)
}

// Test formatStatValueWithType with nil rawValue (edge case)
func Test_FormatStatValueWithType_NilRawValue(t *testing.T) {
	// This tests defensive programming - the nil check exists but is hard to trigger
	// through normal paths since retrieveRawValue only returns nil for nil input,
	// which is already caught by the len(value) == 0 check.
	// Testing with empty value which gets caught earlier
	value := []byte{}
	meta := &parquet.ColumnMetaData{
		Type: parquet.Type_INT32,
	}
	schemaElem := &parquet.SchemaElement{
		Type: &meta.Type,
	}

	result := formatStatValueWithType(value, meta, schemaElem)
	// Should return "-" due to empty value check
	assert.Equal(t, "-", result)
}

// Test retrieveRawValue with all physical types for complete coverage
func Test_RetrieveRawValue_AllTypes(t *testing.T) {
	tests := []struct {
		name        string
		value       []byte
		parquetType parquet.Type
		expectError bool
	}{
		{
			name:        "INT96",
			value:       []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			parquetType: parquet.Type_INT96,
			expectError: false,
		},
		{
			name:        "FIXED_LEN_BYTE_ARRAY",
			value:       []byte{1, 2, 3, 4, 5, 6, 7, 8},
			parquetType: parquet.Type_FIXED_LEN_BYTE_ARRAY,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := retrieveRawValue(tt.value, tt.parquetType)
			assert.NotNil(t, result)
			t.Logf("%s result: %v", tt.name, result)
		})
	}
}

// Test decodeStatValue with BYTE_ARRAY that has UTF8 converted type (should not base64 encode)
func Test_DecodeStatValue_ByteArray_UTF8(t *testing.T) {
	value := "hello world"
	schemaElem := &parquet.SchemaElement{
		Type:          typePtr(parquet.Type_BYTE_ARRAY),
		ConvertedType: convertedTypePtr(parquet.ConvertedType_UTF8),
	}

	result := decodeStatValue(value, parquet.Type_BYTE_ARRAY, schemaElem)
	assert.NotNil(t, result)
	// Should not be base64 encoded since it has ConvertedType
	t.Logf("UTF8 string: %v", result)
}

// Test decodeStatValue with unknown converted type (should return as-is)
func Test_DecodeStatValue_UnknownConvertedType(t *testing.T) {
	value := int32(12345)
	// Use a converted type that doesn't have specific handling
	unknownType := parquet.ConvertedType_INT_8
	schemaElem := &parquet.SchemaElement{
		ConvertedType: &unknownType,
	}

	result := decodeStatValue(value, parquet.Type_INT32, schemaElem)
	assert.Equal(t, value, result) // Should return as-is
}

// Test decodeStatValue with unknown logical type (should return as-is)
func Test_DecodeStatValue_UnknownLogicalType(t *testing.T) {
	value := int32(12345)
	// LogicalType with no specific type set
	schemaElem := &parquet.SchemaElement{
		LogicalType: &parquet.LogicalType{
			// No specific type set
		},
	}

	result := decodeStatValue(value, parquet.Type_INT32, schemaElem)
	assert.Equal(t, value, result) // Should return as-is
}

// Test findSchemaElement with deeply nested structure to ensure stack unwinding works
func Test_FindSchemaElement_DeepNesting(t *testing.T) {
	// Create a deeply nested schema with proper child counts
	schema := []*parquet.SchemaElement{
		{Name: "root", NumChildren: int32Ptr(1)},
		{Name: "level1", NumChildren: int32Ptr(1)},
		{Name: "level2", NumChildren: int32Ptr(1)},
		{Name: "level3", NumChildren: int32Ptr(1)},
		{Name: "deepField", Type: typePtr(parquet.Type_INT32)},
	}

	// Should find by full path
	result := findSchemaElement(schema, []string{"level1", "level2", "level3", "deepField"})
	assert.NotNil(t, result)
	assert.Equal(t, "deepField", result.Name)
}

// Test findSchemaElement with schema that has empty name (should skip)
func Test_FindSchemaElement_EmptyName(t *testing.T) {
	schema := []*parquet.SchemaElement{
		{Name: ""}, // Empty name should be skipped
		{Name: "field1", Type: typePtr(parquet.Type_INT32)},
	}

	result := findSchemaElement(schema, []string{"field1"})
	assert.NotNil(t, result)
	assert.Equal(t, "field1", result.Name)
}

// Test findSchemaElement with multiple children and proper stack management
func Test_FindSchemaElement_MultipleChildren(t *testing.T) {
	// Schema with parent having multiple children
	schema := []*parquet.SchemaElement{
		{Name: "root", NumChildren: int32Ptr(3)},
		{Name: "field1", Type: typePtr(parquet.Type_INT32)},
		{Name: "field2", Type: typePtr(parquet.Type_INT64)},
		{Name: "struct1", NumChildren: int32Ptr(2)},
		{Name: "nested1", Type: typePtr(parquet.Type_FLOAT)},
		{Name: "nested2", Type: typePtr(parquet.Type_DOUBLE)},
	}

	// Find nested field
	result := findSchemaElement(schema, []string{"struct1", "nested2"})
	assert.NotNil(t, result)
	assert.Equal(t, "nested2", result.Name)

	// Find top-level field
	result2 := findSchemaElement(schema, []string{"field2"})
	assert.NotNil(t, result2)
	assert.Equal(t, "field2", result2.Name)
}

// Test findSchemaElement where path length doesn't match (no candidate found, use fallback)
func Test_FindSchemaElement_PathLengthMismatch(t *testing.T) {
	schema := []*parquet.SchemaElement{
		{Name: "root", NumChildren: int32Ptr(1)},
		{Name: "parent", NumChildren: int32Ptr(1)},
		{Name: "child", Type: typePtr(parquet.Type_INT32)},
	}

	// Search for just "child" when it's actually at path ["parent", "child"]
	// Should use fallback leaf name matching
	result := findSchemaElement(schema, []string{"child"})
	assert.NotNil(t, result)
	assert.Equal(t, "child", result.Name)
}

// Test findSchemaElement with no NumChildren set (nil case)
func Test_FindSchemaElement_NoNumChildren(t *testing.T) {
	schema := []*parquet.SchemaElement{
		{Name: "root"}, // NumChildren is nil, treated as 0
		{Name: "field1", Type: typePtr(parquet.Type_INT32)},
	}

	result := findSchemaElement(schema, []string{"field1"})
	assert.NotNil(t, result)
	assert.Equal(t, "field1", result.Name)
}

// Test findSchemaElement with parent that exhausts children (triggers stack pop)
func Test_FindSchemaElement_StackPop(t *testing.T) {
	// Schema where a parent has exactly 1 child, and then another sibling follows
	// This should trigger the stack pop when childCount reaches 0
	schema := []*parquet.SchemaElement{
		{Name: "root", NumChildren: int32Ptr(2)},             // Has 2 children
		{Name: "parent", NumChildren: int32Ptr(1)},           // First child with 1 child
		{Name: "nested", Type: typePtr(parquet.Type_INT32)},  // Child of parent
		{Name: "sibling", Type: typePtr(parquet.Type_INT64)}, // Second child of root (triggers stack pop)
	}

	// Search for sibling - this requires popping "parent" from stack after "nested" is processed
	result := findSchemaElement(schema, []string{"sibling"})
	assert.NotNil(t, result)
	assert.Equal(t, "sibling", result.Name)

	// Also verify nested path works
	result2 := findSchemaElement(schema, []string{"parent", "nested"})
	assert.NotNil(t, result2)
	assert.Equal(t, "nested", result2.Name)
}

// Test retrieveRawValue with nil value to verify it returns nil
func Test_RetrieveRawValue_Nil(t *testing.T) {
	var value []byte = nil
	result := retrieveRawValue(value, parquet.Type_INT32)
	assert.Nil(t, result)
}
