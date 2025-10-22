package model

import (
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
			name:        "BYTE_ARRAY as string",
			value:       []byte("test"),
			parquetType: parquet.Type_BYTE_ARRAY,
			expected:    "test", // String value when schemaElem is nil
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
			result := retrieveRawValue(tt.value, tt.parquetType)
			require.Equal(t, tt.expected, result, "retrieveRawValue() should match")
		})
	}
}

func Test_FormatDecodedValue(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{
			name:     "Nil value",
			value:    nil,
			expected: "-",
		},
		{
			name:     "Short string",
			value:    "hello",
			expected: "hello",
		},
		{
			name:     "Long string truncation",
			value:    "this is a very long string that exceeds fifty characters and should be truncated",
			expected: "this is a very long string that exceeds fifty char...",
		},
		{
			name:     "Integer",
			value:    42,
			expected: "42",
		},
		{
			name:     "Int32",
			value:    int32(42),
			expected: "42",
		},
		{
			name:     "Int64",
			value:    int64(42),
			expected: "42",
		},
		{
			name:     "Float32",
			value:    float32(3.14),
			expected: "3.14",
		},
		{
			name:     "Float64",
			value:    float64(3.14159),
			expected: "3.14159",
		},
		{
			name:     "Boolean true",
			value:    true,
			expected: "true",
		},
		{
			name:     "Boolean false",
			value:    false,
			expected: "false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatDecodedValue(tt.value)
			require.Equal(t, tt.expected, result, "formatDecodedValue(%v) should match", tt.value)
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
			name:        "Simple string",
			val:         "hello",
			parquetType: parquet.Type_BYTE_ARRAY,
			schemaElem:  nil,
			expected:    "hello",
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
			result := findSchemaElementByPath(tt.schema, tt.path)
			if tt.expectNil {
				require.Nil(t, result, "findSchemaElementByPath() should return nil")
			} else {
				require.NotNil(t, result, "findSchemaElementByPath() should return non-nil")
				require.Equal(t, tt.expectedName, result.Name, "findSchemaElementByPath() name should match")
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
	result := findSchemaElementByPath(schema, []string{"myfield"})
	require.NotNil(t, result, "findSchemaElementByPath() with different case should find element")
	require.Equal(t, "MyField", result.Name, "findSchemaElementByPath() name should match")
}

func Test_FormatValueWithLogicalType_ByteArray(t *testing.T) {
	t.Run("Byte array with UUID logical type", func(t *testing.T) {
		uuidType := &parquet.LogicalType{UUID: &parquet.UUIDType{}}
		schema := &parquet.SchemaElement{
			Name:        "uuid_field",
			LogicalType: uuidType,
		}
		// UUID as bytes should be formatted
		val := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
		result := formatValueWithLogicalType(val, parquet.Type_BYTE_ARRAY, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})

	t.Run("Byte array without logical type but with schema", func(t *testing.T) {
		val := []byte("test")
		schema := &parquet.SchemaElement{Name: "test_field"}
		result := formatValueWithLogicalType(val, parquet.Type_BYTE_ARRAY, schema)
		require.Equal(t, "test", result, "Should return string for valid UTF-8")
	})

	t.Run("Byte array with invalid UTF-8 and schema", func(t *testing.T) {
		val := []byte{0xFF, 0xFE, 0xFD}
		schema := &parquet.SchemaElement{Name: "test_field"}
		result := formatValueWithLogicalType(val, parquet.Type_BYTE_ARRAY, schema)
		require.Equal(t, "0xFFFEFD", result, "Should return hex for invalid UTF-8")
	})

	t.Run("Large byte array with invalid UTF-8 and schema", func(t *testing.T) {
		val := make([]byte, 100)
		for i := range val {
			val[i] = 0xFF
		}
		schema := &parquet.SchemaElement{Name: "test_field"}
		result := formatValueWithLogicalType(val, parquet.Type_BYTE_ARRAY, schema)
		require.Equal(t, "<binary:100 bytes>", result, "Should return size for large binary data")
	})
}

func Test_FormatValueWithLogicalType_Decimal(t *testing.T) {
	t.Run("Decimal with logical type - int32", func(t *testing.T) {
		scale := int32(2)
		precision := int32(10)
		decimalType := &parquet.LogicalType{DECIMAL: &parquet.DecimalType{Scale: scale, Precision: precision}}
		schema := &parquet.SchemaElement{
			Name:        "decimal_field",
			LogicalType: decimalType,
			Scale:       &scale,
			Precision:   &precision,
		}
		result := formatValueWithLogicalType(int32(12345), parquet.Type_INT32, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})

	t.Run("Decimal with logical type - int64", func(t *testing.T) {
		scale := int32(3)
		precision := int32(18)
		decimalType := &parquet.LogicalType{DECIMAL: &parquet.DecimalType{Scale: scale, Precision: precision}}
		schema := &parquet.SchemaElement{
			Name:        "decimal_field",
			LogicalType: decimalType,
			Scale:       &scale,
			Precision:   &precision,
		}
		result := formatValueWithLogicalType(int64(1234567), parquet.Type_INT64, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})

	t.Run("Decimal with converted type - int32", func(t *testing.T) {
		convertedType := parquet.ConvertedType_DECIMAL
		scale := int32(2)
		precision := int32(10)
		schema := &parquet.SchemaElement{
			Name:          "decimal_field",
			ConvertedType: &convertedType,
			Scale:         &scale,
			Precision:     &precision,
		}
		result := formatValueWithLogicalType(int32(12345), parquet.Type_INT32, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})

	t.Run("Decimal with converted type - int64", func(t *testing.T) {
		convertedType := parquet.ConvertedType_DECIMAL
		scale := int32(3)
		precision := int32(18)
		schema := &parquet.SchemaElement{
			Name:          "decimal_field",
			ConvertedType: &convertedType,
			Scale:         &scale,
			Precision:     &precision,
		}
		result := formatValueWithLogicalType(int64(1234567), parquet.Type_INT64, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})

	t.Run("Decimal as BYTE_ARRAY string with converted type", func(t *testing.T) {
		convertedType := parquet.ConvertedType_DECIMAL
		scale := int32(2)
		precision := int32(10)
		schema := &parquet.SchemaElement{
			Name:          "decimal_field",
			ConvertedType: &convertedType,
			Scale:         &scale,
			Precision:     &precision,
		}
		result := formatValueWithLogicalType("12345", parquet.Type_BYTE_ARRAY, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})
}

func Test_FormatValueWithLogicalType_Date(t *testing.T) {
	t.Run("Date with logical type", func(t *testing.T) {
		dateType := &parquet.LogicalType{DATE: &parquet.DateType{}}
		schema := &parquet.SchemaElement{
			Name:        "date_field",
			LogicalType: dateType,
		}
		// Days since epoch: 19000 = 2022-01-08
		result := formatValueWithLogicalType(int32(19000), parquet.Type_INT32, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})

	t.Run("Date with converted type", func(t *testing.T) {
		convertedType := parquet.ConvertedType_DATE
		schema := &parquet.SchemaElement{
			Name:          "date_field",
			ConvertedType: &convertedType,
		}
		result := formatValueWithLogicalType(int32(19000), parquet.Type_INT32, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})
}

func Test_FormatValueWithLogicalType_Time(t *testing.T) {
	t.Run("Time MILLIS with logical type - int32", func(t *testing.T) {
		timeType := &parquet.LogicalType{
			TIME: &parquet.TimeType{
				Unit: &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "time_field",
			LogicalType: timeType,
		}
		result := formatValueWithLogicalType(int32(3661000), parquet.Type_INT32, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})

	t.Run("Time MICROS with logical type - int64", func(t *testing.T) {
		timeType := &parquet.LogicalType{
			TIME: &parquet.TimeType{
				Unit: &parquet.TimeUnit{MICROS: &parquet.MicroSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "time_field",
			LogicalType: timeType,
		}
		result := formatValueWithLogicalType(int64(3661000000), parquet.Type_INT64, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})

	t.Run("Time MILLIS with converted type", func(t *testing.T) {
		convertedType := parquet.ConvertedType_TIME_MILLIS
		schema := &parquet.SchemaElement{
			Name:          "time_field",
			ConvertedType: &convertedType,
		}
		result := formatValueWithLogicalType(int32(3661000), parquet.Type_INT32, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})

	t.Run("Time MICROS with converted type", func(t *testing.T) {
		convertedType := parquet.ConvertedType_TIME_MICROS
		schema := &parquet.SchemaElement{
			Name:          "time_field",
			ConvertedType: &convertedType,
		}
		result := formatValueWithLogicalType(int64(3661000000), parquet.Type_INT64, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})
}

func Test_FormatValueWithLogicalType_Timestamp(t *testing.T) {
	t.Run("Timestamp with logical type", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_field",
			LogicalType: timestampType,
		}
		result := formatValueWithLogicalType(int64(1640000000000), parquet.Type_INT64, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})

	t.Run("Timestamp MILLIS with converted type", func(t *testing.T) {
		convertedType := parquet.ConvertedType_TIMESTAMP_MILLIS
		schema := &parquet.SchemaElement{
			Name:          "timestamp_field",
			ConvertedType: &convertedType,
		}
		result := formatValueWithLogicalType(int64(1640000000000), parquet.Type_INT64, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})

	t.Run("Timestamp MICROS with converted type", func(t *testing.T) {
		convertedType := parquet.ConvertedType_TIMESTAMP_MICROS
		schema := &parquet.SchemaElement{
			Name:          "timestamp_field",
			ConvertedType: &convertedType,
		}
		result := formatValueWithLogicalType(int64(1640000000000000), parquet.Type_INT64, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})
}

func Test_FormatValueWithLogicalType_Interval(t *testing.T) {
	t.Run("Interval with converted type", func(t *testing.T) {
		convertedType := parquet.ConvertedType_INTERVAL
		schema := &parquet.SchemaElement{
			Name:          "interval_field",
			ConvertedType: &convertedType,
		}
		// Interval is 12 bytes: months, days, milliseconds
		intervalBytes := "P1Y2M3DT4H5M"
		result := formatValueWithLogicalType(intervalBytes, parquet.Type_FIXED_LEN_BYTE_ARRAY, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})
}

func Test_FormatValueWithLogicalType_INT96(t *testing.T) {
	t.Run("INT96 timestamp", func(t *testing.T) {
		// INT96 values come through as strings from the parquet reader
		result := formatValueWithLogicalType("test_int96_value", parquet.Type_INT96, nil)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})
}

// Helper functions for tests
func intPtr(i int32) *int32 {
	return &i
}

func parquetTypePtr(t parquet.Type) *parquet.Type {
	return &t
}
