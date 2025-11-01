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
	t.Run("INT96 timestamp with string value", func(t *testing.T) {
		// INT96 values come through as strings from the parquet reader
		schema := &parquet.SchemaElement{
			Name: "int96_field",
		}
		result := formatValueWithLogicalType("test_int96_value", parquet.Type_INT96, schema)
		require.NotNil(t, result, "formatValueWithLogicalType() should return value")
	})

	t.Run("INT96 timestamp with nil schema", func(t *testing.T) {
		// INT96 values come through as strings from the parquet reader
		result := formatValueWithLogicalType("test_int96_value", parquet.Type_INT96, nil)
		require.Equal(t, "test_int96_value", result, "Should return value as-is when schema is nil")
	})

	t.Run("INT96 with non-string value (error path)", func(t *testing.T) {
		// Test the error path where INT96 value is not a string
		// This tests line 284: the implicit return when type assertion fails
		schema := &parquet.SchemaElement{
			Name: "int96_field",
		}
		// Pass a non-string value to trigger the error path
		result := formatValueWithLogicalType(int64(12345), parquet.Type_INT96, schema)
		// Since type assertion fails, it should skip the decodeValue call
		// and continue to the next checks, eventually returning the value as-is
		require.Equal(t, int64(12345), result, "Should return value as-is when type assertion fails")
	})

	t.Run("INT96 with byte array value", func(t *testing.T) {
		schema := &parquet.SchemaElement{
			Name: "int96_field",
		}
		// Test with byte array (another non-string type)
		result := formatValueWithLogicalType([]byte("int96bytes"), parquet.Type_INT96, schema)
		// Should go through byte array handling path
		require.NotNil(t, result, "Should handle byte array")
	})
}

// Test decodeStatValue with INTERVAL converted type
func Test_DecodeStatValue_Interval(t *testing.T) {
	t.Run("INTERVAL with string value", func(t *testing.T) {
		convertedType := parquet.ConvertedType_INTERVAL
		schema := &parquet.SchemaElement{
			Name:          "interval_field",
			ConvertedType: &convertedType,
		}
		// Interval is stored as 12 bytes
		intervalBytes := "P1Y2M3DT4H5M"
		result := decodeStatValue(intervalBytes, parquet.Type_FIXED_LEN_BYTE_ARRAY, schema)
		require.NotNil(t, result, "decodeStatValue() should return value")
	})

	t.Run("INTERVAL with non-string value", func(t *testing.T) {
		convertedType := parquet.ConvertedType_INTERVAL
		schema := &parquet.SchemaElement{
			Name:          "interval_field",
			ConvertedType: &convertedType,
		}
		// Test with non-string value - should return value as-is
		result := decodeStatValue(int64(12345), parquet.Type_FIXED_LEN_BYTE_ARRAY, schema)
		require.Equal(t, int64(12345), result, "Should return value as-is for non-string")
	})

	t.Run("INTERVAL with nil value", func(t *testing.T) {
		convertedType := parquet.ConvertedType_INTERVAL
		schema := &parquet.SchemaElement{
			Name:          "interval_field",
			ConvertedType: &convertedType,
		}
		result := decodeStatValue(nil, parquet.Type_FIXED_LEN_BYTE_ARRAY, schema)
		require.Nil(t, result, "Should return nil for nil value")
	})

	t.Run("INTERVAL with nil schema", func(t *testing.T) {
		result := decodeStatValue("P1Y2M3DT4H5M", parquet.Type_FIXED_LEN_BYTE_ARRAY, nil)
		require.Equal(t, "P1Y2M3DT4H5M", result, "Should return value as-is when schema is nil")
	})
}

// Test formatValueWithLogicalType with float32 and float64 for DECIMAL
func Test_FormatValueWithLogicalType_FloatTypes(t *testing.T) {
	t.Run("DECIMAL with float32 value", func(t *testing.T) {
		scale := int32(2)
		precision := int32(10)
		decimalType := &parquet.LogicalType{DECIMAL: &parquet.DecimalType{Scale: scale, Precision: precision}}
		schema := &parquet.SchemaElement{
			Name:        "decimal_field",
			LogicalType: decimalType,
			Scale:       &scale,
			Precision:   &precision,
		}
		// Test with float32 - should return as-is (line 338)
		result := formatValueWithLogicalType(float32(123.45), parquet.Type_FLOAT, schema)
		require.Equal(t, float32(123.45), result, "Should return float32 as-is")
	})

	t.Run("DECIMAL with float64 value", func(t *testing.T) {
		scale := int32(2)
		precision := int32(10)
		decimalType := &parquet.LogicalType{DECIMAL: &parquet.DecimalType{Scale: scale, Precision: precision}}
		schema := &parquet.SchemaElement{
			Name:        "decimal_field",
			LogicalType: decimalType,
			Scale:       &scale,
			Precision:   &precision,
		}
		// Test with float64 - should return as-is (line 338)
		result := formatValueWithLogicalType(float64(123.45), parquet.Type_DOUBLE, schema)
		require.Equal(t, float64(123.45), result, "Should return float64 as-is")
	})

	t.Run("DECIMAL with zero scale and float32", func(t *testing.T) {
		scale := int32(0) // Zero scale - won't trigger the float path
		precision := int32(10)
		decimalType := &parquet.LogicalType{DECIMAL: &parquet.DecimalType{Scale: scale, Precision: precision}}
		schema := &parquet.SchemaElement{
			Name:        "decimal_field",
			LogicalType: decimalType,
			Scale:       &scale,
			Precision:   &precision,
		}
		// With zero scale, should not go through the float path
		result := formatValueWithLogicalType(float32(123.45), parquet.Type_FLOAT, schema)
		require.Equal(t, float32(123.45), result, "Should return value as-is")
	})

	t.Run("DECIMAL with nil scale and float32", func(t *testing.T) {
		precision := int32(10)
		decimalType := &parquet.LogicalType{DECIMAL: &parquet.DecimalType{Precision: precision}}
		schema := &parquet.SchemaElement{
			Name:        "decimal_field",
			LogicalType: decimalType,
			Scale:       nil, // Nil scale
			Precision:   &precision,
		}
		// With nil scale, should not go through the float path
		result := formatValueWithLogicalType(float32(123.45), parquet.Type_FLOAT, schema)
		require.Equal(t, float32(123.45), result, "Should return value as-is")
	})

	t.Run("Non-DECIMAL float32 value", func(t *testing.T) {
		schema := &parquet.SchemaElement{
			Name: "float_field",
		}
		// Float without decimal logical type
		result := formatValueWithLogicalType(float32(3.14), parquet.Type_FLOAT, schema)
		require.Equal(t, float32(3.14), result, "Should return float32 as-is")
	})

	t.Run("Non-DECIMAL float64 value", func(t *testing.T) {
		schema := &parquet.SchemaElement{
			Name: "double_field",
		}
		// Float without decimal logical type
		result := formatValueWithLogicalType(float64(3.14159), parquet.Type_DOUBLE, schema)
		require.Equal(t, float64(3.14159), result, "Should return float64 as-is")
	})
}

// Test formatValueWithLogicalType with UUID logical type
func Test_FormatValueWithLogicalType_UUID(t *testing.T) {
	t.Run("UUID with string value", func(t *testing.T) {
		uuidType := &parquet.LogicalType{UUID: &parquet.UUIDType{}}
		schema := &parquet.SchemaElement{
			Name:        "uuid_field",
			LogicalType: uuidType,
		}
		// Test UUID as string (line 356-359)
		uuidStr := "550e8400-e29b-41d4-a716-446655440000"
		result := formatValueWithLogicalType(uuidStr, parquet.Type_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle UUID string")
	})

	t.Run("UUID with non-string value", func(t *testing.T) {
		uuidType := &parquet.LogicalType{UUID: &parquet.UUIDType{}}
		schema := &parquet.SchemaElement{
			Name:        "uuid_field",
			LogicalType: uuidType,
		}
		// Test with non-string value - should not trigger UUID conversion
		result := formatValueWithLogicalType(int32(12345), parquet.Type_BYTE_ARRAY, schema)
		require.Equal(t, int32(12345), result, "Should return value as-is for non-string")
	})

	t.Run("UUID with byte array", func(t *testing.T) {
		uuidType := &parquet.LogicalType{UUID: &parquet.UUIDType{}}
		schema := &parquet.SchemaElement{
			Name:        "uuid_field",
			LogicalType: uuidType,
		}
		// UUID as byte array (16 bytes)
		uuidBytes := []byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
		result := formatValueWithLogicalType(uuidBytes, parquet.Type_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle UUID bytes")
	})
}

// Test formatValueWithLogicalType with INTERVAL and DECIMAL converted types
func Test_FormatValueWithLogicalType_ConvertedTypes(t *testing.T) {
	t.Run("ConvertedType_INTERVAL with string value", func(t *testing.T) {
		convertedType := parquet.ConvertedType_INTERVAL
		schema := &parquet.SchemaElement{
			Name:          "interval_field",
			ConvertedType: &convertedType,
		}
		// Test INTERVAL converted type (line 366-369)
		intervalStr := "P1Y2M3DT4H5M"
		result := formatValueWithLogicalType(intervalStr, parquet.Type_FIXED_LEN_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle INTERVAL string")
	})

	t.Run("ConvertedType_INTERVAL with non-string value", func(t *testing.T) {
		convertedType := parquet.ConvertedType_INTERVAL
		schema := &parquet.SchemaElement{
			Name:          "interval_field",
			ConvertedType: &convertedType,
		}
		// Test with non-string value - should not trigger INTERVAL conversion (line 369)
		result := formatValueWithLogicalType(int64(12345), parquet.Type_FIXED_LEN_BYTE_ARRAY, schema)
		require.Equal(t, int64(12345), result, "Should return value as-is for non-string")
	})

	t.Run("ConvertedType_INTERVAL with byte array", func(t *testing.T) {
		convertedType := parquet.ConvertedType_INTERVAL
		schema := &parquet.SchemaElement{
			Name:          "interval_field",
			ConvertedType: &convertedType,
		}
		// Test INTERVAL with byte array - should be handled through byte array path
		intervalBytes := []byte{0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00}
		result := formatValueWithLogicalType(intervalBytes, parquet.Type_FIXED_LEN_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle INTERVAL byte array")
	})

	t.Run("ConvertedType_DECIMAL as BYTE_ARRAY with string", func(t *testing.T) {
		convertedType := parquet.ConvertedType_DECIMAL
		scale := int32(2)
		precision := int32(10)
		schema := &parquet.SchemaElement{
			Name:          "decimal_field",
			ConvertedType: &convertedType,
			Scale:         &scale,
			Precision:     &precision,
		}
		// Test DECIMAL as BYTE_ARRAY with string (line 372-375)
		result := formatValueWithLogicalType("12345", parquet.Type_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle DECIMAL as BYTE_ARRAY string")
	})

	t.Run("ConvertedType_DECIMAL as FIXED_LEN_BYTE_ARRAY with string", func(t *testing.T) {
		convertedType := parquet.ConvertedType_DECIMAL
		scale := int32(2)
		precision := int32(10)
		schema := &parquet.SchemaElement{
			Name:          "decimal_field",
			ConvertedType: &convertedType,
			Scale:         &scale,
			Precision:     &precision,
		}
		// Test DECIMAL as FIXED_LEN_BYTE_ARRAY with string (line 372-375)
		result := formatValueWithLogicalType("12345", parquet.Type_FIXED_LEN_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle DECIMAL as FIXED_LEN_BYTE_ARRAY string")
	})

	t.Run("ConvertedType_DECIMAL as BYTE_ARRAY with byte array", func(t *testing.T) {
		convertedType := parquet.ConvertedType_DECIMAL
		scale := int32(2)
		precision := int32(10)
		schema := &parquet.SchemaElement{
			Name:          "decimal_field",
			ConvertedType: &convertedType,
			Scale:         &scale,
			Precision:     &precision,
		}
		// Test DECIMAL as BYTE_ARRAY with byte array
		decimalBytes := []byte{0x01, 0x02, 0x03, 0x04}
		result := formatValueWithLogicalType(decimalBytes, parquet.Type_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle DECIMAL as BYTE_ARRAY with bytes")
	})

	t.Run("ConvertedType_DECIMAL with int32", func(t *testing.T) {
		convertedType := parquet.ConvertedType_DECIMAL
		scale := int32(2)
		precision := int32(10)
		schema := &parquet.SchemaElement{
			Name:          "decimal_field",
			ConvertedType: &convertedType,
			Scale:         &scale,
			Precision:     &precision,
		}
		// Test DECIMAL with int32 (line 378-382)
		result := formatValueWithLogicalType(int32(12345), parquet.Type_INT32, schema)
		require.NotNil(t, result, "Should handle DECIMAL int32")
	})

	t.Run("ConvertedType_DECIMAL with int64", func(t *testing.T) {
		convertedType := parquet.ConvertedType_DECIMAL
		scale := int32(3)
		precision := int32(18)
		schema := &parquet.SchemaElement{
			Name:          "decimal_field",
			ConvertedType: &convertedType,
			Scale:         &scale,
			Precision:     &precision,
		}
		// Test DECIMAL with int64 (line 378-382)
		result := formatValueWithLogicalType(int64(123456789), parquet.Type_INT64, schema)
		require.NotNil(t, result, "Should handle DECIMAL int64")
	})

	t.Run("ConvertedType_DECIMAL with non-string non-int", func(t *testing.T) {
		convertedType := parquet.ConvertedType_DECIMAL
		scale := int32(2)
		precision := int32(10)
		schema := &parquet.SchemaElement{
			Name:          "decimal_field",
			ConvertedType: &convertedType,
			Scale:         &scale,
			Precision:     &precision,
		}
		// Test with type that doesn't match any case
		result := formatValueWithLogicalType(float32(123.45), parquet.Type_INT32, schema)
		require.Equal(t, float32(123.45), result, "Should return value as-is")
	})

	t.Run("ConvertedType_DECIMAL as BYTE_ARRAY with non-BYTE_ARRAY type", func(t *testing.T) {
		convertedType := parquet.ConvertedType_DECIMAL
		scale := int32(2)
		precision := int32(10)
		schema := &parquet.SchemaElement{
			Name:          "decimal_field",
			ConvertedType: &convertedType,
			Scale:         &scale,
			Precision:     &precision,
		}
		// Test DECIMAL with string but wrong parquet type (not BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY)
		result := formatValueWithLogicalType("12345", parquet.Type_INT32, schema)
		// Should go through the int32/int64 switch instead
		require.NotNil(t, result, "Should handle but skip BYTE_ARRAY path")
	})

	t.Run("ConvertedType_DECIMAL as BYTE_ARRAY with non-string value", func(t *testing.T) {
		convertedType := parquet.ConvertedType_DECIMAL
		scale := int32(2)
		precision := int32(10)
		schema := &parquet.SchemaElement{
			Name:          "decimal_field",
			ConvertedType: &convertedType,
			Scale:         &scale,
			Precision:     &precision,
		}
		// Test DECIMAL as BYTE_ARRAY but value is not string
		result := formatValueWithLogicalType(int32(999), parquet.Type_BYTE_ARRAY, schema)
		require.Equal(t, int32(999), result, "Should return value as-is")
	})

	t.Run("ConvertedType_INTERVAL with nil schema", func(t *testing.T) {
		// Test with nil schema - should return value as-is
		result := formatValueWithLogicalType("P1Y2M3DT4H5M", parquet.Type_FIXED_LEN_BYTE_ARRAY, nil)
		require.Equal(t, "P1Y2M3DT4H5M", result, "Should return value as-is with nil schema")
	})

	t.Run("ConvertedType_DECIMAL with nil schema", func(t *testing.T) {
		// Test with nil schema - should return value as-is
		result := formatValueWithLogicalType(int32(12345), parquet.Type_INT32, nil)
		require.Equal(t, int32(12345), result, "Should return value as-is with nil schema")
	})
}

// Test decodeValue with Type_INT96 and plain Type_BYTE_ARRAY/Type_FIXED_LEN_BYTE_ARRAY
func Test_DecodeValue_INT96_And_ByteArrays(t *testing.T) {
	t.Run("Type_INT96 with string value", func(t *testing.T) {
		schema := &parquet.SchemaElement{
			Name: "int96_field",
		}
		// Test INT96 conversion (line 416-421)
		result := decodeValue("int96testvalue", parquet.Type_INT96, schema)
		require.NotNil(t, result, "Should convert INT96 string")
	})

	t.Run("Type_INT96 with non-string value", func(t *testing.T) {
		schema := &parquet.SchemaElement{
			Name: "int96_field",
		}
		// Test INT96 with non-string (line 420 - return value as-is)
		result := decodeValue(int64(12345), parquet.Type_INT96, schema)
		require.Equal(t, int64(12345), result, "Should return value as-is for non-string")
	})

	t.Run("Type_INT96 with nil schema", func(t *testing.T) {
		// Test INT96 with nil schema (line 411-413)
		result := decodeValue("int96testvalue", parquet.Type_INT96, nil)
		require.Equal(t, "int96testvalue", result, "Should return value as-is when schema is nil")
	})

	t.Run("Type_BYTE_ARRAY without logical/converted type with string", func(t *testing.T) {
		schema := &parquet.SchemaElement{
			Name:          "byte_array_field",
			ConvertedType: nil,
			LogicalType:   nil,
		}
		// Test plain BYTE_ARRAY without type annotations (line 424-430)
		result := decodeValue("testbytes", parquet.Type_BYTE_ARRAY, schema)
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		// Should be base64 encoded
		require.NotEqual(t, "testbytes", resultStr, "Should be base64 encoded")
	})

	t.Run("Type_BYTE_ARRAY without logical/converted type with non-string", func(t *testing.T) {
		schema := &parquet.SchemaElement{
			Name:          "byte_array_field",
			ConvertedType: nil,
			LogicalType:   nil,
		}
		// Test plain BYTE_ARRAY with non-string value (line 429 - return value)
		result := decodeValue(int32(12345), parquet.Type_BYTE_ARRAY, schema)
		require.Equal(t, int32(12345), result, "Should return value as-is for non-string")
	})

	t.Run("Type_FIXED_LEN_BYTE_ARRAY without logical/converted type with string", func(t *testing.T) {
		schema := &parquet.SchemaElement{
			Name:          "fixed_byte_array_field",
			ConvertedType: nil,
			LogicalType:   nil,
		}
		// Test plain FIXED_LEN_BYTE_ARRAY without type annotations (line 424-430)
		result := decodeValue("fixedbytes", parquet.Type_FIXED_LEN_BYTE_ARRAY, schema)
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		// Should be base64 encoded
		require.NotEqual(t, "fixedbytes", resultStr, "Should be base64 encoded")
	})

	t.Run("Type_FIXED_LEN_BYTE_ARRAY without logical/converted type with non-string", func(t *testing.T) {
		schema := &parquet.SchemaElement{
			Name:          "fixed_byte_array_field",
			ConvertedType: nil,
			LogicalType:   nil,
		}
		// Test plain FIXED_LEN_BYTE_ARRAY with non-string value (line 429)
		result := decodeValue(int32(54321), parquet.Type_FIXED_LEN_BYTE_ARRAY, schema)
		require.Equal(t, int32(54321), result, "Should return value as-is for non-string")
	})

	t.Run("Type_BYTE_ARRAY with logical type (should not be base64 encoded)", func(t *testing.T) {
		stringType := &parquet.LogicalType{STRING: &parquet.StringType{}}
		schema := &parquet.SchemaElement{
			Name:        "string_field",
			LogicalType: stringType,
		}
		// With logical type, should not go through the plain BYTE_ARRAY path
		result := decodeValue("normalstring", parquet.Type_BYTE_ARRAY, schema)
		require.Equal(t, "normalstring", result, "Should return string as-is with logical type")
	})

	t.Run("Type_BYTE_ARRAY with converted type (should not be base64 encoded)", func(t *testing.T) {
		convertedType := parquet.ConvertedType_UTF8
		schema := &parquet.SchemaElement{
			Name:          "utf8_field",
			ConvertedType: &convertedType,
		}
		// With converted type, should not go through the plain BYTE_ARRAY path
		result := decodeValue("utf8string", parquet.Type_BYTE_ARRAY, schema)
		require.Equal(t, "utf8string", result, "Should return string as-is with converted type")
	})
}

// Test retrieveRawValue error paths - when binary.Read fails
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
			result := retrieveRawValue(tt.value, tt.parquetType)
			if tt.expectError {
				// Should return an error string
				resultStr, ok := result.(string)
				require.True(t, ok, "Result should be a string for error case")
				require.Contains(t, resultStr, "error:", "Error string should contain 'error:'")
			} else {
				require.NotContains(t, result, "error:", "Result should not contain error")
			}
		})
	}
}

// Test retrieveRawValue with edge cases
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
			result := retrieveRawValue(tt.value, tt.parquetType)
			require.Equal(t, tt.expected, result, "retrieveRawValue() should match")
		})
	}
}

// Test retrieveRawValue with maximum and minimum values
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
			result := retrieveRawValue(tt.value, tt.parquetType)
			require.Equal(t, tt.expected, result, "retrieveRawValue() should match")
		})
	}
}

// Test TIME converted types with proper type-specific handling
func Test_DecodeStatValue_TIME_ConvertedTypes(t *testing.T) {
	t.Run("ConvertedType_TIME_MILLIS with int32", func(t *testing.T) {
		convertedType := parquet.ConvertedType_TIME_MILLIS
		schema := &parquet.SchemaElement{
			Name:          "time_millis_field",
			ConvertedType: &convertedType,
		}
		// Test TIME_MILLIS with int32 (milliseconds since midnight)
		result := decodeStatValue(int32(3661000), parquet.Type_INT32, schema)
		require.NotNil(t, result, "Should convert TIME_MILLIS")
		// Should return time format string
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		require.NotEmpty(t, resultStr, "Result should not be empty")
	})

	t.Run("ConvertedType_TIME_MILLIS with non-int32", func(t *testing.T) {
		convertedType := parquet.ConvertedType_TIME_MILLIS
		schema := &parquet.SchemaElement{
			Name:          "time_millis_field",
			ConvertedType: &convertedType,
		}
		// Test with wrong type - should return value as-is
		result := decodeStatValue("notanint", parquet.Type_INT32, schema)
		require.Equal(t, "notanint", result, "Should return value as-is for non-int32")
	})

	t.Run("ConvertedType_TIME_MICROS with int64", func(t *testing.T) {
		convertedType := parquet.ConvertedType_TIME_MICROS
		schema := &parquet.SchemaElement{
			Name:          "time_micros_field",
			ConvertedType: &convertedType,
		}
		// Test TIME_MICROS with int64 (microseconds since midnight)
		result := decodeStatValue(int64(3661000000), parquet.Type_INT64, schema)
		require.NotNil(t, result, "Should convert TIME_MICROS")
		// Should return time format string
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		require.NotEmpty(t, resultStr, "Result should not be empty")
	})

	t.Run("ConvertedType_TIME_MICROS with non-int64", func(t *testing.T) {
		convertedType := parquet.ConvertedType_TIME_MICROS
		schema := &parquet.SchemaElement{
			Name:          "time_micros_field",
			ConvertedType: &convertedType,
		}
		// Test with wrong type - should return value as-is
		result := decodeStatValue(int32(12345), parquet.Type_INT64, schema)
		require.Equal(t, int32(12345), result, "Should return value as-is for non-int64")
	})
}

// Test TIME converted types in decodeValue
func Test_DecodeValue_TIME_ConvertedTypes(t *testing.T) {
	t.Run("ConvertedType_TIME_MILLIS with int32", func(t *testing.T) {
		convertedType := parquet.ConvertedType_TIME_MILLIS
		schema := &parquet.SchemaElement{
			Name:          "time_millis_field",
			ConvertedType: &convertedType,
		}
		result := decodeValue(int32(3661000), parquet.Type_INT32, schema)
		require.NotNil(t, result, "Should convert TIME_MILLIS")
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		require.NotEmpty(t, resultStr, "Result should not be empty")
	})

	t.Run("ConvertedType_TIME_MICROS with int64", func(t *testing.T) {
		convertedType := parquet.ConvertedType_TIME_MICROS
		schema := &parquet.SchemaElement{
			Name:          "time_micros_field",
			ConvertedType: &convertedType,
		}
		result := decodeValue(int64(3661000000), parquet.Type_INT64, schema)
		require.NotNil(t, result, "Should convert TIME_MICROS")
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		require.NotEmpty(t, resultStr, "Result should not be empty")
	})
}

// Test TIMESTAMP logical types with MICROS and NANOS
func Test_DecodeStatValue_TIMESTAMP_LogicalTypes(t *testing.T) {
	t.Run("TIMESTAMP with MILLIS unit", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_millis_field",
			LogicalType: timestampType,
		}
		result := decodeStatValue(int64(1640000000000), parquet.Type_INT64, schema)
		require.NotNil(t, result, "Should convert TIMESTAMP_MILLIS")
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		require.Contains(t, resultStr, "2021", "Should contain year 2021")
	})

	t.Run("TIMESTAMP with MICROS unit", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{MICROS: &parquet.MicroSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_micros_field",
			LogicalType: timestampType,
		}
		result := decodeStatValue(int64(1640000000000000), parquet.Type_INT64, schema)
		require.NotNil(t, result, "Should convert TIMESTAMP_MICROS")
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		require.Contains(t, resultStr, "2021", "Should contain year 2021")
	})

	t.Run("TIMESTAMP with NANOS unit", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_nanos_field",
			LogicalType: timestampType,
		}
		result := decodeStatValue(int64(1640000000000000000), parquet.Type_INT64, schema)
		require.NotNil(t, result, "Should convert TIMESTAMP_NANOS")
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		require.Contains(t, resultStr, "2021", "Should contain year 2021")
	})

	t.Run("TIMESTAMP with non-int64 value", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_field",
			LogicalType: timestampType,
		}
		// Test with non-int64 - should return value as-is
		result := decodeStatValue("not_a_timestamp", parquet.Type_INT64, schema)
		require.Equal(t, "not_a_timestamp", result, "Should return value as-is for non-int64")
	})
}

// Test UUID, BSON, and FLOAT16 logical types in decodeStatValue
func Test_DecodeStatValue_UUID_BSON_FLOAT16(t *testing.T) {
	t.Run("UUID logical type with string", func(t *testing.T) {
		uuidType := &parquet.LogicalType{UUID: &parquet.UUIDType{}}
		schema := &parquet.SchemaElement{
			Name:        "uuid_field",
			LogicalType: uuidType,
		}
		// UUID as string
		uuidStr := "550e8400-e29b-41d4-a716-446655440000"
		result := decodeStatValue(uuidStr, parquet.Type_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle UUID")
	})

	t.Run("BSON logical type with string", func(t *testing.T) {
		bsonType := &parquet.LogicalType{BSON: &parquet.BsonType{}}
		schema := &parquet.SchemaElement{
			Name:        "bson_field",
			LogicalType: bsonType,
		}
		// BSON data as string
		result := decodeStatValue("bson_data", parquet.Type_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle BSON")
	})

	t.Run("BSON converted type with string", func(t *testing.T) {
		convertedType := parquet.ConvertedType_BSON
		schema := &parquet.SchemaElement{
			Name:          "bson_field",
			ConvertedType: &convertedType,
		}
		result := decodeStatValue("bson_data", parquet.Type_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle BSON converted type")
	})

	t.Run("FLOAT16 logical type", func(t *testing.T) {
		float16Type := &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}}
		schema := &parquet.SchemaElement{
			Name:        "float16_field",
			LogicalType: float16Type,
		}
		// FLOAT16 stored as 2 bytes
		result := decodeStatValue("f16", parquet.Type_FIXED_LEN_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle FLOAT16")
	})
}

// Test UUID, BSON, and FLOAT16 logical types in decodeValue
func Test_DecodeValue_UUID_BSON_FLOAT16(t *testing.T) {
	t.Run("UUID logical type", func(t *testing.T) {
		uuidType := &parquet.LogicalType{UUID: &parquet.UUIDType{}}
		schema := &parquet.SchemaElement{
			Name:        "uuid_field",
			LogicalType: uuidType,
		}
		uuidStr := "550e8400-e29b-41d4-a716-446655440000"
		result := decodeValue(uuidStr, parquet.Type_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle UUID")
	})

	t.Run("BSON logical type", func(t *testing.T) {
		bsonType := &parquet.LogicalType{BSON: &parquet.BsonType{}}
		schema := &parquet.SchemaElement{
			Name:        "bson_field",
			LogicalType: bsonType,
		}
		result := decodeValue("bson_data", parquet.Type_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle BSON")
	})

	t.Run("BSON converted type", func(t *testing.T) {
		convertedType := parquet.ConvertedType_BSON
		schema := &parquet.SchemaElement{
			Name:          "bson_field",
			ConvertedType: &convertedType,
		}
		result := decodeValue("bson_data", parquet.Type_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle BSON converted type")
	})

	t.Run("FLOAT16 logical type", func(t *testing.T) {
		float16Type := &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}}
		schema := &parquet.SchemaElement{
			Name:        "float16_field",
			LogicalType: float16Type,
		}
		result := decodeValue("f16", parquet.Type_FIXED_LEN_BYTE_ARRAY, schema)
		require.NotNil(t, result, "Should handle FLOAT16")
	})
}

// Test TIMESTAMP logical types with MICROS and NANOS in decodeValue
func Test_DecodeValue_TIMESTAMP_LogicalTypes(t *testing.T) {
	t.Run("TIMESTAMP with MILLIS unit and int64", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_millis_field",
			LogicalType: timestampType,
		}
		// Timestamp in milliseconds: December 20, 2021 @ 17:33:20 UTC
		result := decodeValue(int64(1640000000000), parquet.Type_INT64, schema)
		require.NotNil(t, result, "Should convert TIMESTAMP_MILLIS")
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		require.Contains(t, resultStr, "2021", "Should contain year 2021")
	})

	t.Run("TIMESTAMP with MICROS unit and int64", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{MICROS: &parquet.MicroSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_micros_field",
			LogicalType: timestampType,
		}
		// Timestamp in microseconds: December 20, 2021 @ 17:33:20 UTC
		result := decodeValue(int64(1640000000000000), parquet.Type_INT64, schema)
		require.NotNil(t, result, "Should convert TIMESTAMP_MICROS")
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		require.Contains(t, resultStr, "2021", "Should contain year 2021")
	})

	t.Run("TIMESTAMP with NANOS unit and int64", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_nanos_field",
			LogicalType: timestampType,
		}
		// Timestamp in nanoseconds: December 20, 2021 @ 17:33:20 UTC
		result := decodeValue(int64(1640000000000000000), parquet.Type_INT64, schema)
		require.NotNil(t, result, "Should convert TIMESTAMP_NANOS")
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		require.Contains(t, resultStr, "2021", "Should contain year 2021")
	})

	t.Run("TIMESTAMP with MILLIS unit and non-int64", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_millis_field",
			LogicalType: timestampType,
		}
		// Test with non-int64 value - should return value as-is (line 491)
		result := decodeValue("not_a_timestamp", parquet.Type_INT64, schema)
		require.Equal(t, "not_a_timestamp", result, "Should return value as-is for non-int64")
	})

	t.Run("TIMESTAMP with MICROS unit and non-int64", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{MICROS: &parquet.MicroSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_micros_field",
			LogicalType: timestampType,
		}
		// Test with non-int64 value - should return value as-is (line 491)
		result := decodeValue(int32(12345), parquet.Type_INT64, schema)
		require.Equal(t, int32(12345), result, "Should return value as-is for non-int64")
	})

	t.Run("TIMESTAMP with NANOS unit and non-int64", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_nanos_field",
			LogicalType: timestampType,
		}
		// Test with non-int64 value - should return value as-is (line 491)
		result := decodeValue([]byte("bytes"), parquet.Type_INT64, schema)
		require.Equal(t, []byte("bytes"), result, "Should return value as-is for non-int64")
	})

	t.Run("TIMESTAMP with nil schema", func(t *testing.T) {
		// Test with nil schema - should return value as-is (line 411-413)
		result := decodeValue(int64(1640000000000), parquet.Type_INT64, nil)
		require.Equal(t, int64(1640000000000), result, "Should return value as-is when schema is nil")
	})

	t.Run("TIMESTAMP with MILLIS unit and edge case timestamps", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_millis_field",
			LogicalType: timestampType,
		}

		// Test zero timestamp (Unix epoch)
		result := decodeValue(int64(0), parquet.Type_INT64, schema)
		require.NotNil(t, result, "Should handle zero timestamp")
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		require.Contains(t, resultStr, "1970", "Should contain year 1970")
	})

	t.Run("TIMESTAMP with MICROS precision test", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{MICROS: &parquet.MicroSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_micros_field",
			LogicalType: timestampType,
		}
		// Test a timestamp with microsecond precision
		// 1640000000123456 microseconds = December 20, 2021 @ 17:33:20.123456 UTC
		result := decodeValue(int64(1640000000123456), parquet.Type_INT64, schema)
		require.NotNil(t, result, "Should convert TIMESTAMP_MICROS with precision")
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		require.NotEmpty(t, resultStr, "Result should not be empty")
	})

	t.Run("TIMESTAMP with NANOS precision test", func(t *testing.T) {
		timestampType := &parquet.LogicalType{
			TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			},
		}
		schema := &parquet.SchemaElement{
			Name:        "timestamp_nanos_field",
			LogicalType: timestampType,
		}
		// Test a timestamp with nanosecond precision
		// 1640000000123456789 nanoseconds = December 20, 2021 @ 17:33:20.123456789 UTC
		result := decodeValue(int64(1640000000123456789), parquet.Type_INT64, schema)
		require.NotNil(t, result, "Should convert TIMESTAMP_NANOS with precision")
		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		require.NotEmpty(t, resultStr, "Result should not be empty")
	})
}

// Helper functions for tests
func intPtr(i int32) *int32 {
	return &i
}

func parquetTypePtr(t parquet.Type) *parquet.Type {
	return &t
}
