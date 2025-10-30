package model

import (
	"testing"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/stretchr/testify/require"
)

func Test_GetTotalSize(t *testing.T) {
	tests := []struct {
		name     string
		rowGroup *parquet.RowGroup
		expected int64
	}{
		{
			name: "Empty row group",
			rowGroup: &parquet.RowGroup{
				Columns: []*parquet.ColumnChunk{},
			},
			expected: 0,
		},
		{
			name: "Single column",
			rowGroup: &parquet.RowGroup{
				Columns: []*parquet.ColumnChunk{
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 1024}},
				},
			},
			expected: 1024,
		},
		{
			name: "Multiple columns",
			rowGroup: &parquet.RowGroup{
				Columns: []*parquet.ColumnChunk{
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 1024}},
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 2048}},
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 512}},
				},
			},
			expected: 3584,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getTotalSize(tt.rowGroup)
			require.Equal(t, tt.expected, result, "getTotalSize() should match")
		})
	}
}

func Test_CountLeafColumns(t *testing.T) {
	tests := []struct {
		name     string
		schema   []*parquet.SchemaElement
		expected int
	}{
		{
			name:     "Empty schema",
			schema:   []*parquet.SchemaElement{},
			expected: 0,
		},
		{
			name: "Only leaf columns",
			schema: []*parquet.SchemaElement{
				{Name: "root", NumChildren: intPtr(2)},
				{Name: "col1", Type: parquetTypePtr(parquet.Type_INT32)},
				{Name: "col2", Type: parquetTypePtr(parquet.Type_BYTE_ARRAY)},
			},
			expected: 2,
		},
		{
			name: "Mixed leaf and group nodes",
			schema: []*parquet.SchemaElement{
				{Name: "root", NumChildren: intPtr(3)},
				{Name: "col1", Type: parquetTypePtr(parquet.Type_INT32)},
				{Name: "group", NumChildren: intPtr(2)},
				{Name: "col2", Type: parquetTypePtr(parquet.Type_INT64)},
				{Name: "col3", Type: parquetTypePtr(parquet.Type_BOOLEAN)},
			},
			expected: 3, // Only col1, col2, col3 are leaf columns
		},
		{
			name: "Only group nodes",
			schema: []*parquet.SchemaElement{
				{Name: "root", NumChildren: intPtr(2)},
				{Name: "group1", NumChildren: intPtr(1)},
				{Name: "group2", NumChildren: intPtr(1)},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := countLeafColumns(tt.schema)
			require.Equal(t, tt.expected, result, "countLeafColumns() should match")
		})
	}
}

func Test_FormatPathInSchema(t *testing.T) {
	tests := []struct {
		name     string
		path     []string
		expected string
	}{
		{
			name:     "Empty path",
			path:     []string{},
			expected: "",
		},
		{
			name:     "Single element",
			path:     []string{"field1"},
			expected: "field1",
		},
		{
			name:     "Multiple elements",
			path:     []string{"parent", "child", "grandchild"},
			expected: "parent.child.grandchild",
		},
		{
			name:     "With special characters",
			path:     []string{"my_field", "sub_field"},
			expected: "my_field.sub_field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatPathInSchema(tt.path)
			require.Equal(t, tt.expected, result, "formatPathInSchema() should match")
		})
	}
}

func Test_FormatLogicalType(t *testing.T) {
	tests := []struct {
		name        string
		logicalType *parquet.LogicalType
		expected    string
	}{
		{
			name:        "Nil logical type",
			logicalType: nil,
			expected:    "-",
		},
		{
			name:        "STRING type",
			logicalType: &parquet.LogicalType{STRING: &parquet.StringType{}},
			expected:    "STRING",
		},
		{
			name:        "MAP type",
			logicalType: &parquet.LogicalType{MAP: &parquet.MapType{}},
			expected:    "MAP",
		},
		{
			name:        "LIST type",
			logicalType: &parquet.LogicalType{LIST: &parquet.ListType{}},
			expected:    "LIST",
		},
		{
			name:        "ENUM type",
			logicalType: &parquet.LogicalType{ENUM: &parquet.EnumType{}},
			expected:    "ENUM",
		},
		{
			name: "DECIMAL type",
			logicalType: &parquet.LogicalType{
				DECIMAL: &parquet.DecimalType{Precision: 10, Scale: 2},
			},
			expected: "DECIMAL(10,2)",
		},
		{
			name:        "DATE type",
			logicalType: &parquet.LogicalType{DATE: &parquet.DateType{}},
			expected:    "DATE",
		},
		{
			name:        "JSON type",
			logicalType: &parquet.LogicalType{JSON: &parquet.JsonType{}},
			expected:    "JSON",
		},
		{
			name:        "BSON type",
			logicalType: &parquet.LogicalType{BSON: &parquet.BsonType{}},
			expected:    "BSON",
		},
		{
			name:        "UUID type",
			logicalType: &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			expected:    "UUID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatLogicalType(tt.logicalType)
			require.Equal(t, tt.expected, result, "formatLogicalType() should match")
		})
	}
}

func Test_FormatConvertedType(t *testing.T) {
	tests := []struct {
		name          string
		convertedType *parquet.ConvertedType
		expected      string
	}{
		{
			name:          "Nil converted type",
			convertedType: nil,
			expected:      "-",
		},
		{
			name:          "UTF8 type",
			convertedType: convertedTypePtr(parquet.ConvertedType_UTF8),
			expected:      "UTF8",
		},
		{
			name:          "MAP type",
			convertedType: convertedTypePtr(parquet.ConvertedType_MAP),
			expected:      "MAP",
		},
		{
			name:          "LIST type",
			convertedType: convertedTypePtr(parquet.ConvertedType_LIST),
			expected:      "LIST",
		},
		{
			name:          "DECIMAL type",
			convertedType: convertedTypePtr(parquet.ConvertedType_DECIMAL),
			expected:      "DECIMAL",
		},
		{
			name:          "DATE type",
			convertedType: convertedTypePtr(parquet.ConvertedType_DATE),
			expected:      "DATE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatConvertedType(tt.convertedType)
			require.Equal(t, tt.expected, result, "formatConvertedType() should match")
		})
	}
}

// Compression and value decoding tests are covered by the parquet-go library
// The wrapper functions have been removed - callers now use compress.Uncompress() and encoding.ReadPlain() directly

// LZ4 and Zstd decompression tests are covered by the parquet-go library

// Helper function
func convertedTypePtr(ct parquet.ConvertedType) *parquet.ConvertedType {
	return &ct
}

// Gzip decompression and plain value decoding tests are covered by the parquet-go library

// Test findSchemaElement with various paths
func Test_FindSchemaElement(t *testing.T) {
	schema := []*parquet.SchemaElement{
		{Name: "Parquet_go_root", NumChildren: intPtr(3)},
		{Name: "field1", Type: parquetTypePtr(parquet.Type_INT32)},
		{Name: "group1", NumChildren: intPtr(2)},
		{Name: "nested1", Type: parquetTypePtr(parquet.Type_INT64)},
		{Name: "nested2", Type: parquetTypePtr(parquet.Type_BOOLEAN)},
		{Name: "field2", Type: parquetTypePtr(parquet.Type_BYTE_ARRAY)},
	}

	tests := []struct {
		name     string
		path     []string
		expected string // expected element name or empty if nil
	}{
		{
			name:     "Empty path",
			path:     []string{},
			expected: "",
		},
		{
			name:     "Root level field",
			path:     []string{"field1"},
			expected: "field1",
		},
		{
			name:     "Nested field",
			path:     []string{"group1", "nested1"},
			expected: "nested1",
		},
		{
			name:     "Case insensitive match",
			path:     []string{"FIELD1"},
			expected: "field1",
		},
		{
			name:     "Non-existent path",
			path:     []string{"does_not_exist"},
			expected: "",
		},
		{
			name:     "Partial match only",
			path:     []string{"group1"},
			expected: "group1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findSchemaElement(schema, tt.path)
			if tt.expected == "" {
				require.Nil(t, result)
			} else {
				require.NotNil(t, result)
				require.Equal(t, tt.expected, result.Name)
			}
		})
	}
}

// Test formatLogicalType with time types
func Test_FormatLogicalType_TimeTypes(t *testing.T) {
	tests := []struct {
		name        string
		logicalType *parquet.LogicalType
		expected    string
	}{
		{
			name: "TIME with millis",
			logicalType: &parquet.LogicalType{
				TIME: &parquet.TimeType{
					IsAdjustedToUTC: true,
					Unit:            &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
				},
			},
			expected: "TIME(TimeUnit({MILLIS:MilliSeconds({}) MICROS:<nil> NANOS:<nil>}),true)",
		},
		{
			name: "TIME with micros",
			logicalType: &parquet.LogicalType{
				TIME: &parquet.TimeType{
					IsAdjustedToUTC: false,
					Unit:            &parquet.TimeUnit{MICROS: &parquet.MicroSeconds{}},
				},
			},
			expected: "TIME(TimeUnit({MILLIS:<nil> MICROS:MicroSeconds({}) NANOS:<nil>}),false)",
		},
		{
			name: "TIME with nanos",
			logicalType: &parquet.LogicalType{
				TIME: &parquet.TimeType{
					IsAdjustedToUTC: true,
					Unit:            &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
				},
			},
			expected: "TIME(TimeUnit({MILLIS:<nil> MICROS:<nil> NANOS:NanoSeconds({})}),true)",
		},
		{
			name: "TIMESTAMP with millis",
			logicalType: &parquet.LogicalType{
				TIMESTAMP: &parquet.TimestampType{
					IsAdjustedToUTC: true,
					Unit:            &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
				},
			},
			expected: "TIMESTAMP(TimeUnit({MILLIS:MilliSeconds({}) MICROS:<nil> NANOS:<nil>}),true)",
		},
		{
			name: "TIMESTAMP with micros",
			logicalType: &parquet.LogicalType{
				TIMESTAMP: &parquet.TimestampType{
					IsAdjustedToUTC: false,
					Unit:            &parquet.TimeUnit{MICROS: &parquet.MicroSeconds{}},
				},
			},
			expected: "TIMESTAMP(TimeUnit({MILLIS:<nil> MICROS:MicroSeconds({}) NANOS:<nil>}),false)",
		},
		{
			name: "TIMESTAMP with nanos",
			logicalType: &parquet.LogicalType{
				TIMESTAMP: &parquet.TimestampType{
					IsAdjustedToUTC: true,
					Unit:            &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
				},
			},
			expected: "TIMESTAMP(TimeUnit({MILLIS:<nil> MICROS:<nil> NANOS:NanoSeconds({})}),true)",
		},
		{
			name: "INTEGER with bit width",
			logicalType: &parquet.LogicalType{
				INTEGER: &parquet.IntType{
					BitWidth: 8,
					IsSigned: true,
				},
			},
			expected: "INTEGER(8,signed)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatLogicalType(tt.logicalType)
			require.Equal(t, tt.expected, result)
		})
	}
}

// Test formatLogicalType with additional types
func Test_FormatLogicalType_AdditionalTypes(t *testing.T) {
	tests := []struct {
		name        string
		logicalType *parquet.LogicalType
		expected    string
	}{
		{
			name:        "UNKNOWN type",
			logicalType: &parquet.LogicalType{UNKNOWN: &parquet.NullType{}},
			expected:    "UNKNOWN",
		},
		{
			name:        "JSON type",
			logicalType: &parquet.LogicalType{JSON: &parquet.JsonType{}},
			expected:    "JSON",
		},
		{
			name:        "BSON type",
			logicalType: &parquet.LogicalType{BSON: &parquet.BsonType{}},
			expected:    "BSON",
		},
		{
			name:        "UUID type",
			logicalType: &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			expected:    "UUID",
		},
		{
			name:        "FLOAT16 type",
			logicalType: &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			expected:    "FLOAT16",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatLogicalType(tt.logicalType)
			require.Equal(t, tt.expected, result)
		})
	}
}
