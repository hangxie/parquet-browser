package model

import (
	"context"
	"testing"

	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

func Test_FormatColumnName(t *testing.T) {
	tests := []struct {
		name     string
		path     []string
		expected string
	}{
		{
			name:     "Simple path",
			path:     []string{"column1"},
			expected: "column1",
		},
		{
			name:     "Nested path",
			path:     []string{"parent", "child"},
			expected: "parent.child",
		},
		{
			name:     "Deep nesting",
			path:     []string{"a", "b", "c", "d"},
			expected: "a.b.c.d",
		},
		{
			name:     "Empty path",
			path:     []string{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatColumnName(tt.path)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_GetColumnChunkInfo_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	info, err := pr.GetColumnChunkInfo(0, 0)
	require.NoError(t, err)

	require.Equal(t, 0, info.Index)
	require.NotEmpty(t, info.PathInSchema)
	require.NotEmpty(t, info.Name)
	require.NotEmpty(t, info.PhysicalType)
	require.NotEmpty(t, info.Codec)
	require.GreaterOrEqual(t, info.NumValues, int64(0))
}

func Test_GetColumnChunkInfo_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	tests := []struct {
		name     string
		rgIndex  int
		colIndex int
		wantErr  error
	}{
		{"Invalid row group", -1, 0, ErrInvalidRowGroupIndex},
		{"Row group out of bounds", 9999, 0, ErrInvalidRowGroupIndex},
		{"Invalid column", 0, -1, ErrInvalidColumnIndex},
		{"Column out of bounds", 0, 9999, ErrInvalidColumnIndex},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pr.GetColumnChunkInfo(tt.rgIndex, tt.colIndex)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func Test_GetAllColumnChunksInfo_ValidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	infos, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err)
	require.NotEmpty(t, infos)

	for i, info := range infos {
		require.Equal(t, i, info.Index)
		require.NotEmpty(t, info.Name)
	}
}

func Test_GetAllColumnChunksInfo_InvalidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	tests := []struct {
		name    string
		rgIndex int
	}{
		{"Negative index", -1},
		{"Out of bounds", 9999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pr.GetAllColumnChunksInfo(tt.rgIndex)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidRowGroupIndex)
		})
	}
}

// Legacy LIST/MAP layouts are normalized to List.Element, with schema lookups renamed in step.
func Test_ListMapLayoutFile_Columns(t *testing.T) {
	pr := openFixture(t, "list-map-layout.parquet")

	tests := []struct {
		name          string
		path          []string
		physicalType  string
		logicalType   string
		convertedType string
		firstPage     []string
	}{
		{"Athena.List.Element", []string{"Athena", "List", "Element"}, "BYTE_ARRAY", "STRING", "UTF8", []string{"a", "", "b"}},
		{"Athena_logical.List.Element", []string{"Athena_logical", "List", "Element"}, "BYTE_ARRAY", "STRING", "UTF8", []string{"c"}},
		{"Logical_list.List.Element", []string{"Logical_list", "List", "Element"}, "INT32", "-", "-", []string{"1", "2", "3"}},
		{"Logical_map.Key_value.Key", []string{"Logical_map", "Key_value", "Key"}, "BYTE_ARRAY", "STRING", "UTF8", []string{"x"}},
		{"Logical_map.Key_value.Value", []string{"Logical_map", "Key_value", "Value"}, "INT32", "-", "-", []string{"1"}},
		{"Legacy.List.Element", []string{"Legacy", "List", "Element"}, "INT32", "-", "-", []string{"1", "2", "3"}},
		{"Entries_map.Entries.Key", []string{"Entries_map", "Entries", "Key"}, "BYTE_ARRAY", "STRING", "UTF8", []string{"x"}},
		{"Entries_map.Entries.Value", []string{"Entries_map", "Entries", "Value"}, "INT32", "-", "-", []string{"42"}},
		{"Entries_logical.Entries.Key", []string{"Entries_logical", "Entries", "Key"}, "BYTE_ARRAY", "STRING", "UTF8", []string{"x"}},
		{"Entries_logical.Entries.Value", []string{"Entries_logical", "Entries", "Value"}, "INT32", "-", "-", []string{"42"}},
	}

	columns, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err)
	require.Len(t, columns, len(tests))

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := columns[i]
			require.Equal(t, tt.name, col.Name)
			require.Equal(t, tt.path, col.PathInSchema)
			require.Equal(t, tt.physicalType, col.PhysicalType)
			require.Equal(t, tt.logicalType, col.LogicalType)
			require.Equal(t, tt.convertedType, col.ConvertedType)

			values, err := pr.GetPageContentFormatted(context.Background(), 0, i, 0)
			require.NoError(t, err)
			require.Equal(t, tt.firstPage, values)
		})
	}
}

// A two-level list without a LIST annotation is a plain repeated group and keeps its names.
func Test_NestedArrayFile_Columns(t *testing.T) {
	pr := openFixture(t, "nested-array.parquet")

	columns, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err)
	require.Len(t, columns, 1)
	require.Equal(t, "Array.Array", columns[0].Name)
	require.Equal(t, "INT32", columns[0].PhysicalType)

	values, err := pr.GetPageContentFormatted(context.Background(), 0, 0, 0)
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2", "3", "4"}, values)
}
