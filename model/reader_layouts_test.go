package model

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

// openLayoutFixture opens one of the parquet-tools fixtures downloaded by
// `make test` and closes it when the test ends.
func openLayoutFixture(t *testing.T, name string) *ParquetReader {
	t.Helper()

	path := filepath.Join("..", "build", "testdata", name)
	if _, err := os.Stat(path); err != nil {
		t.Skipf("Test file %s not found - run 'make test' to download test files", name)
	}

	parquetReader, err := pio.NewParquetFileReader(context.Background(), path, pio.ReadOption{})
	require.NoError(t, err)

	pr := NewParquetReader(parquetReader)
	t.Cleanup(func() { _ = pr.Close() })
	return pr
}

// Files with a corrupt footer used to open and expose garbage metadata; they are
// now rejected while the footer is read.
func Test_NewParquetFileReader_CorruptFooter(t *testing.T) {
	tests := []struct {
		file    string
		wantErr string
	}{
		{file: "bad-footer-magic.parquet", wantErr: `invalid footer magic "XXXX"`},
		{file: "negative-num-rows.parquet", wantErr: "num_rows -1 is negative"},
	}

	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			path := filepath.Join("..", "build", "testdata", tt.file)
			if _, err := os.Stat(path); err != nil {
				t.Skipf("Test file %s not found - run 'make test' to download test files", tt.file)
			}

			parquetReader, err := pio.NewParquetFileReader(context.Background(), path, pio.ReadOption{})
			require.Error(t, err)
			require.Nil(t, parquetReader)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// A schema without leaf columns still has rows; it must open and report an
// empty column list rather than failing.
func Test_ZeroColumnFile(t *testing.T) {
	pr := openLayoutFixture(t, "zero-column.parquet")

	info := pr.GetFileInfo()
	require.Equal(t, 1, info.NumRowGroups)
	require.Equal(t, int64(3), info.NumRows)
	require.Zero(t, info.NumLeafColumns)
	require.Zero(t, info.TotalCompressedSize)
	require.Zero(t, info.TotalUncompressedSize)
	require.Zero(t, info.CompressionRatio)

	rowGroups := pr.GetAllRowGroupsInfo()
	require.Len(t, rowGroups, 1)
	require.Equal(t, int64(3), rowGroups[0].NumRows)
	require.Zero(t, rowGroups[0].NumColumns)
	require.Zero(t, rowGroups[0].CompressionRatio)

	columns, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err)
	require.Empty(t, columns)

	_, err = pr.GetColumnChunkInfo(0, 0)
	require.ErrorIs(t, err, ErrInvalidColumnIndex)

	_, err = pr.GetPageMetadataList(context.Background(), 0, 0)
	require.ErrorIs(t, err, ErrInvalidColumnIndex)

	_, err = pr.GetPageContentFormatted(context.Background(), 0, 0, 0)
	require.ErrorIs(t, err, ErrInvalidColumnIndex)
}

// Legacy and logical-type-only LIST/MAP layouts are normalized by the reader:
// LIST wrappers become List.Element, while the schema elements are renamed in
// step so type lookups and value formatting keep working.
func Test_ListMapLayoutFile_Columns(t *testing.T) {
	pr := openLayoutFixture(t, "list-map-layout.parquet")

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

// A two-level list without a LIST annotation is a plain repeated group, so its
// leaf path keeps the original field names.
func Test_NestedArrayFile_Columns(t *testing.T) {
	pr := openLayoutFixture(t, "nested-array.parquet")

	columns, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err)
	require.Len(t, columns, 1)
	require.Equal(t, "Array.Array", columns[0].Name)
	require.Equal(t, "INT32", columns[0].PhysicalType)

	values, err := pr.GetPageContentFormatted(context.Background(), 0, 0, 0)
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2", "3", "4"}, values)
}
