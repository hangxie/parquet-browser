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

// Test GetColumnChunkInfo with valid indices
func Test_GetColumnChunkInfo_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	// Test first column of first row group
	info, err := pr.GetColumnChunkInfo(0, 0)
	require.NoError(t, err)

	require.Equal(t, 0, info.Index)
	require.NotEmpty(t, info.PathInSchema)
	require.NotEmpty(t, info.Name)
	require.NotEmpty(t, info.PhysicalType)
	require.NotEmpty(t, info.Codec)
	require.GreaterOrEqual(t, info.NumValues, int64(0))
}

// Test GetColumnChunkInfo with invalid indices
func Test_GetColumnChunkInfo_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
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

// Test GetAllColumnChunksInfo
func Test_GetAllColumnChunksInfo_ValidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	infos, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err)
	require.NotEmpty(t, infos)

	// Verify each column info is populated
	for i, info := range infos {
		require.Equal(t, i, info.Index)
		require.NotEmpty(t, info.Name)
	}
}

// Test GetAllColumnChunksInfo with invalid index
func Test_GetAllColumnChunksInfo_InvalidIndex(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
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
