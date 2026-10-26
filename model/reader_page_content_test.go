package model

import (
	"context"
	"testing"

	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

// Test GetPageContent
func Test_GetPageContent_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	// First, check if page 0 is a data page
	pages, err := pr.GetPageMetadataList(context.Background(), 0, 0)
	require.NoError(t, err)
	require.NotEqual(t, 0, len(pages))

	values, err := pr.GetPageContent(context.Background(), 0, 0, 1)
	require.NoError(t, err)
	require.NotEqual(t, 0, len(values))
}

// Test GetPageContent with dictionary page
func Test_GetPageContent_DictionaryPage(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	// Find a column with a dictionary page
	allColumns, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err)
	require.Equal(t, 57, len(allColumns))

	pages, err := pr.GetPageMetadataList(context.Background(), 0, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(pages))

	values, err := pr.GetPageContent(context.Background(), 0, 0, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(values))

	pages, err = pr.GetPageMetadataList(context.Background(), 0, 16)
	require.NoError(t, err)
	require.Equal(t, 4, len(pages))

	values, err = pr.GetPageContent(context.Background(), 0, 1, 0)
	require.NoError(t, err)
	require.Equal(t, 5, len(values))
}

// Test GetPageContent with invalid indices
func Test_GetPageContent_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	tests := []struct {
		name      string
		rgIndex   int
		colIndex  int
		pageIndex int
	}{
		{"Invalid row group", -1, 0, 0},
		{"Invalid column", 0, -1, 0},
		{"Invalid page", 0, 0, -1},
		{"Page out of bounds", 0, 0, 9999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pr.GetPageContent(context.Background(), tt.rgIndex, tt.colIndex, tt.pageIndex)
			require.Error(t, err)
		})
	}
}

// Test GetPageContentFormatted
func Test_GetPageContentFormatted_ValidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	// Find the first data page
	pages, err := pr.GetPageMetadataList(context.Background(), 0, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(pages))

	values, err := pr.GetPageContentFormatted(context.Background(), 0, 0, 1)
	require.NoError(t, err)
	require.Equal(t, 2, len(values))
}

// Test GetPageContentFormatted with invalid indices
func Test_GetPageContentFormatted_InvalidIndices(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	tests := []struct {
		name      string
		rgIndex   int
		colIndex  int
		pageIndex int
	}{
		{"Invalid row group", -1, 0, 0},
		{"Invalid column", 0, -1, 0},
		{"Invalid page", 0, 0, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pr.GetPageContentFormatted(context.Background(), tt.rgIndex, tt.colIndex, tt.pageIndex)
			require.Error(t, err)
		})
	}
}

// Test readDictionaryPageContent (indirectly through GetPageContent)
func Test_ReadDictionaryPageContent_Coverage(t *testing.T) {
	parquetReader, err := pio.NewParquetFileReader(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = parquetReader.ReadStopWithContext(context.Background()) }()

	pr := NewParquetReader(parquetReader)

	// Scan all columns looking for dictionary pages
	allColumns, err := pr.GetAllColumnChunksInfo(0)
	require.NoError(t, err)
	require.Equal(t, 57, len(allColumns))

	values, err := pr.GetPageContent(context.Background(), 0, 0, 1)
	require.NoError(t, err)
	require.Equal(t, 2, len(values))
}
