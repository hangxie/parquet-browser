package cmd

import (
	"testing"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/stretchr/testify/assert"
)

// Helper function to create int64 pointer
func int64Ptr(v int64) *int64 {
	return &v
}

func Test_FileInfo_GetFileInfo(t *testing.T) {
	// Create mock metadata
	createdBy := "test-app"
	metadata := &parquet.FileMetaData{
		Version:   1,
		NumRows:   1000,
		CreatedBy: &createdBy,
		Schema: []*parquet.SchemaElement{
			{Name: "root"}, // Group node
			{Name: "id", Type: parquet.TypePtr(parquet.Type_INT64)},        // Leaf
			{Name: "name", Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY)}, // Leaf
			{Name: "list_field"}, // Group node (LIST)
			{Name: "element", Type: parquet.TypePtr(parquet.Type_INT32)}, // Leaf
		},
		RowGroups: []*parquet.RowGroup{
			{
				NumRows:             500,
				TotalByteSize:       10000,
				TotalCompressedSize: int64Ptr(5000),
			},
			{
				NumRows:             500,
				TotalByteSize:       10000,
				TotalCompressedSize: int64Ptr(5000),
			},
		},
	}

	pr := &ParquetReader{
		metadata: metadata,
	}

	info := pr.GetFileInfo()

	assert.Equal(t, int32(1), info.Version)
	assert.Equal(t, 2, info.NumRowGroups)
	assert.Equal(t, int64(1000), info.NumRows)
	assert.Equal(t, 3, info.NumLeafColumns) // Only leaf columns
	assert.Equal(t, int64(10000), info.TotalCompressedSize)
	assert.Equal(t, int64(20000), info.TotalUncompressedSize)
	assert.Equal(t, 2.0, info.CompressionRatio)
	assert.Equal(t, "test-app", info.CreatedBy)
}

func Test_RowGroupInfo_GetRowGroupInfo(t *testing.T) {
	metadata := &parquet.FileMetaData{
		RowGroups: []*parquet.RowGroup{
			{
				NumRows:             500,
				TotalByteSize:       10000,
				TotalCompressedSize: int64Ptr(5000),
				Columns: []*parquet.ColumnChunk{
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 2500}},
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 2500}},
				},
			},
		},
	}

	pr := &ParquetReader{
		metadata: metadata,
	}

	info, err := pr.GetRowGroupInfo(0)
	assert.NoError(t, err)
	assert.Equal(t, 0, info.Index)
	assert.Equal(t, int64(500), info.NumRows)
	assert.Equal(t, 2, info.NumColumns)
	assert.Equal(t, int64(5000), info.CompressedSize)
	assert.Equal(t, int64(10000), info.UncompressedSize)
	assert.Equal(t, 2.0, info.CompressionRatio)

	// Test invalid index
	_, err = pr.GetRowGroupInfo(5)
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidRowGroupIndex, err)
}

func Test_ColumnChunkInfo_GetColumnChunkInfo(t *testing.T) {
	nullCount := int64(10)
	metadata := &parquet.FileMetaData{
		Schema: []*parquet.SchemaElement{
			{Name: "root"},
			{
				Name: "id",
				Type: parquet.TypePtr(parquet.Type_INT64),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 64,
						IsSigned: true,
					},
				},
				ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64),
			},
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
								NullCount: &nullCount,
								MinValue:  []byte{0x01},
								MaxValue:  []byte{0xFF},
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
	assert.Equal(t, 0, info.Index)
	assert.Equal(t, []string{"id"}, info.PathInSchema)
	assert.Equal(t, "id", info.Name)
	assert.Equal(t, "INT64", info.PhysicalType)
	assert.Equal(t, "INTEGER(64,signed)", info.LogicalType)
	assert.Equal(t, "INT_64", info.ConvertedType)
	assert.Equal(t, "SNAPPY", info.Codec)
	assert.Equal(t, int64(100), info.NumValues)
	assert.NotNil(t, info.NullCount)
	assert.Equal(t, int64(10), *info.NullCount)
	assert.Equal(t, int64(500), info.CompressedSize)
	assert.Equal(t, int64(800), info.UncompressedSize)
	assert.Equal(t, 1.6, info.CompressionRatio)
	assert.Equal(t, []byte{0x01}, info.MinValue)
	assert.Equal(t, []byte{0xFF}, info.MaxValue)

	// Test invalid indices
	_, err = pr.GetColumnChunkInfo(5, 0)
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidRowGroupIndex, err)

	_, err = pr.GetColumnChunkInfo(0, 5)
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidColumnIndex, err)
}

func Test_GetAllRowGroupsInfo(t *testing.T) {
	metadata := &parquet.FileMetaData{
		RowGroups: []*parquet.RowGroup{
			{
				NumRows:             500,
				TotalByteSize:       10000,
				TotalCompressedSize: int64Ptr(5000),
				Columns: []*parquet.ColumnChunk{
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 5000}},
				},
			},
			{
				NumRows:             300,
				TotalByteSize:       6000,
				TotalCompressedSize: int64Ptr(3000),
				Columns: []*parquet.ColumnChunk{
					{MetaData: &parquet.ColumnMetaData{TotalCompressedSize: 3000}},
				},
			},
		},
	}

	pr := &ParquetReader{
		metadata: metadata,
	}

	infos := pr.GetAllRowGroupsInfo()
	assert.Len(t, infos, 2)
	assert.Equal(t, int64(500), infos[0].NumRows)
	assert.Equal(t, int64(300), infos[1].NumRows)
}

func Test_GetAllColumnChunksInfo(t *testing.T) {
	metadata := &parquet.FileMetaData{
		Schema: []*parquet.SchemaElement{
			{Name: "root"},
			{Name: "id", Type: parquet.TypePtr(parquet.Type_INT64)},
			{Name: "name", Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY)},
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
						},
					},
					{
						MetaData: &parquet.ColumnMetaData{
							Type:                  parquet.Type_BYTE_ARRAY,
							PathInSchema:          []string{"name"},
							Codec:                 parquet.CompressionCodec_GZIP,
							NumValues:             100,
							TotalCompressedSize:   300,
							TotalUncompressedSize: 600,
						},
					},
				},
			},
		},
	}

	pr := &ParquetReader{
		metadata: metadata,
	}

	infos, err := pr.GetAllColumnChunksInfo(0)
	assert.NoError(t, err)
	assert.Len(t, infos, 2)
	assert.Equal(t, "id", infos[0].Name)
	assert.Equal(t, "name", infos[1].Name)

	// Test invalid index
	_, err = pr.GetAllColumnChunksInfo(5)
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidRowGroupIndex, err)
}

// Test NewParquetReader constructor
func Test_NewParquetReader(t *testing.T) {
	metadata := &parquet.FileMetaData{
		Version: 1,
		NumRows: 1000,
		Schema: []*parquet.SchemaElement{
			{Name: "root"},
			{Name: "id", Type: parquet.TypePtr(parquet.Type_INT64)},
		},
		RowGroups: []*parquet.RowGroup{},
	}

	// Create a mock parquet-go reader
	mockGoReader := &reader.ParquetReader{
		Footer: metadata,
	}

	// Test NewParquetReader constructor
	pr := NewParquetReader(mockGoReader)

	assert.NotNil(t, pr)
	assert.Equal(t, mockGoReader, pr.reader)
	assert.Equal(t, metadata, pr.metadata)
	assert.Equal(t, mockGoReader.Footer, pr.metadata)
}
