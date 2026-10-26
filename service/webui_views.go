package service

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	"github.com/hangxie/parquet-browser/model"
)

// handleIndexPage serves the main HTML page
func (s *ParquetService) handleIndexPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	data := struct {
		Version string
	}{
		Version: getVersion(),
	}
	err := templates.ExecuteTemplate(w, "index.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleMainView serves the main view with file info and row groups
func (s *ParquetService) handleMainView(w http.ResponseWriter, r *http.Request) {
	info := s.reader.GetFileInfo()

	// Get row groups
	rowGroups := s.reader.GetAllRowGroupsInfo()

	// Format the row groups for display
	type FormattedRowGroup struct {
		Index            int
		NumRows          int64
		NumColumns       int
		CompressedSize   string
		UncompressedSize string
		CompressionRatio string
	}

	formatted := make([]FormattedRowGroup, len(rowGroups))
	for i, rg := range rowGroups {
		formatted[i] = FormattedRowGroup{
			Index:            rg.Index,
			NumRows:          rg.NumRows,
			NumColumns:       rg.NumColumns,
			CompressedSize:   model.FormatBytes(rg.CompressedSize),
			UncompressedSize: model.FormatBytes(rg.UncompressedSize),
			CompressionRatio: formatRatio(rg.CompressionRatio),
		}
	}

	// Create a wrapper struct with additional fields for display
	data := struct {
		FileName              string
		Version               int32
		NumRowGroups          int
		TotalRows             int64
		LeafColumns           int
		TotalCompressedSize   string
		TotalUncompressedSize string
		CompressionRatio      string
		CreatedBy             string
		Encryption            string
		RowGroups             []FormattedRowGroup
	}{
		FileName:              s.uri,
		Version:               info.Version,
		NumRowGroups:          info.NumRowGroups,
		TotalRows:             info.NumRows,
		LeafColumns:           info.NumLeafColumns,
		TotalCompressedSize:   model.FormatBytes(info.TotalCompressedSize),
		TotalUncompressedSize: model.FormatBytes(info.TotalUncompressedSize),
		CompressionRatio:      formatRatio(info.CompressionRatio),
		CreatedBy:             info.CreatedBy,
		Encryption:            info.Encryption,
		RowGroups:             formatted,
	}

	err := renderPartial(w, r, "main", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleRowGroupsView serves the row groups list view
func (s *ParquetService) handleRowGroupsView(w http.ResponseWriter, r *http.Request) {
	rowGroups := s.reader.GetAllRowGroupsInfo()
	fileInfo := s.reader.GetFileInfo()

	// Format the row groups for display
	type FormattedRowGroup struct {
		Index            int
		NumRows          int64
		NumColumns       int
		CompressedSize   string
		UncompressedSize string
		CompressionRatio string
		FileOffset       string
	}

	// Calculate totals
	var totalRows int64
	var totalCompressed, totalUncompressed int64
	formatted := make([]FormattedRowGroup, len(rowGroups))
	for i, rg := range rowGroups {
		totalRows += rg.NumRows
		totalCompressed += rg.CompressedSize
		totalUncompressed += rg.UncompressedSize

		formatted[i] = FormattedRowGroup{
			Index:            rg.Index,
			NumRows:          rg.NumRows,
			NumColumns:       rg.NumColumns,
			CompressedSize:   model.FormatBytes(rg.CompressedSize),
			UncompressedSize: model.FormatBytes(rg.UncompressedSize),
			CompressionRatio: formatRatio(rg.CompressionRatio),
			FileOffset:       fmt.Sprintf("0x%X", rg.CompressedSize), // Using compressed size as placeholder
		}
	}

	data := struct {
		RowGroups         []FormattedRowGroup
		TotalRowGroups    int
		TotalRows         int64
		TotalColumns      int
		TotalCompressed   string
		TotalUncompressed string
		OverallRatio      string
	}{
		RowGroups:         formatted,
		TotalRowGroups:    len(rowGroups),
		TotalRows:         totalRows,
		TotalColumns:      fileInfo.NumLeafColumns,
		TotalCompressed:   model.FormatBytes(totalCompressed),
		TotalUncompressed: model.FormatBytes(totalUncompressed),
		OverallRatio:      formatRatio(float64(totalUncompressed) / float64(totalCompressed)),
	}

	err := renderPartial(w, r, "rowgroups", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleColumnsView serves the columns view for a row group
func (s *ParquetService) handleColumnsView(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rgIndex, err := strconv.Atoi(vars["rgIndex"])
	if err != nil {
		http.Error(w, "Invalid row group index", http.StatusBadRequest)
		return
	}

	columns, err := s.reader.GetAllColumnChunksInfo(rgIndex)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Get row group info for summary
	rowGroupInfo, err := s.reader.GetRowGroupInfo(rgIndex)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Format columns for display
	type FormattedColumn struct {
		Index            int
		ColumnPath       string
		PhysicalType     string
		LogicalType      string
		ConvertedType    string
		Codec            string
		NumValues        int64
		NullCount        string
		CompressedSize   string
		UncompressedSize string
		MinValue         string
		MaxValue         string
	}

	// Calculate totals
	var totalValues int64
	var totalNulls int64
	var totalCompressed, totalUncompressed int64
	formatted := make([]FormattedColumn, len(columns))
	for i, col := range columns {
		totalValues += col.NumValues
		totalCompressed += col.CompressedSize
		totalUncompressed += col.UncompressedSize

		nullCountStr := "0"
		if col.NullCount != nil {
			nullCountStr = fmt.Sprintf("%d", *col.NullCount)
			totalNulls += *col.NullCount
		}

		minValue := col.MinValue
		if minValue == "" {
			minValue = "-"
		}
		maxValue := col.MaxValue
		if maxValue == "" {
			maxValue = "-"
		}

		formatted[i] = FormattedColumn{
			Index:            col.Index,
			ColumnPath:       col.Name,
			PhysicalType:     col.PhysicalType,
			LogicalType:      col.LogicalType,
			ConvertedType:    col.ConvertedType,
			Codec:            col.Codec,
			NumValues:        col.NumValues,
			NullCount:        nullCountStr,
			CompressedSize:   model.FormatBytes(col.CompressedSize),
			UncompressedSize: model.FormatBytes(col.UncompressedSize),
			MinValue:         minValue,
			MaxValue:         maxValue,
		}
	}

	data := struct {
		RowGroupIndex     int
		Columns           []FormattedColumn
		NumRows           int64
		TotalColumns      int
		TotalValues       int64
		TotalNulls        int64
		TotalCompressed   string
		TotalUncompressed string
		CompressionRatio  string
	}{
		RowGroupIndex:     rgIndex,
		Columns:           formatted,
		NumRows:           rowGroupInfo.NumRows,
		TotalColumns:      len(columns),
		TotalValues:       totalValues,
		TotalNulls:        totalNulls,
		TotalCompressed:   model.FormatBytes(totalCompressed),
		TotalUncompressed: model.FormatBytes(totalUncompressed),
		CompressionRatio:  formatRatio(float64(totalUncompressed) / float64(totalCompressed)),
	}

	err = renderPartial(w, r, "columns", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
