package service

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	"github.com/hangxie/parquet-browser/model"
)

// handlePagesView serves the pages view for a column
func (s *ParquetService) handlePagesView(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rgIndex, err := strconv.Atoi(vars["rgIndex"])
	if err != nil {
		http.Error(w, "Invalid row group index", http.StatusBadRequest)
		return
	}

	colIndex, err := strconv.Atoi(vars["colIndex"])
	if err != nil {
		http.Error(w, "Invalid column index", http.StatusBadRequest)
		return
	}

	pages, err := s.reader.GetPageMetadataList(r.Context(), rgIndex, colIndex)
	if err != nil {
		renderPagesError(w, r, err)
		return
	}

	// Get column info for breadcrumb and summary
	colInfo, err := s.reader.GetColumnChunkInfo(rgIndex, colIndex)
	var columnPath, physicalType, logicalType, convertedType, codec string
	var columnNumValues int64
	var columnNullCount string
	var columnCompressedSize, columnUncompressedSize string
	var columnCompressionRatio string
	var columnMinValue, columnMaxValue string
	if err == nil {
		columnPath = colInfo.Name
		physicalType = colInfo.PhysicalType
		logicalType = colInfo.LogicalType
		convertedType = colInfo.ConvertedType
		codec = colInfo.Codec
		columnNumValues = colInfo.NumValues
		if colInfo.NullCount != nil {
			columnNullCount = fmt.Sprintf("%d", *colInfo.NullCount)
		} else {
			columnNullCount = "0"
		}
		columnCompressedSize = model.FormatBytes(colInfo.CompressedSize)
		columnUncompressedSize = model.FormatBytes(colInfo.UncompressedSize)
		columnCompressionRatio = formatRatio(colInfo.CompressionRatio)
		columnMinValue = colInfo.MinValue
		if columnMinValue == "" {
			columnMinValue = "-"
		}
		columnMaxValue = colInfo.MaxValue
		if columnMaxValue == "" {
			columnMaxValue = "-"
		}
	}

	// Format pages for display
	type FormattedPage struct {
		Index            int
		PageType         string
		Offset           string
		CompressedSize   string
		UncompressedSize string
		NumValues        int32
		Encoding         string
		MinValue         string
		MaxValue         string
	}

	// Calculate totals
	var totalValues int32
	var totalCompressed, totalUncompressed int64
	formatted := make([]FormattedPage, len(pages))
	for i, page := range pages {
		totalValues += page.NumValues
		totalCompressed += int64(page.CompressedSize)
		totalUncompressed += int64(page.UncompressedSize)

		minValue := page.MinValue
		if minValue == "" {
			minValue = "-"
		}
		maxValue := page.MaxValue
		if maxValue == "" {
			maxValue = "-"
		}

		formatted[i] = FormattedPage{
			Index:            page.Index,
			PageType:         page.PageType,
			Offset:           fmt.Sprintf("0x%X", page.Offset),
			CompressedSize:   model.FormatBytes(int64(page.CompressedSize)),
			UncompressedSize: model.FormatBytes(int64(page.UncompressedSize)),
			NumValues:        page.NumValues,
			Encoding:         page.Encoding,
			MinValue:         minValue,
			MaxValue:         maxValue,
		}
	}

	data := struct {
		RowGroupIndex          int
		ColumnIndex            int
		ColumnPath             string
		PhysicalType           string
		LogicalType            string
		ConvertedType          string
		Codec                  string
		ColumnNumValues        int64
		ColumnNullCount        string
		ColumnCompressedSize   string
		ColumnUncompressedSize string
		ColumnCompressionRatio string
		ColumnMinValue         string
		ColumnMaxValue         string
		Pages                  []FormattedPage
		TotalPages             int
		TotalValues            int32
		TotalCompressed        string
		TotalUncompressed      string
		CompressionRatio       string
	}{
		RowGroupIndex:          rgIndex,
		ColumnIndex:            colIndex,
		ColumnPath:             columnPath,
		PhysicalType:           physicalType,
		LogicalType:            logicalType,
		ConvertedType:          convertedType,
		Codec:                  codec,
		ColumnNumValues:        columnNumValues,
		ColumnNullCount:        columnNullCount,
		ColumnCompressedSize:   columnCompressedSize,
		ColumnUncompressedSize: columnUncompressedSize,
		ColumnCompressionRatio: columnCompressionRatio,
		ColumnMinValue:         columnMinValue,
		ColumnMaxValue:         columnMaxValue,
		Pages:                  formatted,
		TotalPages:             len(pages),
		TotalValues:            totalValues,
		TotalCompressed:        model.FormatBytes(totalCompressed),
		TotalUncompressed:      model.FormatBytes(totalUncompressed),
		CompressionRatio:       formatRatio(float64(totalUncompressed) / float64(totalCompressed)),
	}

	err = renderPartial(w, r, "pages", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handlePageContentView serves the page content view
func (s *ParquetService) handlePageContentView(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rgIndex, err := strconv.Atoi(vars["rgIndex"])
	if err != nil {
		http.Error(w, "Invalid row group index", http.StatusBadRequest)
		return
	}

	colIndex, err := strconv.Atoi(vars["colIndex"])
	if err != nil {
		http.Error(w, "Invalid column index", http.StatusBadRequest)
		return
	}

	pageIndex, err := strconv.Atoi(vars["pageIndex"])
	if err != nil {
		http.Error(w, "Invalid page index", http.StatusBadRequest)
		return
	}

	// Get page metadata
	pages, err := s.reader.GetPageMetadataList(r.Context(), rgIndex, colIndex)
	if err != nil {
		renderPagesError(w, r, err)
		return
	}

	if pageIndex < 0 || pageIndex >= len(pages) {
		renderPagesError(w, r, fmt.Errorf("page index %d out of range [0, %d)", pageIndex, len(pages)))
		return
	}

	pageMetadata := pages[pageIndex]

	values, err := s.reader.GetPageContentFormatted(r.Context(), rgIndex, colIndex, pageIndex)
	if err != nil {
		renderPagesError(w, r, err)
		return
	}

	data := struct {
		RowGroupIndex    int
		ColumnIndex      int
		PageIndex        int
		PageType         string
		Offset           string
		CompressedSize   string
		UncompressedSize string
		CompressionRatio string
		NumValues        int32
		NullCount        string
		Encoding         string
		Values           []string
		Count            int
	}{
		RowGroupIndex:    rgIndex,
		ColumnIndex:      colIndex,
		PageIndex:        pageIndex,
		PageType:         pageMetadata.PageType,
		Offset:           fmt.Sprintf("0x%X", pageMetadata.Offset),
		CompressedSize:   model.FormatBytes(int64(pageMetadata.CompressedSize)),
		UncompressedSize: model.FormatBytes(int64(pageMetadata.UncompressedSize)),
		CompressionRatio: formatRatio(float64(pageMetadata.UncompressedSize) / float64(pageMetadata.CompressedSize)),
		NumValues:        pageMetadata.NumValues,
		NullCount:        formatNullCount(pageMetadata.NullCount),
		Encoding:         pageMetadata.Encoding,
		Values:           values,
		Count:            len(values),
	}

	err = renderPartial(w, r, "page_content", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
