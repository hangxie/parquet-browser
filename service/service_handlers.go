package service

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// handleFileInfo returns file-level metadata
func (s *ParquetService) handleFileInfo(w http.ResponseWriter, r *http.Request) {
	info := s.reader.GetFileInfo()
	WriteJSON(w, http.StatusOK, info)
}

// handleRowGroups returns all row groups
func (s *ParquetService) handleRowGroups(w http.ResponseWriter, r *http.Request) {
	rowGroups := s.reader.GetAllRowGroupsInfo()
	WriteJSON(w, http.StatusOK, rowGroups)
}

// handleRowGroupInfo returns info for a specific row group
func (s *ParquetService) handleRowGroupInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rgIndex, err := strconv.Atoi(vars["rgIndex"])
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Invalid row group index")
		return
	}

	info, err := s.reader.GetRowGroupInfo(rgIndex)
	if err != nil {
		WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	WriteJSON(w, http.StatusOK, info)
}

// handleColumnChunks returns all column chunks for a row group
func (s *ParquetService) handleColumnChunks(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rgIndex, err := strconv.Atoi(vars["rgIndex"])
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Invalid row group index")
		return
	}

	columns, err := s.reader.GetAllColumnChunksInfo(rgIndex)
	if err != nil {
		WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	WriteJSON(w, http.StatusOK, columns)
}

// handleColumnChunkInfo returns info for a specific column chunk
func (s *ParquetService) handleColumnChunkInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rgIndex, err := strconv.Atoi(vars["rgIndex"])
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Invalid row group index")
		return
	}

	colIndex, err := strconv.Atoi(vars["colIndex"])
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Invalid column index")
		return
	}

	info, err := s.reader.GetColumnChunkInfo(rgIndex, colIndex)
	if err != nil {
		WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	WriteJSON(w, http.StatusOK, info)
}

// handlePages returns page metadata for a column chunk
func (s *ParquetService) handlePages(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rgIndex, err := strconv.Atoi(vars["rgIndex"])
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Invalid row group index")
		return
	}

	colIndex, err := strconv.Atoi(vars["colIndex"])
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Invalid column index")
		return
	}

	pages, err := s.reader.GetPageMetadataList(r.Context(), rgIndex, colIndex)
	if err != nil {
		WriteError(w, contextErrorStatus(err, http.StatusNotFound), err.Error())
		return
	}

	WriteJSON(w, http.StatusOK, pages)
}

// handlePageInfo returns info for a specific page
func (s *ParquetService) handlePageInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rgIndex, err := strconv.Atoi(vars["rgIndex"])
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Invalid row group index")
		return
	}

	colIndex, err := strconv.Atoi(vars["colIndex"])
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Invalid column index")
		return
	}

	pageIndex, err := strconv.Atoi(vars["pageIndex"])
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Invalid page index")
		return
	}

	pageInfo, err := s.reader.GetPageMetadata(r.Context(), rgIndex, colIndex, pageIndex)
	if err != nil {
		WriteError(w, contextErrorStatus(err, http.StatusNotFound), err.Error())
		return
	}

	WriteJSON(w, http.StatusOK, pageInfo)
}

// handlePageContent returns the content/values of a specific page
func (s *ParquetService) handlePageContent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rgIndex, err := strconv.Atoi(vars["rgIndex"])
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Invalid row group index")
		return
	}

	colIndex, err := strconv.Atoi(vars["colIndex"])
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Invalid column index")
		return
	}

	pageIndex, err := strconv.Atoi(vars["pageIndex"])
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Invalid page index")
		return
	}

	values, err := s.reader.GetPageContentFormatted(r.Context(), rgIndex, colIndex, pageIndex)
	if err != nil {
		WriteError(w, contextErrorStatus(err, http.StatusNotFound), err.Error())
		return
	}

	response := map[string]interface{}{
		"values": values,
		"count":  len(values),
	}
	WriteJSON(w, http.StatusOK, response)
}
