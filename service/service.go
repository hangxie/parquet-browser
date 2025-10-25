package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/hangxie/parquet-go/v2/reader"
	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"

	"github.com/hangxie/parquet-browser/model"
)

// ParquetService manages the Parquet file and provides HTTP endpoints
type ParquetService struct {
	reader        *model.ParquetReader
	parquetReader *reader.ParquetReader // Raw reader for schema generation
	uri           string
}

// NewParquetService creates a new service instance
func NewParquetService(uri string, readOpts pio.ReadOption) (*ParquetService, error) {
	parquetReader, err := pio.NewParquetFileReader(uri, readOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	return &ParquetService{
		reader:        model.NewParquetReader(parquetReader),
		parquetReader: parquetReader,
		uri:           uri,
	}, nil
}

// Close closes the underlying parquet file
func (s *ParquetService) Close() error {
	if s.reader != nil {
		return nil // The reader doesn't expose Close, handle at caller level
	}
	return nil
}

// CreateRouter creates a new router with all routes configured
// If quiet is true, disables logging middleware (useful for embedded servers)
func CreateRouter(s *ParquetService, quiet bool) *mux.Router {
	r := mux.NewRouter()
	s.SetupRoutes(r)
	r.Use(CORSMiddleware)
	if !quiet {
		r.Use(LoggingMiddleware)
	}
	return r
}

// SetupRoutes configures all HTTP routes
func (s *ParquetService) SetupRoutes(r *mux.Router) {
	// Schema endpoints
	r.HandleFunc("/schema/go", s.handleSchemaGo).Methods("GET")
	r.HandleFunc("/schema/json", s.handleSchemaJSON).Methods("GET")
	r.HandleFunc("/schema/raw", s.handleSchemaRaw).Methods("GET")
	r.HandleFunc("/schema/csv", s.handleSchemaCSV).Methods("GET")

	// File info endpoint
	r.HandleFunc("/info", s.handleFileInfo).Methods("GET")

	// Row groups endpoints
	r.HandleFunc("/rowgroups", s.handleRowGroups).Methods("GET")
	r.HandleFunc("/rowgroups/{rgIndex}", s.handleRowGroupInfo).Methods("GET")

	// Column chunks endpoints
	r.HandleFunc("/rowgroups/{rgIndex}/columnchunks", s.handleColumnChunks).Methods("GET")
	r.HandleFunc("/rowgroups/{rgIndex}/columnchunks/{colIndex}", s.handleColumnChunkInfo).Methods("GET")

	// Page endpoints
	r.HandleFunc("/rowgroups/{rgIndex}/columnchunks/{colIndex}/pages", s.handlePages).Methods("GET")
	r.HandleFunc("/rowgroups/{rgIndex}/columnchunks/{colIndex}/pages/{pageIndex}", s.handlePageInfo).Methods("GET")
	r.HandleFunc("/rowgroups/{rgIndex}/columnchunks/{colIndex}/pages/{pageIndex}/content", s.handlePageContent).Methods("GET")
}

// handleSchemaGo returns schema in Go struct format
func (s *ParquetService) handleSchemaGo(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := pschema.NewSchemaTree(s.parquetReader, pschema.SchemaOption{FailOnInt96: false})
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to generate schema: %v", err))
		return
	}

	schemaText, err := schemaRoot.GoStruct(false)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to format Go schema: %v", err))
		return
	}

	// Format the Go code for better readability
	formatted, err := FormatGoCode(schemaText)
	if err != nil {
		// If formatting fails, return the unformatted version
		formatted = schemaText
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(formatted))
}

// handleSchemaJSON returns schema in JSON format
func (s *ParquetService) handleSchemaJSON(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := pschema.NewSchemaTree(s.parquetReader, pschema.SchemaOption{FailOnInt96: false})
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to generate schema: %v", err))
		return
	}

	schemaText := schemaRoot.JSONSchema()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(schemaText))
}

// handleSchemaRaw returns the raw schema tree structure as JSON
func (s *ParquetService) handleSchemaRaw(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := pschema.NewSchemaTree(s.parquetReader, pschema.SchemaOption{FailOnInt96: false})
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to generate schema: %v", err))
		return
	}

	// Marshal the raw SchemaNode structure to JSON (compact format)
	rawJSON, err := json.Marshal(*schemaRoot)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to marshal raw schema: %v", err))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(rawJSON)
}

// handleSchemaCSV returns schema in CSV format
func (s *ParquetService) handleSchemaCSV(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := pschema.NewSchemaTree(s.parquetReader, pschema.SchemaOption{FailOnInt96: false})
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to generate schema: %v", err))
		return
	}

	schemaText, err := schemaRoot.CSVSchema()
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to format CSV schema: %v", err))
		return
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(schemaText))
}

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

	pages, err := s.reader.GetPageMetadataList(rgIndex, colIndex)
	if err != nil {
		WriteError(w, http.StatusNotFound, err.Error())
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

	pageInfo, err := s.reader.GetPageMetadata(rgIndex, colIndex, pageIndex)
	if err != nil {
		WriteError(w, http.StatusNotFound, err.Error())
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

	// Get pre-formatted values ready for display
	values, err := s.reader.GetPageContentFormatted(rgIndex, colIndex, pageIndex)
	if err != nil {
		WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	// Return the formatted values as JSON
	response := map[string]interface{}{
		"values": values,
		"count":  len(values),
	}
	WriteJSON(w, http.StatusOK, response)
}

// StartServer starts the HTTP server with verbose output
func StartServer(service *ParquetService, addr string) error {
	r := CreateRouter(service, false) // verbose mode (not quiet)

	fmt.Printf("Starting Parquet Browser API server on %s\n", addr)
	fmt.Printf("Available endpoints:\n")
	fmt.Printf("  GET /info                                                    - File metadata\n")
	fmt.Printf("  GET /schema/go                                               - Schema (Go format)\n")
	fmt.Printf("  GET /schema/json?pretty=true                                 - Schema (JSON format)\n")
	fmt.Printf("  GET /schema/raw?pretty=true                                  - Schema (Raw format)\n")
	fmt.Printf("  GET /schema/csv                                              - Schema (CSV format)\n")
	fmt.Printf("  GET /rowgroups                                               - All row groups\n")
	fmt.Printf("  GET /rowgroups/{rgIndex}                                     - Row group info\n")
	fmt.Printf("  GET /rowgroups/{rgIndex}/columnchunks                        - All column chunks\n")
	fmt.Printf("  GET /rowgroups/{rgIndex}/columnchunks/{colIndex}             - Column chunk info\n")
	fmt.Printf("  GET /rowgroups/{rgIndex}/columnchunks/{colIndex}/pages       - All pages\n")
	fmt.Printf("  GET /rowgroups/{rgIndex}/columnchunks/{colIndex}/pages/{pageIndex} - Page info\n")
	fmt.Printf("  GET /rowgroups/{rgIndex}/columnchunks/{colIndex}/pages/{pageIndex}/content - Page content\n")
	fmt.Println()

	return http.ListenAndServe(addr, r)
}
