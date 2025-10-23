package service

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"os/exec"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/gorilla/mux"
	pschema "github.com/hangxie/parquet-tools/schema"

	"github.com/hangxie/parquet-browser/model"
)

//go:embed templates/*.html
var templatesFS embed.FS

var templates *template.Template

func init() {
	var err error
	templates, err = template.ParseFS(templatesFS, "templates/*.html")
	if err != nil {
		panic(fmt.Sprintf("Failed to parse templates: %v", err))
	}
}

// SetupWebUIRoutes configures all web UI routes
func (s *ParquetService) SetupWebUIRoutes(r *mux.Router) {
	// Main UI routes
	r.HandleFunc("/", s.handleIndexPage).Methods("GET")
	r.HandleFunc("/ui/main", s.handleMainView).Methods("GET")
	r.HandleFunc("/ui/schema", s.handleSchemaView).Methods("GET")
	r.HandleFunc("/ui/schema/go", s.handleSchemaGoView).Methods("GET")
	r.HandleFunc("/ui/schema/json", s.handleSchemaJSONView).Methods("GET")
	r.HandleFunc("/ui/schema/csv", s.handleSchemaCSVView).Methods("GET")
	r.HandleFunc("/ui/schema/raw", s.handleSchemaRawView).Methods("GET")
	r.HandleFunc("/ui/rowgroups", s.handleRowGroupsView).Methods("GET")
	r.HandleFunc("/ui/rowgroups/{rgIndex}/columns", s.handleColumnsView).Methods("GET")
	r.HandleFunc("/ui/rowgroups/{rgIndex}/columns/{colIndex}/pages", s.handlePagesView).Methods("GET")
	r.HandleFunc("/ui/rowgroups/{rgIndex}/columns/{colIndex}/pages/{pageIndex}/content", s.handlePageContentView).Methods("GET")

	// Catch-all for static files and other resources (favicon, service worker, etc.)
	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Silently return 404 for common browser requests
		w.WriteHeader(http.StatusNotFound)
	})
}

// handleIndexPage serves the main HTML page
func (s *ParquetService) handleIndexPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	err := templates.ExecuteTemplate(w, "index.html", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleMainView serves the main view with file info
func (s *ParquetService) handleMainView(w http.ResponseWriter, r *http.Request) {
	info := s.reader.GetFileInfo()

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
	}{
		FileName:              s.uri,
		Version:               info.Version,
		NumRowGroups:          info.NumRowGroups,
		TotalRows:             info.NumRows,
		LeafColumns:           info.NumLeafColumns,
		TotalCompressedSize:   formatBytes(info.TotalCompressedSize),
		TotalUncompressedSize: formatBytes(info.TotalUncompressedSize),
		CompressionRatio:      formatRatio(info.CompressionRatio),
		CreatedBy:             info.CreatedBy,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	err := templates.ExecuteTemplate(w, "main", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleSchemaView serves the schema viewer page
func (s *ParquetService) handleSchemaView(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	err := templates.ExecuteTemplate(w, "schema", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleSchemaGoView returns schema in Go format for HTMX
func (s *ParquetService) handleSchemaGoView(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := pschema.NewSchemaTree(s.parquetReader, pschema.SchemaOption{FailOnInt96: false})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to generate schema: %v", err), http.StatusInternalServerError)
		return
	}

	schemaText, err := schemaRoot.GoStruct(false)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to format Go schema: %v", err), http.StatusInternalServerError)
		return
	}

	formatted, err := FormatGoCode(schemaText)
	if err != nil {
		formatted = schemaText
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(formatted))
}

// handleSchemaJSONView returns schema in JSON format for HTMX
func (s *ParquetService) handleSchemaJSONView(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := pschema.NewSchemaTree(s.parquetReader, pschema.SchemaOption{FailOnInt96: false})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to generate schema: %v", err), http.StatusInternalServerError)
		return
	}

	schemaText := schemaRoot.JSONSchema()

	// Pretty print the JSON
	var jsonData interface{}
	var output []byte
	if err := json.Unmarshal([]byte(schemaText), &jsonData); err == nil {
		prettyBytes, err := json.MarshalIndent(jsonData, "", "  ")
		if err == nil {
			output = prettyBytes
		} else {
			output = []byte(schemaText)
		}
	} else {
		output = []byte(schemaText)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(output)
}

// handleSchemaCSVView returns schema in CSV format for HTMX
func (s *ParquetService) handleSchemaCSVView(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := pschema.NewSchemaTree(s.parquetReader, pschema.SchemaOption{FailOnInt96: false})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to generate schema: %v", err), http.StatusInternalServerError)
		return
	}

	schemaText, err := schemaRoot.CSVSchema()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to format CSV schema: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(schemaText))
}

// handleSchemaRawView returns raw schema for HTMX
func (s *ParquetService) handleSchemaRawView(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := pschema.NewSchemaTree(s.parquetReader, pschema.SchemaOption{FailOnInt96: false})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to generate schema: %v", err), http.StatusInternalServerError)
		return
	}

	// Convert to JSON for pretty printing
	rawJSON, err := json.MarshalIndent(*schemaRoot, "", "  ")
	if err != nil {
		// Fallback to Go's %+v format if JSON marshaling fails
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte(fmt.Sprintf("%+v", *schemaRoot)))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(rawJSON)
}

// handleRowGroupsView serves the row groups list view
func (s *ParquetService) handleRowGroupsView(w http.ResponseWriter, r *http.Request) {
	rowGroups := s.reader.GetAllRowGroupsInfo()

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
			CompressedSize:   formatBytes(rg.CompressedSize),
			UncompressedSize: formatBytes(rg.UncompressedSize),
			CompressionRatio: formatRatio(rg.CompressionRatio),
			FileOffset:       fmt.Sprintf("0x%X", rg.CompressedSize), // Using compressed size as placeholder
		}
	}

	data := struct {
		RowGroups         []FormattedRowGroup
		TotalRowGroups    int
		TotalRows         int64
		TotalCompressed   string
		TotalUncompressed string
		OverallRatio      string
	}{
		RowGroups:         formatted,
		TotalRowGroups:    len(rowGroups),
		TotalRows:         totalRows,
		TotalCompressed:   formatBytes(totalCompressed),
		TotalUncompressed: formatBytes(totalUncompressed),
		OverallRatio:      formatRatio(float64(totalUncompressed) / float64(totalCompressed)),
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	err := templates.ExecuteTemplate(w, "rowgroups", data)
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

		formatted[i] = FormattedColumn{
			Index:            col.Index,
			ColumnPath:       col.Name,
			PhysicalType:     col.PhysicalType,
			LogicalType:      col.LogicalType,
			ConvertedType:    col.ConvertedType,
			Codec:            col.Codec,
			NumValues:        col.NumValues,
			NullCount:        nullCountStr,
			CompressedSize:   formatBytes(col.CompressedSize),
			UncompressedSize: formatBytes(col.UncompressedSize),
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
		TotalCompressed:   formatBytes(totalCompressed),
		TotalUncompressed: formatBytes(totalUncompressed),
		CompressionRatio:  formatRatio(float64(totalUncompressed) / float64(totalCompressed)),
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	err = templates.ExecuteTemplate(w, "columns", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

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

	pages, err := s.reader.GetPageMetadataList(rgIndex, colIndex)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Get column info for breadcrumb and summary
	colInfo, err := s.reader.GetColumnChunkInfo(rgIndex, colIndex)
	var columnPath, physicalType, logicalType, convertedType, codec string
	if err == nil {
		columnPath = colInfo.Name
		physicalType = colInfo.PhysicalType
		logicalType = colInfo.LogicalType
		convertedType = colInfo.ConvertedType
		codec = colInfo.Codec
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
	}

	// Calculate totals
	var totalValues int32
	var totalCompressed, totalUncompressed int64
	formatted := make([]FormattedPage, len(pages))
	for i, page := range pages {
		totalValues += page.NumValues
		totalCompressed += int64(page.CompressedSize)
		totalUncompressed += int64(page.UncompressedSize)

		formatted[i] = FormattedPage{
			Index:            page.Index,
			PageType:         page.PageType,
			Offset:           fmt.Sprintf("0x%X", page.Offset),
			CompressedSize:   formatBytes(int64(page.CompressedSize)),
			UncompressedSize: formatBytes(int64(page.UncompressedSize)),
			NumValues:        page.NumValues,
			Encoding:         page.Encoding,
		}
	}

	data := struct {
		RowGroupIndex     int
		ColumnIndex       int
		ColumnPath        string
		PhysicalType      string
		LogicalType       string
		ConvertedType     string
		Codec             string
		Pages             []FormattedPage
		TotalPages        int
		TotalValues       int32
		TotalCompressed   string
		TotalUncompressed string
		CompressionRatio  string
	}{
		RowGroupIndex:     rgIndex,
		ColumnIndex:       colIndex,
		ColumnPath:        columnPath,
		PhysicalType:      physicalType,
		LogicalType:       logicalType,
		ConvertedType:     convertedType,
		Codec:             codec,
		Pages:             formatted,
		TotalPages:        len(pages),
		TotalValues:       totalValues,
		TotalCompressed:   formatBytes(totalCompressed),
		TotalUncompressed: formatBytes(totalUncompressed),
		CompressionRatio:  formatRatio(float64(totalUncompressed) / float64(totalCompressed)),
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	err = templates.ExecuteTemplate(w, "pages", data)
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

	values, err := s.reader.GetPageContentFormatted(rgIndex, colIndex, pageIndex)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	data := struct {
		RowGroupIndex int
		ColumnIndex   int
		PageIndex     int
		Values        []string
		Count         int
	}{
		RowGroupIndex: rgIndex,
		ColumnIndex:   colIndex,
		PageIndex:     pageIndex,
		Values:        values,
		Count:         len(values),
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	err = templates.ExecuteTemplate(w, "page_content", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// CreateWebUIRouter creates a router configured for the web UI
func CreateWebUIRouter(s *ParquetService) *mux.Router {
	r := mux.NewRouter()
	s.SetupWebUIRoutes(r)
	r.Use(CORSMiddleware)
	r.Use(LoggingMiddleware)
	return r
}

// openBrowser tries to open the URL in the default browser
func openBrowser(url string) error {
	if testing.Testing() {
		// do not launch browser under unit test
		return nil
	}

	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "linux":
		cmd = exec.Command("xdg-open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		return fmt.Errorf("unsupported platform")
	}

	return cmd.Start()
}

// StartWebUIServer starts the web UI server
func StartWebUIServer(service *ParquetService, addr string) error {
	r := CreateWebUIRouter(service)

	// Construct the full URL
	url := fmt.Sprintf("http://localhost%s", addr)

	fmt.Printf("Starting Parquet Browser Web UI on %s\n", addr)
	fmt.Printf("Opening browser to: %s\n", url)
	fmt.Println()

	// Open browser in a goroutine with a small delay to ensure server is ready
	go func() {
		time.Sleep(500 * time.Millisecond)
		if err := openBrowser(url); err != nil {
			fmt.Printf("Note: Could not automatically open browser: %v\n", err)
			fmt.Printf("Please open your browser and navigate to: %s\n", url)
		}
	}()

	return http.ListenAndServe(addr, r)
}

// Helper functions for formatting display values

func formatBytes(bytes int64) string {
	return model.FormatBytes(bytes)
}

func formatRatio(ratio float64) string {
	if ratio == 0 {
		return "N/A"
	}
	return fmt.Sprintf("%.2fx", ratio)
}
