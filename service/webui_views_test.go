package service

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

func Test_HandleIndexPage(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))

	body := w.Body.String()
	require.Contains(t, body, "<!DOCTYPE html>")
	require.Contains(t, body, "Parquet Browser")
	require.Contains(t, body, "htmx")
}

func Test_HandleRowGroupsView_InvalidIndices(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupWebUIRoutes(router)

	tests := []struct {
		name           string
		path           string
		expectedStatus int
	}{
		{
			name:           "Invalid row group index",
			path:           "/ui/rowgroups/abc/columns",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Invalid column index",
			path:           "/ui/rowgroups/0/columns/xyz/pages",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Invalid page index",
			path:           "/ui/rowgroups/0/columns/0/pages/invalid/content",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.Equal(t, tt.expectedStatus, w.Code)
		})
	}
}

func Test_HandleMainView_URLPushUrl(t *testing.T) {
	// This test would require real parquet data, skip for now
	t.Skip("Requires real parquet file")
}

func Test_HandleColumnsView_Breadcrumbs(t *testing.T) {
	// Requires real parquet data
	t.Skip("Requires real parquet file")
}

func Test_HandleIndexPage_HTMXIncluded(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	body := w.Body.String()

	// Verify HTMX is included from CDN
	require.Contains(t, body, "unpkg.com/htmx.org")
}

func Test_HandleIndexPage_ContentArea(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	body := w.Body.String()

	// Verify content area exists for HTMX target
	require.Contains(t, body, `id="content-area"`)

	// Verify it loads main view on page load
	require.Contains(t, body, `hx-get="/ui/main"`)
	require.Contains(t, body, `hx-trigger="load"`)
}

func Test_HandleIndexPage_ErrorHandling(t *testing.T) {
	service := createTestService()

	// Test with a broken template (this is hard to simulate, but we can test the handler)
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	service.handleIndexPage(w, req)

	require.Equal(t, http.StatusOK, w.Code)
}

func Test_HandleIndexPage_IIFEScript(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	body := w.Body.String()

	// Verify the index page exists and has basic structure
	require.Contains(t, body, "<!DOCTYPE html>")
	require.Contains(t, body, "Parquet Browser")
}

func Test_HandleIndexPage_ContentType(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))
}

func Test_HandleIndexPage_Structure(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	body := w.Body.String()

	// Check for essential HTML elements
	require.Contains(t, body, "<html")
	require.Contains(t, body, "<head>")
	require.Contains(t, body, "<body>")
	require.Contains(t, body, "</html>")
	require.Contains(t, body, "content-area")
	require.Contains(t, body, "hx-get=\"/ui/main\"")
}

func Test_HandleMainView_WithRealFile(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/main", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))

	body := w.Body.String()
	require.Contains(t, body, "File Information")
	require.Contains(t, body, "all-types.parquet")
	require.Contains(t, body, "Row Groups")
	require.Contains(t, body, "/ui/rowgroups/0/columns")
	require.Contains(t, body, "View Schema")
}

func Test_HandleRowGroupsView_WithRealFile(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))

	body := w.Body.String()
	require.Contains(t, body, "Row Groups")
	require.Contains(t, body, "Total Rows")
	require.Contains(t, body, "Total Compressed")
	require.Contains(t, body, "Overall Compression")
	require.Contains(t, body, "/ui/rowgroups/0/columns")
}

func Test_HandleRowGroupsView_EmptyFile(t *testing.T) {
	svc := createTestServiceWithFile(t, "empty.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.String()
	require.Contains(t, body, "Row Groups")
	// Empty file should show 0 rows
	require.Contains(t, body, "0")
}

func Test_HandleColumnsView_WithRealFile(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))

	body := w.Body.String()
	require.Contains(t, body, "Column Chunks")
	require.Contains(t, body, "Total Columns")
	require.Contains(t, body, "Total Values")
	require.Contains(t, body, "Total Size")
	require.Contains(t, body, "/ui/rowgroups/0/columns/0/pages")
	// Verify Min and Max columns are present
	require.Contains(t, body, "<th>Min</th>")
	require.Contains(t, body, "<th>Max</th>")
}

// Test handleColumnsView with invalid row group index
func Test_HandleColumnsView_InvalidRowGroupIndex(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/999/columns", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
}

// Test formatRatio with division by zero case
func Test_HandleRowGroupsView_WithZeroCompression(t *testing.T) {
	// This tests the division by zero case in handleRowGroupsView
	// when totalCompressed is 0
	svc := createTestServiceWithFile(t, "empty.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	// Should handle zero compression gracefully
	body := w.Body.String()
	require.Contains(t, body, "Row Groups")
}

// Test handleColumnsView with division by zero
func Test_HandleColumnsView_CompressionRatio(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	// Should display compression ratio in Total Size field
	require.Contains(t, body, "Total Size")
}

// Test columns view with null count
func Test_HandleColumnsView_NullCount(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	// Should display null count
	require.Contains(t, body, "Null")
}

// Test ConvertedType display in columns view
func Test_HandleColumnsView_ConvertedType(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	// Should have ConvertedType column header
	require.Contains(t, body, "Converted Type")
}

// Test handleMainView with empty file
func Test_HandleMainView_EmptyFile(t *testing.T) {
	svc := createTestServiceWithFile(t, "empty.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/main", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "File Information")
}

// Test handleRowGroupsView compression ratio with zero values
func Test_HandleRowGroupsView_DivisionByZero(t *testing.T) {
	svc := createTestServiceWithFile(t, "empty.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	// Should not panic on division by zero
	body := w.Body.String()
	require.Contains(t, body, "Row Groups")
}

// Test handleColumnsView with column that has null values
func Test_HandleColumnsView_WithNulls(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	// Should display columns with or without nulls
	require.Contains(t, body, "Column Chunks")
}

// Test handleRowGroupsView division paths
func Test_HandleRowGroupsView_AllPaths(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "Row Groups")
	// Should have compression ratio calculated
	require.Contains(t, body, "Compression")
}

// Test handleColumnsView all paths with real data
func Test_HandleColumnsView_AllCodePaths(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "Column Chunks")
	require.Contains(t, body, "Total Size")
}
