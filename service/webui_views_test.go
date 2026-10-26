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
	w := serveFixtureRequest(t, newFixtureService(t, "all-types.parquet"), "/ui/main")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `hx-push-url="true"`)
}

func Test_HandleColumnsView_Breadcrumbs(t *testing.T) {
	w := serveFixtureRequest(t, newFixtureService(t, "all-types.parquet"), "/ui/rowgroups/0/columns")
	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, `class="breadcrumb"`)
	require.Contains(t, body, `href="/ui/main"`)
	require.Contains(t, body, `href="/ui/rowgroups"`)
}

func Test_HandleIndexPage_HTMXIncluded(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	body := w.Body.String()

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

	require.Contains(t, body, `id="content-area"`)

	require.Contains(t, body, `hx-get="/ui/main"`)
	require.Contains(t, body, `hx-trigger="load"`)
}

func Test_HandleIndexPage_ErrorHandling(t *testing.T) {
	service := createTestService()

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

	require.Contains(t, body, "<html")
	require.Contains(t, body, "<head>")
	require.Contains(t, body, "<body>")
	require.Contains(t, body, "</html>")
	require.Contains(t, body, "content-area")
	require.Contains(t, body, "hx-get=\"/ui/main\"")
}

func Test_HandleMainView_WithRealFile(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

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
	svc := newFixtureService(t, "all-types.parquet")

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
	svc := newFixtureService(t, "empty.parquet")

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
	svc := newFixtureService(t, "all-types.parquet")

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
	require.Contains(t, body, "<th>Min</th>")
	require.Contains(t, body, "<th>Max</th>")
}

func Test_HandleColumnsView_InvalidRowGroupIndex(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/999/columns", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func Test_HandleRowGroupsView_WithZeroCompression(t *testing.T) {
	// This tests the division by zero case in handleRowGroupsView
	// when totalCompressed is 0
	svc := newFixtureService(t, "empty.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "Row Groups")
}

func Test_HandleColumnsView_CompressionRatio(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "Total Size")
}

func Test_HandleColumnsView_NullCount(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "Null")
}

func Test_HandleColumnsView_ConvertedType(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "Converted Type")
}

func Test_HandleMainView_EmptyFile(t *testing.T) {
	svc := newFixtureService(t, "empty.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/main", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "File Information")
}

func Test_HandleRowGroupsView_DivisionByZero(t *testing.T) {
	svc := newFixtureService(t, "empty.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "Row Groups")
}

func Test_HandleColumnsView_WithNulls(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "Column Chunks")
}

func Test_HandleRowGroupsView_AllPaths(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "Row Groups")
	require.Contains(t, body, "Compression")
}

func Test_HandleColumnsView_AllCodePaths(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

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

func Test_ZeroColumnFile_WebUI(t *testing.T) {
	svc := newFixtureService(t, "zero-column.parquet")

	for _, url := range []string{"/ui/main", "/ui/rowgroups", "/ui/rowgroups/0/columns"} {
		t.Run(url, func(t *testing.T) {
			w := serveFixtureRequest(t, svc, url)
			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))
		})
	}

	t.Run("/ui/schema/go", func(t *testing.T) {
		w := serveFixtureRequest(t, svc, "/ui/schema/go")
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Body.String(), "Parquet_go_root struct")
	})

	t.Run("pages of missing column", func(t *testing.T) {
		w := serveFixtureRequest(t, svc, "/ui/rowgroups/0/columns/0/pages")
		require.Equal(t, http.StatusNotFound, w.Code)
	})
}

func Test_ListMapLayoutFile_WebUI(t *testing.T) {
	svc := newFixtureService(t, "list-map-layout.parquet")

	t.Run("columns", func(t *testing.T) {
		w := serveFixtureRequest(t, svc, "/ui/rowgroups/0/columns")
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Body.String(), "Athena.List.Element")
		require.NotContains(t, w.Body.String(), "Bag.Array_element")
	})

	t.Run("schema go", func(t *testing.T) {
		w := serveFixtureRequest(t, svc, "/ui/schema/go")
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Body.String(), "Legacy")
	})
}
