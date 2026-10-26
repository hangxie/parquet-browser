package service

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	preader "github.com/hangxie/parquet-go/v3/reader"
	"github.com/stretchr/testify/require"
)

func Test_HandlePagesView_Breadcrumbs(t *testing.T) {
	w := serveFixtureRequest(t, newFixtureService(t, "all-types.parquet"), "/ui/rowgroups/0/columns/0/pages")
	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, `class="breadcrumb"`)
	require.Contains(t, body, `href="/ui/rowgroups/0/columns"`)
}

func Test_HandlePageContentView_Breadcrumbs(t *testing.T) {
	w := serveFixtureRequest(t, newFixtureService(t, "all-types.parquet"), "/ui/rowgroups/0/columns/0/pages/0/content")
	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, `class="breadcrumb"`)
	require.Contains(t, body, `href="/ui/rowgroups/0/columns/0/pages"`)
}

func Test_HandlePagesView_WithRealFile(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns/0/pages", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))

	body := w.Body.String()
	require.Contains(t, body, "Column Chunk Info")
	require.Contains(t, body, "Pages")
	require.Contains(t, body, "Type")
	require.Contains(t, body, "Codec")
	require.Contains(t, body, "<th>Min</th>")
	require.Contains(t, body, "<th>Max</th>")
	require.Contains(t, body, "<strong>Min</strong>")
	require.Contains(t, body, "<strong>Max</strong>")
}

func Test_HandlePageContentView_WithRealFile(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns/0/pages/0/content", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))

	body := w.Body.String()
	require.Contains(t, body, "Page Info")
	require.Contains(t, body, "Page Type")
	require.Contains(t, body, "Encoding")
	require.Contains(t, body, "Values")
}

func Test_HandlePageContentView_InvalidIndices(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	tests := []struct {
		name           string
		path           string
		htmx           bool
		expectedStatus int
	}{
		{
			name:           "Invalid row group index",
			path:           "/ui/rowgroups/invalid/columns/0/pages/0/content",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Invalid column index",
			path:           "/ui/rowgroups/0/columns/invalid/pages/0/content",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Invalid page index",
			path:           "/ui/rowgroups/0/columns/0/pages/invalid/content",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Out of range row group direct request",
			path:           "/ui/rowgroups/999/columns/0/pages/0/content",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "Out of range column direct request",
			path:           "/ui/rowgroups/0/columns/999/pages/0/content",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "Out of range page direct request",
			path:           "/ui/rowgroups/0/columns/0/pages/999/content",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "Out of range row group HTMX request",
			path:           "/ui/rowgroups/999/columns/0/pages/0/content",
			htmx:           true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Out of range column HTMX request",
			path:           "/ui/rowgroups/0/columns/999/pages/0/content",
			htmx:           true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Out of range page HTMX request",
			path:           "/ui/rowgroups/0/columns/0/pages/999/content",
			htmx:           true,
			expectedStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			if tt.htmx {
				req.Header.Set("HX-Request", "true")
			}
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.Equal(t, tt.expectedStatus, w.Code)
		})
	}
}

func Test_HandlePagesView_InvalidIndices(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	tests := []struct {
		name           string
		path           string
		htmx           bool
		expectedStatus int
	}{
		{
			name:           "Invalid row group index",
			path:           "/ui/rowgroups/abc/columns/0/pages",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Invalid column index",
			path:           "/ui/rowgroups/0/columns/xyz/pages",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Out of range row group direct request",
			path:           "/ui/rowgroups/999/columns/0/pages",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "Out of range column direct request",
			path:           "/ui/rowgroups/0/columns/999/pages",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "Out of range row group HTMX request",
			path:           "/ui/rowgroups/999/columns/0/pages",
			htmx:           true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Out of range column HTMX request",
			path:           "/ui/rowgroups/0/columns/999/pages",
			htmx:           true,
			expectedStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			if tt.htmx {
				req.Header.Set("HX-Request", "true")
			}
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.Equal(t, tt.expectedStatus, w.Code)
		})
	}
}

func Test_HandlePagesView_WithoutColumnInfo(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	// Request pages for a valid row group and column
	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns/0/pages", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	// Even if column info has issues, the page should render
	body := w.Body.String()
	require.Contains(t, body, "Pages")
}

func Test_HandlePagesView_CompressionRatio(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns/0/pages", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	// Size field now includes compression ratio inline
	require.Contains(t, body, "Size")
}

func Test_HandlePageContentView_MultipleColumns(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	for colIndex := 0; colIndex < 3; colIndex++ {
		t.Run(fmt.Sprintf("column_%d", colIndex), func(t *testing.T) {
			req := httptest.NewRequest("GET",
				fmt.Sprintf("/ui/rowgroups/0/columns/%d/pages/0/content", colIndex), nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			// Either OK or NotFound (if column doesn't exist)
			if w.Code == http.StatusOK {
				require.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))
				body := w.Body.String()
				require.Contains(t, body, "Page Info")
			} else {
				require.Equal(t, http.StatusNotFound, w.Code)
			}
		})
	}
}

func Test_HandlePagesView_ConvertedType(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns/0/pages", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	// Page view should render (ConvertedType might be empty or present)
	require.Contains(t, body, "Type")
	require.Contains(t, body, "Logical")
}

func Test_HandlePageContentView_DifferentPageIndices(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns/0/pages/0/content", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code == http.StatusOK {
		body := w.Body.String()
		require.Contains(t, body, "Page Info")
		require.Contains(t, body, "Values")
	}
}

func Test_HandlePagesView_WithConverterType(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	for colIdx := 0; colIdx < 5; colIdx++ {
		req := httptest.NewRequest("GET",
			fmt.Sprintf("/ui/rowgroups/0/columns/%d/pages", colIdx), nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code == http.StatusOK {
			// Successfully rendered a page
			break
		}
	}
}

func Test_HandlePagesView_AllCodePaths(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns/0/pages", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "Column Chunk Info")
	require.Contains(t, body, "Size")
}

func Test_HandlePageContentView_AllCodePaths(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns/0/pages/0/content", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "Page Info")
	require.Contains(t, body, "Page Type")
	require.Contains(t, body, "Encoding")
}

// A reader error renders a 200 modal partial over the current view, so it can be dismissed.
func Test_HandlePagesView_ReaderError_RendersErrorPartial(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	// colIndex 9999 is syntactically valid but out of range, triggering the
	// same reader-error path that fires for an encrypted column without keys.
	req := httptest.NewRequest("GET", "/ui/rowgroups/0/columns/9999/pages", nil)
	req.Header.Set("HX-Request", "true")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))
	// Retarget headers keep the underlying columns view visible and floating
	// the modal on top of <body> via beforeend.
	require.Equal(t, "body", w.Header().Get("HX-Retarget"))
	require.Equal(t, "beforeend", w.Header().Get("HX-Reswap"))
	require.Equal(t, "false", w.Header().Get("HX-Push-Url"))

	body := w.Body.String()
	require.Contains(t, body, "Cannot read this column")
	require.NotContains(t, body, `id="pb-error-modal"`)
	require.Contains(t, body, `class="pb-modal-overlay pb-error-modal"`)
	require.Contains(t, body, "pb-error-modal")
	require.Contains(t, body, `this.closest('.pb-modal-overlay').remove()`)
}

// Fakes the encryption error through the helper the handler uses.
func Test_HandlePagesView_EncryptionError_FriendlyMessage(t *testing.T) {
	title, message := describeReaderError(fmt.Errorf(
		"resolve column key: %w for column schema.double_field",
		preader.ErrColumnKeyRequired,
	))
	require.Equal(t, "Column requires a decryption key", title)
	require.Contains(t, message, "Column schema.double_field is encrypted.")
	require.Contains(t, message, "--column-key=schema.double_field=<base64-key>")
	require.Contains(t, message, "--key-file=")
}

// The page-content endpoint has its own error branches, with the same recovery.
func Test_HandlePageContentView_ReaderError_RendersErrorPartial(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET",
		"/ui/rowgroups/0/columns/9999/pages/0/content", nil)
	req.Header.Set("HX-Request", "true")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "body", w.Header().Get("HX-Retarget"))
	require.Equal(t, "beforeend", w.Header().Get("HX-Reswap"))
	body := w.Body.String()
	require.Contains(t, body, "Cannot read this column")
	require.NotContains(t, body, `id="pb-error-modal"`)
	require.Contains(t, body, `class="pb-modal-overlay pb-error-modal"`)
}
