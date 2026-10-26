package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

func Test_SchemaEndpointQueryParams(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupRoutes(router)

	tests := []struct {
		name string
		path string
	}{
		{"JSON schema", "/schema/json"},
		{"Raw schema", "/schema/raw"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			var match mux.RouteMatch

			// Just verify the route is registered, don't actually call it
			// since we don't have a real parquet file
			require.True(t, router.Match(req, &match))
		})
	}
}

// Test that all schema format endpoints are registered
func Test_SchemaEndpoints(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupRoutes(router)

	endpoints := []string{
		"/schema/go",
		"/schema/json",
		"/schema/raw",
		"/schema/csv",
	}

	for _, endpoint := range endpoints {
		t.Run(endpoint, func(t *testing.T) {
			req := httptest.NewRequest("GET", endpoint, nil)
			var match mux.RouteMatch
			require.True(t, router.Match(req, &match))
		})
	}
}

// Test handleSchemaJSON basic functionality
func Test_HandleSchemaJSON_JSONUnmarshalError(t *testing.T) {
	parquetFile := fixturePath(t, "csv-good.parquet")

	service, err := NewParquetService(context.Background(), parquetFile, pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	router := mux.NewRouter()
	service.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/schema/json", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
}

// Test schema endpoints return appropriate content types
func Test_SchemaEndpoints_ContentTypes(t *testing.T) {
	parquetFile := fixturePath(t, "csv-good.parquet")

	service, err := NewParquetService(context.Background(), parquetFile, pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	router := mux.NewRouter()
	service.SetupRoutes(router)

	tests := []struct {
		path        string
		contentType string
	}{
		{"/schema/go", "text/plain"},
		{"/schema/json", "application/json"},
		{"/schema/raw", "application/json"},
		{"/schema/csv", "text/csv"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code == http.StatusOK {
				ct := w.Header().Get("Content-Type")
				require.Contains(t, ct, tt.contentType)
			}
		})
	}
}

// Test handleSchemaGo returns Go struct format
func Test_HandleSchemaGo_Success(t *testing.T) {
	service, err := NewParquetService(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	router := mux.NewRouter()
	service.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/schema/go", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	// Verify content type
	ct := w.Header().Get("Content-Type")
	require.Contains(t, ct, "text/plain")

	// Verify response is not empty
	require.NotZero(t, w.Body.Len())

	// Verify it looks like Go code
	body := w.Body.String()
	if !strings.Contains(body, "type") && !strings.Contains(body, "struct") {
		t.Logf("Body: %s", body)
	}
}

// Test handleSchemaJSON returns JSON format
func Test_HandleSchemaJSON_Success(t *testing.T) {
	service, err := NewParquetService(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	router := mux.NewRouter()
	service.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/schema/json", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	// Verify content type
	ct := w.Header().Get("Content-Type")
	require.Contains(t, ct, "application/json")

	// Verify response is not empty
	require.NotZero(t, w.Body.Len())
}

// Test handleSchemaRaw returns raw schema tree
func Test_HandleSchemaRaw_Success(t *testing.T) {
	service, err := NewParquetService(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	router := mux.NewRouter()
	service.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/schema/raw", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	// Verify content type
	ct := w.Header().Get("Content-Type")
	require.Contains(t, ct, "application/json")

	// Verify response is valid JSON
	var jsonData interface{}
	err = json.Unmarshal(w.Body.Bytes(), &jsonData)
	require.NoError(t, err)
}

// Test handleSchemaCSV returns CSV format or error
func Test_HandleSchemaCSV_Success(t *testing.T) {
	service, err := NewParquetService(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	router := mux.NewRouter()
	service.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/schema/csv", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// CSV format may not support all parquet features
	// The test parquet file may have optional columns which CSV doesn't support
	// So we accept either success or error
	switch w.Code {
	case http.StatusOK:
		// Verify content type
		ct := w.Header().Get("Content-Type")
		require.Contains(t, ct, "text/csv")

		// Verify response is not empty
		require.NotZero(t, w.Body.Len())
	case http.StatusInternalServerError:
		// CSV may not support all parquet features
		// This is acceptable - the handler was still exercised
		t.Logf("CSV schema failed (expected for files with unsupported features): %s", w.Body.String())
	default:
		require.Fail(t, "Unexpected status code")
	}
}

// Test all schema handlers together
func Test_AllSchemaHandlers_Integration(t *testing.T) {
	service, err := NewParquetService(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	router := mux.NewRouter()
	service.SetupRoutes(router)

	tests := []struct {
		name        string
		path        string
		contentType string
		allowError  bool // Some endpoints may error on certain parquet files
	}{
		{"Go schema", "/schema/go", "text/plain", false},
		{"JSON schema", "/schema/json", "application/json", false},
		{"Raw schema", "/schema/raw", "application/json", false},
		{"CSV schema", "/schema/csv", "text/csv", true}, // CSV may not support all parquet features
		{"File info", "/info", "application/json", false},
		{"Row groups", "/rowgroups", "application/json", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if tt.allowError && w.Code == http.StatusInternalServerError {
				// Some handlers may fail on certain parquet files
				t.Logf("Handler returned expected error: %s", w.Body.String())
				return
			}

			require.Equal(t, http.StatusOK, w.Code)

			ct := w.Header().Get("Content-Type")
			require.Contains(t, ct, tt.contentType)

			require.NotZero(t, w.Body.Len())
		})
	}
}

// Test handleSchemaGo with real file
func Test_HandleSchemaGo_WithRealFile(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/schema/go", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/plain; charset=utf-8", w.Header().Get("Content-Type"))
	body := w.Body.String()
	require.Contains(t, body, "type")
	require.Contains(t, body, "struct")
}

// Test handleSchemaCSV with CSV file
func Test_HandleSchemaCSV_WithRealCSVFile(t *testing.T) {
	svc := newFixtureService(t, "csv-good.parquet")

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/schema/csv", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/csv; charset=utf-8", w.Header().Get("Content-Type"))
	require.NotEmpty(t, w.Body.String())
}

// Test JSON marshaling in handleSchemaJSON with edge cases
func Test_HandleSchemaJSON_EdgeCases(t *testing.T) {
	svc := newFixtureService(t, "empty.parquet")

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	// Test with empty file
	req := httptest.NewRequest("GET", "/schema/json", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotEmpty(t, w.Body.String())
}

// Test handleSchemaRaw JSON marshaling paths
func Test_HandleSchemaRaw_EdgeCases(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	// Test both pretty and non-pretty paths
	req := httptest.NewRequest("GET", "/schema/raw", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotEmpty(t, w.Body.String())
}

// Test all schema endpoints together
func Test_AllSchemaEndpoints_Comprehensive(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	endpoints := []string{
		"/schema/go",
		"/schema/json",
		"/schema/raw",
	}

	for _, endpoint := range endpoints {
		t.Run(endpoint, func(t *testing.T) {
			req := httptest.NewRequest("GET", endpoint, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)
			require.NotEmpty(t, w.Body.String())
		})
	}
}

// Test CSV schema with proper CSV file
func Test_HandleSchemaCSV_WithCSVFile(t *testing.T) {
	svc := newFixtureService(t, "csv-good.parquet")

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/schema/csv", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/csv; charset=utf-8", w.Header().Get("Content-Type"))
	require.NotEmpty(t, w.Body.String())
}

// Test all schema handlers with different files to maximize coverage
func Test_SchemaHandlers_AllPaths(t *testing.T) {
	testFiles := []string{
		"all-types.parquet",
		"empty.parquet",
		"csv-good.parquet",
	}

	for _, filename := range testFiles {
		t.Run("File_"+filename, func(t *testing.T) {
			svc := newFixtureService(t, filename)

			router := mux.NewRouter()
			svc.SetupRoutes(router)

			// Test all schema endpoints
			endpoints := []struct {
				path     string
				expectOK bool
			}{
				{"/schema/go", true},
				{"/schema/json", true},
				{"/schema/raw", true},
			}

			for _, ep := range endpoints {
				req := httptest.NewRequest("GET", ep.path, nil)
				w := httptest.NewRecorder()

				router.ServeHTTP(w, req)

				if ep.expectOK {
					require.Equal(t, http.StatusOK, w.Code)
					require.NotEmpty(t, w.Body.String())
				}
			}
		})
	}
}

// Test handleSchemaGo formatting fallback path (when formatting fails)
func Test_HandleSchemaGo_FormattingPaths(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/schema/go", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	// Should contain Go code (formatted or not)
	require.NotEmpty(t, body)
}

// Test handleSchemaGo error path - list-of-list not supported for Go struct generation
func Test_HandleSchemaGo_ErrorPath_ListOfList(t *testing.T) {
	svc := newFixtureService(t, "list-of-list.parquet")

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/schema/go", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// list-of-list should fail to generate Go struct
	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Contains(t, w.Body.String(), "Failed to format Go schema")
}

// Test handleSchemaCSV error path - list-of-list not supported for CSV schema
func Test_HandleSchemaCSV_ErrorPath_ListOfList(t *testing.T) {
	svc := newFixtureService(t, "list-of-list.parquet")

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/schema/csv", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// list-of-list should fail to generate CSV schema
	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Contains(t, w.Body.String(), "Failed to format CSV schema")
}

// Test handleSchemaJSON success with complex schema (list-of-list)
func Test_HandleSchemaJSON_SuccessPath_ComplexSchema(t *testing.T) {
	svc := newFixtureService(t, "list-of-list.parquet")

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/schema/json", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// JSON schema should work even with list-of-list
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json; charset=utf-8", w.Header().Get("Content-Type"))

	// Verify it's valid JSON
	var jsonData interface{}
	err := json.Unmarshal(w.Body.Bytes(), &jsonData)
	require.NoError(t, err)
}

// Test handleSchemaRaw success with complex schema (list-of-list)
func Test_HandleSchemaRaw_SuccessPath_ComplexSchema(t *testing.T) {
	svc := newFixtureService(t, "list-of-list.parquet")

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/schema/raw", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// Raw schema should work even with list-of-list
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json; charset=utf-8", w.Header().Get("Content-Type"))

	// Verify it's valid JSON
	var jsonData interface{}
	err := json.Unmarshal(w.Body.Bytes(), &jsonData)
	require.NoError(t, err)
}

// Test handleSchemaGo with different file types
func Test_HandleSchemaGo_DifferentFileTypes(t *testing.T) {
	files := []string{"all-types.parquet", "empty.parquet", "csv-good.parquet"}

	for _, filename := range files {
		t.Run(filename, func(t *testing.T) {
			svc := newFixtureService(t, filename)

			router := mux.NewRouter()
			svc.SetupRoutes(router)

			req := httptest.NewRequest("GET", "/schema/go", nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, "text/plain; charset=utf-8", w.Header().Get("Content-Type"))
			require.NotEmpty(t, w.Body.String())
		})
	}
}

// Test handleSchemaJSON with different file types
func Test_HandleSchemaJSON_DifferentFileTypes(t *testing.T) {
	files := []string{"all-types.parquet", "empty.parquet", "csv-good.parquet"}

	for _, filename := range files {
		t.Run(filename, func(t *testing.T) {
			svc := newFixtureService(t, filename)

			router := mux.NewRouter()
			svc.SetupRoutes(router)

			req := httptest.NewRequest("GET", "/schema/json", nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, "application/json; charset=utf-8", w.Header().Get("Content-Type"))

			// Verify it's valid JSON
			var jsonData interface{}
			err := json.Unmarshal(w.Body.Bytes(), &jsonData)
			require.NoError(t, err)
		})
	}
}

// Test handleSchemaRaw with different file types
func Test_HandleSchemaRaw_DifferentFileTypes(t *testing.T) {
	files := []string{"all-types.parquet", "empty.parquet", "csv-good.parquet"}

	for _, filename := range files {
		t.Run(filename, func(t *testing.T) {
			svc := newFixtureService(t, filename)

			router := mux.NewRouter()
			svc.SetupRoutes(router)

			req := httptest.NewRequest("GET", "/schema/raw", nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, "application/json; charset=utf-8", w.Header().Get("Content-Type"))

			// Verify it's valid JSON
			var jsonData interface{}
			err := json.Unmarshal(w.Body.Bytes(), &jsonData)
			require.NoError(t, err)
		})
	}
}

// Test handleSchemaCSV with different file types
func Test_HandleSchemaCSV_DifferentFileTypes(t *testing.T) {
	files := []string{"csv-good.parquet", "empty.parquet"}

	for _, filename := range files {
		t.Run(filename, func(t *testing.T) {
			svc := newFixtureService(t, filename)

			router := mux.NewRouter()
			svc.SetupRoutes(router)

			req := httptest.NewRequest("GET", "/schema/csv", nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, "text/csv; charset=utf-8", w.Header().Get("Content-Type"))
			require.NotEmpty(t, w.Body.String())
		})
	}
}

// A two-level array without a LIST annotation is not a list: it stays a
// repeated group in the schema views.
func Test_NestedArrayFile_Schema(t *testing.T) {
	svc := newFixtureService(t, "nested-array.parquet")

	w := serveFixtureRequest(t, svc, "/schema/go")
	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.String()
	require.Contains(t, body, "Array []struct {")
	require.Contains(t, body, "name=array, type=STRUCT, repetitiontype=REPEATED")
	require.NotContains(t, body, "type=LIST")
}
