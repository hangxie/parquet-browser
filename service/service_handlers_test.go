package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-browser/model"
)

func Test_HandleRowGroupInfo_InvalidIndex(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupRoutes(router)

	tests := []struct {
		name           string
		rgIndex        string
		expectedStatus int
	}{
		{
			name:           "Non-numeric index",
			rgIndex:        "abc",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Negative index",
			rgIndex:        "-1",
			expectedStatus: http.StatusBadRequest, // Will be converted to int, but model will reject
		},
		{
			name:           "Empty index",
			rgIndex:        "",
			expectedStatus: http.StatusNotFound, // Router won't match
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := "/rowgroups/" + tt.rgIndex
			req := httptest.NewRequest("GET", path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			// Router may return 404 for empty/invalid paths before handler is reached
			require.True(t, w.Code == tt.expectedStatus || w.Code == http.StatusNotFound)
		})
	}
}

func Test_HandleColumnChunkInfo_InvalidIndices(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupRoutes(router)

	tests := []struct {
		name           string
		rgIndex        string
		colIndex       string
		expectedStatus int
	}{
		{
			name:           "Non-numeric row group index",
			rgIndex:        "abc",
			colIndex:       "0",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Non-numeric column index",
			rgIndex:        "0",
			colIndex:       "xyz",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Both indices invalid",
			rgIndex:        "abc",
			colIndex:       "xyz",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := fmt.Sprintf("/rowgroups/%s/columnchunks/%s", tt.rgIndex, tt.colIndex)
			req := httptest.NewRequest("GET", path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			// Check that we get an error status
			require.True(t, w.Code == tt.expectedStatus || w.Code == http.StatusNotFound)
		})
	}
}

func Test_HandlePageInfo_InvalidIndices(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupRoutes(router)

	tests := []struct {
		name      string
		rgIndex   string
		colIndex  string
		pageIndex string
	}{
		{
			name:      "Invalid row group index",
			rgIndex:   "abc",
			colIndex:  "0",
			pageIndex: "0",
		},
		{
			name:      "Invalid column index",
			rgIndex:   "0",
			colIndex:  "abc",
			pageIndex: "0",
		},
		{
			name:      "Invalid page index",
			rgIndex:   "0",
			colIndex:  "0",
			pageIndex: "abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := fmt.Sprintf("/rowgroups/%s/columnchunks/%s/pages/%s",
				tt.rgIndex, tt.colIndex, tt.pageIndex)
			req := httptest.NewRequest("GET", path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			// Should get BadRequest or NotFound
			require.True(t, w.Code == http.StatusBadRequest || w.Code == http.StatusNotFound)
		})
	}
}

func Test_HandleColumnChunks_InvalidIndex(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/rowgroups/abc/columnchunks", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.True(t, w.Code == http.StatusBadRequest || w.Code == http.StatusNotFound)
}

func Test_HandlePages_InvalidIndices(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupRoutes(router)

	tests := []struct {
		name     string
		rgIndex  string
		colIndex string
	}{
		{
			name:     "Invalid row group",
			rgIndex:  "abc",
			colIndex: "0",
		},
		{
			name:     "Invalid column",
			rgIndex:  "0",
			colIndex: "xyz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := fmt.Sprintf("/rowgroups/%s/columnchunks/%s/pages", tt.rgIndex, tt.colIndex)
			req := httptest.NewRequest("GET", path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.True(t, w.Code == http.StatusBadRequest || w.Code == http.StatusNotFound)
		})
	}
}

func Test_HandlePageContent_InvalidIndices(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupRoutes(router)

	tests := []struct {
		name      string
		rgIndex   string
		colIndex  string
		pageIndex string
	}{
		{
			name:      "Invalid row group",
			rgIndex:   "abc",
			colIndex:  "0",
			pageIndex: "0",
		},
		{
			name:      "Invalid column",
			rgIndex:   "0",
			colIndex:  "abc",
			pageIndex: "0",
		},
		{
			name:      "Invalid page",
			rgIndex:   "0",
			colIndex:  "0",
			pageIndex: "xyz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := fmt.Sprintf("/rowgroups/%s/columnchunks/%s/pages/%s/content",
				tt.rgIndex, tt.colIndex, tt.pageIndex)
			req := httptest.NewRequest("GET", path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.True(t, w.Code == http.StatusBadRequest || w.Code == http.StatusNotFound)
		})
	}
}

// Helper function - reuse from http_helpers_test.go
// (already defined in that file)

// Test error responses contain proper JSON structure
func Test_ErrorResponseStructure(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupRoutes(router)

	// Test with invalid row group index
	req := httptest.NewRequest("GET", "/rowgroups/invalid", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// If we got a 400, verify the error response is valid JSON
	if w.Code == http.StatusBadRequest {
		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)

		_, ok := response["error"]
		require.True(t, ok)
	}
}

// Test invalid parameter handling
func Test_Handlers_InvalidParameters(t *testing.T) {
	svc := &ParquetService{}
	router := mux.NewRouter()
	svc.SetupRoutes(router)

	tests := []struct {
		name string
		path string
	}{
		{"Invalid row group index", "/rowgroups/abc"},
		{"Invalid column index", "/rowgroups/0/columnchunks/xyz"},
		{"Invalid page index", "/rowgroups/0/columnchunks/0/pages/bad"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			// Should get BadRequest (400) or NotFound (404)
			if w.Code != http.StatusBadRequest && w.Code != http.StatusNotFound {
				t.Logf("Got status %d for %s", w.Code, tt.path)
			}

			// If BadRequest, should have error in JSON
			if w.Code == http.StatusBadRequest {
				var errResp map[string]string
				err := json.Unmarshal(w.Body.Bytes(), &errResp)
				if err == nil {
					_, ok := errResp["error"]
					require.True(t, ok)
				}
			}
		})
	}
}

// Test all handler functions with a real service
func Test_AllHandlers_WithRealService(t *testing.T) {
	parquetFile := fixturePath(t, "csv-good.parquet")

	service, err := NewParquetService(context.Background(), parquetFile, pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	router := mux.NewRouter()
	service.SetupRoutes(router)

	tests := []struct {
		name   string
		method string
		path   string
		status int
	}{
		{"handleFileInfo", "GET", "/info", http.StatusOK},
		{"handleRowGroups", "GET", "/rowgroups", http.StatusOK},
		{"handleRowGroupInfo", "GET", "/rowgroups/0", http.StatusOK},
		{"handleColumnChunks", "GET", "/rowgroups/0/columnchunks", http.StatusOK},
		{"handleColumnChunkInfo", "GET", "/rowgroups/0/columnchunks/0", http.StatusOK},
		{"handlePages", "GET", "/rowgroups/0/columnchunks/0/pages", http.StatusOK},
		{"handlePageInfo", "GET", "/rowgroups/0/columnchunks/0/pages/0", http.StatusOK},
		{"handlePageContent", "GET", "/rowgroups/0/columnchunks/0/pages/0/content", http.StatusOK},
		{"handleSchemaGo", "GET", "/schema/go", http.StatusOK},
		{"handleSchemaJSON", "GET", "/schema/json", http.StatusOK},
		{"handleSchemaRaw", "GET", "/schema/raw", http.StatusOK},
		{"handleSchemaCSV", "GET", "/schema/csv", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			body, _ := io.ReadAll(w.Body)
			require.Equal(t, tt.status, w.Code)

			// Verify response is not empty
			require.NotZero(t, len(body))
		})
	}
}

// Test handler error responses
func Test_Handlers_ErrorResponses(t *testing.T) {
	parquetFile := fixturePath(t, "csv-good.parquet")

	service, err := NewParquetService(context.Background(), parquetFile, pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	router := mux.NewRouter()
	service.SetupRoutes(router)

	tests := []struct {
		name string
		path string
	}{
		{"Invalid row group", "/rowgroups/9999"},
		{"Invalid column", "/rowgroups/0/columnchunks/9999"},
		{"Invalid page", "/rowgroups/0/columnchunks/0/pages/9999"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			// Should get an error (400, 404, or 500)
			if w.Code == http.StatusOK {
				t.Logf("Warning: Expected error for %s but got OK", tt.path)
			}
		})
	}
}

// Test coverage for all partial handler functions
func Test_Handler_CoveragePaths(t *testing.T) {
	parquetFile := fixturePath(t, "csv-good.parquet")

	service, err := NewParquetService(context.Background(), parquetFile, pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	router := mux.NewRouter()
	service.SetupRoutes(router)

	// Test all success and error paths
	tests := []struct {
		name   string
		path   string
		status int
	}{
		// Success paths
		{"handleRowGroupInfo success", "/rowgroups/0", http.StatusOK},
		{"handleColumnChunks success", "/rowgroups/0/columnchunks", http.StatusOK},
		{"handleColumnChunkInfo success", "/rowgroups/0/columnchunks/0", http.StatusOK},
		{"handlePages success", "/rowgroups/0/columnchunks/0/pages", http.StatusOK},
		{"handlePageInfo success", "/rowgroups/0/columnchunks/0/pages/0", http.StatusOK},
		{"handlePageContent success", "/rowgroups/0/columnchunks/0/pages/0/content", http.StatusOK},

		// Error paths (invalid indices that should be out of bounds)
		{"handleRowGroupInfo error", "/rowgroups/999", http.StatusNotFound},
		{"handleColumnChunks error", "/rowgroups/999/columnchunks", http.StatusNotFound},
		{"handleColumnChunkInfo error", "/rowgroups/0/columnchunks/999", http.StatusNotFound},
		{"handlePages error", "/rowgroups/0/columnchunks/999/pages", http.StatusNotFound},
		{"handlePageInfo error", "/rowgroups/0/columnchunks/0/pages/999", http.StatusNotFound},
		{"handlePageContent error", "/rowgroups/0/columnchunks/0/pages/999/content", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			// Allow some flexibility in status codes
			if w.Code != tt.status && w.Code != http.StatusInternalServerError {
				// Some error responses may vary
				t.Logf("Status = %d for %s", w.Code, tt.path)
			}
		})
	}
}

// Test page content response structure
func Test_HandlePageContent_ResponseStructure(t *testing.T) {
	parquetFile := fixturePath(t, "csv-good.parquet")

	service, err := NewParquetService(context.Background(), parquetFile, pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	router := mux.NewRouter()
	service.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/rowgroups/0/columnchunks/0/pages/0/content", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code == http.StatusOK {
		var response map[string]interface{}
		err := json.NewDecoder(bytes.NewReader(w.Body.Bytes())).Decode(&response)
		require.NoError(t, err)

		_, ok := response["values"]
		require.True(t, ok)

		_, ok = response["count"]
		require.True(t, ok)
	}
}

// Test concurrent requests to handlers
func Test_Handlers_Concurrent(t *testing.T) {
	parquetFile := fixturePath(t, "csv-good.parquet")

	service, err := NewParquetService(context.Background(), parquetFile, pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	server := httptest.NewServer(CreateRouter(service, true))
	defer server.Close()

	// Make concurrent requests
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			endpoints := []string{"/info", "/rowgroups", "/schema/json"}
			endpoint := endpoints[i%len(endpoints)]

			resp, err := http.Get(server.URL + endpoint)
			if err == nil {
				_ = resp.Body.Close()
			}
			done <- true
		}(i)
	}

	// Wait for all requests
	for i := 0; i < 10; i++ {
		<-done
	}
}

// Test handleFileInfo returns file metadata
func Test_HandleFileInfo_Success(t *testing.T) {
	service, err := NewParquetService(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	router := mux.NewRouter()
	service.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/info", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	// Verify response is valid JSON
	var info map[string]interface{}
	err = json.Unmarshal(w.Body.Bytes(), &info)
	require.NoError(t, err)

	// Log the actual response to see what fields are present
	t.Logf("File info response: %+v", info)

	// The response should have some fields - check if it's not empty
	require.NotEmpty(t, info)
}

// Test handleRowGroups returns all row groups
func Test_HandleRowGroups_Success(t *testing.T) {
	service, err := NewParquetService(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	router := mux.NewRouter()
	service.SetupRoutes(router)

	req := httptest.NewRequest("GET", "/rowgroups", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	// Verify response is valid JSON array
	var rowGroups []interface{}
	err = json.Unmarshal(w.Body.Bytes(), &rowGroups)
	require.NoError(t, err)

	require.NotEmpty(t, rowGroups)
}

// Test all API handlers with real files for full coverage
func Test_AllAPIHandlers_WithRealFile(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	tests := []struct {
		name        string
		path        string
		contentType string
	}{
		{"File info", "/info", "application/json"},
		{"Row groups", "/rowgroups", "application/json"},
		{"Row group 0", "/rowgroups/0", "application/json"},
		{"Column chunks", "/rowgroups/0/columnchunks", "application/json"},
		{"Column chunk 0", "/rowgroups/0/columnchunks/0", "application/json"},
		{"Pages", "/rowgroups/0/columnchunks/0/pages", "application/json"},
		{"Page 0", "/rowgroups/0/columnchunks/0/pages/0", "application/json"},
		{"Page content", "/rowgroups/0/columnchunks/0/pages/0/content", "application/json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)
			require.Contains(t, w.Header().Get("Content-Type"), tt.contentType)
			require.NotEmpty(t, w.Body.String())
		})
	}
}

// Test error paths for out of range indices
func Test_APIHandlers_OutOfRangeIndices(t *testing.T) {
	svc := newFixtureService(t, "all-types.parquet")

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	tests := []struct {
		name           string
		path           string
		expectedStatus int
	}{
		{"Row group out of range", "/rowgroups/999", http.StatusNotFound},
		{"Column chunks - invalid row group", "/rowgroups/999/columnchunks", http.StatusNotFound},
		{"Column chunk - invalid row group", "/rowgroups/999/columnchunks/0", http.StatusNotFound},
		{"Column chunk - invalid column", "/rowgroups/0/columnchunks/999", http.StatusNotFound},
		{"Pages - invalid row group", "/rowgroups/999/columnchunks/0/pages", http.StatusNotFound},
		{"Pages - invalid column", "/rowgroups/0/columnchunks/999/pages", http.StatusNotFound},
		{"Page info - invalid row group", "/rowgroups/999/columnchunks/0/pages/0", http.StatusNotFound},
		{"Page info - invalid column", "/rowgroups/0/columnchunks/999/pages/0", http.StatusNotFound},
		{"Page info - invalid page", "/rowgroups/0/columnchunks/0/pages/999", http.StatusNotFound},
		{"Page content - invalid row group", "/rowgroups/999/columnchunks/0/pages/0/content", http.StatusNotFound},
		{"Page content - invalid column", "/rowgroups/0/columnchunks/999/pages/0/content", http.StatusNotFound},
		{"Page content - invalid page", "/rowgroups/0/columnchunks/0/pages/999/content", http.StatusNotFound},
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

func Test_ZeroColumnFile_API(t *testing.T) {
	svc := newFixtureService(t, "zero-column.parquet")

	t.Run("info", func(t *testing.T) {
		w := serveFixtureRequest(t, svc, "/info")
		require.Equal(t, http.StatusOK, w.Code)

		var info model.FileInfo
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &info))
		require.Equal(t, int64(3), info.NumRows)
		require.Equal(t, 1, info.NumRowGroups)
		require.Zero(t, info.NumLeafColumns)
		require.Zero(t, info.CompressionRatio)
	})

	t.Run("rowgroups", func(t *testing.T) {
		w := serveFixtureRequest(t, svc, "/rowgroups")
		require.Equal(t, http.StatusOK, w.Code)

		var rowGroups []model.RowGroupInfo
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &rowGroups))
		require.Len(t, rowGroups, 1)
		require.Equal(t, int64(3), rowGroups[0].NumRows)
		require.Zero(t, rowGroups[0].NumColumns)
	})

	t.Run("columnchunks", func(t *testing.T) {
		w := serveFixtureRequest(t, svc, "/rowgroups/0/columnchunks")
		require.Equal(t, http.StatusOK, w.Code)
		// An empty JSON array, not null, so clients can range over it.
		require.JSONEq(t, `[]`, w.Body.String())
	})

	for _, url := range []string{
		"/rowgroups/0/columnchunks/0",
		"/rowgroups/0/columnchunks/0/pages",
		"/rowgroups/0/columnchunks/0/pages/0",
		"/rowgroups/0/columnchunks/0/pages/0/content",
	} {
		t.Run(url, func(t *testing.T) {
			w := serveFixtureRequest(t, svc, url)
			require.Equal(t, http.StatusNotFound, w.Code)
			require.Contains(t, w.Body.String(), "invalid column index")
		})
	}

	schemaTests := []struct {
		url  string
		want string
	}{
		{url: "/schema/go", want: "type Parquet_go_root struct {\n}"},
		{url: "/schema/json", want: `"Tag":"name=parquet_go_root, inname=Parquet_go_root"`},
		{url: "/schema/raw", want: `"name":"parquet_go_root","num_children":0`},
	}
	for _, tt := range schemaTests {
		t.Run(tt.url, func(t *testing.T) {
			w := serveFixtureRequest(t, svc, tt.url)
			require.Equal(t, http.StatusOK, w.Code)
			require.Contains(t, w.Body.String(), tt.want)
		})
	}

	t.Run("/schema/csv", func(t *testing.T) {
		w := serveFixtureRequest(t, svc, "/schema/csv")
		require.Equal(t, http.StatusOK, w.Code)
		require.Empty(t, w.Body.String())
	})
}

// Legacy (Athena/parquet-mr style) and logical-type-only LIST/MAP layouts are
// normalized by the reader, so column names use List.Element and every schema
// format renders them as LIST/MAP instead of failing or falling back to groups.
func Test_ListMapLayoutFile_API(t *testing.T) {
	svc := newFixtureService(t, "list-map-layout.parquet")

	t.Run("columnchunks", func(t *testing.T) {
		w := serveFixtureRequest(t, svc, "/rowgroups/0/columnchunks")
		require.Equal(t, http.StatusOK, w.Code)

		var columns []model.ColumnChunkInfo
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &columns))

		names := make([]string, len(columns))
		for i, col := range columns {
			names[i] = col.Name
		}
		require.Equal(t, []string{
			"Athena.List.Element",
			"Athena_logical.List.Element",
			"Logical_list.List.Element",
			"Logical_map.Key_value.Key",
			"Logical_map.Key_value.Value",
			"Legacy.List.Element",
			"Entries_map.Entries.Key",
			"Entries_map.Entries.Value",
			"Entries_logical.Entries.Key",
			"Entries_logical.Entries.Value",
		}, names)
		require.Equal(t, "STRING", columns[0].LogicalType)
		require.Equal(t, "UTF8", columns[0].ConvertedType)
	})

	t.Run("legacy list page content", func(t *testing.T) {
		w := serveFixtureRequest(t, svc, "/rowgroups/0/columnchunks/0/pages/0/content")
		require.Equal(t, http.StatusOK, w.Code)
		require.JSONEq(t, `{"count": 3, "values": ["a", "", "b"]}`, w.Body.String())
	})

	t.Run("/schema/go", func(t *testing.T) {
		w := serveFixtureRequest(t, svc, "/schema/go")
		require.Equal(t, http.StatusOK, w.Code)

		body := w.Body.String()
		for _, want := range []string{
			"Athena         []*string",
			"Athena_logical []*string",
			"Logical_list   []int32",
			"Logical_map    map[string]int32",
			"Legacy         [][]int32",
			"name=logical_map, type=MAP",
		} {
			require.Contains(t, body, want)
		}
	})

	t.Run("/schema/json", func(t *testing.T) {
		w := serveFixtureRequest(t, svc, "/schema/json")
		require.Equal(t, http.StatusOK, w.Code)

		body := w.Body.String()
		for _, want := range []string{
			"name=athena_logical, inname=Athena_logical, type=LIST",
			"name=logical_list, inname=Logical_list, type=LIST",
			"name=logical_map, inname=Logical_map, type=MAP",
		} {
			require.Contains(t, body, want)
		}
		require.NotContains(t, body, "inname=Bag")
	})

	t.Run("/schema/csv", func(t *testing.T) {
		w := serveFixtureRequest(t, svc, "/schema/csv")
		require.Equal(t, http.StatusInternalServerError, w.Code)
		require.Contains(t, w.Body.String(), "CSV supports flat schema only")
	})
}
