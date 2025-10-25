package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-browser/model"
)

func Test_NewParquetClient(t *testing.T) {
	baseURL := "http://localhost:8080"
	client := NewParquetClient(baseURL)

	require.NotNil(t, client, "NewParquetClient() should return non-nil client")

	require.Equal(t, baseURL, client.baseURL, "baseURL should match")

	require.NotNil(t, client.client, "HTTP client should not be nil")
}

func Test_GetFileInfo(t *testing.T) {
	expectedInfo := model.FileInfo{
		Version:             1,
		NumRows:             1000,
		NumRowGroups:        5,
		NumLeafColumns:      10,
		CreatedBy:           "test",
		TotalCompressedSize: 10240,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/info", r.URL.Path, "Expected path /info")
		require.Equal(t, "GET", r.Method, "Expected GET method")

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedInfo)
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	info, err := client.GetFileInfo()

	require.NoError(t, err, "GetFileInfo() error = %v")

	require.Equal(t, expectedInfo.CreatedBy, info.CreatedBy, "CreatedBy should match")
	require.Equal(t, expectedInfo.NumRows, info.NumRows, "NumRows should match")
}

func Test_GetAllRowGroupsInfo(t *testing.T) {
	expectedGroups := []model.RowGroupInfo{
		{NumRows: 100, CompressedSize: 1024, NumColumns: 5},
		{NumRows: 200, CompressedSize: 2048, NumColumns: 5},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups", r.URL.Path, "Expected path /rowgroups")

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedGroups)
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	groups, err := client.GetAllRowGroupsInfo()

	require.NoError(t, err, "GetAllRowGroupsInfo() error = %v")

	require.Len(t, groups, 2, "Expected 2 groups")

	require.Equal(t, int64(100), groups[0].NumRows, "groups[0].NumRows should match")
}

func Test_GetRowGroupInfo(t *testing.T) {
	expectedInfo := model.RowGroupInfo{
		NumRows:        100,
		CompressedSize: 1024,
		NumColumns:     5,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups/0", r.URL.Path, "Expected path /rowgroups/0")

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedInfo)
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	info, err := client.GetRowGroupInfo(0)

	require.NoError(t, err, "GetRowGroupInfo() error = %v")

	require.Equal(t, expectedInfo.NumRows, info.NumRows, "NumRows should match")
}

func Test_GetAllColumnChunksInfo(t *testing.T) {
	expectedColumns := []model.ColumnChunkInfo{
		{Name: "col1", PathInSchema: []string{"col1"}},
		{Name: "col2", PathInSchema: []string{"col2"}},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups/0/columnchunks", r.URL.Path, "Expected path /rowgroups/0/columnchunks")

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedColumns)
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	columns, err := client.GetAllColumnChunksInfo(0)

	require.NoError(t, err, "GetAllColumnChunksInfo() error = %v")

	require.Len(t, columns, 2, "Expected 2 columns")
}

func Test_GetColumnChunkInfo(t *testing.T) {
	expectedInfo := model.ColumnChunkInfo{
		Name:         "col1",
		PathInSchema: []string{"col1"},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups/0/columnchunks/0", r.URL.Path, "Expected path /rowgroups/0/columnchunks/0")

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedInfo)
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	info, err := client.GetColumnChunkInfo(0, 0)

	require.NoError(t, err, "GetColumnChunkInfo() error = %v")

	require.Equal(t, expectedInfo.Name, info.Name, "Name should match")
}

func Test_GetAllPagesInfo(t *testing.T) {
	expectedPages := []model.PageMetadata{
		{PageType: "DATA_PAGE", NumValues: 100},
		{PageType: "DATA_PAGE", NumValues: 200},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups/0/columnchunks/0/pages", r.URL.Path, "Expected path /rowgroups/0/columnchunks/0/pages")

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedPages)
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	pages, err := client.GetAllPagesInfo(0, 0)

	require.NoError(t, err, "GetAllPagesInfo() error = %v")

	require.Len(t, pages, 2, "Expected 2 pages")
}

func Test_GetPageInfo(t *testing.T) {
	expectedInfo := model.PageMetadata{
		PageType:  "DATA_PAGE",
		NumValues: 100,
		Encoding:  "PLAIN",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups/0/columnchunks/0/pages/0", r.URL.Path, "Expected path /rowgroups/0/columnchunks/0/pages/0")

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedInfo)
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	info, err := client.GetPageInfo(0, 0, 0)

	require.NoError(t, err, "GetPageInfo() error = %v")

	require.Equal(t, expectedInfo.PageType, info.PageType, "PageType should match")
}

func Test_GetPageContent(t *testing.T) {
	expectedValues := []string{"value1", "value2", "value3"}
	response := struct {
		Values []string `json:"values"`
		Count  int      `json:"count"`
	}{
		Values: expectedValues,
		Count:  len(expectedValues),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups/0/columnchunks/0/pages/0/content", r.URL.Path, "Expected path /rowgroups/0/columnchunks/0/pages/0/content")

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	values, err := client.GetPageContent(0, 0, 0)

	require.NoError(t, err, "GetPageContent() error = %v")

	require.Len(t, values, 3, "Expected 3 values")

	require.Equal(t, "value1", values[0], "values[0] should match")
}

func Test_GetSchemaGo(t *testing.T) {
	expectedSchema := "package main\n\ntype MyStruct struct {\n\tField1 string\n}"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/schema/go", r.URL.Path, "Expected path /schema/go")

		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(expectedSchema))
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	schema, err := client.GetSchemaGo()

	require.NoError(t, err, "GetSchemaGo() error = %v")

	require.Equal(t, expectedSchema, schema, "schema should match")
}

func Test_GetSchemaJSON(t *testing.T) {
	expectedSchema := `{"type":"struct"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/schema/json", r.URL.Path, "Expected path should match")

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(expectedSchema))
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	schema, err := client.GetSchemaJSON()

	require.NoError(t, err, "GetSchemaJSON() should not error")
	require.Equal(t, expectedSchema, schema, "schema should match")
}

func Test_GetSchemaRaw(t *testing.T) {
	expectedSchema := `{"raw":"schema"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/schema/raw", r.URL.Path, "Expected path should match")

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(expectedSchema))
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	schema, err := client.GetSchemaRaw()

	require.NoError(t, err, "GetSchemaRaw() should not error")
	require.Equal(t, expectedSchema, schema, "schema should match")
}

func Test_GetSchemaCSV(t *testing.T) {
	expectedSchema := "column,type,encoding\ncol1,INT32,PLAIN\ncol2,BYTE_ARRAY,PLAIN"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/schema/csv", r.URL.Path, "Expected path /schema/csv")

		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte(expectedSchema))
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	schema, err := client.GetSchemaCSV()

	require.NoError(t, err, "GetSchemaCSV() error = %v")

	require.Equal(t, expectedSchema, schema, "schema should match")
}

func Test_Get_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	_, err := client.GetFileInfo()

	require.Error(t, err, "Expected error for HTTP 404, got nil")

	// Error should contain status code
	require.Contains(t, err.Error(), "404", "Error should contain status code 404")
}

func Test_Get_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	_, err := client.GetFileInfo()

	require.Error(t, err, "Expected error for invalid JSON, got nil")

	require.Contains(t, err.Error(), "failed to decode", "Error should mention failed to decode")
}

func Test_GetText_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)
	_, err := client.GetSchemaGo()

	require.Error(t, err, "Expected error for HTTP 500, got nil")

	require.Contains(t, err.Error(), "500", "Error should contain status code 500")
}

func Test_Client_InvalidURL(t *testing.T) {
	// Create client with invalid URL that can't be reached
	client := NewParquetClient("http://invalid-host-that-does-not-exist:99999")

	_, err := client.GetFileInfo()
	require.Error(t, err, "Expected error for unreachable server, got nil")

	require.Contains(t, err.Error(), "HTTP request failed", "Error should mention HTTP request failed")
}

func Test_GetText_InvalidURL(t *testing.T) {
	client := NewParquetClient("http://invalid-host-that-does-not-exist:99999")

	_, err := client.GetSchemaGo()
	require.Error(t, err, "Expected error for unreachable server, got nil")

	require.Contains(t, err.Error(), "HTTP request failed", "Error should mention HTTP request failed")
}

func Test_Multiple_Requests(t *testing.T) {
	// Test that the client can handle multiple requests
	callCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		info := model.FileInfo{CreatedBy: "test"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(info)
	}))
	defer server.Close()

	client := NewParquetClient(server.URL)

	// Make multiple requests
	for i := 0; i < 3; i++ {
		_, err := client.GetFileInfo()
		require.NoError(t, err, "Request %d should not fail", i)
	}

	require.Equal(t, 3, callCount, "Expected 3 requests")
}
