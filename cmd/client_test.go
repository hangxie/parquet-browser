package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-browser/model"
)

func Test_newParquetClient(t *testing.T) {
	baseURL := "http://localhost:8080"
	client := newParquetClient(baseURL)

	require.NotNil(t, client)

	require.Equal(t, baseURL, client.baseURL)

	require.NotNil(t, client.client)
}

func Test_getFileInfo(t *testing.T) {
	expectedInfo := model.FileInfo{
		Version:             1,
		NumRows:             1000,
		NumRowGroups:        5,
		NumLeafColumns:      10,
		CreatedBy:           "test",
		TotalCompressedSize: 10240,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/info", r.URL.Path)
		require.Equal(t, "GET", r.Method)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedInfo)
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	info, err := client.getFileInfo()

	require.NoError(t, err)

	require.Equal(t, expectedInfo.CreatedBy, info.CreatedBy)
	require.Equal(t, expectedInfo.NumRows, info.NumRows)
}

func Test_getAllRowGroupsInfo(t *testing.T) {
	expectedGroups := []model.RowGroupInfo{
		{NumRows: 100, CompressedSize: 1024, NumColumns: 5},
		{NumRows: 200, CompressedSize: 2048, NumColumns: 5},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedGroups)
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	groups, err := client.getAllRowGroupsInfo()

	require.NoError(t, err)

	require.Len(t, groups, 2)

	require.Equal(t, int64(100), groups[0].NumRows)
}

func Test_getRowGroupInfo(t *testing.T) {
	expectedInfo := model.RowGroupInfo{
		NumRows:        100,
		CompressedSize: 1024,
		NumColumns:     5,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups/0", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedInfo)
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	info, err := client.getRowGroupInfo(0)

	require.NoError(t, err)

	require.Equal(t, expectedInfo.NumRows, info.NumRows)
}

func Test_getAllColumnChunksInfo(t *testing.T) {
	expectedColumns := []model.ColumnChunkInfo{
		{Name: "col1", PathInSchema: []string{"col1"}},
		{Name: "col2", PathInSchema: []string{"col2"}},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups/0/columnchunks", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedColumns)
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	columns, err := client.getAllColumnChunksInfo(0)

	require.NoError(t, err)

	require.Len(t, columns, 2)
}

func Test_getColumnChunkInfo(t *testing.T) {
	expectedInfo := model.ColumnChunkInfo{
		Name:         "col1",
		PathInSchema: []string{"col1"},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups/0/columnchunks/0", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedInfo)
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	info, err := client.getColumnChunkInfo(0, 0)

	require.NoError(t, err)

	require.Equal(t, expectedInfo.Name, info.Name)
}

func Test_getAllPagesInfo(t *testing.T) {
	expectedPages := []model.PageMetadata{
		{PageType: "DATA_PAGE", NumValues: 100},
		{PageType: "DATA_PAGE", NumValues: 200},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups/0/columnchunks/0/pages", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedPages)
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	pages, err := client.getAllPagesInfo(0, 0)

	require.NoError(t, err)

	require.Len(t, pages, 2)
}

func Test_getPageInfo(t *testing.T) {
	expectedInfo := model.PageMetadata{
		PageType:  "DATA_PAGE",
		NumValues: 100,
		Encoding:  "PLAIN",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups/0/columnchunks/0/pages/0", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedInfo)
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	info, err := client.getPageInfo(0, 0, 0)

	require.NoError(t, err)

	require.Equal(t, expectedInfo.PageType, info.PageType)
}

func Test_getPageContent(t *testing.T) {
	expectedValues := []string{"value1", "value2", "value3"}
	response := struct {
		Values []string `json:"values"`
		Count  int      `json:"count"`
	}{
		Values: expectedValues,
		Count:  len(expectedValues),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rowgroups/0/columnchunks/0/pages/0/content", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	values, err := client.getPageContent(0, 0, 0)

	require.NoError(t, err)

	require.Len(t, values, 3)

	require.Equal(t, "value1", values[0])
}

func Test_getSchemaGo(t *testing.T) {
	expectedSchema := "package main\n\ntype MyStruct struct {\n\tField1 string\n}"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/schema/go", r.URL.Path)

		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(expectedSchema))
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	schema, err := client.getSchemaGo()

	require.NoError(t, err)

	require.Equal(t, expectedSchema, schema)
}

func Test_getSchemaJSON(t *testing.T) {
	expectedSchema := `{"type":"struct"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/schema/json", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(expectedSchema))
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	schema, err := client.getSchemaJSON()

	require.NoError(t, err)
	require.Equal(t, expectedSchema, schema)
}

func Test_getSchemaRaw(t *testing.T) {
	expectedSchema := `{"raw":"schema"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/schema/raw", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(expectedSchema))
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	schema, err := client.getSchemaRaw()

	require.NoError(t, err)
	require.Equal(t, expectedSchema, schema)
}

func Test_getSchemaCSV(t *testing.T) {
	expectedSchema := "column,type,encoding\ncol1,INT32,PLAIN\ncol2,BYTE_ARRAY,PLAIN"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/schema/csv", r.URL.Path)

		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte(expectedSchema))
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	schema, err := client.getSchemaCSV()

	require.NoError(t, err)

	require.Equal(t, expectedSchema, schema)
}

func Test_get_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	_, err := client.getFileInfo()

	require.Error(t, err)

	// Error should contain status code
	require.Contains(t, err.Error(), "404")
}

func Test_get_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	_, err := client.getFileInfo()

	require.Error(t, err)

	require.Contains(t, err.Error(), "failed to decode")
}

func Test_getText_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := newParquetClient(server.URL)
	_, err := client.getSchemaGo()

	require.Error(t, err)

	require.Contains(t, err.Error(), "500")
}

func Test_client_InvalidURL(t *testing.T) {
	// Create client with invalid URL that can't be reached
	client := newParquetClient("http://invalid-host-that-does-not-exist:99999")

	_, err := client.getFileInfo()
	require.Error(t, err)

	require.Contains(t, err.Error(), "HTTP request failed")
}

func Test_getText_InvalidURL(t *testing.T) {
	client := newParquetClient("http://invalid-host-that-does-not-exist:99999")

	_, err := client.getSchemaGo()
	require.Error(t, err)

	require.Contains(t, err.Error(), "HTTP request failed")
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

	client := newParquetClient(server.URL)

	// Make multiple requests
	for i := 0; i < 3; i++ {
		_, err := client.getFileInfo()
		require.NoError(t, err)
	}

	require.Equal(t, 3, callCount)
}
