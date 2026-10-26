package cmd

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_TUIApp_createColumnChunksList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rowgroups/0/columnchunks" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{
				"Index": 0,
				"Name": "user.email",
				"PhysicalType": "BYTE_ARRAY",
				"Codec": "SNAPPY",
				"CompressedSize": 2048,
				"MinValue": "abcdefghijklmnopqrstuvwxyz",
				"MaxValue": "zyxwvutsrqponmlkjihgfedcba"
			}
		]`))
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)

	table := app.createColumnChunksList(0)

	require.NotNil(t, table)
	assert.Equal(t, 2, table.GetRowCount())
	assert.Equal(t, "#", table.GetCell(0, 0).Text)
	assert.Equal(t, "Name", table.GetCell(0, 1).Text)
	assert.Equal(t, "user.email", table.GetCell(1, 1).Text)
	assert.Equal(t, "BYTE_ARRAY", table.GetCell(1, 2).Text)
	assert.Equal(t, "SNAPPY", table.GetCell(1, 3).Text)
	assert.Equal(t, "2.0 KB", table.GetCell(1, 4).Text)
	assert.Equal(t, "abcdefghijklmnopqrst...", table.GetCell(1, 5).Text)
	assert.Equal(t, "zyxwvutsrqponmlkjihg...", table.GetCell(1, 6).Text)
}

func Test_TUIApp_createColumnChunksList_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "broken", http.StatusInternalServerError)
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)

	table := app.createColumnChunksList(0)

	require.NotNil(t, table)
	cell := table.GetCell(1, 0)
	require.NotNil(t, cell)
	assert.Contains(t, cell.Text, "Error loading columns")
}

func Test_TUIApp_showColumnChunksView(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/rowgroups/0":
			_, _ = w.Write([]byte(`{
				"Index": 0,
				"NumRows": 1000,
				"NumColumns": 2,
				"CompressedSize": 1024,
				"UncompressedSize": 2048,
				"CompressionRatio": 2.0
			}`))
		case "/rowgroups/0/columnchunks":
			_, _ = w.Write([]byte(`[
				{
					"Index": 0,
					"Name": "id",
					"PhysicalType": "INT32",
					"Codec": "SNAPPY",
					"NumValues": 1000,
					"NullCount": 1,
					"CompressedSize": 1024
				}
			]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)

	app.showColumnChunksView(0)

	assert.True(t, app.pages.HasPage("columnsview"))
}

func Test_TUIApp_showColumnChunksView_Error(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{
			name: "row group request fails",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "broken", http.StatusInternalServerError)
			},
		},
		{
			name: "column chunks request fails",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/rowgroups/0" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{
						"Index": 0,
						"NumRows": 1000,
						"NumColumns": 2,
						"CompressedSize": 1024,
						"UncompressedSize": 2048,
						"CompressionRatio": 2.0
					}`))
					return
				}
				http.Error(w, "broken", http.StatusInternalServerError)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()

			app := NewTUIApp()
			app.httpClient = newParquetClient(server.URL)

			app.showColumnChunksView(0)

			assert.True(t, app.pages.HasPage("error"))
		})
	}
}
