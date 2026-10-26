package cmd

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewTUIApp(t *testing.T) {
	app := NewTUIApp()

	require.NotNil(t, app)
	assert.NotNil(t, app.tviewApp)
	assert.NotNil(t, app.pages)
}

func Test_TUIApp_readPageHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/pages") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[
				{
					"Index": 0,
					"Offset": 1024,
					"PageType": "DATA_PAGE",
					"CompressedSize": 512,
					"UncompressedSize": 1024,
					"NumValues": 100,
					"Encoding": "PLAIN"
				}
			]`))
		}
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)

	pages, err := app.readPageHeaders(0, 0)
	require.NoError(t, err)
	require.Len(t, pages, 1)
	assert.Equal(t, 0, pages[0].Index)
	assert.Equal(t, "DATA_PAGE", pages[0].PageType)
	assert.Equal(t, int32(100), pages[0].NumValues)
}

func Test_TUIApp_readPageHeaders_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not found"}`))
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)

	_, err := app.readPageHeaders(0, 0)
	require.Error(t, err)
}

func Test_TUIApp_readPageContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/content") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"values": ["value1", "value2", "NULL", "value3"], "count": 4}`))
		}
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)

	values, err := app.readPageContent(0, 0, 0)
	require.NoError(t, err)
	require.Len(t, values, 4)
	assert.Equal(t, "value1", values[0])
	assert.Equal(t, "value2", values[1])
	assert.Equal(t, "NULL", values[2])
	assert.Equal(t, "value3", values[3])
}

func Test_TUIApp_showSchema(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/schema/raw") {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("message schema {\n  required int32 id;\n}"))
		}
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)

	screen := tcell.NewSimulationScreen("UTF-8")
	err := screen.Init()
	require.NoError(t, err)
	defer screen.Fini()

	app.tviewApp.SetScreen(screen)

	// Show schema should not panic
	assert.NotPanics(t, func() {
		app.showSchema()
	})
}
