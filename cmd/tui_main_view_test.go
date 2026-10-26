package cmd

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rivo/tview"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_TUIApp_getHeaderHeight(t *testing.T) {
	tests := []struct {
		name     string
		setupApp func() *TUIApp
		expected int
	}{
		{
			name: "Nil headerView returns default",
			setupApp: func() *TUIApp {
				return &TUIApp{
					tviewApp: tview.NewApplication(),
					pages:    tview.NewPages(),
				}
			},
			expected: 3,
		},
		{
			name: "HeaderView with single line",
			setupApp: func() *TUIApp {
				app := &TUIApp{
					tviewApp: tview.NewApplication(),
					pages:    tview.NewPages(),
				}
				app.headerView = tview.NewTextView()
				app.headerView.SetText("Single line")
				return app
			},
			expected: 3, // 1 line + 2 borders
		},
		{
			name: "HeaderView with multiple lines",
			setupApp: func() *TUIApp {
				app := &TUIApp{
					tviewApp: tview.NewApplication(),
					pages:    tview.NewPages(),
				}
				app.headerView = tview.NewTextView()
				app.headerView.SetText("Line 1\nLine 2\nLine 3\nLine 4")
				return app
			},
			expected: 6, // 4 lines + 2 borders
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := tt.setupApp()
			result := app.getHeaderHeight()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func Test_TUIApp_createStatusLine(t *testing.T) {
	app := &TUIApp{
		tviewApp: tview.NewApplication(),
		pages:    tview.NewPages(),
	}

	app.createStatusLine()

	require.NotNil(t, app.statusLine)

	text := app.statusLine.GetText(false)
	assert.Contains(t, text, "ESC=quit")
	assert.Contains(t, text, "s=schema")
}

func Test_TUIApp_createHeaderView(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/info") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"Version": 2,
				"NumRows": 1000,
				"NumRowGroups": 5,
				"NumLeafColumns": 10,
				"TotalCompressedSize": 50000,
				"TotalUncompressedSize": 100000,
				"CompressionRatio": 2.0,
				"CreatedBy": "test"
			}`))
		}
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)
	app.createHeaderView()

	require.NotNil(t, app.headerView)

	text := app.headerView.GetText(false)
	assert.Contains(t, text, "Version:")
	assert.Contains(t, text, "Rows:")
}

func Test_TUIApp_createRowGroupList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/rowgroups") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[
				{
					"Index": 0,
					"NumRows": 100,
					"NumColumns": 5,
					"CompressedSize": 1000,
					"UncompressedSize": 2000,
					"FileOffset": 0
				}
			]`))
		}
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)
	app.createRowGroupList()

	require.NotNil(t, app.rowGroupList)

	assert.Greater(t, app.rowGroupList.GetRowCount(), 0)
}

func Test_TUIApp_showMainView(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/info":
			_, _ = w.Write([]byte(`{
				"Version": 2,
				"NumRows": 1000,
				"NumRowGroups": 1,
				"NumLeafColumns": 2,
				"TotalCompressedSize": 512,
				"TotalUncompressedSize": 1024,
				"CompressionRatio": 2.0,
				"CreatedBy": "test-writer"
			}`))
		case "/rowgroups":
			_, _ = w.Write([]byte(`[
				{
					"Index": 0,
					"NumRows": 1000,
					"NumColumns": 2,
					"CompressedSize": 512,
					"UncompressedSize": 1024,
					"CompressionRatio": 2.0
				}
			]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)
	app.currentFile = "/tmp/example.parquet"

	app.showMainView()

	require.NotNil(t, app.mainLayout)
	require.NotNil(t, app.headerView)
	require.NotNil(t, app.rowGroupList)
	require.NotNil(t, app.statusLine)
	assert.Contains(t, app.headerView.GetText(false), "example.parquet")
	assert.Contains(t, app.headerView.GetText(false), "test-writer")
	assert.Equal(t, 2, app.rowGroupList.GetRowCount())
	assert.Contains(t, app.statusLine.GetText(false), "Enter=see item details")
}

func Test_TUIApp_createHeaderView_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "broken", http.StatusInternalServerError)
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)

	app.createHeaderView()

	require.NotNil(t, app.headerView)
	assert.Contains(t, app.headerView.GetText(false), "Error loading file info")
}

func Test_TUIApp_createRowGroupList_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "broken", http.StatusInternalServerError)
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)

	app.createRowGroupList()

	require.NotNil(t, app.rowGroupList)
	cell := app.rowGroupList.GetCell(1, 0)
	require.NotNil(t, cell)
	assert.Contains(t, cell.Text, "Error loading row groups")
}
