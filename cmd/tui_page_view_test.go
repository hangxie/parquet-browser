package cmd

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rivo/tview"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-browser/model"
)

func Test_getColumnChunkInfoHeight(t *testing.T) {
	tests := []struct {
		name     string
		infoView *tview.TextView
		expected int
	}{
		{
			name:     "Nil infoView returns default height",
			infoView: nil,
			expected: 3,
		},
		{
			name: "Single line text",
			infoView: func() *tview.TextView {
				tv := tview.NewTextView()
				tv.SetText("Single line")
				return tv
			}(),
			expected: 3, // 1 line + 2 borders
		},
		{
			name: "Multi-line text",
			infoView: func() *tview.TextView {
				tv := tview.NewTextView()
				tv.SetText("Line 1\nLine 2\nLine 3")
				return tv
			}(),
			expected: 5, // 3 lines + 2 borders
		},
		{
			name: "Empty text",
			infoView: func() *tview.TextView {
				tv := tview.NewTextView()
				tv.SetText("")
				return tv
			}(),
			expected: 3, // 1 line (empty) + 2 borders
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getColumnChunkInfoHeight(tt.infoView)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func Test_TUIApp_showPageView(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/rowgroups/0/columnchunks/0":
			_, _ = w.Write([]byte(`{
				"Name": "id",
				"PathInSchema": ["id"],
				"PhysicalType": "INT32",
				"Codec": "SNAPPY",
				"NumValues": 3,
				"CompressedSize": 128,
				"UncompressedSize": 256
			}`))
		case "/rowgroups/0/columnchunks/0/pages":
			_, _ = w.Write([]byte(`[
				{
					"Index": 0,
					"Offset": 1024,
					"PageType": "DATA_PAGE",
					"CompressedSize": 64,
					"UncompressedSize": 128,
					"CompressedSizeFormatted": "64 B",
					"UncompressedSizeFormatted": "128 B",
					"NumValues": 3,
					"Encoding": "PLAIN",
					"MinValue": "1",
					"MaxValue": "3"
				}
			]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)
	stop := startTUIAppForTest(t, app)
	defer stop()

	queueTUIUpdate(t, app, func() {
		app.showPageView(0, 0)
	})

	primitive := waitForTUIPage(t, app, "pageview")
	require.IsType(t, &tview.Flex{}, primitive)
	var hasLoading bool
	queueTUIUpdate(t, app, func() {
		hasLoading = app.pages.HasPage("page-loading")
	})
	assert.False(t, hasLoading)
}

func Test_TUIApp_showPageView_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "broken", http.StatusInternalServerError)
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)
	stop := startTUIAppForTest(t, app)
	defer stop()

	queueTUIUpdate(t, app, func() {
		app.showPageView(0, 0)
	})

	primitive := waitForTUIPage(t, app, "page-error")
	require.IsType(t, &tview.Modal{}, primitive)
	var hasLoading bool
	queueTUIUpdate(t, app, func() {
		hasLoading = app.pages.HasPage("page-loading")
	})
	assert.False(t, hasLoading)
}

func Test_TUIApp_showPageView_PagesError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/rowgroups/0/columnchunks/0" {
			_, _ = w.Write([]byte(`{
				"Name": "id",
				"PathInSchema": ["id"],
				"PhysicalType": "INT32",
				"Codec": "SNAPPY",
				"NumValues": 3,
				"CompressedSize": 128,
				"UncompressedSize": 256
			}`))
			return
		}
		http.Error(w, "broken", http.StatusInternalServerError)
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)
	stop := startTUIAppForTest(t, app)
	defer stop()

	queueTUIUpdate(t, app, func() {
		app.showPageView(0, 0)
	})

	primitive := waitForTUIPage(t, app, "page-error")
	require.IsType(t, &tview.Modal{}, primitive)
	var hasLoading bool
	queueTUIUpdate(t, app, func() {
		hasLoading = app.pages.HasPage("page-loading")
	})
	assert.False(t, hasLoading)
}

func Test_TUIApp_buildColumnChunkInfoViewFromHTTP(t *testing.T) {
	app := NewTUIApp()

	nullCount := int64(10)
	colInfo := model.ColumnChunkInfo{
		Name:             "test_column",
		PhysicalType:     "INT32",
		LogicalType:      "INT(32, true)",
		Codec:            "SNAPPY",
		NumValues:        1000,
		NullCount:        &nullCount,
		CompressedSize:   5000,
		UncompressedSize: 10000,
		MinValue:         "1",
		MaxValue:         "1000",
	}

	infoView := app.buildColumnChunkInfoViewFromHTTP(colInfo, 5)

	require.NotNil(t, infoView)

	text := infoView.GetText(false)
	assert.Contains(t, text, "test_column")
	assert.Contains(t, text, "INT32")
	assert.Contains(t, text, "Pages:")
	assert.Contains(t, text, "5")
}
