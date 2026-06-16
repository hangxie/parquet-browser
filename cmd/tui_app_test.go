package cmd

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/rivo/tview"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-browser/model"
)

func Test_parsePhysicalType(t *testing.T) {
	tests := []struct {
		name     string
		typeStr  string
		expected parquet.Type
	}{
		{
			name:     "BOOLEAN type",
			typeStr:  "BOOLEAN",
			expected: parquet.Type_BOOLEAN,
		},
		{
			name:     "INT32 type",
			typeStr:  "INT32",
			expected: parquet.Type_INT32,
		},
		{
			name:     "INT64 type",
			typeStr:  "INT64",
			expected: parquet.Type_INT64,
		},
		{
			name:     "INT96 type",
			typeStr:  "INT96",
			expected: parquet.Type_INT96,
		},
		{
			name:     "FLOAT type",
			typeStr:  "FLOAT",
			expected: parquet.Type_FLOAT,
		},
		{
			name:     "DOUBLE type",
			typeStr:  "DOUBLE",
			expected: parquet.Type_DOUBLE,
		},
		{
			name:     "BYTE_ARRAY type",
			typeStr:  "BYTE_ARRAY",
			expected: parquet.Type_BYTE_ARRAY,
		},
		{
			name:     "FIXED_LEN_BYTE_ARRAY type",
			typeStr:  "FIXED_LEN_BYTE_ARRAY",
			expected: parquet.Type_FIXED_LEN_BYTE_ARRAY,
		},
		{
			name:     "Unknown type defaults to BYTE_ARRAY",
			typeStr:  "UNKNOWN",
			expected: parquet.Type_BYTE_ARRAY,
		},
		{
			name:     "Empty string defaults to BYTE_ARRAY",
			typeStr:  "",
			expected: parquet.Type_BYTE_ARRAY,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parsePhysicalType(tt.typeStr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func Test_parseCompressionCodec(t *testing.T) {
	tests := []struct {
		name     string
		codecStr string
		expected parquet.CompressionCodec
	}{
		{
			name:     "UNCOMPRESSED codec",
			codecStr: "UNCOMPRESSED",
			expected: parquet.CompressionCodec_UNCOMPRESSED,
		},
		{
			name:     "SNAPPY codec",
			codecStr: "SNAPPY",
			expected: parquet.CompressionCodec_SNAPPY,
		},
		{
			name:     "GZIP codec",
			codecStr: "GZIP",
			expected: parquet.CompressionCodec_GZIP,
		},
		{
			name:     "LZO codec",
			codecStr: "LZO",
			expected: parquet.CompressionCodec_LZO,
		},
		{
			name:     "BROTLI codec",
			codecStr: "BROTLI",
			expected: parquet.CompressionCodec_BROTLI,
		},
		{
			name:     "LZ4 codec",
			codecStr: "LZ4",
			expected: parquet.CompressionCodec_LZ4,
		},
		{
			name:     "ZSTD codec",
			codecStr: "ZSTD",
			expected: parquet.CompressionCodec_ZSTD,
		},
		{
			name:     "LZ4_RAW codec",
			codecStr: "LZ4_RAW",
			expected: parquet.CompressionCodec_LZ4_RAW,
		},
		{
			name:     "Unknown codec defaults to UNCOMPRESSED",
			codecStr: "UNKNOWN",
			expected: parquet.CompressionCodec_UNCOMPRESSED,
		},
		{
			name:     "Empty string defaults to UNCOMPRESSED",
			codecStr: "",
			expected: parquet.CompressionCodec_UNCOMPRESSED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseCompressionCodec(tt.codecStr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

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

func Test_NewTUIApp(t *testing.T) {
	app := NewTUIApp()

	require.NotNil(t, app)
	assert.NotNil(t, app.tviewApp)
	assert.NotNil(t, app.pages)
	assert.Equal(t, -1, app.lastRGIndex)
	assert.Equal(t, int64(0), app.lastRGPosition)
}

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

func Test_TUIApp_readPageHeaders(t *testing.T) {
	// Create a mock HTTP server
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
	// Create a mock HTTP server that returns error
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
	// Create a mock HTTP server
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

func Test_TUIApp_createStatusLine(t *testing.T) {
	app := &TUIApp{
		tviewApp: tview.NewApplication(),
		pages:    tview.NewPages(),
	}

	app.createStatusLine()

	require.NotNil(t, app.statusLine)

	// Verify status line text contains key bindings
	text := app.statusLine.GetText(false)
	assert.Contains(t, text, "ESC=quit")
	assert.Contains(t, text, "s=schema")
}

func Test_TUIApp_createHeaderView(t *testing.T) {
	// Create a mock HTTP server
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

	// Verify header contains file info
	text := app.headerView.GetText(false)
	assert.Contains(t, text, "Version:")
	assert.Contains(t, text, "Rows:")
}

func Test_TUIApp_createRowGroupList(t *testing.T) {
	// Create a mock HTTP server
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

	// Verify table has been populated (at least header row)
	assert.Greater(t, app.rowGroupList.GetRowCount(), 0)
}

func Test_TUIApp_showSchema(t *testing.T) {
	// Create a mock HTTP server
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

	// Create a screen for the app
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

func Test_TUIApp_showPageContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rowgroups/0/columnchunks/0/pages/0/content" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values": ["1", "2", "3"], "count": 3}`))
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)
	stop := startTUIAppForTest(t, app)
	defer stop()

	pages := []model.PageMetadata{
		{
			Index:                     0,
			Offset:                    1024,
			PageType:                  "DATA_PAGE",
			CompressedSize:            64,
			UncompressedSize:          128,
			CompressedSizeFormatted:   "64 B",
			UncompressedSizeFormatted: "128 B",
			NumValues:                 3,
			Encoding:                  "PLAIN",
		},
	}
	meta := &parquet.ColumnMetaData{Type: parquet.Type_INT32}

	queueTUIUpdate(t, app, func() {
		app.showPageContent(0, 0, 0, pages, meta)
	})

	primitive := waitForTUIPage(t, app, "page-content")
	require.IsType(t, &tview.Flex{}, primitive)
	var (
		hasLoading bool
		focus      tview.Primitive
	)
	queueTUIUpdate(t, app, func() {
		hasLoading = app.pages.HasPage("page-content-loading")
		focus = app.tviewApp.GetFocus()
	})
	assert.False(t, hasLoading)
	assert.IsType(t, &tview.Table{}, focus)
}

func Test_TUIApp_showPageContent_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "broken", http.StatusInternalServerError)
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)
	stop := startTUIAppForTest(t, app)
	defer stop()

	pages := []model.PageMetadata{
		{
			Index:                     0,
			Offset:                    1024,
			PageType:                  "DATA_PAGE",
			CompressedSize:            64,
			UncompressedSize:          128,
			CompressedSizeFormatted:   "64 B",
			UncompressedSizeFormatted: "128 B",
			NumValues:                 3,
			Encoding:                  "PLAIN",
		},
	}
	meta := &parquet.ColumnMetaData{Type: parquet.Type_INT32}

	queueTUIUpdate(t, app, func() {
		app.showPageContent(0, 0, 0, pages, meta)
	})

	primitive := waitForTUIPage(t, app, "page-content")
	errorView, ok := primitive.(*tview.TextView)
	require.True(t, ok)
	var (
		text       string
		hasLoading bool
	)
	queueTUIUpdate(t, app, func() {
		text = errorView.GetText(false)
		hasLoading = app.pages.HasPage("page-content-loading")
	})
	assert.Contains(t, text, "Error reading page content")
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

	// Verify info view contains column information
	text := infoView.GetText(false)
	assert.Contains(t, text, "test_column")
	assert.Contains(t, text, "INT32")
	assert.Contains(t, text, "Pages:")
	assert.Contains(t, text, "5")
}
