package cmd

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/hangxie/parquet-browser/client"
	"github.com/hangxie/parquet-browser/model"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/rivo/tview"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		name      string
		codecStr  string
		expected  parquet.CompressionCodec
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
		name        string
		setupApp    func() *TUIApp
		expected    int
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
			w.Write([]byte(`[
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
	app.httpClient = client.NewParquetClient(server.URL)

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
		w.Write([]byte(`{"error":"not found"}`))
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = client.NewParquetClient(server.URL)

	_, err := app.readPageHeaders(0, 0)
	require.Error(t, err)
}

func Test_TUIApp_readPageContent(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/content") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"values": ["value1", "value2", "NULL", "value3"], "count": 4}`))
		}
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = client.NewParquetClient(server.URL)

	// Mock metadata
	meta := &parquet.ColumnMetaData{
		Type: parquet.Type_BYTE_ARRAY,
	}

	values, err := app.readPageContent(0, 0, 0, nil, meta)
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
			w.Write([]byte(`{
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
	app.httpClient = client.NewParquetClient(server.URL)
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
			w.Write([]byte(`[
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
	app.httpClient = client.NewParquetClient(server.URL)
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
			w.Write([]byte("message schema {\n  required int32 id;\n}"))
		}
	}))
	defer server.Close()

	app := NewTUIApp()
	app.httpClient = client.NewParquetClient(server.URL)

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
