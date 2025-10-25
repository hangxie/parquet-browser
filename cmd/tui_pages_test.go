package cmd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hangxie/parquet-browser/client"
	"github.com/hangxie/parquet-browser/model"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/rivo/tview"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_pageTableBuilder_readPageHeadersBatch(t *testing.T) {
	tests := []struct {
		name          string
		setupBuilder  func() *pageTableBuilder
		expectError   bool
		expectedPages int
	}{
		{
			name: "Successfully reads page headers",
			setupBuilder: func() *pageTableBuilder {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`[
						{"Index": 0, "PageType": "DATA_PAGE", "Offset": 1024, "CompressedSize": 512, "UncompressedSize": 1024, "NumValues": 100, "Encoding": "PLAIN"},
						{"Index": 1, "PageType": "DATA_PAGE", "Offset": 2048, "CompressedSize": 512, "UncompressedSize": 1024, "NumValues": 100, "Encoding": "PLAIN"}
					]`))
				}))
				t.Cleanup(server.Close)

				app := NewTUIApp()
				app.httpClient = client.NewParquetClient(server.URL)

				return &pageTableBuilder{
					app:     app,
					rgIndex: 0,
					colIndex: 0,
					table:   tview.NewTable(),
				}
			},
			expectError:   false,
			expectedPages: 2,
		},
		{
			name: "Returns error when HTTP request fails",
			setupBuilder: func() *pageTableBuilder {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(`{"error": "not found"}`))
				}))
				t.Cleanup(server.Close)

				app := NewTUIApp()
				app.httpClient = client.NewParquetClient(server.URL)

				return &pageTableBuilder{
					app:     app,
					rgIndex: 0,
					colIndex: 0,
					table:   tview.NewTable(),
				}
			},
			expectError:   true,
			expectedPages: 0,
		},
		{
			name: "Returns cached error on subsequent calls",
			setupBuilder: func() *pageTableBuilder {
				app := NewTUIApp()

				return &pageTableBuilder{
					app:       app,
					rgIndex:   0,
					colIndex:  0,
					table:     tview.NewTable(),
					loadError: assert.AnError,
				}
			},
			expectError:   true,
			expectedPages: 0,
		},
		{
			name: "Reuses already loaded pages",
			setupBuilder: func() *pageTableBuilder {
				app := NewTUIApp()

				return &pageTableBuilder{
					app:     app,
					rgIndex: 0,
					colIndex: 0,
					table:   tview.NewTable(),
					pages: []model.PageMetadata{
						{Index: 0, PageType: "DATA_PAGE"},
						{Index: 1, PageType: "DATA_PAGE"},
					},
				}
			},
			expectError:   false,
			expectedPages: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := tt.setupBuilder()
			err := builder.readPageHeadersBatch(0, 0)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPages, len(builder.pages))
			}
		})
	}
}

func Test_pageTableBuilder_setupHeader(t *testing.T) {
	app := NewTUIApp()
	table := tview.NewTable()

	builder := &pageTableBuilder{
		app:   app,
		table: table,
	}

	builder.setupHeader()

	// Verify header row was set
	assert.Equal(t, 1, table.GetRowCount()) // Only header row

	// Verify header cells
	cell := table.GetCell(0, 0)
	require.NotNil(t, cell)
	assert.Equal(t, "#", cell.Text)

	cell = table.GetCell(0, 1)
	require.NotNil(t, cell)
	assert.Equal(t, "Page Type", cell.Text)
}

func Test_pageTableBuilder_build(t *testing.T) {
	tests := []struct {
		name         string
		setupBuilder func() *pageTableBuilder
		expectedRows int
		checkError   bool
	}{
		{
			name: "Successfully builds table with pages",
			setupBuilder: func() *pageTableBuilder {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`[
						{
							"Index": 0,
							"PageType": "DATA_PAGE",
							"Offset": 1024,
							"CompressedSize": 512,
							"UncompressedSize": 1024,
							"NumValues": 100,
							"Encoding": "PLAIN",
							"CompressedSizeFormatted": "512 B",
							"UncompressedSizeFormatted": "1.0 KB"
						}
					]`))
				}))
				t.Cleanup(server.Close)

				app := NewTUIApp()
				app.httpClient = client.NewParquetClient(server.URL)

				return &pageTableBuilder{
					app:      app,
					rgIndex:  0,
					colIndex: 0,
					table:    tview.NewTable(),
				}
			},
			expectedRows: 2, // 1 header + 1 data row
			checkError:   false,
		},
		{
			name: "Shows error message when loading fails",
			setupBuilder: func() *pageTableBuilder {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(`{"error": "internal error"}`))
				}))
				t.Cleanup(server.Close)

				app := NewTUIApp()
				app.httpClient = client.NewParquetClient(server.URL)

				return &pageTableBuilder{
					app:      app,
					rgIndex:  0,
					colIndex: 0,
					table:    tview.NewTable(),
				}
			},
			expectedRows: 2, // 1 header + 1 error row
			checkError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := tt.setupBuilder()
			table := builder.build()

			require.NotNil(t, table)
			assert.Equal(t, tt.expectedRows, table.GetRowCount())

			if tt.checkError {
				// Verify error message is displayed
				cell := table.GetCell(1, 0)
				require.NotNil(t, cell)
				assert.Contains(t, cell.Text, "Error reading pages")
			}
		})
	}
}

func Test_pageContentBuilder_setupHeader(t *testing.T) {
	app := NewTUIApp()
	table := tview.NewTable()

	builder := &pageContentBuilder{
		app:   app,
		table: table,
	}

	builder.setupHeader()

	// Verify header row was set
	assert.Equal(t, 1, table.GetRowCount()) // Only header row

	// Verify header cells
	cell := table.GetCell(0, 0)
	require.NotNil(t, cell)
	assert.Equal(t, "#", cell.Text)

	cell = table.GetCell(0, 1)
	require.NotNil(t, cell)
	assert.Equal(t, "Value", cell.Text)
}

func Test_pageContentBuilder_updateHeaderInfo(t *testing.T) {
	app := NewTUIApp()
	headerView := tview.NewTextView().SetDynamicColors(true)

	nullCount := int64(5)
	builder := &pageContentBuilder{
		app:        app,
		headerView: headerView,
		pageInfo: model.PageMetadata{
			PageType:                  "DATA_PAGE",
			Offset:                    1024,
			CompressedSize:            512,
			UncompressedSize:          1024,
			CompressedSizeFormatted:   "512 B",
			UncompressedSizeFormatted: "1.0 KB",
			NumValues:                 100,
			NullCount:                 &nullCount,
			Encoding:                  "PLAIN",
			MinValue:                  "1",
			MaxValue:                  "100",
		},
		allValues: make([]string, 100),
	}

	builder.updateHeaderInfo()

	text := headerView.GetText(false)

	// Verify header contains page info
	assert.Contains(t, text, "DATA_PAGE")
	assert.Contains(t, text, "0x400") // Offset 1024 in hex
	assert.Contains(t, text, "512 B")
	assert.Contains(t, text, "1.0 KB")
	assert.Contains(t, text, "100")    // NumValues
	assert.Contains(t, text, "5")      // NullCount
	assert.Contains(t, text, "PLAIN")  // Encoding
	assert.Contains(t, text, "Min:")
	assert.Contains(t, text, "1")
	assert.Contains(t, text, "Max:")
	assert.Contains(t, text, "100")
}

func Test_pageContentBuilder_build(t *testing.T) {
	tests := []struct {
		name         string
		setupBuilder func() *pageContentBuilder
		expectedRows int
		expectError  bool
	}{
		{
			name: "Successfully builds table with values",
			setupBuilder: func() *pageContentBuilder {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{"values": ["value1", "value2", "value3"], "count": 3}`))
				}))
				t.Cleanup(server.Close)

				app := NewTUIApp()
				app.httpClient = client.NewParquetClient(server.URL)

				ctx, cancel := context.WithCancel(context.Background())

				return &pageContentBuilder{
					app:        app,
					rgIndex:    0,
					colIndex:   0,
					pageIndex:  0,
					table:      tview.NewTable(),
					headerView: tview.NewTextView().SetDynamicColors(true),
					pageInfo: model.PageMetadata{
						PageType:                  "DATA_PAGE",
						Offset:                    1024,
						CompressedSize:            512,
						UncompressedSize:          1024,
						CompressedSizeFormatted:   "512 B",
						UncompressedSizeFormatted: "1.0 KB",
						NumValues:                 3,
						Encoding:                  "PLAIN",
					},
					meta: &parquet.ColumnMetaData{
						Type: parquet.Type_BYTE_ARRAY,
					},
					ctx:    ctx,
					cancel: cancel,
				}
			},
			expectedRows: 4, // 1 header + 3 data rows
			expectError:  false,
		},
		{
			name: "Returns error when loading fails",
			setupBuilder: func() *pageContentBuilder {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(`{"error": "internal error"}`))
				}))
				t.Cleanup(server.Close)

				app := NewTUIApp()
				app.httpClient = client.NewParquetClient(server.URL)

				ctx, cancel := context.WithCancel(context.Background())

				return &pageContentBuilder{
					app:        app,
					rgIndex:    0,
					colIndex:   0,
					pageIndex:  0,
					table:      tview.NewTable(),
					headerView: tview.NewTextView().SetDynamicColors(true),
					pageInfo: model.PageMetadata{
						PageType: "DATA_PAGE",
					},
					meta: &parquet.ColumnMetaData{
						Type: parquet.Type_BYTE_ARRAY,
					},
					ctx:    ctx,
					cancel: cancel,
				}
			},
			expectedRows: 0,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := tt.setupBuilder()
			table, err := builder.build()

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, table)
				assert.Equal(t, tt.expectedRows, table.GetRowCount())

				// Verify header info was updated
				headerText := builder.headerView.GetText(false)
				assert.NotEmpty(t, headerText)
			}
		})
	}
}
