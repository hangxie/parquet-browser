package cmd

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestSchemaViewer(t *testing.T) (*schemaViewer, *httptest.Server) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schema/json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"name":"root","fields":["id"]}`))
		case "/schema/raw":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"raw":true}`))
		case "/schema/go":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("type Row struct {\n\tID int32\n}"))
		case "/schema/csv":
			w.Header().Set("Content-Type", "text/csv")
			_, _ = w.Write([]byte("name,type\nid,INT32\n"))
		default:
			http.NotFound(w, r)
		}
	}))

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)

	return &schemaViewer{
		app:           app,
		schemaFormats: []string{"json", "raw", "go", "csv"},
		currentFormat: 0,
		isPretty:      true,
		textView:      tview.NewTextView().SetDynamicColors(true),
		titleBar:      tview.NewTextView().SetDynamicColors(true),
		statusBar:     tview.NewTextView().SetDynamicColors(true),
	}, server
}

func Test_schemaViewer_updateDisplayAndSwitchToFormat(t *testing.T) {
	viewer, server := newTestSchemaViewer(t)
	defer server.Close()

	viewer.updateDisplay()

	assert.Contains(t, viewer.textView.GetText(false), "\n  \"fields\":")
	assert.Contains(t, viewer.titleBar.GetText(false), "Schema [JSON - Pretty]")

	viewer.statusBar.SetText("stale status")
	viewer.switchToFormat("csv")

	require.Equal(t, 3, viewer.currentFormat)
	assert.Equal(t, "", viewer.statusBar.GetText(false))
	assert.Equal(t, "name,type\nid,INT32\n", viewer.textView.GetText(false))
	assert.Contains(t, viewer.titleBar.GetText(false), "Schema [CSV]")

	viewer.switchToFormat("missing")
	assert.Equal(t, 3, viewer.currentFormat)
}

func Test_schemaViewer_togglePretty(t *testing.T) {
	viewer, server := newTestSchemaViewer(t)
	defer server.Close()

	viewer.isPretty = false
	viewer.statusBar.SetText("stale status")
	viewer.togglePretty()

	assert.True(t, viewer.isPretty)
	assert.Equal(t, "", viewer.statusBar.GetText(false))
	assert.Contains(t, viewer.textView.GetText(false), "\n  \"fields\":")

	viewer.togglePretty()
	assert.False(t, viewer.isPretty)
	assert.Equal(t, `{"name":"root","fields":["id"]}`, viewer.textView.GetText(false))

	viewer.switchToFormat("go")
	viewer.togglePretty()
	assert.False(t, viewer.isPretty, "pretty mode should not toggle for Go schema")
}

func Test_schemaViewer_handleInput(t *testing.T) {
	viewer, server := newTestSchemaViewer(t)
	defer server.Close()

	viewer.app.pages.AddPage("schema", tview.NewBox(), true, true)

	result := viewer.handleInput(tcell.NewEventKey(tcell.KeyRune, 'g', tcell.ModNone))
	require.Nil(t, result)
	assert.Equal(t, 2, viewer.currentFormat)
	assert.Contains(t, viewer.textView.GetText(false), "type Row struct")

	result = viewer.handleInput(tcell.NewEventKey(tcell.KeyRune, 'j', tcell.ModNone))
	require.Nil(t, result)
	assert.Equal(t, 0, viewer.currentFormat)

	wasPretty := viewer.isPretty
	result = viewer.handleInput(tcell.NewEventKey(tcell.KeyRune, 'p', tcell.ModNone))
	require.Nil(t, result)
	assert.NotEqual(t, wasPretty, viewer.isPretty)

	unknown := tcell.NewEventKey(tcell.KeyRune, 'x', tcell.ModNone)
	assert.Same(t, unknown, viewer.handleInput(unknown))

	enter := tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone)
	assert.Same(t, enter, viewer.handleInput(enter))

	result = viewer.handleInput(tcell.NewEventKey(tcell.KeyEscape, 0, tcell.ModNone))
	require.Nil(t, result)
	assert.False(t, viewer.app.pages.HasPage("schema"))
}

func Test_schemaViewer_formatJSON(t *testing.T) {
	viewer := &schemaViewer{}

	assert.Equal(t, "{\n  \"id\": 1\n}", viewer.formatJSON(`{"id":1}`))
	assert.Equal(t, "not json", viewer.formatJSON("not json"))
}

func Test_schemaViewer_updateTitle(t *testing.T) {
	tests := []struct {
		name          string
		schemaFormats []string
		currentFormat int
		isPretty      bool
		expected      string
	}{
		{
			name:          "go schema",
			schemaFormats: []string{"go"},
			expected:      "Schema [Go Struct]",
		},
		{
			name:          "csv schema",
			schemaFormats: []string{"csv"},
			expected:      "Schema [CSV]",
		},
		{
			name:          "raw compact schema",
			schemaFormats: []string{"raw"},
			expected:      "Schema [RAW - Compact]",
		},
		{
			name:          "unknown schema",
			schemaFormats: []string{"yaml"},
			expected:      "Schema [YAML]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viewer := &schemaViewer{
				schemaFormats: tt.schemaFormats,
				currentFormat: tt.currentFormat,
				isPretty:      tt.isPretty,
				titleBar:      tview.NewTextView().SetDynamicColors(true),
			}

			viewer.updateTitle()

			assert.Contains(t, viewer.titleBar.GetText(false), tt.expected)
		})
	}
}
