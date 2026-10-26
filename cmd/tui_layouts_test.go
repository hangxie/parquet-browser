package cmd

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-browser/service"
)

// layoutFixturePath returns the path of a parquet-tools fixture downloaded by
// `make test`, skipping the test when it is missing.
func layoutFixturePath(t *testing.T, name string) string {
	t.Helper()

	path := filepath.Join("..", "build", "testdata", name)
	if _, err := os.Stat(path); err != nil {
		t.Skipf("Test file %s not found - run 'make test' to download test files", name)
	}
	return path
}

// newTUIAppForFixture wires a TUIApp to a real service backed by fixture, the
// same way the TUI talks to its embedded server.
func newTUIAppForFixture(t *testing.T, name string) *TUIApp {
	t.Helper()

	svc, err := service.NewParquetService(context.Background(), layoutFixturePath(t, name), pio.ReadOption{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	server := httptest.NewServer(service.CreateRouter(svc, true))
	t.Cleanup(server.Close)

	app := NewTUIApp()
	app.httpClient = newParquetClient(server.URL)
	return app
}

func Test_startHTTPServer_CorruptFooter(t *testing.T) {
	tests := []struct {
		file    string
		wantErr string
	}{
		{file: "bad-footer-magic.parquet", wantErr: `invalid footer magic "XXXX"`},
		{file: "negative-num-rows.parquet", wantErr: "num_rows -1 is negative"},
	}

	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			path := layoutFixturePath(t, tt.file)
			resultChan := make(chan serverResult, 1)
			go startHTTPServer(context.Background(), path, pio.ReadOption{}, resultChan)

			select {
			case result := <-resultChan:
				require.Error(t, result.err)
				require.Contains(t, result.err.Error(), tt.wantErr)
				require.Nil(t, result.server)
				require.Nil(t, result.svc)
			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for server result")
			}
		})
	}
}

func Test_ServeCmd_Run_CorruptFooter(t *testing.T) {
	cmd := ServeCmd{URI: layoutFixturePath(t, "bad-footer-magic.parquet"), Addr: "127.0.0.1:0"}

	err := cmd.Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create service")
	require.Contains(t, err.Error(), "invalid footer magic")
}

func Test_TUIApp_ZeroColumnFile(t *testing.T) {
	app := newTUIAppForFixture(t, "zero-column.parquet")

	app.createRowGroupList()
	require.Equal(t, 2, app.rowGroupList.GetRowCount())
	assert.Equal(t, "3", app.rowGroupList.GetCell(1, 1).Text)

	// Only the header row: no column chunks to select.
	table := app.createColumnChunksList(0)
	assert.Equal(t, 1, table.GetRowCount())
	assert.Equal(t, "Name", table.GetCell(0, 1).Text)

	app.showColumnChunksView(0)
	assert.True(t, app.pages.HasPage("columnsview"))
	assert.False(t, app.pages.HasPage("error"))
}

func Test_TUIApp_ListMapLayoutFile(t *testing.T) {
	app := newTUIAppForFixture(t, "list-map-layout.parquet")

	table := app.createColumnChunksList(0)
	require.Equal(t, 11, table.GetRowCount())
	assert.Equal(t, "Athena.List.Element", table.GetCell(1, 1).Text)
	assert.Equal(t, "Legacy.List.Element", table.GetCell(6, 1).Text)

	values, err := app.readPageContent(0, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "", "b"}, values)
}
