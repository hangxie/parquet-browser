package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-browser/model"
)

// serveLayoutFixture routes one GET request through both the API and Web UI
// routes of a service backed by fixture.
func serveLayoutFixture(t *testing.T, svc *ParquetService, url string) *httptest.ResponseRecorder {
	t.Helper()

	router := mux.NewRouter()
	svc.SetupRoutes(router)
	svc.SetupWebUIRoutes(router)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, url, nil))
	return w
}

func layoutFixtureService(t *testing.T, filename string) *ParquetService {
	t.Helper()

	svc := createTestServiceWithFile(t, filename)
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func Test_NewParquetService_CorruptFooter(t *testing.T) {
	tests := []struct {
		file    string
		wantErr string
	}{
		{file: "bad-footer-magic.parquet", wantErr: `invalid footer magic "XXXX"`},
		{file: "negative-num-rows.parquet", wantErr: "num_rows -1 is negative"},
	}

	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			path := getTestParquetPath(tt.file)
			if path == "" {
				t.Skipf("Test file %s not found - run 'make test' to download test files", tt.file)
			}

			svc, err := NewParquetService(context.Background(), path, pio.ReadOption{})
			require.Error(t, err)
			require.Nil(t, svc)
			require.Contains(t, err.Error(), "failed to open parquet file")
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func Test_ZeroColumnFile_API(t *testing.T) {
	svc := layoutFixtureService(t, "zero-column.parquet")

	t.Run("info", func(t *testing.T) {
		w := serveLayoutFixture(t, svc, "/info")
		require.Equal(t, http.StatusOK, w.Code)

		var info model.FileInfo
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &info))
		require.Equal(t, int64(3), info.NumRows)
		require.Equal(t, 1, info.NumRowGroups)
		require.Zero(t, info.NumLeafColumns)
		require.Zero(t, info.CompressionRatio)
	})

	t.Run("rowgroups", func(t *testing.T) {
		w := serveLayoutFixture(t, svc, "/rowgroups")
		require.Equal(t, http.StatusOK, w.Code)

		var rowGroups []model.RowGroupInfo
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &rowGroups))
		require.Len(t, rowGroups, 1)
		require.Equal(t, int64(3), rowGroups[0].NumRows)
		require.Zero(t, rowGroups[0].NumColumns)
	})

	t.Run("columnchunks", func(t *testing.T) {
		w := serveLayoutFixture(t, svc, "/rowgroups/0/columnchunks")
		require.Equal(t, http.StatusOK, w.Code)
		// An empty JSON array, not null, so clients can range over it.
		require.JSONEq(t, `[]`, w.Body.String())
	})

	for _, url := range []string{
		"/rowgroups/0/columnchunks/0",
		"/rowgroups/0/columnchunks/0/pages",
		"/rowgroups/0/columnchunks/0/pages/0",
		"/rowgroups/0/columnchunks/0/pages/0/content",
	} {
		t.Run(url, func(t *testing.T) {
			w := serveLayoutFixture(t, svc, url)
			require.Equal(t, http.StatusNotFound, w.Code)
			require.Contains(t, w.Body.String(), "invalid column index")
		})
	}

	schemaTests := []struct {
		url  string
		want string
	}{
		{url: "/schema/go", want: "type Parquet_go_root struct {\n}"},
		{url: "/schema/json", want: `"Tag":"name=parquet_go_root, inname=Parquet_go_root"`},
		{url: "/schema/raw", want: `"name":"parquet_go_root","num_children":0`},
	}
	for _, tt := range schemaTests {
		t.Run(tt.url, func(t *testing.T) {
			w := serveLayoutFixture(t, svc, tt.url)
			require.Equal(t, http.StatusOK, w.Code)
			require.Contains(t, w.Body.String(), tt.want)
		})
	}

	t.Run("/schema/csv", func(t *testing.T) {
		w := serveLayoutFixture(t, svc, "/schema/csv")
		require.Equal(t, http.StatusOK, w.Code)
		require.Empty(t, w.Body.String())
	})
}

func Test_ZeroColumnFile_WebUI(t *testing.T) {
	svc := layoutFixtureService(t, "zero-column.parquet")

	for _, url := range []string{"/ui/main", "/ui/rowgroups", "/ui/rowgroups/0/columns"} {
		t.Run(url, func(t *testing.T) {
			w := serveLayoutFixture(t, svc, url)
			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))
		})
	}

	t.Run("/ui/schema/go", func(t *testing.T) {
		w := serveLayoutFixture(t, svc, "/ui/schema/go")
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Body.String(), "Parquet_go_root struct")
	})

	t.Run("pages of missing column", func(t *testing.T) {
		w := serveLayoutFixture(t, svc, "/ui/rowgroups/0/columns/0/pages")
		require.Equal(t, http.StatusNotFound, w.Code)
	})
}

// Legacy (Athena/parquet-mr style) and logical-type-only LIST/MAP layouts are
// normalized by the reader, so column names use List.Element and every schema
// format renders them as LIST/MAP instead of failing or falling back to groups.
func Test_ListMapLayoutFile_API(t *testing.T) {
	svc := layoutFixtureService(t, "list-map-layout.parquet")

	t.Run("columnchunks", func(t *testing.T) {
		w := serveLayoutFixture(t, svc, "/rowgroups/0/columnchunks")
		require.Equal(t, http.StatusOK, w.Code)

		var columns []model.ColumnChunkInfo
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &columns))

		names := make([]string, len(columns))
		for i, col := range columns {
			names[i] = col.Name
		}
		require.Equal(t, []string{
			"Athena.List.Element",
			"Athena_logical.List.Element",
			"Logical_list.List.Element",
			"Logical_map.Key_value.Key",
			"Logical_map.Key_value.Value",
			"Legacy.List.Element",
			"Entries_map.Entries.Key",
			"Entries_map.Entries.Value",
			"Entries_logical.Entries.Key",
			"Entries_logical.Entries.Value",
		}, names)
		require.Equal(t, "STRING", columns[0].LogicalType)
		require.Equal(t, "UTF8", columns[0].ConvertedType)
	})

	t.Run("legacy list page content", func(t *testing.T) {
		w := serveLayoutFixture(t, svc, "/rowgroups/0/columnchunks/0/pages/0/content")
		require.Equal(t, http.StatusOK, w.Code)
		require.JSONEq(t, `{"count": 3, "values": ["a", "", "b"]}`, w.Body.String())
	})

	t.Run("/schema/go", func(t *testing.T) {
		w := serveLayoutFixture(t, svc, "/schema/go")
		require.Equal(t, http.StatusOK, w.Code)

		body := w.Body.String()
		for _, want := range []string{
			"Athena         []*string",
			"Athena_logical []*string",
			"Logical_list   []int32",
			"Logical_map    map[string]int32",
			"Legacy         [][]int32",
			"name=logical_map, type=MAP",
		} {
			require.Contains(t, body, want)
		}
	})

	t.Run("/schema/json", func(t *testing.T) {
		w := serveLayoutFixture(t, svc, "/schema/json")
		require.Equal(t, http.StatusOK, w.Code)

		body := w.Body.String()
		for _, want := range []string{
			"name=athena_logical, inname=Athena_logical, type=LIST",
			"name=logical_list, inname=Logical_list, type=LIST",
			"name=logical_map, inname=Logical_map, type=MAP",
		} {
			require.Contains(t, body, want)
		}
		require.NotContains(t, body, "inname=Bag")
	})

	t.Run("/schema/csv", func(t *testing.T) {
		w := serveLayoutFixture(t, svc, "/schema/csv")
		require.Equal(t, http.StatusInternalServerError, w.Code)
		require.Contains(t, w.Body.String(), "CSV supports flat schema only")
	})
}

func Test_ListMapLayoutFile_WebUI(t *testing.T) {
	svc := layoutFixtureService(t, "list-map-layout.parquet")

	t.Run("columns", func(t *testing.T) {
		w := serveLayoutFixture(t, svc, "/ui/rowgroups/0/columns")
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Body.String(), "Athena.List.Element")
		require.NotContains(t, w.Body.String(), "Bag.Array_element")
	})

	t.Run("schema go", func(t *testing.T) {
		w := serveLayoutFixture(t, svc, "/ui/schema/go")
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Body.String(), "Legacy")
	})
}

// A two-level array without a LIST annotation is not a list: it stays a
// repeated group in the schema views.
func Test_NestedArrayFile_Schema(t *testing.T) {
	svc := layoutFixtureService(t, "nested-array.parquet")

	w := serveLayoutFixture(t, svc, "/schema/go")
	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.String()
	require.Contains(t, body, "Array []struct {")
	require.Contains(t, body, "name=array, type=STRUCT, repetitiontype=REPEATED")
	require.NotContains(t, body, "type=LIST")
}
