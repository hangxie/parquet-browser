package service

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a test service
func createTestService() *ParquetService {
	// Note: This would normally require a real parquet file
	// For unit tests, we're testing the HTTP layer without file I/O
	return &ParquetService{
		reader:        nil, // Would be populated with mock data in real tests
		parquetReader: nil,
		uri:           "test://file.parquet",
	}
}

func Test_ParquetService_Close(t *testing.T) {
	service := &ParquetService{
		reader: nil,
	}

	err := service.Close()
	require.NoError(t, err)
}

func Test_CreateRouter(t *testing.T) {
	service := createTestService()

	t.Run("With logging middleware", func(t *testing.T) {
		router := CreateRouter(service, false)
		require.NotNil(t, router)
	})

	t.Run("Without logging middleware (quiet mode)", func(t *testing.T) {
		router := CreateRouter(service, true)
		require.NotNil(t, router)
	})
}

func Test_NewParquetService_InvalidFile(t *testing.T) {
	// Test with non-existent file
	_, err := NewParquetService(context.Background(), "nonexistent.parquet", pio.ReadOption{})
	require.Error(t, err)

	// Check error message
	require.Contains(t, err.Error(), "failed to open parquet file")
}

// Test route matching without executing handlers
func Test_RouteMatching(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupRoutes(router)

	tests := []struct {
		name   string
		method string
		path   string
		match  bool
	}{
		{"Info endpoint", "GET", "/info", true},
		{"Row groups", "GET", "/rowgroups", true},
		{"Row group by index", "GET", "/rowgroups/0", true},
		{"Column chunks", "GET", "/rowgroups/0/columnchunks", true},
		{"Column chunk by index", "GET", "/rowgroups/0/columnchunks/0", true},
		{"Pages", "GET", "/rowgroups/0/columnchunks/0/pages", true},
		{"Page by index", "GET", "/rowgroups/0/columnchunks/0/pages/0", true},
		{"Page content", "GET", "/rowgroups/0/columnchunks/0/pages/0/content", true},
		{"Invalid endpoint", "GET", "/notfound", false},
		{"Wrong method", "POST", "/info", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			var routeMatch mux.RouteMatch
			matched := router.Match(req, &routeMatch)

			require.Equal(t, tt.match, matched)
		})
	}
}

// Test NewParquetService with various invalid URIs
func Test_NewParquetService_InvalidURIs(t *testing.T) {
	tests := []struct {
		name string
		uri  string
	}{
		{"Nonexistent file", "/tmp/does_not_exist_" + fmt.Sprintf("%d", 12345) + ".parquet"},
		{"Empty string", ""},
		{"Directory", "/tmp"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, err := NewParquetService(context.Background(), tt.uri, pio.ReadOption{})
			require.Error(t, err)
			if svc != nil {
				_ = svc.Close()
			}
			require.Contains(t, err.Error(), "failed to open parquet file")
		})
	}
}

// Test Close with nil reader
func Test_Close_NilReader(t *testing.T) {
	svc := &ParquetService{reader: nil}
	err := svc.Close()
	require.NoError(t, err)
}

// Test CreateRouter quiet and verbose modes
func Test_CreateRouter_Modes(t *testing.T) {
	svc := &ParquetService{}

	// Test quiet mode
	router := CreateRouter(svc, true)
	require.NotNil(t, router)

	// Test verbose mode
	router = CreateRouter(svc, false)
	require.NotNil(t, router)
}

// Test SetupRoutes registers all routes
func Test_SetupRoutes_AllRoutes(t *testing.T) {
	svc := &ParquetService{}
	router := mux.NewRouter()
	svc.SetupRoutes(router)

	// Test that routes are registered
	routes := []struct {
		method string
		path   string
	}{
		{"GET", "/info"},
		{"GET", "/rowgroups"},
		{"GET", "/rowgroups/0"},
		{"GET", "/rowgroups/0/columnchunks"},
		{"GET", "/rowgroups/0/columnchunks/0"},
		{"GET", "/rowgroups/0/columnchunks/0/pages"},
		{"GET", "/rowgroups/0/columnchunks/0/pages/0"},
		{"GET", "/rowgroups/0/columnchunks/0/pages/0/content"},
		{"GET", "/schema/go"},
		{"GET", "/schema/json"},
		{"GET", "/schema/raw"},
		{"GET", "/schema/csv"},
	}

	for _, route := range routes {
		t.Run(route.path, func(t *testing.T) {
			req := httptest.NewRequest(route.method, route.path, nil)
			var match mux.RouteMatch
			matched := router.Match(req, &match)
			require.True(t, matched)
		})
	}
}

// Test Close when reader is nil (else branch)
func Test_Close_NilReaderBranch(t *testing.T) {
	// Create service with explicitly nil reader
	service := &ParquetService{
		reader:        nil,
		parquetReader: nil,
		uri:           "",
	}

	err := service.Close()
	require.NoError(t, err)
}

// Test NewParquetService success path with a real parquet file
func Test_NewParquetService_Success_Path(t *testing.T) {
	// Try to create a minimal test parquet file
	parquetFile := fixturePath(t, "csv-good.parquet")

	service, err := NewParquetService(context.Background(), parquetFile, pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	require.NotNil(t, service)
	require.NotNil(t, service.reader)
	require.NotNil(t, service.parquetReader)
	require.Equal(t, parquetFile, service.uri)
}

// Test Close with non-nil reader (success path)
func Test_Close_Success_Path(t *testing.T) {
	parquetFile := fixturePath(t, "csv-good.parquet")

	service, err := NewParquetService(context.Background(), parquetFile, pio.ReadOption{})
	require.NoError(t, err)

	// Close with non-nil reader
	err = service.Close()
	require.NoError(t, err)
}

// Test StartServer
func Test_StartServer_Success(t *testing.T) {
	parquetFile := fixturePath(t, "csv-good.parquet")

	service, err := NewParquetService(context.Background(), parquetFile, pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	// Find a free port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	_ = listener.Close()

	// Start server in background
	ctx, cancel := context.WithCancel(context.Background())
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- StartServer(ctx, service, addr)
	}()

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/info", addr))
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 20*time.Millisecond)

	cancel()
	require.NoError(t, <-serverErr)
}

// serveLayoutFixture routes one GET request through both the API and Web UI
// routes of a service backed by fixture.
func serveFixtureRequest(t *testing.T, svc *ParquetService, url string) *httptest.ResponseRecorder {
	t.Helper()

	router := mux.NewRouter()
	svc.SetupRoutes(router)
	svc.SetupWebUIRoutes(router)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, url, nil))
	return w
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
			path := fixturePath(t, tt.file)

			svc, err := NewParquetService(context.Background(), path, pio.ReadOption{})
			require.Error(t, err)
			require.Nil(t, svc)
			require.Contains(t, err.Error(), "failed to open parquet file")
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// fixturePath returns a fixture downloaded by `make test`, failing the test when it is missing.
func fixturePath(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join("..", "build", "testdata", name)
	_, err := os.Stat(path)
	require.NoError(t, err, "missing fixture; run `make test` to download it")
	return path
}

// newFixtureService opens a service on a fixture and closes it when the test ends.
func newFixtureService(t *testing.T, name string) *ParquetService {
	t.Helper()
	svc, err := NewParquetService(context.Background(), fixturePath(t, name), pio.ReadOption{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

// Test_ConcurrentReaderAccess overlaps requests against the single shared
// reader, which would race without serialization. Half the workers cancel
// mid-flight to exercise cross-request cancellation. Run under -race.
func Test_ConcurrentReaderAccess(t *testing.T) {
	svc, err := NewParquetService(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = svc.Close() }()

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	// Endpoints spanning schema, page-metadata, and page-content reads — the paths
	// touching the shared reader's IO. Keep those returning 200 for this fixture
	// and record single-threaded bodies as a baseline for corruption under load.
	candidates := []string{
		"/schema/json",
		"/rowgroups/0/columnchunks/0/pages",
		"/rowgroups/0/columnchunks/0/pages/0",
		"/rowgroups/0/columnchunks/0/pages/0/content",
	}
	baseline := map[string]string{}
	var endpoints []string
	for _, ep := range candidates {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, ep, nil))
		if w.Code == http.StatusOK {
			baseline[ep] = w.Body.String()
			endpoints = append(endpoints, ep)
		}
	}
	require.GreaterOrEqual(t, len(endpoints), 2, "need at least two reader-backed endpoints to exercise concurrency")

	const workers = 32
	const iterations = 8

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cancelling := i%2 == 0
			for j := 0; j < iterations; j++ {
				ep := endpoints[(i+j)%len(endpoints)]

				ctx, cancel := context.WithCancel(context.Background())
				req := httptest.NewRequest(http.MethodGet, ep, nil).WithContext(ctx)
				if cancelling {
					// Race the cancellation against the in-flight read.
					go cancel()
				}

				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)

				// Requests that never cancel must always observe correct,
				// uncorrupted data regardless of what concurrent requests do to
				// the shared reader.
				if !cancelling {
					assert.Equal(t, http.StatusOK, w.Code, ep)
					assert.Equal(t, baseline[ep], w.Body.String(), ep)
				}
				cancel()
			}
		}(i)
	}
	wg.Wait()
}

// Test_CancelledRequest_NotReportedAsNotFound verifies that with an already
// cancelled request context, reader-backed handlers report a client-cancelled
// status rather than a 404 or a generic 500 for schema.
func Test_CancelledRequest_NotReportedAsNotFound(t *testing.T) {
	svc, err := NewParquetService(context.Background(), fixturePath(t, "all-types.parquet"), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = svc.Close() }()

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	endpoints := []string{
		"/schema/json",
		"/rowgroups/0/columnchunks/0/pages",
		"/rowgroups/0/columnchunks/0/pages/0",
		"/rowgroups/0/columnchunks/0/pages/0/content",
	}
	for _, ep := range endpoints {
		t.Run(ep, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // already cancelled before the handler runs
			req := httptest.NewRequest(http.MethodGet, ep, nil).WithContext(ctx)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			require.Equal(t, StatusClientClosedRequest, w.Code,
				"cancelled request should report client-closed, not %d", w.Code)
			require.NotEqual(t, http.StatusNotFound, w.Code)
		})
	}
}
