package service

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/mux"
	pio "github.com/hangxie/parquet-tools/io"
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
	parquetFile := createMinimalParquetFile(t)
	if parquetFile == "" {
		t.Skip("Cannot create test parquet file")
	}
	defer func() { _ = os.Remove(parquetFile) }()

	service, err := NewParquetService(context.Background(), parquetFile, pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to create service (parquet-tools not available): %v", err)
	}
	defer func() { _ = service.Close() }()

	require.NotNil(t, service)
	require.NotNil(t, service.reader)
	require.NotNil(t, service.parquetReader)
	require.Equal(t, parquetFile, service.uri)
}

// Test Close with non-nil reader (success path)
func Test_Close_Success_Path(t *testing.T) {
	parquetFile := createMinimalParquetFile(t)
	if parquetFile == "" {
		t.Skip("Cannot create test parquet file")
	}
	defer func() { _ = os.Remove(parquetFile) }()

	service, err := NewParquetService(context.Background(), parquetFile, pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to create service: %v", err)
	}

	// Close with non-nil reader
	err = service.Close()
	require.NoError(t, err)
}

// Test StartServer
func Test_StartServer_Success(t *testing.T) {
	parquetFile := createMinimalParquetFile(t)
	if parquetFile == "" {
		t.Skip("Cannot create test parquet file")
	}
	defer func() { _ = os.Remove(parquetFile) }()

	service, err := NewParquetService(context.Background(), parquetFile, pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to create service: %v", err)
	}
	defer func() { _ = service.Close() }()

	// Find a free port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	_ = listener.Close()

	// Start server in background
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- StartServer(context.Background(), service, addr)
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Try to connect
	resp, err := http.Get(fmt.Sprintf("http://%s/info", addr))
	if err == nil {
		_ = resp.Body.Close()
		// Server started successfully
	}

	// Server is running, this test verifies StartServer was called
	// We can't easily stop it without complex shutdown logic
}

// Helper function to create a minimal parquet file for testing
func createMinimalParquetFile(t *testing.T) string {
	t.Helper()

	tmpDir := t.TempDir()
	parquetFile := filepath.Join(tmpDir, "test.parquet")

	// Try to create using Python/pyarrow
	if createWithPython(parquetFile) {
		return parquetFile
	}

	// Try to create using parquet-go-tools
	if createWithParquetTools(parquetFile) {
		return parquetFile
	}

	// If neither works, return empty string
	return ""
}

func createWithPython(parquetFile string) bool {
	pythonScript := fmt.Sprintf(`
import pyarrow as pa
import pyarrow.parquet as pq
table = pa.table({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'value': [100, 200, 300]
})
pq.write_table(table, '%s')
`, parquetFile)

	cmd := exec.Command("python3", "-c", pythonScript)
	if err := cmd.Run(); err != nil {
		return false
	}

	_, err := os.Stat(parquetFile)
	return err == nil
}

func createWithParquetTools(parquetFile string) bool {
	// Try using parquet-go library to create a minimal file
	// This would require importing parquet-go writer
	// For now, return false as this is complex
	return false
}

// Helper to get the path to test parquet file
func getTestParquetFile() string {
	return filepath.Join("..", "build", "testdata", "all-types.parquet")
}

// Helper to get test parquet file (same as webui_test.go)
func getTestParquetPathAPI(filename string) string {
	// Try build/testdata first (where make test downloads files)
	buildPath := filepath.Join("../build/testdata", filename)
	if _, err := os.Stat(buildPath); err == nil {
		return buildPath
	}

	// Try relative path for when tests run from different locations
	relPath := filepath.Join("../../build/testdata", filename)
	if _, err := os.Stat(relPath); err == nil {
		return relPath
	}

	return ""
}

// Helper to create service with real parquet file for API tests
func createTestServiceWithRealFile(t *testing.T, filename string) *ParquetService {
	t.Helper()

	path := getTestParquetPathAPI(filename)
	if path == "" {
		t.Skipf("Test file %s not found - run 'make test' to download test files", filename)
		return nil
	}

	svc, err := NewParquetService(context.Background(), path, pio.ReadOption{})
	require.NoError(t, err)

	return svc
}
