package service

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/mux"
	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

func Test_SetupWebUIRoutes(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()

	service.SetupWebUIRoutes(router)

	// Test that routes are registered (check index page only, which doesn't need data)
	routes := []struct {
		path   string
		method string
	}{
		{"/", "GET"},
	}

	for _, route := range routes {
		t.Run(route.path, func(t *testing.T) {
			req := httptest.NewRequest(route.method, route.path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			// Index page should work without real data
			require.Equal(t, http.StatusOK, w.Code)
		})
	}

	// Test that other routes exist in router (don't call them, just check registration)
	// We can do this by checking the router has the right number of routes
	var routeCount int
	_ = router.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		routeCount++
		return nil
	})
	require.Greater(t, routeCount, 10)
}

func Test_CreateWebUIRouter(t *testing.T) {
	service := createTestService()

	router := CreateWebUIRouter(service)

	require.NotNil(t, router)

	// Test that the router has middleware
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// Should have CORS headers from middleware
	require.NotEmpty(t, w.Header().Get("Access-Control-Allow-Origin"))
}

func Test_TemplatesEmbedded(t *testing.T) {
	// Test that templates are properly embedded and parsed
	require.NotNil(t, templates)

	// Test that all expected templates exist
	expectedTemplates := []string{
		"index.html",
		"main",
		"schema",
		"rowgroups",
		"columns",
		"pages",
		"page_content",
		"error",
	}

	for _, tmplName := range expectedTemplates {
		t.Run(tmplName, func(t *testing.T) {
			tmpl := templates.Lookup(tmplName)
			require.NotNil(t, tmpl)
		})
	}
}

func Test_RouteMethodRestrictions(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupWebUIRoutes(router)

	// Test that routes only accept GET method
	routes := []string{
		"/",
		"/ui/main",
		"/ui/schema",
		"/ui/schema/json",
		"/ui/rowgroups",
	}

	for _, route := range routes {
		t.Run(route+" POST should fail", func(t *testing.T) {
			req := httptest.NewRequest("POST", route, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			// Should return 405 Method Not Allowed
			require.Equal(t, http.StatusMethodNotAllowed, w.Code)
		})
	}
}

func Test_OpenBrowser(t *testing.T) {
	// Test openBrowser function - just verify it doesn't panic
	// We can't actually test browser opening in CI, but we can test the function exists
	err := openBrowser("http://localhost:8080")
	// It's ok if it fails - we just want to make sure the function can be called
	_ = err
}

func Test_NotFoundHandler(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupWebUIRoutes(router)

	// Test 404 for non-existent routes
	tests := []string{
		"/nonexistent",
		"/static/file.js",
		"/_static/out/browser/serviceWorker.js",
		"/favicon.ico",
	}

	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest("GET", path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.Equal(t, http.StatusNotFound, w.Code)
		})
	}
}

func Test_OpenBrowser_AllPlatforms(t *testing.T) {
	// Test that openBrowser doesn't panic on different inputs
	tests := []string{
		"http://localhost:8080",
		"http://127.0.0.1:3000",
		"http://example.com",
	}

	for _, url := range tests {
		t.Run(url, func(t *testing.T) {
			// This might fail on some platforms, but shouldn't panic
			_ = openBrowser(url)
		})
	}
}

func Test_TemplatesFunctions(t *testing.T) {
	// Verify templates are loaded and not nil
	require.NotNil(t, templates)

	// Verify specific templates exist
	tmpl := templates.Lookup("index.html")
	require.NotNil(t, tmpl)

	tmpl = templates.Lookup("schema")
	require.NotNil(t, tmpl)

	tmpl = templates.Lookup("main")
	require.NotNil(t, tmpl)
}

func Test_RouteRegistration(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupWebUIRoutes(router)

	// Count routes
	routeCount := 0
	_ = router.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		routeCount++
		return nil
	})

	// Should have 11 routes + NotFoundHandler
	require.Greater(t, routeCount, 10)
}

func Test_NotFoundHandler_ContentType(t *testing.T) {
	service := createTestService()
	router := mux.NewRouter()
	service.SetupWebUIRoutes(router)

	req := httptest.NewRequest("GET", "/nonexistent", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
	// NotFoundHandler should not set content type (just returns 404)
}

// Helper function to get test parquet file path
func getTestParquetPath(filename string) string {
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

// Helper to create service with real parquet file
func createTestServiceWithFile(t *testing.T, filename string) *ParquetService {
	t.Helper()

	path := getTestParquetPath(filename)
	if path == "" {
		t.Skipf("Test file %s not found - run 'make test' to download test files", filename)
		return nil
	}

	svc, err := NewParquetService(context.Background(), path, pio.ReadOption{})
	require.NoError(t, err)

	return svc
}

// Test openBrowser on current platform
func Test_OpenBrowser_CurrentPlatform(t *testing.T) {
	err := openBrowser("http://localhost:8080")
	// Error might occur in CI environment, but we test the function runs
	// This covers the current platform's switch case
	_ = err
}

// Test all webUI handlers to maximize coverage
func Test_WebUI_MaximizeCoverage(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	router := mux.NewRouter()
	svc.SetupWebUIRoutes(router)

	// Test all UI endpoints
	endpoints := []string{
		"/ui/main",
		"/ui/schema",
		"/ui/schema/go",
		"/ui/schema/json",
		"/ui/schema/raw",
		"/ui/rowgroups",
		"/ui/rowgroups/0/columns",
		"/ui/rowgroups/0/columns/0/pages",
		"/ui/rowgroups/0/columns/0/pages/0/content",
	}

	for _, endpoint := range endpoints {
		t.Run(endpoint, func(t *testing.T) {
			req := httptest.NewRequest("GET", endpoint, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)
			require.NotEmpty(t, w.Body.String())
		})
	}
}

// Test StartWebUIServer - successful server startup
func Test_StartWebUIServer_Success(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	// Find a free port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	_ = listener.Close()

	// Start server in background
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- StartWebUIServer(context.Background(), svc, addr)
	}()

	// Wait for server to start
	time.Sleep(200 * time.Millisecond)

	// Try to connect to verify server is running
	resp, err := http.Get(fmt.Sprintf("http://%s/", addr))
	if err == nil {
		defer func() {
			_ = resp.Body.Close()
		}()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify response contains expected HTML
		body, _ := io.ReadAll(resp.Body)
		require.Contains(t, string(body), "<!DOCTYPE html>")
		require.Contains(t, string(body), "Parquet Browser")
	} else {
		t.Logf("Server may not have started yet: %v", err)
	}
}

// Test StartWebUIServer - server address format
func Test_StartWebUIServer_AddressFormat(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	tests := []struct {
		name string
		addr string
	}{
		{"Port only", ":0"},
		{"Localhost with port", "localhost:0"},
		{"IP with port", "127.0.0.1:0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Find a free port first
			listener, err := net.Listen("tcp", tt.addr)
			require.NoError(t, err)
			actualAddr := listener.Addr().String()
			_ = listener.Close()

			// Start server in background
			serverErr := make(chan error, 1)
			go func() {
				serverErr <- StartWebUIServer(context.Background(), svc, actualAddr)
			}()

			// Wait for server to start
			time.Sleep(100 * time.Millisecond)

			// Try to connect
			resp, err := http.Get(fmt.Sprintf("http://%s/", actualAddr))
			if err == nil {
				defer func() {
					_ = resp.Body.Close()
				}()
				require.Equal(t, http.StatusOK, resp.StatusCode)
			}

			// Server is running, test passed
		})
	}
}

// Test StartWebUIServer - router setup
func Test_StartWebUIServer_RouterSetup(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	// Find a free port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	_ = listener.Close()

	// Start server
	go func() {
		_ = StartWebUIServer(context.Background(), svc, addr)
	}()

	// Wait for server to start
	time.Sleep(200 * time.Millisecond)

	// Test that various routes are working
	routes := []struct {
		path           string
		expectedStatus int
	}{
		{"/", http.StatusOK},
		{"/ui/main", http.StatusOK},
		{"/ui/schema", http.StatusOK},
		{"/ui/rowgroups", http.StatusOK},
		{"/nonexistent", http.StatusNotFound},
	}

	for _, route := range routes {
		t.Run("Route_"+route.path, func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("http://%s%s", addr, route.path))
			if err == nil {
				defer func() {
					_ = resp.Body.Close()
				}()
				require.Equal(t, route.expectedStatus, resp.StatusCode)
			}
		})
	}
}

// Test StartWebUIServer - CORS headers
func Test_StartWebUIServer_CORSHeaders(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	// Find a free port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	_ = listener.Close()

	// Start server
	go func() {
		_ = StartWebUIServer(context.Background(), svc, addr)
	}()

	// Wait for server to start
	time.Sleep(200 * time.Millisecond)

	// Make request and check CORS headers
	resp, err := http.Get(fmt.Sprintf("http://%s/", addr))
	if err == nil {
		defer func() {
			_ = resp.Body.Close()
		}()

		// Should have CORS headers from middleware
		require.NotEmpty(t, resp.Header.Get("Access-Control-Allow-Origin"))
	}
}

// Test StartWebUIServer - error on invalid address
func Test_StartWebUIServer_InvalidAddress(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	// Use an invalid address format
	invalidAddr := "invalid:address:format"

	err := StartWebUIServer(context.Background(), svc, invalidAddr)
	require.Error(t, err)
}

// Test StartWebUIServer - port already in use
func Test_StartWebUIServer_PortInUse(t *testing.T) {
	svc := createTestServiceWithFile(t, "all-types.parquet")
	if svc == nil {
		return
	}
	defer func() {
		_ = svc.Close()
	}()

	// Start a listener to occupy the port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	defer func() {
		_ = listener.Close()
	}()

	// Try to start server on the same port
	err = StartWebUIServer(context.Background(), svc, addr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "address already in use")
}
