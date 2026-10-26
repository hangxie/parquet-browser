package service

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// VersionGetter is a function type for getting version string
type VersionGetter func() string

// versionGetter holds the function to get the version
var versionGetter VersionGetter

// SetVersionGetter sets the function used to retrieve the version
func SetVersionGetter(fn VersionGetter) {
	versionGetter = fn
}

// getVersion returns the current version string
func getVersion() string {
	if versionGetter != nil {
		return versionGetter()
	}
	return ""
}

//go:embed templates/*.html
var templatesFS embed.FS

var templates *template.Template

func init() {
	var err error
	templates, err = template.ParseFS(templatesFS, "templates/*.html")
	if err != nil {
		panic(fmt.Sprintf("Failed to parse templates: %v", err))
	}
}

// SetupWebUIRoutes configures all web UI routes
func (s *ParquetService) SetupWebUIRoutes(r *mux.Router) {
	// Main UI routes
	r.HandleFunc("/", s.handleIndexPage).Methods("GET")
	r.HandleFunc("/ui/main", s.handleMainView).Methods("GET")
	r.HandleFunc("/ui/schema", s.handleSchemaView).Methods("GET")
	r.HandleFunc("/ui/schema/go", s.handleSchemaGoView).Methods("GET")
	r.HandleFunc("/ui/schema/json", s.handleSchemaJSONView).Methods("GET")
	r.HandleFunc("/ui/schema/csv", s.handleSchemaCSVView).Methods("GET")
	r.HandleFunc("/ui/schema/raw", s.handleSchemaRawView).Methods("GET")
	r.HandleFunc("/ui/rowgroups", s.handleRowGroupsView).Methods("GET")
	r.HandleFunc("/ui/rowgroups/{rgIndex}/columns", s.handleColumnsView).Methods("GET")
	r.HandleFunc("/ui/rowgroups/{rgIndex}/columns/{colIndex}/pages", s.handlePagesView).Methods("GET")
	r.HandleFunc("/ui/rowgroups/{rgIndex}/columns/{colIndex}/pages/{pageIndex}/content", s.handlePageContentView).Methods("GET")

	// Catch-all for static files and other resources (favicon, service worker, etc.)
	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Silently return 404 for common browser requests
		w.WriteHeader(http.StatusNotFound)
	})
}

// CreateWebUIRouter creates a router configured for the web UI
func CreateWebUIRouter(s *ParquetService) *mux.Router {
	r := mux.NewRouter()
	s.SetupWebUIRoutes(r)
	r.Use(CORSMiddleware)
	r.Use(LoggingMiddleware)
	return r
}

// openBrowser tries to open the URL in the default browser
func openBrowser(url string) error {
	if testing.Testing() {
		// do not launch browser under unit test
		return nil
	}

	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "linux":
		cmd = exec.Command("xdg-open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		return fmt.Errorf("unsupported platform")
	}

	return cmd.Start()
}

// StartWebUIServer starts the web UI server
func StartWebUIServer(ctx context.Context, service *ParquetService, addr string) error {
	r := CreateWebUIRouter(service)

	// Construct the full URL
	// If addr already contains host (e.g., "127.0.0.1:8080"), use it directly
	// Otherwise prepend localhost (e.g., ":8080" -> "localhost:8080")
	url := ""
	if strings.HasPrefix(addr, ":") {
		url = fmt.Sprintf("http://localhost%s", addr)
	} else {
		url = fmt.Sprintf("http://%s", addr)
	}

	fmt.Printf("Starting Parquet Browser Web UI on %s\n", addr)
	fmt.Printf("Opening browser to: %s\n", url)
	fmt.Println()

	// Open browser in a goroutine with a small delay to ensure server is ready.
	// Skip it if the context is already cancelled (e.g. shutdown before the
	// delay elapses) so we do not launch a browser at a URL that never came up.
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
		if err := openBrowser(url); err != nil {
			fmt.Printf("Note: Could not automatically open browser: %v\n", err)
			fmt.Printf("Please open your browser and navigate to: %s\n", url)
		}
	}()

	return serveWithShutdown(ctx, addr, r)
}
