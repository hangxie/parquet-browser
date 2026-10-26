package service

import (
	"context"
	"fmt"

	"github.com/gorilla/mux"
	"github.com/hangxie/parquet-go/v3/reader"
	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"

	"github.com/hangxie/parquet-browser/model"
)

// ParquetService manages the Parquet file and provides HTTP endpoints
type ParquetService struct {
	reader        *model.ParquetReader
	parquetReader *reader.ParquetReader // Raw reader for schema generation
	uri           string
}

// NewParquetService creates a new service instance
func NewParquetService(ctx context.Context, uri string, readOpts pio.ReadOption) (*ParquetService, error) {
	parquetReader, err := pio.NewParquetFileReader(ctx, uri, readOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	return &ParquetService{
		reader:        model.NewParquetReader(parquetReader),
		parquetReader: parquetReader,
		uri:           uri,
	}, nil
}

// Close releases the parquet file handles once no requests are in flight.
func (s *ParquetService) Close() error {
	if s.reader != nil {
		return s.reader.Close()
	}
	return nil
}

// schemaTree builds the schema tree under the reader lock; every schema handler goes through it.
func (s *ParquetService) schemaTree(ctx context.Context) (*pschema.SchemaNode, error) {
	var root *pschema.SchemaNode
	err := s.reader.WithLock(ctx, func() error {
		var e error
		root, e = pschema.NewSchemaTree(ctx, s.parquetReader, pschema.SchemaOption{FailOnInt96: false})
		return e
	})
	return root, err
}

// CreateRouter builds the API router; quiet drops request logging for embedded servers.
func CreateRouter(s *ParquetService, quiet bool) *mux.Router {
	r := mux.NewRouter()
	s.SetupRoutes(r)
	r.Use(CORSMiddleware)
	if !quiet {
		r.Use(LoggingMiddleware)
	}
	return r
}

// SetupRoutes configures all HTTP routes
func (s *ParquetService) SetupRoutes(r *mux.Router) {
	r.HandleFunc("/schema/go", s.handleSchemaGo).Methods("GET")
	r.HandleFunc("/schema/json", s.handleSchemaJSON).Methods("GET")
	r.HandleFunc("/schema/raw", s.handleSchemaRaw).Methods("GET")
	r.HandleFunc("/schema/csv", s.handleSchemaCSV).Methods("GET")

	r.HandleFunc("/info", s.handleFileInfo).Methods("GET")

	r.HandleFunc("/rowgroups", s.handleRowGroups).Methods("GET")
	r.HandleFunc("/rowgroups/{rgIndex}", s.handleRowGroupInfo).Methods("GET")

	r.HandleFunc("/rowgroups/{rgIndex}/columnchunks", s.handleColumnChunks).Methods("GET")
	r.HandleFunc("/rowgroups/{rgIndex}/columnchunks/{colIndex}", s.handleColumnChunkInfo).Methods("GET")

	r.HandleFunc("/rowgroups/{rgIndex}/columnchunks/{colIndex}/pages", s.handlePages).Methods("GET")
	r.HandleFunc("/rowgroups/{rgIndex}/columnchunks/{colIndex}/pages/{pageIndex}", s.handlePageInfo).Methods("GET")
	r.HandleFunc("/rowgroups/{rgIndex}/columnchunks/{colIndex}/pages/{pageIndex}/content", s.handlePageContent).Methods("GET")
}

// StartServer serves the API with request logging until it fails or ctx is cancelled.
func StartServer(ctx context.Context, service *ParquetService, addr string) error {
	r := CreateRouter(service, false) // log requests

	fmt.Printf("Starting Parquet Browser API server on %s\n", addr)
	fmt.Printf("Available endpoints:\n")
	fmt.Printf("  GET /info                                                    - File metadata\n")
	fmt.Printf("  GET /schema/go                                               - Schema (Go format)\n")
	fmt.Printf("  GET /schema/json?pretty=true                                 - Schema (JSON format)\n")
	fmt.Printf("  GET /schema/raw?pretty=true                                  - Schema (Raw format)\n")
	fmt.Printf("  GET /schema/csv                                              - Schema (CSV format)\n")
	fmt.Printf("  GET /rowgroups                                               - All row groups\n")
	fmt.Printf("  GET /rowgroups/{rgIndex}                                     - Row group info\n")
	fmt.Printf("  GET /rowgroups/{rgIndex}/columnchunks                        - All column chunks\n")
	fmt.Printf("  GET /rowgroups/{rgIndex}/columnchunks/{colIndex}             - Column chunk info\n")
	fmt.Printf("  GET /rowgroups/{rgIndex}/columnchunks/{colIndex}/pages       - All pages\n")
	fmt.Printf("  GET /rowgroups/{rgIndex}/columnchunks/{colIndex}/pages/{pageIndex} - Page info\n")
	fmt.Printf("  GET /rowgroups/{rgIndex}/columnchunks/{colIndex}/pages/{pageIndex}/content - Page content\n")
	fmt.Println()

	return serveWithShutdown(ctx, addr, r)
}
