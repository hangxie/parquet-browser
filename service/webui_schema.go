package service

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// handleSchemaView serves the schema viewer page
func (s *ParquetService) handleSchemaView(w http.ResponseWriter, r *http.Request) {
	err := renderPartial(w, r, "schema", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleSchemaGoView returns schema in Go format for HTMX
func (s *ParquetService) handleSchemaGoView(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := s.schemaTree(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to generate schema: %v", err), contextErrorStatus(err, http.StatusInternalServerError))
		return
	}

	schemaText, err := schemaRoot.GoStruct(false)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to format Go schema: %v", err), http.StatusInternalServerError)
		return
	}

	formatted, err := FormatGoCode(schemaText)
	if err != nil {
		formatted = schemaText
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(formatted))
}

// handleSchemaJSONView returns schema in JSON format for HTMX
func (s *ParquetService) handleSchemaJSONView(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := s.schemaTree(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to generate schema: %v", err), contextErrorStatus(err, http.StatusInternalServerError))
		return
	}

	schemaText := schemaRoot.JSONSchema()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write([]byte(schemaText))
}

// handleSchemaCSVView returns schema in CSV format for HTMX
func (s *ParquetService) handleSchemaCSVView(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := s.schemaTree(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to generate schema: %v", err), contextErrorStatus(err, http.StatusInternalServerError))
		return
	}

	schemaText, err := schemaRoot.CSVSchema()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to format CSV schema: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(schemaText))
}

// handleSchemaRawView returns raw schema for HTMX (compact JSON)
func (s *ParquetService) handleSchemaRawView(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := s.schemaTree(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to generate schema: %v", err), contextErrorStatus(err, http.StatusInternalServerError))
		return
	}

	// Convert to compact JSON
	rawJSON, err := json.Marshal(*schemaRoot)
	if err != nil {
		// Fallback to Go's %+v format if JSON marshaling fails
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintf(w, "%+v", *schemaRoot)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(rawJSON)
}
