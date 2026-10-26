package service

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// handleSchemaGo returns schema in Go struct format
func (s *ParquetService) handleSchemaGo(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := s.schemaTree(r.Context())
	if err != nil {
		WriteError(w, contextErrorStatus(err, http.StatusInternalServerError), fmt.Sprintf("Failed to generate schema: %v", err))
		return
	}

	schemaText, err := schemaRoot.GoStruct(false)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to format Go schema: %v", err))
		return
	}

	formatted, err := FormatGoCode(schemaText)
	if err != nil {
		// Fall back to the unformatted source.
		formatted = schemaText
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(formatted))
}

// handleSchemaJSON returns schema in JSON format
func (s *ParquetService) handleSchemaJSON(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := s.schemaTree(r.Context())
	if err != nil {
		WriteError(w, contextErrorStatus(err, http.StatusInternalServerError), fmt.Sprintf("Failed to generate schema: %v", err))
		return
	}

	schemaText := schemaRoot.JSONSchema()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(schemaText))
}

// handleSchemaRaw returns the raw schema tree structure as JSON
func (s *ParquetService) handleSchemaRaw(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := s.schemaTree(r.Context())
	if err != nil {
		WriteError(w, contextErrorStatus(err, http.StatusInternalServerError), fmt.Sprintf("Failed to generate schema: %v", err))
		return
	}

	rawJSON, err := json.Marshal(*schemaRoot)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to marshal raw schema: %v", err))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(rawJSON)
}

// handleSchemaCSV returns schema in CSV format
func (s *ParquetService) handleSchemaCSV(w http.ResponseWriter, r *http.Request) {
	schemaRoot, err := s.schemaTree(r.Context())
	if err != nil {
		WriteError(w, contextErrorStatus(err, http.StatusInternalServerError), fmt.Sprintf("Failed to generate schema: %v", err))
		return
	}

	schemaText, err := schemaRoot.CSVSchema()
	if err != nil {
		WriteError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to format CSV schema: %v", err))
		return
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(schemaText))
}
