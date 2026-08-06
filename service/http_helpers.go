package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/format"
	"net/http"
	"strings"
)

// StatusClientClosedRequest is the non-standard status (popularized by nginx)
// used when the client's request context is cancelled before a response is
// produced. It avoids misreporting a cancelled request as 404 Not Found.
const StatusClientClosedRequest = 499

// contextErrorStatus maps a cancelled or timed-out request context to an
// appropriate HTTP status, returning fallback otherwise, so handlers do not
// report an interrupted request as a missing resource.
func contextErrorStatus(err error, fallback int) int {
	switch {
	case errors.Is(err, context.Canceled):
		return StatusClientClosedRequest
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable
	default:
		return fallback
	}
}

// WriteJSON writes a JSON response with pretty printing
func WriteJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		// If encoding fails, try to write error
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

// WriteError writes an error response as JSON
func WriteError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, map[string]string{
		"error": message,
	})
}

// CORSMiddleware adds CORS headers to HTTP responses
func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// LoggingMiddleware logs HTTP requests
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("%s %s\n", r.Method, r.URL.Path)

		// Extract query params for logging
		if len(r.URL.RawQuery) > 0 {
			params := make([]string, 0)
			for key, values := range r.URL.Query() {
				for _, value := range values {
					params = append(params, fmt.Sprintf("%s=%s", key, value))
				}
			}
			if len(params) > 0 {
				fmt.Printf("  Query: %s\n", strings.Join(params, "&"))
			}
		}

		next.ServeHTTP(w, r)
	})
}

// FormatGoCode formats Go source code using go/format
func FormatGoCode(code string) (string, error) {
	formatted, err := format.Source([]byte(code))
	if err != nil {
		return "", err
	}
	return string(formatted), nil
}
