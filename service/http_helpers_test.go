package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_WriteJSON(t *testing.T) {
	tests := []struct {
		name           string
		data           interface{}
		expectedStatus int
		validateBody   func(t *testing.T, body string)
	}{
		{
			name:           "Simple map",
			data:           map[string]string{"key": "value"},
			expectedStatus: http.StatusOK,
			validateBody: func(t *testing.T, body string) {
				var result map[string]string
				err := json.Unmarshal([]byte(body), &result)
				require.NoError(t, err, "Failed to unmarshal response")
				require.Equal(t, "value", result["key"], "Expected key=value")
			},
		},
		{
			name:           "Struct data",
			data:           struct{ Name string }{"test"},
			expectedStatus: http.StatusCreated,
			validateBody: func(t *testing.T, body string) {
				var result struct{ Name string }
				err := json.Unmarshal([]byte(body), &result)
				require.NoError(t, err, "Failed to unmarshal response")
				require.Equal(t, "test", result.Name, "Expected Name=test")
			},
		},
		{
			name:           "Array data",
			data:           []int{1, 2, 3},
			expectedStatus: http.StatusOK,
			validateBody: func(t *testing.T, body string) {
				var result []int
				err := json.Unmarshal([]byte(body), &result)
				require.NoError(t, err, "Failed to unmarshal response")
				require.Len(t, result, 3, "Expected 3 elements")
			},
		},
		{
			name:           "Nil data",
			data:           nil,
			expectedStatus: http.StatusOK,
			validateBody: func(t *testing.T, body string) {
				require.Equal(t, "null\n", body, "Expected 'null\\n'")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			WriteJSON(w, tt.expectedStatus, tt.data)

			require.Equal(t, tt.expectedStatus, w.Code, "Status code should match")

			contentType := w.Header().Get("Content-Type")
			require.Equal(t, "application/json", contentType, "Content-Type should be application/json")

			if tt.validateBody != nil {
				tt.validateBody(t, w.Body.String())
			}
		})
	}
}

func Test_WriteError(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		message        string
		expectedStatus int
	}{
		{
			name:           "Bad request",
			status:         http.StatusBadRequest,
			message:        "Invalid input",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Not found",
			status:         http.StatusNotFound,
			message:        "Resource not found",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "Internal server error",
			status:         http.StatusInternalServerError,
			message:        "Something went wrong",
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			WriteError(w, tt.status, tt.message)

			require.Equal(t, tt.expectedStatus, w.Code, "Status code should match")

			var result map[string]string
			err := json.Unmarshal(w.Body.Bytes(), &result)
			require.NoError(t, err, "Failed to unmarshal error response")

			require.Equal(t, tt.message, result["error"], "Error message should match")
		})
	}
}

func Test_CORSMiddleware(t *testing.T) {
	t.Run("Regular request", func(t *testing.T) {
		handler := CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest("GET", "/test", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		origin := w.Header().Get("Access-Control-Allow-Origin")
		require.Equal(t, "*", origin, "Access-Control-Allow-Origin should be *")

		methods := w.Header().Get("Access-Control-Allow-Methods")
		require.NotEmpty(t, methods, "Access-Control-Allow-Methods should be set")

		headers := w.Header().Get("Access-Control-Allow-Headers")
		require.NotEmpty(t, headers, "Access-Control-Allow-Headers should be set")

		require.Equal(t, http.StatusOK, w.Code, "Status code should match")
	})

	t.Run("OPTIONS request", func(t *testing.T) {
		handler := CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// This should not be called for OPTIONS
			t.Error("Handler should not be called for OPTIONS request")
		}))

		req := httptest.NewRequest("OPTIONS", "/test", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code, "Status code should match")
	})
}

func Test_LoggingMiddleware(t *testing.T) {
	t.Run("Request without query params", func(t *testing.T) {
		called := false
		handler := LoggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest("GET", "/test", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		require.True(t, called, "Handler should have been called")
		require.Equal(t, http.StatusOK, w.Code, "Status code should match")
	})

	t.Run("Request with query params", func(t *testing.T) {
		called := false
		handler := LoggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest("GET", "/test?foo=bar&baz=qux", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		require.True(t, called, "Handler should have been called")
	})

	t.Run("POST request", func(t *testing.T) {
		called := false
		handler := LoggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(http.StatusCreated)
		}))

		req := httptest.NewRequest("POST", "/test", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		require.True(t, called, "Handler should have been called")
		require.Equal(t, http.StatusCreated, w.Code, "Status code should match")
	})
}

func Test_FormatGoCode(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expectErr bool
	}{
		{
			name:      "Valid Go code",
			input:     "package main\nfunc main() {\n}\n",
			expectErr: false,
		},
		{
			name:      "Valid Go code with formatting issues",
			input:     "package main\nfunc main(){}\n",
			expectErr: false,
		},
		{
			name:      "Invalid Go code",
			input:     "this is not valid go code!!!",
			expectErr: true,
		},
		{
			name:      "Empty string",
			input:     "",
			expectErr: false, // go/format can handle empty input
		},
		{
			name:      "Valid struct definition",
			input:     "package main\ntype Foo struct {\nBar string\n}\n",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := FormatGoCode(tt.input)
			if tt.expectErr {
				require.Error(t, err, "FormatGoCode() should return error for invalid code")
			} else {
				require.NoError(t, err, "FormatGoCode() should not return error")
				// Note: empty string input returns empty result, which is valid
			}
		})
	}
}

func Test_FormatGoCode_PreservesSemantics(t *testing.T) {
	// Test that formatting preserves the semantic meaning
	input := `package main

import "fmt"

func main() {
    fmt.Println("Hello, World!")
}
`
	result, err := FormatGoCode(input)
	require.NoError(t, err, "FormatGoCode() should not return error")

	// The formatted code should still be valid
	require.NotEmpty(t, result, "FormatGoCode() should not return empty string")

	// Should contain the essential parts
	require.True(t, contains(result, "package main"), "Formatted code should contain 'package main'")
	require.True(t, contains(result, "func main()"), "Formatted code should contain 'func main()'")
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
