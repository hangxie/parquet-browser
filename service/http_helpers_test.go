package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_contextErrorStatus(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		fallback int
		want     int
	}{
		{"canceled", context.Canceled, http.StatusNotFound, StatusClientClosedRequest},
		{"deadline", context.DeadlineExceeded, http.StatusNotFound, http.StatusServiceUnavailable},
		{"wrapped canceled", fmt.Errorf("read failed: %w", context.Canceled), http.StatusNotFound, StatusClientClosedRequest},
		{"other error uses fallback", errors.New("boom"), http.StatusNotFound, http.StatusNotFound},
		{"other error uses 500 fallback", errors.New("boom"), http.StatusInternalServerError, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, contextErrorStatus(tt.err, tt.fallback))
		})
	}
}

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
				require.NoError(t, err)
				require.Equal(t, "value", result["key"])
			},
		},
		{
			name:           "Struct data",
			data:           struct{ Name string }{"test"},
			expectedStatus: http.StatusCreated,
			validateBody: func(t *testing.T, body string) {
				var result struct{ Name string }
				err := json.Unmarshal([]byte(body), &result)
				require.NoError(t, err)
				require.Equal(t, "test", result.Name)
			},
		},
		{
			name:           "Array data",
			data:           []int{1, 2, 3},
			expectedStatus: http.StatusOK,
			validateBody: func(t *testing.T, body string) {
				var result []int
				err := json.Unmarshal([]byte(body), &result)
				require.NoError(t, err)
				require.Len(t, result, 3)
			},
		},
		{
			name:           "Nil data",
			data:           nil,
			expectedStatus: http.StatusOK,
			validateBody: func(t *testing.T, body string) {
				require.Equal(t, "null\n", body)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			WriteJSON(w, tt.expectedStatus, tt.data)

			require.Equal(t, tt.expectedStatus, w.Code)

			contentType := w.Header().Get("Content-Type")
			require.Equal(t, "application/json", contentType)

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

			require.Equal(t, tt.expectedStatus, w.Code)

			var result map[string]string
			err := json.Unmarshal(w.Body.Bytes(), &result)
			require.NoError(t, err)

			require.Equal(t, tt.message, result["error"])
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
		require.Equal(t, "*", origin)

		methods := w.Header().Get("Access-Control-Allow-Methods")
		require.NotEmpty(t, methods)

		headers := w.Header().Get("Access-Control-Allow-Headers")
		require.NotEmpty(t, headers)

		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("OPTIONS request", func(t *testing.T) {
		handler := CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// This should not be called for OPTIONS
			t.Error("Handler should not be called for OPTIONS request")
		}))

		req := httptest.NewRequest("OPTIONS", "/test", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
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

		require.True(t, called)
		require.Equal(t, http.StatusOK, w.Code)
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

		require.True(t, called)
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

		require.True(t, called)
		require.Equal(t, http.StatusCreated, w.Code)
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
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Note: empty string input returns empty result, which is valid
			}
		})
	}
}

func Test_FormatGoCode_PreservesSemantics(t *testing.T) {
	input := `package main

import "fmt"

func main() {
    fmt.Println("Hello, World!")
}
`
	result, err := FormatGoCode(input)
	require.NoError(t, err)

	require.NotEmpty(t, result)

	require.Contains(t, result, "package main")
	require.Contains(t, result, "func main()")
}

func Test_WriteJSON_EncodingError(t *testing.T) {
	w := httptest.NewRecorder()

	// Channels cannot be JSON encoded
	invalidData := make(chan int)

	// This should try to encode and fail, triggering error path
	WriteJSON(w, http.StatusOK, invalidData)

	// Function handles error internally, we just verify no panic
	// The error handling writes an HTTP error response
}

func Test_WriteJSON_WithIndentation(t *testing.T) {
	w := httptest.NewRecorder()

	data := map[string]interface{}{
		"key1": "value1",
		"key2": 42,
		"nested": map[string]string{
			"inner": "value",
		},
	}

	WriteJSON(w, http.StatusOK, data)

	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.String()
	require.Contains(t, body, "\n")

	var result map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &result)
	require.NoError(t, err)
}

func Test_FormatGoCode_ValidInputs(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Simple package",
			input: "package main\n\nfunc main() {}\n",
		},
		{
			name:  "Needs formatting",
			input: "package main\nfunc main(){}\n",
		},
		{
			name:  "With imports",
			input: "package main\n\nimport \"fmt\"\n\nfunc main() { fmt.Println(\"hi\") }\n",
		},
		{
			name:  "Struct",
			input: "package main\n\ntype MyStruct struct {\n\tField string\n}\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := FormatGoCode(tt.input)
			require.NoError(t, err)
			require.NotEmpty(t, result)

			require.Contains(t, result, "package main")
		})
	}
}

func Test_FormatGoCode_InvalidSyntax(t *testing.T) {
	invalidCode := "package main\nthis is not valid go syntax!!!"

	_, err := FormatGoCode(invalidCode)
	require.Error(t, err)
}

func Test_FormatGoCode_EmptyInput(t *testing.T) {
	result, err := FormatGoCode("")
	require.NoError(t, err)
	require.Empty(t, result)
}

func Test_CORSMiddleware_Headers(t *testing.T) {
	called := false
	handler := CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	require.True(t, called)

	require.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
	require.NotEmpty(t, w.Header().Get("Access-Control-Allow-Methods"))
	require.NotEmpty(t, w.Header().Get("Access-Control-Allow-Headers"))
}

func Test_CORSMiddleware_OPTIONS(t *testing.T) {
	handler := CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Handler should not be called for OPTIONS request")
	}))

	req := httptest.NewRequest("OPTIONS", "/test", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	// CORS headers should still be set
	require.NotEmpty(t, w.Header().Get("Access-Control-Allow-Origin"))
}

func Test_LoggingMiddleware_Logging(t *testing.T) {
	called := false
	handler := LoggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	tests := []struct {
		name string
		path string
	}{
		{"Simple path", "/test"},
		{"With query params", "/test?foo=bar"},
		{"Multiple query params", "/test?foo=bar&baz=qux"},
		{"Empty query value", "/test?foo="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			req := httptest.NewRequest("GET", tt.path, nil)
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			require.True(t, called)
			require.Equal(t, http.StatusOK, w.Code)
		})
	}
}

func Test_Write_Functions_Together(t *testing.T) {
	w1 := httptest.NewRecorder()
	WriteJSON(w1, http.StatusOK, map[string]string{"key": "value"})
	require.Equal(t, http.StatusOK, w1.Code)

	w2 := httptest.NewRecorder()
	WriteError(w2, http.StatusBadRequest, "test error")
	require.Equal(t, http.StatusBadRequest, w2.Code)

	var errResp map[string]string
	err := json.Unmarshal(w2.Body.Bytes(), &errResp)
	require.NoError(t, err)

	require.Equal(t, "test error", errResp["error"])
}

func Test_JSON_Roundtrip(t *testing.T) {
	original := map[string]interface{}{
		"string":  "value",
		"number":  42,
		"boolean": true,
		"array":   []int{1, 2, 3},
		"nested": map[string]string{
			"key": "value",
		},
	}

	w := httptest.NewRecorder()
	WriteJSON(w, http.StatusOK, original)

	var decoded map[string]interface{}
	err := json.NewDecoder(bytes.NewReader(w.Body.Bytes())).Decode(&decoded)
	require.NoError(t, err)

	require.Equal(t, "value", decoded["string"])

	// Note: JSON numbers are decoded as float64
	require.Equal(t, float64(42), decoded["number"].(float64))

	require.Equal(t, true, decoded["boolean"])
}
