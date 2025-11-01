package cmd

import (
	"context"
	"net/http"
	"testing"
	"time"

	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

func Test_startHTTPServer_InvalidFile(t *testing.T) {
	ctx := context.Background()
	resultChan := make(chan serverResult, 1)

	// Start server with invalid file
	go startHTTPServer(ctx, "nonexistent.parquet", pio.ReadOption{}, resultChan)

	// Wait for result
	select {
	case result := <-resultChan:
		// Should get an error
		require.Error(t, result.err, "Should return error for non-existent file")
		require.Empty(t, result.serverURL, "Server URL should be empty on error")
		require.Nil(t, result.server, "Server instance should be nil on error")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for server result")
	}
}

func Test_startHTTPServer_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	resultChan := make(chan serverResult, 1)

	// Cancel immediately
	cancel()

	// Start server with cancelled context
	go startHTTPServer(ctx, "test.parquet", pio.ReadOption{}, resultChan)

	// Should return quickly without sending to channel or send error
	select {
	case result := <-resultChan:
		// If we get a result, it should be an error
		require.Error(t, result.err, "Should return error when context is cancelled")
	case <-time.After(500 * time.Millisecond):
		// No result is also acceptable for cancelled context
		// The function returns early without sending to channel
	}
}

func Test_startHTTPServer_RealFile(t *testing.T) {
	ctx := context.Background()
	resultChan := make(chan serverResult, 1)

	// Use the test file from parquet-tools repository
	testFileURL := "https://github.com/hangxie/parquet-tools/raw/refs/heads/main/testdata/good.parquet"

	// Start server with real file
	go startHTTPServer(ctx, testFileURL, pio.ReadOption{}, resultChan)

	// Wait for result
	select {
	case result := <-resultChan:
		// Should succeed
		require.NoError(t, result.err, "Should successfully start server with valid file")
		require.NotEmpty(t, result.serverURL, "Server URL should not be empty")
		require.NotNil(t, result.server, "Server instance should not be nil")

		// Verify server is actually running by making a request
		resp, err := http.Get(result.serverURL + "/info")
		require.NoError(t, err, "Should be able to connect to server")
		require.Equal(t, http.StatusOK, resp.StatusCode, "Server should return 200 OK")
		_ = resp.Body.Close()

		// Clean up - shutdown server
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		err = result.server.Shutdown(shutdownCtx)
		require.NoError(t, err, "Server should shutdown cleanly")

	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for server to start")
	}
}

func Test_serverResult_Struct(t *testing.T) {
	// Test that serverResult struct has expected fields
	result := serverResult{
		serverURL: "http://localhost:8080",
		server:    nil,
		err:       nil,
	}

	require.Equal(t, "http://localhost:8080", result.serverURL)
	require.Nil(t, result.server)
	require.Nil(t, result.err)
}

func Test_TUICmd_FieldAccess(t *testing.T) {
	// Test that TUICmd fields can be accessed and set
	cmd := TUICmd{}

	// Set URI
	cmd.URI = "test.parquet"
	require.Equal(t, "test.parquet", cmd.URI)

	// Set ReadOption fields
	cmd.ReadOption.Anonymous = true
	require.True(t, cmd.ReadOption.Anonymous)

	cmd.ReadOption.ObjectVersion = "v1.0"
	require.Equal(t, "v1.0", cmd.ReadOption.ObjectVersion)

	cmd.ReadOption.HTTPIgnoreTLSError = true
	require.True(t, cmd.ReadOption.HTTPIgnoreTLSError)
}
