package cmd

import (
	"net"
	"net/http"
	"testing"
	"time"

	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

func Test_ServeCmd_Run_InvalidFile(t *testing.T) {
	cmd := ServeCmd{
		URI:  "nonexistent.parquet",
		Addr: ":0",
	}

	err := cmd.Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create service")
}

func Test_ServeCmd_FieldAccess(t *testing.T) {
	cmd := ServeCmd{}
	cmd.URI = "file.parquet"
	cmd.Addr = ":9090"
	cmd.ReadOption = pio.ReadOption{}
	cmd.Anonymous = true
	cmd.ObjectVersion = "v1.0"
	cmd.HTTPIgnoreTLSError = true

	require.Equal(t, "file.parquet", cmd.URI)
	require.Equal(t, ":9090", cmd.Addr)
	require.True(t, cmd.Anonymous)
	require.Equal(t, "v1.0", cmd.ObjectVersion)
	require.True(t, cmd.HTTPIgnoreTLSError)
}

func Test_ServeCmd_Run_RealFile(t *testing.T) {
	testFileURL := "https://github.com/hangxie/parquet-tools/raw/refs/heads/main/testdata/good.parquet"

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	_ = listener.Close()

	cmd := ServeCmd{
		URI:        testFileURL,
		Addr:       addr,
		ReadOption: pio.ReadOption{},
	}

	errChan := make(chan error, 1)

	go func() {
		err := cmd.Run()
		if err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	serverURL := "http://" + addr
	var resp *http.Response
	var connErr error

	for i := 0; i < 50; i++ {
		select {
		case err := <-errChan:
			t.Fatalf("Server failed to start: %v", err)
		default:
		}

		resp, connErr = http.Get(serverURL + "/info")
		if connErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if connErr != nil {
		select {
		case err := <-errChan:
			t.Fatalf("Server failed to start: %v", err)
		default:
			t.Fatalf("Could not connect to server after retries: %v", connErr)
		}
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	require.Equal(t, http.StatusOK, resp.StatusCode)
}
