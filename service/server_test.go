package service

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test_serveWithShutdown_GracefulOnContextCancel verifies that cancelling the
// application context stops the server and returns nil (a clean shutdown),
// modeling the SIGINT/SIGTERM path wired up in ServeCmd/WebUICmd.
func Test_serveWithShutdown_GracefulOnContextCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- serveWithShutdown(ctx, addr, http.NewServeMux()) }()

	// Wait until the server accepts connections.
	require.Eventually(t, func() bool {
		c, derr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if derr != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 20*time.Millisecond, "server never started listening")

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err, "graceful shutdown should return nil")
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down after context cancellation")
	}
}

// Test_serveWithShutdown_ReturnsListenError verifies that a listen failure is
// surfaced rather than swallowed.
func Test_serveWithShutdown_ReturnsListenError(t *testing.T) {
	err := serveWithShutdown(context.Background(), "127.0.0.1:-1", http.NewServeMux())
	require.Error(t, err)
}

// Test_ShutdownServer_ReturnsServingError verifies that a real ListenAndServe
// error observed on serveErr is returned rather than discarded (the graceful
// shutdown itself succeeds here, so the serving error is the meaningful one).
func Test_ShutdownServer_ReturnsServingError(t *testing.T) {
	server := &http.Server{}
	serveErr := make(chan error, 1)
	serveErr <- errors.New("boom")

	err := ShutdownServer(server, serveErr, 50*time.Millisecond)
	require.EqualError(t, err, "boom")
}

// Test_ShutdownServer_NilServeErr_StillShutsDown verifies that a nil serveErr
// skips only the goroutine wait, not the shutdown itself: the server must stop
// accepting connections.
func Test_ShutdownServer_NilServeErr_StillShutsDown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	_ = ln.Close()

	server := &http.Server{Addr: addr, Handler: http.NewServeMux()}
	go func() { _ = server.ListenAndServe() }()
	require.Eventually(t, func() bool {
		c, derr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if derr != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 20*time.Millisecond, "server never started listening")

	require.NoError(t, ShutdownServer(server, nil, time.Second))

	// The listener must be closed now despite the nil serveErr.
	require.Eventually(t, func() bool {
		c, derr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if derr == nil {
			_ = c.Close()
		}
		return derr != nil
	}, 2*time.Second, 20*time.Millisecond, "server still accepting connections after shutdown")
}

// Test_shutdown_ForceClosesOnGraceTimeout pins a handler past the grace period
// and asserts ShutdownServer force-closes the connection (returning nil): the
// client read observes a real close rather than the timeout of an open socket.
func Test_shutdown_ForceClosesOnGraceTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	_ = ln.Close()

	block := make(chan struct{})
	started := make(chan struct{})
	var startOnce sync.Once
	mux := http.NewServeMux()
	mux.HandleFunc("/hang", func(w http.ResponseWriter, r *http.Request) {
		startOnce.Do(func() { close(started) })
		<-block // hold the connection open well past the grace period
	})

	server := &http.Server{Addr: addr, Handler: mux}
	errCh := make(chan error, 1)
	go func() {
		serr := server.ListenAndServe()
		if serr == http.ErrServerClosed {
			serr = nil
		}
		errCh <- serr
	}()
	defer close(block) // let the leaked handler goroutine unwind at test end

	require.Eventually(t, func() bool {
		c, derr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if derr != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 20*time.Millisecond, "server never started listening")

	// Open a raw connection and issue a request that pins a handler in flight.
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.Write([]byte("GET /hang HTTP/1.1\r\nHost: test\r\n\r\n"))
	require.NoError(t, err)
	<-started // the handler is now in flight and will not return on its own

	// Grace period far shorter than the (never-arriving) handler completion.
	// A handled timeout returns nil.
	err = ShutdownServer(server, errCh, 100*time.Millisecond)
	require.NoError(t, err)

	// The connection must have been force-closed: a read now returns a real
	// close, not an i/o timeout. If shutdown had only called Server.Shutdown,
	// the connection would still be open and this read would time out instead.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, rerr := conn.Read(make([]byte, 1))
	require.Error(t, rerr)
	var nerr net.Error
	if errors.As(rerr, &nerr) {
		require.False(t, nerr.Timeout(), "connection was left open (read timed out) instead of being force-closed")
	}
}
