package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"
)

// shutdownGracePeriod bounds how long shutdown waits for in-flight requests to drain.
const shutdownGracePeriod = 10 * time.Second

// serveWithShutdown serves on addr until it fails or ctx is cancelled, then shuts down gracefully.
func serveWithShutdown(ctx context.Context, addr string, handler http.Handler) error {
	server := &http.Server{Addr: addr, Handler: handler}

	errCh := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ShutdownServer(server, errCh, shutdownGracePeriod)
	}
}

// ShutdownServer drains server within grace, then force-closes it; call it at most once.
func ShutdownServer(server *http.Server, serveErr <-chan error, grace time.Duration) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		// Graceful drain timed out: force-close listeners and connections so
		// stuck handlers do not outlive this call.
		_ = server.Close()
		fmt.Fprintf(os.Stderr, "shutdown timed out after %s; force-closed remaining connections\n", grace)
	}
	if serveErr == nil {
		// No serving goroutine to wait on; skip the receive (which on a nil
		// channel would block forever) but still perform the shutdown above.
		return nil
	}
	// A grace timeout is not an error; report only ListenAndServe's result.
	return <-serveErr
}
