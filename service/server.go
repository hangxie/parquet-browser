package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"
)

// shutdownGracePeriod bounds how long a graceful shutdown waits for in-flight
// requests to drain after a termination signal.
const shutdownGracePeriod = 10 * time.Second

// serveWithShutdown runs an HTTP server bound to addr until it fails or ctx is
// cancelled. When ctx is cancelled (e.g. the process received SIGINT/SIGTERM),
// it shuts down gracefully via ShutdownServer. A clean shutdown returns nil.
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

// ShutdownServer stops server within grace, then force-closes connections that
// outlast it (Shutdown alone leaves them running), and waits on serveErr for
// ListenAndServe's result. A grace timeout is not an error. Call at most once.
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
	return <-serveErr
}
