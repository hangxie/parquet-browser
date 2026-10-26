package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// notifyContext returns a context cancelled on SIGINT/SIGTERM, plus its stop func.
func notifyContext() (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// Release the trap on the first signal so a second Ctrl-C is not swallowed during shutdown.
	go func() {
		<-ctx.Done()
		stop()
	}()
	return ctx, stop
}
