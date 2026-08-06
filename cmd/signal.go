package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// notifyContext returns a context cancelled on SIGINT/SIGTERM plus a stop to
// defer. The trap is also released on the first signal, so a second Ctrl-C hits
// the default handler instead of being swallowed for the whole grace period.
func notifyContext() (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()
	return ctx, stop
}
