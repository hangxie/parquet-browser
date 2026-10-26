package cmd

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_notifyContext(t *testing.T) {
	t.Run("stop cancels", func(t *testing.T) {
		ctx, stop := notifyContext()
		require.NoError(t, ctx.Err())

		stop()
		require.Error(t, ctx.Err())
	})

	t.Run("signal cancels", func(t *testing.T) {
		ctx, stop := notifyContext()
		defer stop()

		self, err := os.FindProcess(os.Getpid())
		require.NoError(t, err)
		require.NoError(t, self.Signal(syscall.SIGTERM))

		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("context not cancelled by SIGTERM")
		}
	})
}
