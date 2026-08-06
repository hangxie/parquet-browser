package model

import (
	"context"
	"testing"
	"time"

	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

// Test_ReaderLock_ContextCancelsQueuedAcquire verifies that a request cancelled
// while the lock is held by a slow operation stops waiting instead of queuing
// behind it, where a plain mutex would block until the holder released.
func Test_ReaderLock_ContextCancelsQueuedAcquire(t *testing.T) {
	pf, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = pf.ReadStopWithContext(context.Background()) }()
	pr := NewParquetReader(pf)

	held := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = pr.WithLock(context.Background(), func() error {
			close(held)
			<-release
			return nil
		})
	}()
	<-held // the reader lock is now held by the goroutine above
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // context is already done before we try to acquire

	done := make(chan error, 1)
	go func() {
		_, e := pr.GetPageMetadataList(ctx, 0, 0)
		done <- e
	}()

	select {
	case e := <-done:
		require.ErrorIs(t, e, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("GetPageMetadataList blocked on the held reader lock instead of honoring its cancelled context")
	}
}

// Test_WithLock_PreCancelledContextNeverRunsCallback verifies WithLock never
// invokes the callback for an already-cancelled context even with a free
// semaphore. Repeated many times, since an unguarded select picks at random.
func Test_WithLock_PreCancelledContextNeverRunsCallback(t *testing.T) {
	pf, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = pf.ReadStopWithContext(context.Background()) }()
	pr := NewParquetReader(pf)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled; the semaphore is free

	for i := 0; i < 1000; i++ {
		ran := false
		err := pr.WithLock(ctx, func() error {
			ran = true
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, ran, "callback ran despite an already-cancelled context")
	}
}

// Test_WithLock_ContextCancelsQueuedAcquire is the same guarantee for the
// WithLock path used by schema generation.
func Test_WithLock_ContextCancelsQueuedAcquire(t *testing.T) {
	pf, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = pf.ReadStopWithContext(context.Background()) }()
	pr := NewParquetReader(pf)

	held := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = pr.WithLock(context.Background(), func() error {
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		done <- pr.WithLock(ctx, func() error { return nil })
	}()

	select {
	case e := <-done:
		require.ErrorIs(t, e, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("WithLock blocked on the held reader lock instead of honoring its cancelled context")
	}
}

// Test_ParquetReader_Close verifies Close releases the primary source handle
// (not just the column buffers) and is idempotent.
func Test_ParquetReader_Close(t *testing.T) {
	t.Run("closes the primary handle and is idempotent", func(t *testing.T) {
		pf, err := pio.NewParquetFileReader(context.Background(), getTestParquetFilePath(), pio.ReadOption{})
		require.NoError(t, err)
		pr := NewParquetReader(pf)

		require.NoError(t, pr.Close())

		// The original source handle (Reader.PFile), which ReadStop does not
		// touch, is now closed: a read from it fails.
		_, rerr := pr.Reader.PFile.Read(make([]byte, 4))
		require.Error(t, rerr, "primary source handle should be closed")

		// Idempotent: a second Close returns nil, not a pile of
		// "file already closed" errors.
		require.NoError(t, pr.Close())
	})

	t.Run("nil receiver and nil reader are safe", func(t *testing.T) {
		var pr *ParquetReader
		require.NoError(t, pr.Close())
		require.NoError(t, (&ParquetReader{}).Close())
	})
}

// Test_ReaderLock_ZeroValueDoesNotHang verifies that a lock operation on a
// zero-value reader (nil sem, e.g. not built via NewParquetReader) returns an
// error instead of blocking forever on a send to a nil channel.
func Test_ReaderLock_ZeroValueDoesNotHang(t *testing.T) {
	pr := &ParquetReader{} // sem is nil
	done := make(chan error, 1)
	go func() {
		_, err := pr.GetPageMetadataList(context.Background(), 0, 0)
		done <- err
	}()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("lock operation on a zero-value reader blocked instead of returning an error")
	}
}
