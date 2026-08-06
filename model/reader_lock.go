package model

import (
	"context"
	"errors"
	"time"
)

// errReaderNotInitialized is returned when a lock operation is attempted on a
// zero-value ParquetReader (sem is nil). Readers must be built with
// NewParquetReader.
var errReaderNotInitialized = errors.New("parquet reader not initialized")

// This file holds the reader-lock discipline for ParquetReader: a 1-slot
// semaphore (pr.sem) serializes every operation touching the non-goroutine-safe
// parquet-go reader. External callers go through WithLock.

// acquire takes the reader lock, returning ctx.Err() if ctx is done first. ctx is
// checked before the select (which would otherwise pick at random between a free
// semaphore and a done context) and again after acquiring, to close that race.
func (pr *ParquetReader) acquire(ctx context.Context) error {
	if pr.sem == nil {
		// Zero-value reader: a send on a nil channel would block forever.
		return errReaderNotInitialized
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case pr.sem <- struct{}{}:
		if err := ctx.Err(); err != nil {
			pr.release()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release returns the reader lock. It must only be called after a successful
// acquire.
func (pr *ParquetReader) release() {
	<-pr.sem
}

// WithLock runs fn while holding the reader lock, so callers touching the
// underlying parquet-go reader directly (e.g. schema generation) are serialized
// against the page-read methods. Acquisition honors ctx.
func (pr *ParquetReader) WithLock(ctx context.Context, fn func() error) error {
	if err := pr.acquire(ctx); err != nil {
		return err
	}
	defer pr.release()
	return fn()
}

// closeGrace bounds how long Close waits to serialize with an in-flight read
// before releasing handles best-effort.
const closeGrace = 5 * time.Second

// Close is an idempotent teardown that closes both the per-column buffer handles
// (ReadStop) and the source handle ReadStop leaves behind (Reader.PFile). It
// serializes with in-flight reads, waiting at most closeGrace before proceeding.
func (pr *ParquetReader) Close() error {
	if pr == nil || pr.Reader == nil {
		return nil
	}
	pr.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), closeGrace)
		defer cancel()
		if pr.acquire(ctx) == nil {
			defer pr.release()
		}
		err := pr.Reader.ReadStopWithContext(context.Background())
		if pr.Reader.PFile != nil {
			err = errors.Join(err, pr.Reader.PFile.Close())
		}
		pr.closeErr = err
	})
	return pr.closeErr
}
