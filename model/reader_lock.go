package model

import (
	"context"
	"errors"
	"time"
)

// errReaderNotInitialized reports a zero-value ParquetReader not built with NewParquetReader.
var errReaderNotInitialized = errors.New("parquet reader not initialized")

// acquire takes the reader lock, returning ctx.Err() if ctx is done first.
func (pr *ParquetReader) acquire(ctx context.Context) error {
	if pr.sem == nil {
		// Zero-value reader: a send on a nil channel would block forever.
		return errReaderNotInitialized
	}
	// Checked before the select, which picks at random between a free slot and a done ctx.
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case pr.sem <- struct{}{}:
		// Re-checked after acquiring to close that race.
		if err := ctx.Err(); err != nil {
			pr.release()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release returns the reader lock taken by a successful acquire.
func (pr *ParquetReader) release() {
	<-pr.sem
}

// WithLock runs fn holding the reader lock, serializing direct parquet-go use with page reads.
func (pr *ParquetReader) WithLock(ctx context.Context, fn func() error) error {
	if err := pr.acquire(ctx); err != nil {
		return err
	}
	defer pr.release()
	return fn()
}

// closeGrace bounds how long Close waits for an in-flight read before closing anyway.
const closeGrace = 5 * time.Second

// Close idempotently closes the column buffers and the source file, waiting at most closeGrace.
func (pr *ParquetReader) Close() error {
	if pr == nil || pr.Reader == nil {
		return nil
	}
	pr.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), closeGrace)
		defer cancel()
		// Close anyway if an in-flight read outlasts closeGrace.
		if pr.acquire(ctx) == nil {
			defer pr.release()
		}
		err := pr.Reader.ReadStopWithContext(context.Background())
		// ReadStop leaves the source handle open.
		if pr.Reader.PFile != nil {
			err = errors.Join(err, pr.Reader.PFile.Close())
		}
		pr.closeErr = err
	})
	return pr.closeErr
}
