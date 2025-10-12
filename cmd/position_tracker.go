package cmd

import (
	"context"
	"fmt"
	"io"
)

// positionTracker wraps a file reader and tracks position
type positionTracker struct {
	r   io.Reader
	pos int64
}

func (p *positionTracker) Read(buf []byte) (n int, err error) {
	n, err = p.r.Read(buf)
	p.pos += int64(n)
	return n, err
}

func (p *positionTracker) Write(buf []byte) (int, error) {
	return 0, fmt.Errorf("write not supported")
}

func (p *positionTracker) Close() error {
	return nil
}

func (p *positionTracker) Flush(ctx context.Context) error {
	return nil
}

func (p *positionTracker) RemainingBytes() uint64 {
	return ^uint64(0) // Unknown
}

func (p *positionTracker) Open() error {
	return nil
}

func (p *positionTracker) IsOpen() bool {
	return true
}
