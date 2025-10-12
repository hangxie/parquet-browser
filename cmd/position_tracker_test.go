package cmd

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPositionTracker_Read tests the Read method
func Test_PositionTracker_Read(t *testing.T) {
	tests := []struct {
		name         string
		data         []byte
		bufSize      int
		expectedRead int
		expectedPos  int64
		expectedErr  error
	}{
		{
			name:         "read all data at once",
			data:         []byte("hello world"),
			bufSize:      20,
			expectedRead: 11,
			expectedPos:  11,
			expectedErr:  nil,
		},
		{
			name:         "read with exact buffer size",
			data:         []byte("test"),
			bufSize:      4,
			expectedRead: 4,
			expectedPos:  4,
			expectedErr:  nil,
		},
		{
			name:         "read with smaller buffer",
			data:         []byte("hello"),
			bufSize:      3,
			expectedRead: 3,
			expectedPos:  3,
			expectedErr:  nil,
		},
		{
			name:         "read empty data",
			data:         []byte{},
			bufSize:      10,
			expectedRead: 0,
			expectedPos:  0,
			expectedErr:  io.EOF,
		},
		{
			name:         "multiple reads",
			data:         []byte("abcdefghij"),
			bufSize:      5,
			expectedRead: 5,
			expectedPos:  5,
			expectedErr:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bytes.NewReader(tt.data)
			tracker := &positionTracker{
				r:   reader,
				pos: 0,
			}

			buf := make([]byte, tt.bufSize)
			n, err := tracker.Read(buf)

			assert.Equal(t, tt.expectedRead, n)
			assert.Equal(t, tt.expectedPos, tracker.pos)
			if tt.expectedErr != nil {
				assert.Equal(t, tt.expectedErr, err)
			} else if n > 0 {
				assert.NoError(t, err)
			}
		})
	}
}

// TestPositionTracker_Read_MultipleReads tests multiple sequential reads
func Test_PositionTracker_Read_MultipleReads(t *testing.T) {
	data := []byte("hello world test")
	reader := bytes.NewReader(data)
	tracker := &positionTracker{
		r:   reader,
		pos: 0,
	}

	// First read
	buf1 := make([]byte, 5)
	n1, err1 := tracker.Read(buf1)
	assert.NoError(t, err1)
	assert.Equal(t, 5, n1)
	assert.Equal(t, int64(5), tracker.pos)
	assert.Equal(t, []byte("hello"), buf1)

	// Second read
	buf2 := make([]byte, 6)
	n2, err2 := tracker.Read(buf2)
	assert.NoError(t, err2)
	assert.Equal(t, 6, n2)
	assert.Equal(t, int64(11), tracker.pos)
	assert.Equal(t, []byte(" world"), buf2)

	// Third read
	buf3 := make([]byte, 10)
	n3, err3 := tracker.Read(buf3)
	assert.NoError(t, err3)
	assert.Equal(t, 5, n3)
	assert.Equal(t, int64(16), tracker.pos)
	assert.Equal(t, []byte(" test"), buf3[:5])
}

// TestPositionTracker_Write tests the Write method
func Test_PositionTracker_Write(t *testing.T) {
	tracker := &positionTracker{
		r:   bytes.NewReader([]byte("dummy")),
		pos: 0,
	}

	tests := []struct {
		name        string
		data        []byte
		expectedN   int
		expectError bool
	}{
		{
			name:        "write should fail",
			data:        []byte("hello"),
			expectedN:   0,
			expectError: true,
		},
		{
			name:        "write empty data should fail",
			data:        []byte{},
			expectedN:   0,
			expectError: true,
		},
		{
			name:        "write large data should fail",
			data:        bytes.Repeat([]byte("a"), 1000),
			expectedN:   0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := tracker.Write(tt.data)
			assert.Equal(t, tt.expectedN, n)
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "write not supported")
			}
		})
	}
}

// TestPositionTracker_Close tests the Close method
func Test_PositionTracker_Close(t *testing.T) {
	tracker := &positionTracker{
		r:   bytes.NewReader([]byte("test")),
		pos: 0,
	}

	err := tracker.Close()
	assert.NoError(t, err)

	// Close should be idempotent
	err = tracker.Close()
	assert.NoError(t, err)
}

// TestPositionTracker_Flush tests the Flush method
func Test_PositionTracker_Flush(t *testing.T) {
	tracker := &positionTracker{
		r:   bytes.NewReader([]byte("test")),
		pos: 0,
	}

	ctx := context.Background()
	err := tracker.Flush(ctx)
	assert.NoError(t, err)

	// Flush should be idempotent
	err = tracker.Flush(ctx)
	assert.NoError(t, err)
}

// TestPositionTracker_Flush_WithCanceledContext tests Flush with canceled context
func Test_PositionTracker_Flush_WithCanceledContext(t *testing.T) {
	tracker := &positionTracker{
		r:   bytes.NewReader([]byte("test")),
		pos: 0,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Flush should still succeed even with canceled context
	err := tracker.Flush(ctx)
	assert.NoError(t, err)
}

// TestPositionTracker_RemainingBytes tests the RemainingBytes method
func Test_PositionTracker_RemainingBytes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		pos      int64
		expected uint64
	}{
		{
			name:     "new tracker",
			data:     []byte("hello"),
			pos:      0,
			expected: ^uint64(0), // Max uint64
		},
		{
			name:     "after some reads",
			data:     []byte("hello world"),
			pos:      5,
			expected: ^uint64(0), // Always returns max uint64
		},
		{
			name:     "empty reader",
			data:     []byte{},
			pos:      0,
			expected: ^uint64(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := &positionTracker{
				r:   bytes.NewReader(tt.data),
				pos: tt.pos,
			}

			remaining := tracker.RemainingBytes()
			assert.Equal(t, tt.expected, remaining)
		})
	}
}

// TestPositionTracker_Open tests the Open method
func Test_PositionTracker_Open(t *testing.T) {
	tracker := &positionTracker{
		r:   bytes.NewReader([]byte("test")),
		pos: 0,
	}

	err := tracker.Open()
	assert.NoError(t, err)

	// Open should be idempotent
	err = tracker.Open()
	assert.NoError(t, err)
}

// TestPositionTracker_IsOpen tests the IsOpen method
func Test_PositionTracker_IsOpen(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		pos      int64
		expected bool
	}{
		{
			name:     "new tracker",
			data:     []byte("hello"),
			pos:      0,
			expected: true,
		},
		{
			name:     "after reads",
			data:     []byte("world"),
			pos:      3,
			expected: true,
		},
		{
			name:     "always returns true",
			data:     []byte{},
			pos:      0,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := &positionTracker{
				r:   bytes.NewReader(tt.data),
				pos: tt.pos,
			}

			isOpen := tracker.IsOpen()
			assert.Equal(t, tt.expected, isOpen)
		})
	}
}

// TestPositionTracker_Integration tests full read lifecycle
func Test_PositionTracker_Integration(t *testing.T) {
	data := []byte("This is a complete integration test for position tracker")
	reader := bytes.NewReader(data)
	tracker := &positionTracker{
		r:   reader,
		pos: 0,
	}

	// Check initial state
	assert.True(t, tracker.IsOpen())
	assert.Equal(t, int64(0), tracker.pos)
	assert.Equal(t, ^uint64(0), tracker.RemainingBytes())

	// Open (no-op)
	err := tracker.Open()
	assert.NoError(t, err)

	// Read in chunks
	totalRead := 0
	bufSize := 10
	for {
		buf := make([]byte, bufSize)
		n, err := tracker.Read(buf)
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		totalRead += n
		assert.Equal(t, int64(totalRead), tracker.pos)
	}

	assert.Equal(t, len(data), totalRead)
	assert.Equal(t, int64(len(data)), tracker.pos)

	// Flush (no-op)
	err = tracker.Flush(context.Background())
	assert.NoError(t, err)

	// Write should fail
	n, err := tracker.Write([]byte("test"))
	assert.Equal(t, 0, n)
	assert.Error(t, err)

	// Close (no-op)
	err = tracker.Close()
	assert.NoError(t, err)

	// Still open after close (it's a no-op)
	assert.True(t, tracker.IsOpen())
}

// TestPositionTracker_ReadError tests Read with an error-returning reader
func Test_PositionTracker_ReadError(t *testing.T) {
	// Create a reader that returns an error
	errReader := &errorReader{err: io.ErrUnexpectedEOF}
	tracker := &positionTracker{
		r:   errReader,
		pos: 0,
	}

	buf := make([]byte, 10)
	n, err := tracker.Read(buf)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.ErrUnexpectedEOF, err)
	assert.Equal(t, int64(0), tracker.pos) // Position should not change on error
}

// TestPositionTracker_PositionTracking tests that position is accurately tracked
func Test_PositionTracker_PositionTracking(t *testing.T) {
	data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	reader := bytes.NewReader(data)
	tracker := &positionTracker{
		r:   reader,
		pos: 0,
	}

	readSizes := []int{5, 10, 3, 7, 2, 8}
	expectedPositions := []int64{5, 15, 18, 25, 27, 35}

	for i, size := range readSizes {
		buf := make([]byte, size)
		n, _ := tracker.Read(buf)
		if n > 0 {
			assert.Equal(t, expectedPositions[i], tracker.pos,
				"Position mismatch after read %d", i+1)
		}
	}
}

// Helper type for testing read errors
type errorReader struct {
	err error
}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, e.err
}
