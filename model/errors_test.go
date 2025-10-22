package model

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ErrorVariables(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "ErrInvalidRowGroupIndex",
			err:      ErrInvalidRowGroupIndex,
			expected: "invalid row group index",
		},
		{
			name:     "ErrInvalidColumnIndex",
			err:      ErrInvalidColumnIndex,
			expected: "invalid column index",
		},
		{
			name:     "ErrInvalidPageIndex",
			err:      ErrInvalidPageIndex,
			expected: "invalid page index",
		},
		{
			name:     "ErrInvalidPageType",
			err:      ErrInvalidPageType,
			expected: "cannot read content from non-data page",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotNil(t, tt.err, "%s should not be nil", tt.name)
			require.Equal(t, tt.expected, tt.err.Error(), "%s.Error() should match", tt.name)
		})
	}
}

func Test_ErrorComparison(t *testing.T) {
	t.Run("ErrInvalidRowGroupIndex can be compared", func(t *testing.T) {
		err := ErrInvalidRowGroupIndex
		require.ErrorIs(t, err, ErrInvalidRowGroupIndex, "errors.Is() should return true for same error")
		require.NotErrorIs(t, err, ErrInvalidColumnIndex, "errors.Is() should return false for different error")
	})

	t.Run("ErrInvalidColumnIndex can be compared", func(t *testing.T) {
		err := ErrInvalidColumnIndex
		require.ErrorIs(t, err, ErrInvalidColumnIndex, "errors.Is() should return true for same error")
		require.NotErrorIs(t, err, ErrInvalidPageIndex, "errors.Is() should return false for different error")
	})

	t.Run("ErrInvalidPageIndex can be compared", func(t *testing.T) {
		err := ErrInvalidPageIndex
		require.ErrorIs(t, err, ErrInvalidPageIndex, "errors.Is() should return true for same error")
		require.NotErrorIs(t, err, ErrInvalidRowGroupIndex, "errors.Is() should return false for different error")
	})

	t.Run("ErrInvalidPageType can be compared", func(t *testing.T) {
		err := ErrInvalidPageType
		require.ErrorIs(t, err, ErrInvalidPageType, "errors.Is() should return true for same error")
		require.NotErrorIs(t, err, ErrInvalidColumnIndex, "errors.Is() should return false for different error")
	})
}

func Test_ErrorWrapping(t *testing.T) {
	tests := []struct {
		name        string
		baseErr     error
		wrappedErr  error
		shouldMatch bool
	}{
		{
			name:        "Wrapped ErrInvalidRowGroupIndex",
			baseErr:     ErrInvalidRowGroupIndex,
			wrappedErr:  errors.Join(ErrInvalidRowGroupIndex, errors.New("additional context")),
			shouldMatch: true,
		},
		{
			name:        "Wrapped ErrInvalidColumnIndex",
			baseErr:     ErrInvalidColumnIndex,
			wrappedErr:  errors.Join(ErrInvalidColumnIndex, errors.New("column out of bounds")),
			shouldMatch: true,
		},
		{
			name:        "Wrapped ErrInvalidPageIndex",
			baseErr:     ErrInvalidPageIndex,
			wrappedErr:  errors.Join(ErrInvalidPageIndex, errors.New("page not found")),
			shouldMatch: true,
		},
		{
			name:        "Wrapped ErrInvalidPageType",
			baseErr:     ErrInvalidPageType,
			wrappedErr:  errors.Join(ErrInvalidPageType, errors.New("dictionary page")),
			shouldMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := errors.Is(tt.wrappedErr, tt.baseErr)
			require.Equal(t, tt.shouldMatch, result)
		})
	}
}

func Test_AllErrorsAreDifferent(t *testing.T) {
	allErrors := []error{
		ErrInvalidRowGroupIndex,
		ErrInvalidColumnIndex,
		ErrInvalidPageIndex,
		ErrInvalidPageType,
	}

	// Verify all errors are unique
	for i, err1 := range allErrors {
		for j, err2 := range allErrors {
			if i != j {
				require.NotEqual(t, err1, err2, "Error at index %d and %d should be different", i, j)
			}
		}
	}
}

func Test_ErrorMessages(t *testing.T) {
	// Verify error messages are descriptive and user-friendly
	tests := []struct {
		name          string
		err           error
		minLength     int
		shouldContain string
	}{
		{
			name:          "ErrInvalidRowGroupIndex has descriptive message",
			err:           ErrInvalidRowGroupIndex,
			minLength:     10,
			shouldContain: "row group",
		},
		{
			name:          "ErrInvalidColumnIndex has descriptive message",
			err:           ErrInvalidColumnIndex,
			minLength:     10,
			shouldContain: "column",
		},
		{
			name:          "ErrInvalidPageIndex has descriptive message",
			err:           ErrInvalidPageIndex,
			minLength:     10,
			shouldContain: "page",
		},
		{
			name:          "ErrInvalidPageType has descriptive message",
			err:           ErrInvalidPageType,
			minLength:     10,
			shouldContain: "page",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := tt.err.Error()
			require.GreaterOrEqual(t, len(msg), tt.minLength, "Error message too short: %q", msg)
			require.Contains(t, msg, tt.shouldContain, "Error message should contain expected substring")
		})
	}
}
