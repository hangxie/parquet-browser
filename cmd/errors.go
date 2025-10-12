package cmd

import "errors"

var (
	// ErrInvalidRowGroupIndex is returned when row group index is out of range
	ErrInvalidRowGroupIndex = errors.New("invalid row group index")

	// ErrInvalidColumnIndex is returned when column index is out of range
	ErrInvalidColumnIndex = errors.New("invalid column index")

	// ErrInvalidPageIndex is returned when page index is out of range
	ErrInvalidPageIndex = errors.New("invalid page index")
)
