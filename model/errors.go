package model

import "errors"

var (
	// ErrInvalidRowGroupIndex is returned when an invalid row group index is requested
	ErrInvalidRowGroupIndex = errors.New("invalid row group index")

	// ErrInvalidColumnIndex is returned when an invalid column index is requested
	ErrInvalidColumnIndex = errors.New("invalid column index")

	// ErrInvalidPageIndex is returned when an invalid page index is requested
	ErrInvalidPageIndex = errors.New("invalid page index")

	// ErrInvalidPageType is returned when trying to read content from a non-data page
	ErrInvalidPageType = errors.New("cannot read content from non-data page")
)
