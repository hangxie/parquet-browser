package service

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-browser/model"
)

func Test_FormatBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected string
	}{
		{"Zero bytes", 0, "0 B"},
		{"Bytes", 500, "500 B"},
		{"Kilobytes", 1024, "1.0 KB"},
		{"Megabytes", 1024 * 1024, "1.0 MB"},
		{"Gigabytes", 1024 * 1024 * 1024, "1.0 GB"},
		{"Large value", 1536, "1.5 KB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := model.FormatBytes(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_FormatRatio(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected string
	}{
		{"Zero ratio", 0.0, "N/A"},
		{"One to one", 1.0, "1.00x"},
		{"Compression", 2.5, "2.50x"},
		{"High compression", 10.123, "10.12x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatRatio(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_FormatBytes_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected string
	}{
		{"Negative bytes", -100, "-100 B"},
		{"Just under KB", 1023, "1023 B"},
		{"Exactly 1 KB", 1024, "1.0 KB"},
		{"Just under MB", 1024*1024 - 1, "1024.0 KB"},
		{"Terabytes", 1024 * 1024 * 1024 * 1024, "1.0 TB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := model.FormatBytes(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_FormatRatio_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected string
	}{
		{"Negative ratio", -1.0, "-1.00x"},
		{"Very small ratio", 0.001, "0.00x"},
		{"Very large ratio", 1000.5, "1000.50x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatRatio(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_formatNullCount(t *testing.T) {
	t.Run("Nil value", func(t *testing.T) {
		result := formatNullCount(nil)
		require.Equal(t, "-", result)
	})

	t.Run("Non-nil value", func(t *testing.T) {
		count := int64(42)
		result := formatNullCount(&count)
		require.Equal(t, "42", result)
	})

	t.Run("Zero count", func(t *testing.T) {
		count := int64(0)
		result := formatNullCount(&count)
		require.Equal(t, "0", result)
	})
}
