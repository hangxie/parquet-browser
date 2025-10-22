package cmd

import (
	"testing"

	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

func Test_ServeCmd_Run_InvalidFile(t *testing.T) {
	cmd := ServeCmd{
		URI:  "nonexistent.parquet",
		Addr: ":0", // Use port 0 to get random available port
	}

	err := cmd.Run()
	require.Error(t, err, "Run() should return error for non-existent file")

	// Error should mention failed to create service
	require.Contains(t, err.Error(), "failed to create service", "Error should mention failed to create service")
}

func Test_WebUICmd_Run_InvalidFile(t *testing.T) {
	cmd := WebUICmd{
		URI:  "nonexistent.parquet",
		Addr: ":0",
	}

	err := cmd.Run()
	require.Error(t, err, "Run() should return error for non-existent file")
	require.Contains(t, err.Error(), "failed to create service", "Error should mention failed to create service")
}

func Test_ReadOption_Integration(t *testing.T) {
	// Test that ReadOption is embedded in commands
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "ServeCmd has ReadOption",
			test: func(t *testing.T) {
				cmd := ServeCmd{
					URI:        "test.parquet",
					ReadOption: pio.ReadOption{},
				}
				// Verify the ReadOption field exists
				_ = cmd.ReadOption
			},
		},
		{
			name: "WebUICmd has ReadOption",
			test: func(t *testing.T) {
				cmd := WebUICmd{
					URI:        "test.parquet",
					ReadOption: pio.ReadOption{},
				}
				_ = cmd.ReadOption
			},
		},
		{
			name: "TUICmd has ReadOption",
			test: func(t *testing.T) {
				cmd := TUICmd{
					URI:        "test.parquet",
					ReadOption: pio.ReadOption{},
				}
				_ = cmd.ReadOption
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.test(t)
		})
	}
}

func Test_Commands_ImplementsInterface(t *testing.T) {
	// Verify all commands have a Run method
	tests := []struct {
		name    string
		hasRun  bool
		command interface{}
	}{
		{
			name:    "ServeCmd",
			hasRun:  true,
			command: ServeCmd{},
		},
		{
			name:    "WebUICmd",
			hasRun:  true,
			command: WebUICmd{},
		},
		{
			name:    "TUICmd",
			hasRun:  true,
			command: TUICmd{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// All commands should have a Run() error method
			switch cmd := tt.command.(type) {
			case ServeCmd:
				_ = cmd.Run
			case WebUICmd:
				_ = cmd.Run
			case TUICmd:
				_ = cmd.Run
			default:
				require.Failf(t, "Unknown command type", "Unknown command type: %T", cmd)
			}
		})
	}
}

func Test_Command_Fields(t *testing.T) {
	t.Run("ServeCmd has required fields", func(t *testing.T) {
		cmd := ServeCmd{}
		// Check that fields can be set
		cmd.URI = "file.parquet"
		cmd.Addr = ":9090"

		require.NotEmpty(t, cmd.URI, "URI field should be settable")
		require.NotEmpty(t, cmd.Addr, "Addr field should be settable")
	})

	t.Run("WebUICmd has required fields", func(t *testing.T) {
		cmd := WebUICmd{}
		cmd.URI = "file.parquet"
		cmd.Addr = ":9090"

		require.NotEmpty(t, cmd.URI, "URI field should be settable")
		require.NotEmpty(t, cmd.Addr, "Addr field should be settable")
	})

	t.Run("TUICmd has required fields", func(t *testing.T) {
		cmd := TUICmd{}
		cmd.URI = "file.parquet"

		require.NotEmpty(t, cmd.URI, "URI field should be settable")
	})
}

func Test_MultipleCommands(t *testing.T) {
	// Verify multiple command instances can coexist
	serve := ServeCmd{URI: "test1.parquet", Addr: ":8081"}
	webui := WebUICmd{URI: "test2.parquet", Addr: ":8082"}
	tui := TUICmd{URI: "test3.parquet"}

	require.Equal(t, "test1.parquet", serve.URI)
	require.Equal(t, "test2.parquet", webui.URI)
	require.Equal(t, "test3.parquet", tui.URI)
}
