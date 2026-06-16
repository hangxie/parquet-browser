package cmd

import (
	"testing"

	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

func Test_WebUICmd_Run_InvalidFile(t *testing.T) {
	cmd := WebUICmd{
		URI:  "nonexistent.parquet",
		Addr: ":0",
	}

	err := cmd.Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create service")
}

func Test_WebUICmd_FieldAccess(t *testing.T) {
	cmd := WebUICmd{}
	cmd.URI = "file.parquet"
	cmd.Addr = ":9090"
	cmd.ReadOption = pio.ReadOption{}
	cmd.Anonymous = true
	cmd.ObjectVersion = "v1.0"
	cmd.HTTPIgnoreTLSError = true

	require.Equal(t, "file.parquet", cmd.URI)
	require.Equal(t, ":9090", cmd.Addr)
	require.True(t, cmd.Anonymous)
	require.Equal(t, "v1.0", cmd.ObjectVersion)
	require.True(t, cmd.HTTPIgnoreTLSError)
}
