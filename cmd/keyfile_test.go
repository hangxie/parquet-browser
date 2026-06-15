package cmd

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/require"
)

func writeKeyFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "keys.json")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func Test_loadKeyFile(t *testing.T) {
	t.Run("empty path is a no-op", func(t *testing.T) {
		opt := pio.ReadOption{FooterKey: "existing"}
		require.NoError(t, loadKeyFile("", &opt))
		require.Equal(t, "existing", opt.FooterKey)
	})

	t.Run("missing file returns error", func(t *testing.T) {
		opt := pio.ReadOption{}
		err := loadKeyFile(filepath.Join(t.TempDir(), "nope.json"), &opt)
		require.Error(t, err)
		require.Contains(t, err.Error(), "read key file")
	})

	t.Run("invalid JSON returns error", func(t *testing.T) {
		path := writeKeyFile(t, "{not json")
		opt := pio.ReadOption{}
		err := loadKeyFile(path, &opt)
		require.Error(t, err)
		require.Contains(t, err.Error(), "parse key file")
	})

	t.Run("unknown field returns error", func(t *testing.T) {
		path := writeKeyFile(t, `{"foo": "bar"}`)
		opt := pio.ReadOption{}
		err := loadKeyFile(path, &opt)
		require.Error(t, err)
		require.Contains(t, err.Error(), "parse key file")
	})

	t.Run("populates empty fields from file", func(t *testing.T) {
		path := writeKeyFile(t, `{
			"footer_key": "Zm9vdGVy",
			"aad_prefix": "YWFk",
			"column_keys": {"a.b": "Y29sQQ==", "c": "Y29sQg=="}
		}`)
		opt := pio.ReadOption{}
		require.NoError(t, loadKeyFile(path, &opt))
		require.Equal(t, "Zm9vdGVy", opt.FooterKey)
		require.Equal(t, "YWFk", opt.AADPrefix)
		sort.Strings(opt.ColumnKeys)
		require.Equal(t, []string{"a.b=Y29sQQ==", "c=Y29sQg=="}, opt.ColumnKeys)
	})

	t.Run("CLI values take precedence over file values", func(t *testing.T) {
		path := writeKeyFile(t, `{
			"footer_key": "ZnJvbWZpbGU=",
			"aad_prefix": "ZnJvbWZpbGU=",
			"column_keys": {"a": "ZnJvbWZpbGU="}
		}`)
		opt := pio.ReadOption{
			FooterKey:  "ZnJvbWNsaQ==",
			AADPrefix:  "ZnJvbWNsaQ==",
			ColumnKeys: []string{"a=ZnJvbWNsaQ=="},
		}
		require.NoError(t, loadKeyFile(path, &opt))
		require.Equal(t, "ZnJvbWNsaQ==", opt.FooterKey)
		require.Equal(t, "ZnJvbWNsaQ==", opt.AADPrefix)
		require.Equal(t, []string{"a=ZnJvbWNsaQ=="}, opt.ColumnKeys)
	})

	t.Run("file column keys merge with CLI column keys", func(t *testing.T) {
		path := writeKeyFile(t, `{
			"column_keys": {"a": "ZmlsZUE=", "b": "ZmlsZUI="}
		}`)
		opt := pio.ReadOption{ColumnKeys: []string{"a=Y2xpQQ=="}}
		require.NoError(t, loadKeyFile(path, &opt))
		sort.Strings(opt.ColumnKeys)
		require.Equal(t, []string{"a=Y2xpQQ==", "b=ZmlsZUI="}, opt.ColumnKeys)
	})

	t.Run("empty column key path returns parse error", func(t *testing.T) {
		path := writeKeyFile(t, `{
			"column_keys": {"": "ZmlsZQ=="}
		}`)
		opt := pio.ReadOption{}
		err := loadKeyFile(path, &opt)
		require.Error(t, err)
		require.Contains(t, err.Error(), "parse key file")
		require.Contains(t, err.Error(), "empty column path")
		require.Empty(t, opt.ColumnKeys)
	})

	t.Run("empty JSON object leaves opt unchanged", func(t *testing.T) {
		path := writeKeyFile(t, `{}`)
		opt := pio.ReadOption{FooterKey: "k"}
		require.NoError(t, loadKeyFile(path, &opt))
		require.Equal(t, "k", opt.FooterKey)
		require.Empty(t, opt.AADPrefix)
		require.Empty(t, opt.ColumnKeys)
	})
}
