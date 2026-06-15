package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	pio "github.com/hangxie/parquet-tools/io"
)

// keyFileSchema mirrors the JSON layout accepted by --key-file. All values
// are base64-encoded AES keys (or a base64-encoded AAD prefix); decoding is
// performed downstream by parquet-tools so a single validator owns the
// format rules.
type keyFileSchema struct {
	FooterKey  string            `json:"footer_key,omitempty"`
	AADPrefix  string            `json:"aad_prefix,omitempty"`
	ColumnKeys map[string]string `json:"column_keys,omitempty"`
}

// loadKeyFile reads a JSON key file and merges its contents into opt. CLI
// flag values take precedence: a field already set on opt is never replaced,
// and per-column keys from the file are appended only for column paths not
// already present in opt.ColumnKeys. Empty column paths are rejected.
func loadKeyFile(path string, opt *pio.ReadOption) error {
	if path == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read key file: %w", err)
	}

	var kf keyFileSchema
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&kf); err != nil {
		return fmt.Errorf("parse key file: %w", err)
	}

	if opt.FooterKey == "" {
		opt.FooterKey = kf.FooterKey
	}
	if opt.AADPrefix == "" {
		opt.AADPrefix = kf.AADPrefix
	}

	if len(kf.ColumnKeys) == 0 {
		return nil
	}
	existing := make(map[string]struct{}, len(opt.ColumnKeys))
	for _, ck := range opt.ColumnKeys {
		if i := strings.IndexByte(ck, '='); i > 0 {
			existing[ck[:i]] = struct{}{}
		}
	}
	paths := make([]string, 0, len(kf.ColumnKeys))
	for p := range kf.ColumnKeys {
		if p == "" {
			return fmt.Errorf("parse key file: column_keys contains an empty column path")
		}
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		if _, ok := existing[p]; ok {
			continue
		}
		opt.ColumnKeys = append(opt.ColumnKeys, p+"="+kf.ColumnKeys[p])
	}
	return nil
}
