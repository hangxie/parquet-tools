package io

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

type keyFileSchema struct {
	FooterKey  string            `json:"footer_key,omitempty"`
	AADPrefix  string            `json:"aad_prefix,omitempty"`
	ColumnKeys map[string]string `json:"column_keys,omitempty"`
}

func loadKeyFile(path string, opt *ReadOption) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read key file: %w", err)
	}
	var kf keyFileSchema
	if err := json.Unmarshal(raw, &kf); err != nil {
		return fmt.Errorf("parse key file: %w", err)
	}
	if opt.FooterKey == nil && kf.FooterKey != "" {
		opt.FooterKey = &kf.FooterKey
	}
	if opt.AADPrefix == nil && kf.AADPrefix != "" {
		opt.AADPrefix = &kf.AADPrefix
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
		if _, ok := existing[p]; !ok {
			opt.ColumnKeys = append(opt.ColumnKeys, p+"="+kf.ColumnKeys[p])
		}
	}
	return nil
}
