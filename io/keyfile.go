package io

import (
	"bytes"
	"encoding/json"
	"fmt"
	stdio "io"
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
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var kf keyFileSchema
	if err := dec.Decode(&kf); err != nil {
		return fmt.Errorf("parse key file: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != stdio.EOF {
		if err == nil {
			return fmt.Errorf("parse key file: trailing data after JSON object")
		}
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
