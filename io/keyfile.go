package io

import (
	"bytes"
	"encoding/json"
	"fmt"
	stdio "io"
	"os"
)

type keyFileSchema struct {
	FooterKey  string            `json:"footer_key,omitempty"`
	AADPrefix  string            `json:"aad_prefix,omitempty"`
	ColumnKeys map[string]string `json:"column_keys,omitempty"`
}

func parseKeyFile(path string) (keyFileSchema, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return keyFileSchema{}, fmt.Errorf("read key file: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var kf keyFileSchema
	if err := dec.Decode(&kf); err != nil {
		return keyFileSchema{}, fmt.Errorf("parse key file: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != stdio.EOF {
		if err == nil {
			return keyFileSchema{}, fmt.Errorf("parse key file: trailing data after JSON object")
		}
		return keyFileSchema{}, fmt.Errorf("parse key file: %w", err)
	}
	for p := range kf.ColumnKeys {
		if p == "" {
			return keyFileSchema{}, fmt.Errorf("parse key file: column_keys contains an empty column path")
		}
	}
	return kf, nil
}
