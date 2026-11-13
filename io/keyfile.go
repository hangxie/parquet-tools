package io

import (
	"bytes"
	"encoding/json"
	"fmt"
	stdio "io"
	"maps"
	"os"
	"slices"
)

// keyFileSchema holds the resolved values of a key file.
type keyFileSchema struct {
	FooterKey  string
	AADPrefix  string
	ColumnKeys map[string]string
}

// keyFileJSON is the on-disk layout: plain fields, or *_env fields naming a variable
// that holds the value, never both. Pointers keep a field given as "" counted as given.
type keyFileJSON struct {
	FooterKey     *string           `json:"footer_key"`
	FooterKeyEnv  *string           `json:"footer_key_env"`
	AADPrefix     *string           `json:"aad_prefix"`
	AADPrefixEnv  *string           `json:"aad_prefix_env"`
	ColumnKeys    map[string]string `json:"column_keys"`
	ColumnKeysEnv map[string]string `json:"column_keys_env"`
}

func parseKeyFile(path string) (keyFileSchema, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return keyFileSchema{}, fmt.Errorf("read key file: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var file keyFileJSON
	if err := dec.Decode(&file); err != nil {
		return keyFileSchema{}, fmt.Errorf("parse key file: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != stdio.EOF {
		if err == nil {
			return keyFileSchema{}, fmt.Errorf("parse key file: trailing data after JSON object")
		}
		return keyFileSchema{}, fmt.Errorf("parse key file: %w", err)
	}
	kf, err := file.resolve()
	if err != nil {
		return keyFileSchema{}, fmt.Errorf("parse key file: %w", err)
	}
	return kf, nil
}

// resolveKeyFileEnv returns the value of the variable an _env field names.
func resolveKeyFileEnv(field string, env *string) (string, error) {
	if env == nil {
		return "", nil
	}
	value, err := lookupKeyEnv(*env)
	if err != nil {
		return "", fmt.Errorf("%s: %w", field, err)
	}
	return value, nil
}

func (file keyFileJSON) resolve() (keyFileSchema, error) {
	for p := range file.ColumnKeys {
		if p == "" {
			return keyFileSchema{}, fmt.Errorf("column_keys contains an empty column path")
		}
	}
	usesPlain := file.FooterKey != nil || file.AADPrefix != nil || file.ColumnKeys != nil
	usesEnv := file.FooterKeyEnv != nil || file.AADPrefixEnv != nil || file.ColumnKeysEnv != nil
	if !usesEnv {
		kf := keyFileSchema{ColumnKeys: file.ColumnKeys}
		// An empty value would otherwise read as absent and skip the key and prefix checks.
		if file.FooterKey != nil {
			if *file.FooterKey == "" {
				return keyFileSchema{}, fmt.Errorf("footer_key must not be empty")
			}
			kf.FooterKey = *file.FooterKey
		}
		if file.AADPrefix != nil {
			if *file.AADPrefix == "" {
				return keyFileSchema{}, fmt.Errorf("aad_prefix must not be empty")
			}
			kf.AADPrefix = *file.AADPrefix
		}
		return kf, nil
	}
	if usesPlain {
		return keyFileSchema{}, fmt.Errorf("plain fields (footer_key, aad_prefix, column_keys) and _env fields (footer_key_env, aad_prefix_env, column_keys_env) are mutually exclusive")
	}

	var kf keyFileSchema
	var err error
	if kf.FooterKey, err = resolveKeyFileEnv("footer_key_env", file.FooterKeyEnv); err != nil {
		return keyFileSchema{}, err
	}
	if kf.AADPrefix, err = resolveKeyFileEnv("aad_prefix_env", file.AADPrefixEnv); err != nil {
		return keyFileSchema{}, err
	}
	if len(file.ColumnKeysEnv) > 0 {
		kf.ColumnKeys = make(map[string]string, len(file.ColumnKeysEnv))
	}
	for _, p := range slices.Sorted(maps.Keys(file.ColumnKeysEnv)) {
		if p == "" {
			return keyFileSchema{}, fmt.Errorf("column_keys_env contains an empty column path")
		}
		value, err := lookupKeyEnv(file.ColumnKeysEnv[p])
		if err != nil {
			return keyFileSchema{}, fmt.Errorf("column_keys_env [%s]: %w", p, err)
		}
		kf.ColumnKeys[p] = value
	}
	return kf, nil
}
