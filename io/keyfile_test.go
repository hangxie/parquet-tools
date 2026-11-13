package io

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseKeyFile(t *testing.T) {
	testCases := map[string]struct {
		missing  bool
		contents string
		env      map[string]string
		errMsg   string
		check    func(*testing.T, keyFileSchema)
	}{
		"missing-file": {
			missing: true,
			errMsg:  "read key file",
		},
		"invalid-json": {
			contents: `{not json`,
			errMsg:   "parse key file",
		},
		"trailing-json-rejected": {
			contents: `{"footer_key":"Zm9vdGVy"} {}`,
			errMsg:   "parse key file",
		},
		"trailing-junk-rejected": {
			contents: `{"footer_key":"Zm9vdGVy"} garbage`,
			errMsg:   "parse key file",
		},
		"unknown-fields-rejected": {
			contents: `{"foo": "bar"}`,
			errMsg:   "parse key file",
		},
		"empty-object": {
			contents: `{}`,
			check: func(t *testing.T, kf keyFileSchema) {
				require.Empty(t, kf.FooterKey)
				require.Empty(t, kf.AADPrefix)
				require.Empty(t, kf.ColumnKeys)
			},
		},
		"all-fields": {
			contents: `{"footer_key":"Zm9vdGVy","aad_prefix":"YWFk","column_keys":{"a.b":"Y29sQQ==","c":"Y29sQg=="}}`,
			check: func(t *testing.T, kf keyFileSchema) {
				require.Equal(t, "Zm9vdGVy", kf.FooterKey)
				require.Equal(t, "YWFk", kf.AADPrefix)
				require.Equal(t, map[string]string{"a.b": "Y29sQQ==", "c": "Y29sQg=="}, kf.ColumnKeys)
			},
		},
		"empty-column-path": {
			contents: `{"column_keys":{"":"ZmlsZQ==","valid":"dmFsaWQ="}}`,
			errMsg:   "parse key file",
		},
		"env-all-fields": {
			contents: `{"footer_key_env":"PT_FOOTER","aad_prefix_env":"PT_AAD","column_keys_env":{"a.b":"PT_COL_A","c":"PT_COL_C"}}`,
			env:      map[string]string{"PT_FOOTER": "Zm9vdGVy", "PT_AAD": "YWFk", "PT_COL_A": "Y29sQQ==", "PT_COL_C": "Y29sQg=="},
			check: func(t *testing.T, kf keyFileSchema) {
				require.Equal(t, "Zm9vdGVy", kf.FooterKey)
				require.Equal(t, "YWFk", kf.AADPrefix)
				require.Equal(t, map[string]string{"a.b": "Y29sQQ==", "c": "Y29sQg=="}, kf.ColumnKeys)
			},
		},
		"env-mixed-different-values": {
			contents: `{"footer_key":"Zm9vdGVy","aad_prefix_env":"PT_AAD"}`,
			env:      map[string]string{"PT_AAD": "YWFk"},
			errMsg:   "plain fields (footer_key, aad_prefix, column_keys) and _env fields (footer_key_env, aad_prefix_env, column_keys_env) are mutually exclusive",
		},
		"env-mixed-column-maps": {
			contents: `{"column_keys":{"a.b":"Y29sQQ=="},"column_keys_env":{"c":"PT_COL_C"}}`,
			env:      map[string]string{"PT_COL_C": "Y29sQg=="},
			errMsg:   "plain fields (footer_key, aad_prefix, column_keys) and _env fields (footer_key_env, aad_prefix_env, column_keys_env) are mutually exclusive",
		},
		"env-mixed-empty-column-maps": {
			contents: `{"column_keys":{},"footer_key_env":"PT_FOOTER"}`,
			env:      map[string]string{"PT_FOOTER": "Zm9vdGVy"},
			errMsg:   "plain fields (footer_key, aad_prefix, column_keys) and _env fields (footer_key_env, aad_prefix_env, column_keys_env) are mutually exclusive",
		},
		"env-value-whitespace-trimmed": {
			contents: `{"footer_key_env":"PT_FOOTER"}`,
			env:      map[string]string{"PT_FOOTER": " Zm9vdGVy\n"},
			check: func(t *testing.T, kf keyFileSchema) {
				require.Equal(t, "Zm9vdGVy", kf.FooterKey)
			},
		},
		"env-footer-key-both": {
			contents: `{"footer_key":"Zm9vdGVy","footer_key_env":"PT_FOOTER"}`,
			env:      map[string]string{"PT_FOOTER": "Zm9vdGVy"},
			errMsg:   "plain fields (footer_key, aad_prefix, column_keys) and _env fields (footer_key_env, aad_prefix_env, column_keys_env) are mutually exclusive",
		},
		"env-aad-prefix-both": {
			contents: `{"aad_prefix":"YWFk","aad_prefix_env":"PT_AAD"}`,
			env:      map[string]string{"PT_AAD": "YWFk"},
			errMsg:   "plain fields (footer_key, aad_prefix, column_keys) and _env fields (footer_key_env, aad_prefix_env, column_keys_env) are mutually exclusive",
		},
		"env-column-key-both": {
			contents: `{"column_keys":{"a.b":"Y29sQQ=="},"column_keys_env":{"a.b":"PT_COL_A"}}`,
			env:      map[string]string{"PT_COL_A": "Y29sQQ=="},
			errMsg:   "plain fields (footer_key, aad_prefix, column_keys) and _env fields (footer_key_env, aad_prefix_env, column_keys_env) are mutually exclusive",
		},
		"env-footer-key-both-plain-empty": {
			contents: `{"footer_key":"","footer_key_env":"PT_FOOTER"}`,
			env:      map[string]string{"PT_FOOTER": "Zm9vdGVy"},
			errMsg:   "plain fields (footer_key, aad_prefix, column_keys) and _env fields (footer_key_env, aad_prefix_env, column_keys_env) are mutually exclusive",
		},
		"env-aad-prefix-both-plain-empty": {
			contents: `{"aad_prefix":"","aad_prefix_env":"PT_AAD"}`,
			env:      map[string]string{"PT_AAD": "YWFk"},
			errMsg:   "plain fields (footer_key, aad_prefix, column_keys) and _env fields (footer_key_env, aad_prefix_env, column_keys_env) are mutually exclusive",
		},
		"env-footer-key-both-env-empty": {
			contents: `{"footer_key":"Zm9vdGVy","footer_key_env":""}`,
			errMsg:   "plain fields (footer_key, aad_prefix, column_keys) and _env fields (footer_key_env, aad_prefix_env, column_keys_env) are mutually exclusive",
		},
		"env-footer-key-empty-name": {
			contents: `{"footer_key_env":""}`,
			errMsg:   "footer_key_env: empty environment variable name",
		},
		"env-aad-prefix-empty-name": {
			contents: `{"aad_prefix_env":""}`,
			errMsg:   "aad_prefix_env: empty environment variable name",
		},
		"env-null-fields-are-absent": {
			contents: `{"footer_key":"Zm9vdGVy","footer_key_env":null,"aad_prefix_env":null,"column_keys_env":null}`,
			check: func(t *testing.T, kf keyFileSchema) {
				require.Equal(t, "Zm9vdGVy", kf.FooterKey)
				require.Empty(t, kf.AADPrefix)
				require.Empty(t, kf.ColumnKeys)
			},
		},
		"plain-footer-key-empty": {
			contents: `{"footer_key":""}`,
			errMsg:   "footer_key must not be empty",
		},
		"plain-aad-prefix-empty": {
			contents: `{"footer_key":"Zm9vdGVy","aad_prefix":""}`,
			errMsg:   "aad_prefix must not be empty",
		},
		"env-footer-key-unset": {
			contents: `{"footer_key_env":"PT_FOOTER"}`,
			errMsg:   "footer_key_env: environment variable [PT_FOOTER] is not set or empty",
		},
		"env-aad-prefix-empty": {
			contents: `{"aad_prefix_env":"PT_AAD"}`,
			env:      map[string]string{"PT_AAD": ""},
			errMsg:   "aad_prefix_env: environment variable [PT_AAD] is not set or empty",
		},
		"env-column-key-unset": {
			contents: `{"column_keys_env":{"a.b":"PT_COL_A"}}`,
			errMsg:   "column_keys_env [a.b]: environment variable [PT_COL_A] is not set or empty",
		},
		"env-column-key-empty-name": {
			contents: `{"column_keys_env":{"a.b":""}}`,
			errMsg:   "column_keys_env [a.b]: empty environment variable name",
		},
		"env-empty-column-path": {
			contents: `{"column_keys_env":{"":"PT_COL_A"}}`,
			env:      map[string]string{"PT_COL_A": "Y29sQQ=="},
			errMsg:   "column_keys_env contains an empty column path",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			var path string
			if tc.missing {
				path = filepath.Join(t.TempDir(), "nope.json")
			} else {
				path = filepath.Join(t.TempDir(), "keys.json")
				require.NoError(t, os.WriteFile(path, []byte(tc.contents), 0o600))
			}

			kf, err := parseKeyFile(path)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			if tc.check != nil {
				tc.check(t, kf)
			}
		})
	}
}
