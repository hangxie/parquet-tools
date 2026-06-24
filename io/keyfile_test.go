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
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
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
