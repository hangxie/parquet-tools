package io

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadKeyFile(t *testing.T) {
	testCases := map[string]struct {
		missing  bool
		contents string
		initial  ReadOption
		errMsg   string
		check    func(*testing.T, ReadOption)
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
		"empty-object-no-op": {
			contents: `{}`,
			initial:  ReadOption{FooterKey: new("existing")},
			check: func(t *testing.T, opt ReadOption) {
				require.Equal(t, new("existing"), opt.FooterKey)
				require.Nil(t, opt.AADPrefix)
				require.Empty(t, opt.ColumnKeys)
			},
		},
		"populates-empty-fields": {
			contents: `{"footer_key":"Zm9vdGVy","aad_prefix":"YWFk","column_keys":{"a.b":"Y29sQQ==","c":"Y29sQg=="}}`,
			check: func(t *testing.T, opt ReadOption) {
				require.Equal(t, new("Zm9vdGVy"), opt.FooterKey)
				require.Equal(t, new("YWFk"), opt.AADPrefix)
				sort.Strings(opt.ColumnKeys)
				require.Equal(t, []string{"a.b=Y29sQQ==", "c=Y29sQg=="}, opt.ColumnKeys)
			},
		},
		"cli-footer-key-wins": {
			contents: `{"footer_key":"ZnJvbWZpbGU=","aad_prefix":"ZnJvbWZpbGU="}`,
			initial:  ReadOption{FooterKey: new("ZnJvbWNsaQ=="), AADPrefix: new("ZnJvbWNsaQ==")},
			check: func(t *testing.T, opt ReadOption) {
				require.Equal(t, new("ZnJvbWNsaQ=="), opt.FooterKey)
				require.Equal(t, new("ZnJvbWNsaQ=="), opt.AADPrefix)
			},
		},
		"column-keys-merge": {
			contents: `{"column_keys":{"a":"ZmlsZUE=","b":"ZmlsZUI="}}`,
			initial:  ReadOption{ColumnKeys: []string{"a=Y2xpQQ=="}},
			check: func(t *testing.T, opt ReadOption) {
				sort.Strings(opt.ColumnKeys)
				require.Equal(t, []string{"a=Y2xpQQ==", "b=ZmlsZUI="}, opt.ColumnKeys)
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

			opt := tc.initial
			err := loadKeyFile(path, &opt)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			if tc.check != nil {
				tc.check(t, opt)
			}
		})
	}
}
