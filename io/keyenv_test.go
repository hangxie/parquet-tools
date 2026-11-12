package io

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Values from variables go through the same base64 decoding as plain values,
// whether the variable is named by a flag or by a key file.
func TestKeyEnvValuesAreBase64Decoded(t *testing.T) {
	const rawKey = "raw*key*16bytes!"
	t.Setenv("PT_RAW", rawKey)
	t.Setenv("PT_GOOD", "MDEyMzQ1Njc4OTAxMjM0NQ==")

	keyFile := func(t *testing.T, contents string) *string {
		path := filepath.Join(t.TempDir(), "keys.json")
		require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
		return &path
	}
	read := func(opt ReadOption) error {
		if err := applyReadKeyEnv(&opt); err != nil {
			return err
		}
		if opt.KeyFile != nil {
			kf, err := parseKeyFile(*opt.KeyFile)
			if err != nil {
				return err
			}
			applyKeyFile(kf, &opt)
		}
		_, err := buildReaderOptions(opt)
		return err
	}
	write := func(opt WriteOption) error {
		opt, err := applyWriterKeyFileOption(opt)
		if err != nil {
			return err
		}
		columnKeys, err := parseWriterColumnKeys(opt.WriterColumnKeys, opt.FieldDelimiter)
		if err != nil {
			return err
		}
		_, err = writerEncryptionOpts(opt, columnKeys)
		return err
	}

	testCases := map[string]struct {
		err    error
		errMsg string
	}{
		"read-footer-flag":      {read(ReadOption{FooterKeyEnv: new("PT_RAW")}), "invalid base64 footer key"},
		"read-aad-flag":         {read(ReadOption{AADPrefixEnv: new("PT_RAW")}), "invalid base64 AAD prefix"},
		"read-column-flag":      {read(ReadOption{ColumnKeyEnvs: []string{"c=PT_RAW"}}), "invalid base64 column key for [c]"},
		"read-footer-key-file":  {read(ReadOption{KeyFile: keyFile(t, `{"footer_key_env":"PT_RAW"}`)}), "invalid base64 footer key"},
		"read-aad-key-file":     {read(ReadOption{KeyFile: keyFile(t, `{"aad_prefix_env":"PT_RAW"}`)}), "invalid base64 AAD prefix"},
		"read-column-key-file":  {read(ReadOption{KeyFile: keyFile(t, `{"column_keys_env":{"c":"PT_RAW"}}`)}), "invalid base64 column key for [c]"},
		"write-footer-flag":     {write(WriteOption{WriterFooterKeyEnv: new("PT_RAW")}), "invalid base64 writer footer key"},
		"write-column-flag":     {write(WriteOption{WriterFooterKeyEnv: new("PT_GOOD"), WriterColumnKeyEnvs: []string{"c=PT_RAW"}}), "invalid base64 writer column key for [c]"},
		"write-footer-key-file": {write(WriteOption{WriterKeyFile: keyFile(t, `{"footer_key_env":"PT_RAW"}`)}), "invalid base64 writer footer key"},
		"write-column-key-file": {write(WriteOption{WriterKeyFile: keyFile(t, `{"footer_key_env":"PT_GOOD","column_keys_env":{"c":"PT_RAW"}}`)}), "invalid base64 writer column key for [c]"},
		"read-all-valid": {read(ReadOption{
			FooterKeyEnv: new("PT_GOOD"), AADPrefixEnv: new("PT_GOOD"), ColumnKeyEnvs: []string{"c=PT_GOOD"},
		}), ""},
		"read-key-file-valid":  {read(ReadOption{KeyFile: keyFile(t, `{"footer_key_env":"PT_GOOD","aad_prefix_env":"PT_GOOD","column_keys_env":{"c":"PT_GOOD"}}`)}), ""},
		"write-all-valid":      {write(WriteOption{WriterFooterKeyEnv: new("PT_GOOD"), WriterColumnKeyEnvs: []string{"c=PT_GOOD"}}), ""},
		"write-key-file-valid": {write(WriteOption{WriterKeyFile: keyFile(t, `{"footer_key_env":"PT_GOOD","column_keys_env":{"c":"PT_GOOD"}}`)}), ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if tc.errMsg == "" {
				require.NoError(t, tc.err)
				return
			}
			require.ErrorContains(t, tc.err, tc.errMsg)
			require.NotContains(t, tc.err.Error(), rawKey)
		})
	}
}

func TestApplyReadKeyEnv(t *testing.T) {
	testCases := map[string]struct {
		option ReadOption
		env    map[string]string
		errMsg string
		check  func(*testing.T, ReadOption)
	}{
		"no-env-flags": {
			option: ReadOption{FooterKey: new("Zm9vdGVy"), ColumnKeys: []string{"a.b=Y29sQQ=="}},
			check: func(t *testing.T, opt ReadOption) {
				require.Equal(t, "Zm9vdGVy", *opt.FooterKey)
				require.Nil(t, opt.AADPrefix)
				require.Equal(t, []string{"a.b=Y29sQQ=="}, opt.ColumnKeys)
			},
		},
		"all-env-flags": {
			option: ReadOption{
				FooterKeyEnv:  new("PT_FOOTER"),
				AADPrefixEnv:  new("PT_AAD"),
				ColumnKeyEnvs: []string{"a.b=PT_COL_A", "c=PT_COL_C"},
			},
			env: map[string]string{"PT_FOOTER": "Zm9vdGVy", "PT_AAD": "YWFk", "PT_COL_A": "Y29sQQ==", "PT_COL_C": "Y29sQg=="},
			check: func(t *testing.T, opt ReadOption) {
				require.Equal(t, "Zm9vdGVy", *opt.FooterKey)
				require.Equal(t, "YWFk", *opt.AADPrefix)
				require.Equal(t, []string{"a.b=Y29sQQ==", "c=Y29sQg=="}, opt.ColumnKeys)
			},
		},
		"env-value-whitespace-trimmed": {
			option: ReadOption{ColumnKeyEnvs: []string{"c=PT_COL_C"}},
			env:    map[string]string{"PT_COL_C": " Y29sQg==\n"},
			check: func(t *testing.T, opt ReadOption) {
				require.Equal(t, []string{"c=Y29sQg=="}, opt.ColumnKeys)
			},
		},
		"footer-key-env-unset": {
			option: ReadOption{FooterKeyEnv: new("PT_FOOTER")},
			errMsg: "--footer-key-env: environment variable [PT_FOOTER] is not set or empty",
		},
		"aad-prefix-env-empty": {
			option: ReadOption{AADPrefixEnv: new("PT_AAD")},
			env:    map[string]string{"PT_AAD": ""},
			errMsg: "--aad-prefix-env: environment variable [PT_AAD] is not set or empty",
		},
		"footer-key-env-empty-name": {
			option: ReadOption{FooterKeyEnv: new("")},
			errMsg: "--footer-key-env: empty environment variable name",
		},
		"column-key-env-unset": {
			option: ReadOption{ColumnKeyEnvs: []string{"a.b=PT_COL_A"}},
			errMsg: "--column-key-env [a.b]: environment variable [PT_COL_A] is not set or empty",
		},
		"column-key-env-no-separator": {
			option: ReadOption{ColumnKeyEnvs: []string{"PT_COL_A"}},
			errMsg: "invalid --column-key-env format [PT_COL_A], expected 'column.path=ENV_NAME'",
		},
		"column-key-env-empty-path": {
			option: ReadOption{ColumnKeyEnvs: []string{"=PT_COL_A"}},
			errMsg: "invalid --column-key-env format [=PT_COL_A], expected 'column.path=ENV_NAME'",
		},
		"column-key-env-empty-name": {
			option: ReadOption{ColumnKeyEnvs: []string{"a.b="}},
			errMsg: "invalid --column-key-env format [a.b=], expected 'column.path=ENV_NAME'",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			opt := tc.option
			err := applyReadKeyEnv(&opt)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			tc.check(t, opt)
		})
	}
}

func TestApplyWriteKeyEnv(t *testing.T) {
	testCases := map[string]struct {
		option WriteOption
		env    map[string]string
		errMsg string
		check  func(*testing.T, WriteOption)
	}{
		"no-env-flags": {
			option: WriteOption{WriterFooterKey: new("Zm9vdGVy"), WriterColumnKeys: []string{"a.b=@footer-key"}},
			check: func(t *testing.T, opt WriteOption) {
				require.Equal(t, "Zm9vdGVy", *opt.WriterFooterKey)
				require.Equal(t, []string{"a.b=@footer-key"}, opt.WriterColumnKeys)
			},
		},
		"all-env-flags": {
			option: WriteOption{
				WriterFooterKeyEnv:  new("PT_FOOTER"),
				WriterColumnKeyEnvs: []string{"a.b=PT_COL_A", "c=PT_COL_C"},
			},
			env: map[string]string{"PT_FOOTER": "Zm9vdGVy", "PT_COL_A": "Y29sQQ==", "PT_COL_C": "@footer-key"},
			check: func(t *testing.T, opt WriteOption) {
				require.Equal(t, "Zm9vdGVy", *opt.WriterFooterKey)
				require.Equal(t, []string{"a.b=Y29sQQ==", "c=@footer-key"}, opt.WriterColumnKeys)
			},
		},
		"footer-key-env-unset": {
			option: WriteOption{WriterFooterKeyEnv: new("PT_FOOTER")},
			errMsg: "--writer-footer-key-env: environment variable [PT_FOOTER] is not set or empty",
		},
		"column-key-env-unset": {
			option: WriteOption{WriterColumnKeyEnvs: []string{"a.b=PT_COL_A"}},
			errMsg: "--writer-column-key-env [a.b]: environment variable [PT_COL_A] is not set or empty",
		},
		"column-key-env-bad-format": {
			option: WriteOption{WriterColumnKeyEnvs: []string{"a.b"}},
			errMsg: "invalid --writer-column-key-env format [a.b], expected 'column.path=ENV_NAME'",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			opt := tc.option
			err := applyWriteKeyEnv(&opt)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			tc.check(t, opt)
		})
	}
}
