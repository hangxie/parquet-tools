package io

import (
	"encoding/base64"
	"sort"
	"testing"

	"github.com/hangxie/parquet-go/v3/common"
	parquetschema "github.com/hangxie/parquet-go/v3/schema"
	"github.com/stretchr/testify/require"
)

func TestValidateWriterColumnKeySchemaPaths(t *testing.T) {
	schemaHandler := testWriterNestedSchemaHandler(t)
	columnKey := *testWriterKeyBase64(16)
	nestedLeaf := schemaHandler.ValueColumns[0]
	nestedExPath := schemaHandler.InPathToExPath[nestedLeaf]

	testCases := []struct {
		name   string
		option WriteOption
		schema *parquetschema.SchemaHandler
		errMsg string
	}{
		{
			name:   "no-column-keys",
			option: WriteOption{},
			schema: schemaHandler,
		},
		{
			name: "nil-schema",
			option: WriteOption{
				WriterColumnKeys: []string{"Parent.Child=" + columnKey},
			},
		},
		{
			name: "nested-stripped-ex-path",
			option: WriteOption{
				WriterColumnKeys: []string{stripWriterSchemaRoot(nestedExPath) + "=" + columnKey},
			},
			schema: schemaHandler,
		},
		{
			name: "nested-in-path-rejected",
			option: WriteOption{
				WriterColumnKeys: []string{nestedLeaf + "=" + columnKey},
			},
			schema: schemaHandler,
			errMsg: "not found in schema (use the file-schema form without the schema root",
		},
		{
			name: "nested-ex-path-rejected",
			option: WriteOption{
				WriterColumnKeys: []string{nestedExPath + "=" + columnKey},
			},
			schema: schemaHandler,
			errMsg: "not found in schema (use the file-schema form without the schema root",
		},
		{
			name: "missing-path",
			option: WriteOption{
				WriterColumnKeys: []string{"Parent.Missing=" + columnKey},
			},
			schema: schemaHandler,
			errMsg: "writer column key path [Parent.Missing] not found in schema",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			columnKeys, parseErr := parseWriterColumnKeys(tc.option.WriterColumnKeys)
			require.NoError(t, parseErr)
			var pathToLeaf map[string]string
			if tc.schema != nil {
				pathToLeaf = writerSchemaPathToLeaf(tc.schema)
			}
			err := validateWriterColumnKeySchemaPaths(columnKeys, pathToLeaf)
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

// TestValidateWriterColumnKeySchemaPathsCrossFormDuplicate covers the
// case where the user lists two --writer-column-key directives whose
// paths refer to the same leaf via different forms. The
// ReformPathStr-keyed dedup in parseWriterColumnKeys does not strip
// the schema root, so this used to slip through and produce two
// WithColumnEncrypted calls for the same leaf with conflicting keys.
// Restricting the accepted form to stripped-external rejects the
// root-prefixed entry at schema validation, which is the desired
// behavior.
func TestValidateWriterColumnKeySchemaPathsCrossFormDuplicate(t *testing.T) {
	schemaHandler := testWriterNestedSchemaHandler(t)
	columnKey := *testWriterKeyBase64(16)
	nestedLeaf := schemaHandler.ValueColumns[0]
	nestedExPath := schemaHandler.InPathToExPath[nestedLeaf]
	stripped := stripWriterSchemaRoot(nestedExPath)

	columnKeys, err := parseWriterColumnKeys([]string{
		stripped + "=" + columnKey,
		nestedExPath + "=@footer-key",
	})
	require.NoError(t, err)
	require.Len(t, columnKeys, 2)

	err = validateWriterColumnKeySchemaPaths(columnKeys, writerSchemaPathToLeaf(schemaHandler))
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found in schema (use the file-schema form without the schema root")
}

func TestParseWriterColumnKeys(t *testing.T) {
	key16 := *testWriterKeyBase64(16)

	testCases := map[string]struct {
		raw       []string
		errMsg    string
		wantPaths []string
	}{
		"nil": {
			raw: nil,
		},
		"empty": {
			raw: []string{},
		},
		"single": {
			raw:       []string{"name=" + key16},
			wantPaths: []string{"name"},
		},
		"multiple": {
			raw:       []string{"a=" + key16, "b=@footer-key"},
			wantPaths: []string{"a", "b"},
		},
		"missing-equals": {
			raw:    []string{"name"},
			errMsg: "invalid writer column key format [name]",
		},
		"empty-path": {
			raw:    []string{"=" + key16},
			errMsg: "invalid writer column key format [=",
		},
		"empty-value": {
			raw:    []string{"name="},
			errMsg: "invalid writer column key format [name=]",
		},
		"duplicate-exact": {
			raw:    []string{"name=" + key16, "name=@footer-key"},
			errMsg: "duplicate writer column key path [name]",
		},
		"duplicate-normalized": {
			raw:    []string{"Parent.Child=" + key16, "Parent.Child=@footer-key"},
			errMsg: "duplicate writer column key path [Parent.Child]",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			parsed, err := parseWriterColumnKeys(tc.raw)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				require.Nil(t, parsed)
				return
			}
			require.NoError(t, err)
			require.Len(t, parsed, len(tc.wantPaths))
			for i, p := range tc.wantPaths {
				require.Equal(t, p, parsed[i].Path)
			}
		})
	}
}

func TestWriterSchemaPathToLeaf(t *testing.T) {
	schemaHandler := testWriterNestedSchemaHandler(t)
	pathToLeaf := writerSchemaPathToLeaf(schemaHandler)
	nestedLeaf := schemaHandler.ValueColumns[0]
	nestedExPath := schemaHandler.InPathToExPath[nestedLeaf]
	strippedEx := stripWriterSchemaRoot(nestedExPath)

	// Only the stripped-external (file-schema) form is registered. Map keys
	// are normalized to the ParGoPathDelimiter form, matching how callers look
	// up entries via ReformPathStr(userInput).
	require.Equal(t, nestedLeaf, pathToLeaf[common.ReformPathStr(strippedEx)])
	// The unstripped forms must NOT be registered, so that users cannot
	// smuggle the same leaf in under two different paths.
	_, ok := pathToLeaf[nestedLeaf]
	require.False(t, ok, "in-path form must not be a valid --writer-column-key path")
	_, ok = pathToLeaf[nestedExPath]
	require.False(t, ok, "unstripped ex-path form must not be a valid --writer-column-key path")

	require.Empty(t, stripWriterSchemaRoot("leaf"))
}

// TestWriterSchemaPathToLeafDefensive covers the two paths that real
// parquet-go schemas never produce: a ValueColumns entry missing from
// InPathToExPath (the !ok fallback) and a single-element value column whose
// stripWriterSchemaRoot result is empty.
func TestWriterSchemaPathToLeafDefensive(t *testing.T) {
	root := "synthetic"
	missingMapping := root + common.ParGoPathDelimiter + "Field"
	rootOnly := "RootOnly"
	schemaHandler := &parquetschema.SchemaHandler{
		ValueColumns:   []string{missingMapping, rootOnly},
		InPathToExPath: map[string]string{
			// Intentionally omit missingMapping to exercise the !ok branch.
		},
	}

	pathToLeaf := writerSchemaPathToLeaf(schemaHandler)
	// The !ok branch falls back to the in-path; stripping leaves "Field".
	require.Equal(t, missingMapping, pathToLeaf[common.ReformPathStr("Field")])
	// rootOnly has no children below the root and must be skipped entirely.
	require.NotContains(t, pathToLeaf, common.ReformPathStr(rootOnly))
	require.Len(t, pathToLeaf, 1)

	opts := writerEncryptAllColumnsOpts(
		WriteOption{EncryptAllColumns: true},
		nil,
		schemaHandler,
		pathToLeaf,
	)
	// Only the missing-mapping leaf yields an option; the root-only column
	// is dropped because stripWriterSchemaRoot returns "".
	require.Len(t, opts, 1)
}

func TestWriterEncryptAllColumnsOpts(t *testing.T) {
	schemaHandler := testWriterNestedSchemaHandler(t)
	columnKey := *testWriterKeyBase64(16)

	testCases := []struct {
		name        string
		option      WriteOption
		schema      *parquetschema.SchemaHandler
		expectedLen int
	}{
		{
			name:        "disabled",
			option:      WriteOption{},
			schema:      schemaHandler,
			expectedLen: 0,
		},
		{
			name:        "nil-schema",
			option:      WriteOption{EncryptAllColumns: true},
			expectedLen: 0,
		},
		{
			name:        "all-leaves",
			option:      WriteOption{EncryptAllColumns: true},
			schema:      schemaHandler,
			expectedLen: len(schemaHandler.ValueColumns),
		},
		{
			name: "skips-listed-nested-column",
			option: WriteOption{
				EncryptAllColumns: true,
				WriterColumnKeys:  []string{"Parent.Child=" + columnKey},
			},
			schema:      schemaHandler,
			expectedLen: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			columnKeys, parseErr := parseWriterColumnKeys(tc.option.WriterColumnKeys)
			require.NoError(t, parseErr)
			var pathToLeaf map[string]string
			if tc.schema != nil {
				pathToLeaf = writerSchemaPathToLeaf(tc.schema)
			}
			opts := writerEncryptAllColumnsOpts(tc.option, columnKeys, tc.schema, pathToLeaf)
			require.Len(t, opts, tc.expectedLen)
		})
	}
}

func TestWriterEncryptionRequested(t *testing.T) {
	testCases := map[string]struct {
		option   WriteOption
		expected bool
	}{
		"zero-value": {
			option:   WriteOption{},
			expected: false,
		},
		"footer-key": {
			option:   WriteOption{WriterFooterKey: testWriterKeyBase64(16)},
			expected: true,
		},
		"column-key": {
			option:   WriteOption{WriterColumnKeys: []string{"name=" + *testWriterKeyBase64(16)}},
			expected: true,
		},
		"plaintext-footer": {
			option:   WriteOption{PlaintextFooter: true},
			expected: true,
		},
		"encrypt-all-columns": {
			option:   WriteOption{EncryptAllColumns: true},
			expected: true,
		},
		"default-algorithm": {
			option:   WriteOption{EncryptionAlgorithm: writerEncryptionAlgorithmAESGCMV1},
			expected: false,
		},
		"ctr-algorithm": {
			option:   WriteOption{EncryptionAlgorithm: writerEncryptionAlgorithmAESGCMCTRV1},
			expected: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, writerEncryptionRequested(tc.option))
		})
	}
}

func TestApplyWriterKeyFile(t *testing.T) {
	testCases := map[string]struct {
		kf      keyFileSchema
		initial WriteOption
		check   func(*testing.T, WriteOption)
	}{
		"empty-schema-no-op": {
			kf:      keyFileSchema{},
			initial: WriteOption{WriterFooterKey: testWriterKeyBase64(16)},
			check: func(t *testing.T, opt WriteOption) {
				require.Equal(t, testWriterKeyBase64(16), opt.WriterFooterKey)
				require.Empty(t, opt.WriterColumnKeys)
			},
		},
		"populates-footer-key": {
			kf: keyFileSchema{FooterKey: "Zm9vdGVy"},
			check: func(t *testing.T, opt WriteOption) {
				require.Equal(t, new("Zm9vdGVy"), opt.WriterFooterKey)
			},
		},
		"aad-prefix-ignored": {
			kf: keyFileSchema{AADPrefix: "YWFk"},
			check: func(t *testing.T, opt WriteOption) {
				require.Nil(t, opt.WriterFooterKey)
				require.Empty(t, opt.WriterColumnKeys)
			},
		},
		"cli-wins-for-footer-key": {
			kf:      keyFileSchema{FooterKey: "ZnJvbWZpbGU="},
			initial: WriteOption{WriterFooterKey: new("ZnJvbWNsaQ==")},
			check: func(t *testing.T, opt WriteOption) {
				require.Equal(t, new("ZnJvbWNsaQ=="), opt.WriterFooterKey)
			},
		},
		"column-keys-with-sentinel": {
			kf: keyFileSchema{ColumnKeys: map[string]string{"a.b": "Y29sQQ==", "c": "@footer-key"}},
			check: func(t *testing.T, opt WriteOption) {
				sort.Strings(opt.WriterColumnKeys)
				require.Equal(t, []string{"a.b=Y29sQQ==", "c=@footer-key"}, opt.WriterColumnKeys)
			},
		},
		"column-keys-merge-cli-wins": {
			kf:      keyFileSchema{ColumnKeys: map[string]string{"a": "ZmlsZUE=", "b": "ZmlsZUI="}},
			initial: WriteOption{WriterColumnKeys: []string{"a=Y2xpQQ=="}},
			check: func(t *testing.T, opt WriteOption) {
				sort.Strings(opt.WriterColumnKeys)
				require.Equal(t, []string{"a=Y2xpQQ==", "b=ZmlsZUI="}, opt.WriterColumnKeys)
			},
		},
		"column-keys-cross-form-cli-wins": {
			// CLI uses dot form "a.b"; file uses the same logical path.
			// ReformPathStr normalizes both to the same key so the file
			// entry must be suppressed and CLI value must survive.
			kf:      keyFileSchema{ColumnKeys: map[string]string{"a.b": "ZmlsZQ=="}},
			initial: WriteOption{WriterColumnKeys: []string{"a.b=Y2xp"}},
			check: func(t *testing.T, opt WriteOption) {
				require.Equal(t, []string{"a.b=Y2xp"}, opt.WriterColumnKeys)
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			opt := tc.initial
			applyWriterKeyFile(tc.kf, &opt)
			if tc.check != nil {
				tc.check(t, opt)
			}
		})
	}
}

func TestWriterOpts(t *testing.T) {
	keyBytes := []byte{0xfb, 0xfb, 0xfb, 0xfb, 0xfb, 0xfb, 0xfb, 0xfb, 0xfb, 0xfb, 0xfb, 0xfb, 0xfb, 0xfb, 0xfb, 0xfb}
	base64StdKey := base64.StdEncoding.EncodeToString(keyBytes)
	base64URLKey := base64.URLEncoding.EncodeToString(keyBytes)
	base64RawKey := base64.RawStdEncoding.EncodeToString(keyBytes)

	testCases := map[string]struct {
		option      WriteOption
		errMsg      string
		expectedLen int
	}{
		"all-fields-set": {
			option:      WriteOption{CompressionCodec: "SNAPPY", DataPageVersion: 2, PageSize: 2048, RowGroupSize: 4096},
			expectedLen: 5,
		},
		"valid-gzip": {
			option:      WriteOption{CompressionCodec: "GZIP", DataPageVersion: 1, PageSize: 1024, RowGroupSize: 2048},
			expectedLen: 5,
		},
		"valid-compression-level": {
			option:      WriteOption{CompressionCodec: "GZIP", CompressionLevel: []string{"GZIP=6,ZSTD=3"}, DataPageVersion: 1, PageSize: 1024, RowGroupSize: 2048},
			expectedLen: 7,
		},
		"defaults-only": {
			option:      WriteOption{},
			expectedLen: 2, // DataPageVersion + NP
		},
		"invalid-compression": {
			option: WriteOption{CompressionCodec: "INVALID"},
			errMsg: "not a valid CompressionCodec",
		},
		"unsupported-lzo": {
			option: WriteOption{CompressionCodec: "LZO"},
			errMsg: "compression is not supported at this moment",
		},
		"valid-footer-key-16": {
			option:      WriteOption{WriterFooterKey: testWriterKeyBase64(16)},
			expectedLen: 3,
		},
		"valid-footer-key-24": {
			option:      WriteOption{WriterFooterKey: testWriterKeyBase64(24)},
			expectedLen: 3,
		},
		"valid-footer-key-32": {
			option:      WriteOption{WriterFooterKey: testWriterKeyBase64(32)},
			expectedLen: 3,
		},
		"valid-footer-key-standard-base64": {
			option:      WriteOption{WriterFooterKey: &base64StdKey},
			expectedLen: 3,
		},
		"reject-url-safe-base64-footer-key": {
			option: WriteOption{WriterFooterKey: &base64URLKey},
			errMsg: "invalid base64 writer footer key",
		},
		"reject-unpadded-base64-footer-key": {
			option: WriteOption{WriterFooterKey: &base64RawKey},
			errMsg: "invalid base64 writer footer key",
		},
		"invalid-footer-key-base64": {
			option: WriteOption{WriterFooterKey: new("not base64")},
			errMsg: "invalid base64 writer footer key",
		},
		"invalid-footer-key-size": {
			option: WriteOption{WriterFooterKey: testWriterKeyBase64(15)},
			errMsg: "writer footer key must be 16, 24, or 32 bytes",
		},
		"valid-column-key": {
			option: WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"name=" + *testWriterKeyBase64(16)},
			},
			expectedLen: 4,
		},
		"invalid-column-key-format": {
			option: WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"name"},
			},
			errMsg: "invalid writer column key format",
		},
		"invalid-column-key-base64": {
			option: WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"name=not base64"},
			},
			errMsg: "invalid base64 writer column key for [name]",
		},
		"invalid-column-key-size": {
			option: WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"name=" + *testWriterKeyBase64(15)},
			},
			errMsg: "writer column key for [name] must be 16, 24, or 32 bytes",
		},
		"duplicate-column-key-path": {
			option: WriteOption{
				WriterFooterKey: testWriterKeyBase64(16),
				WriterColumnKeys: []string{
					"name=" + *testWriterKeyBase64(16),
					"name=" + *testWriterKeyBase64(24),
				},
			},
			errMsg: "duplicate writer column key path [name]",
		},
		"column-key-without-footer-key": {
			option: WriteOption{WriterColumnKeys: []string{"name=" + *testWriterKeyBase64(16)}},
			errMsg: "--writer-footer-key is required for encryption",
		},
		"plaintext-footer-without-footer-key": {
			option: WriteOption{PlaintextFooter: true},
			errMsg: "--writer-footer-key is required for encryption",
		},
		"plaintext-footer-signed-only": {
			// No column encryption — the footer is signed for integrity only,
			// column data ships in the clear. Valid Parquet PME mode for
			// tamper detection without confidentiality.
			option: WriteOption{
				WriterFooterKey: testWriterKeyBase64(16),
				PlaintextFooter: true,
			},
			expectedLen: 4,
		},
		"plaintext-footer-with-column-key": {
			option: WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"name=" + *testWriterKeyBase64(16)},
				PlaintextFooter:  true,
			},
			expectedLen: 5,
		},
		"plaintext-footer-with-encrypt-all-columns": {
			option: WriteOption{
				WriterFooterKey:   testWriterKeyBase64(16),
				EncryptAllColumns: true,
				PlaintextFooter:   true,
			},
			expectedLen: 4,
		},
		"plaintext-footer-with-sentinel-column": {
			option: WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"name=@footer-key"},
				PlaintextFooter:  true,
			},
			expectedLen: 5,
		},
		"valid-footer-key-sentinel-column": {
			option: WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"name=@footer-key"},
			},
			expectedLen: 4,
		},
		"valid-encrypt-all-columns": {
			option: WriteOption{
				WriterFooterKey:   testWriterKeyBase64(16),
				EncryptAllColumns: true,
			},
			expectedLen: 3,
		},
		"encrypt-all-columns-without-footer-key": {
			option: WriteOption{EncryptAllColumns: true},
			errMsg: "--writer-footer-key is required for encryption",
		},
		"ctr-algorithm": {
			option: WriteOption{
				WriterFooterKey:     testWriterKeyBase64(16),
				EncryptionAlgorithm: writerEncryptionAlgorithmAESGCMCTRV1,
			},
			expectedLen: 4,
		},
		"ctr-algorithm-without-footer-key": {
			option:      WriteOption{EncryptionAlgorithm: writerEncryptionAlgorithmAESGCMCTRV1},
			expectedLen: 2, // DataPageVersion + NP; algorithm alone is not an encryption trigger
		},
		"explicit-default-algorithm-without-footer-key": {
			option:      WriteOption{EncryptionAlgorithm: writerEncryptionAlgorithmAESGCMV1},
			expectedLen: 2,
		},
		"invalid-algorithm": {
			option: WriteOption{EncryptionAlgorithm: "AES-GCM-V2"},
			errMsg: "invalid encryption algorithm",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			columnKeys, parseErr := parseWriterColumnKeys(tc.option.WriterColumnKeys)
			if parseErr != nil {
				require.NotEmpty(t, tc.errMsg, "unexpected parse error: %v", parseErr)
				require.Contains(t, parseErr.Error(), tc.errMsg)
				return
			}
			opts, err := writerOpts(tc.option, columnKeys)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				require.Nil(t, opts)
				return
			}
			require.NoError(t, err)
			require.Len(t, opts, tc.expectedLen)
		})
	}
}
