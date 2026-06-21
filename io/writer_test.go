package io

import (
	"encoding/base64"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/hangxie/parquet-go/v3/common"
	parquetschema "github.com/hangxie/parquet-go/v3/schema"
	"github.com/stretchr/testify/require"
)

func testWriterKeyBase64(size int) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", size)))
}

func testWriterNestedSchemaHandler(t *testing.T) *parquetschema.SchemaHandler {
	t.Helper()

	schemaHandler, err := parquetschema.NewSchemaHandlerFromJSON(`{
		"Tag": "name=root",
		"Fields": [
			{
				"Tag": "name=Parent",
				"Fields": [
					{"Tag": "name=Child, type=BYTE_ARRAY, convertedtype=UTF8"}
				]
			},
			{"Tag": "name=Other, type=INT64"}
		]
	}`)
	require.NoError(t, err)
	require.Len(t, schemaHandler.ValueColumns, 2)
	return schemaHandler
}

func TestNewParquetFileWriter(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "unit-test.parquet")
	testCases := map[string]struct {
		uri    string
		errMsg string
	}{
		"invalid-uri": {
			"://uri",
			"unable to parse file location",
		},
		"invalid-scheme": {
			"invalid-scheme://something",
			"unknown location scheme",
		},
		"local-file-not-found": {
			"file://path/to/file",
			"no such file or directory",
		},
		"local-not-file": {
			"../testdata/",
			"is a directory",
		},
		"local-file-good": {
			tempFile,
			"",
		},
		"s3-bucket-not-found": {
			"s3://bucket-does-not-exist" + uuid.NewString(),
			"not found",
		},
		"s3-good": {
			"s3://daylight-openstreetmap/will-not-create-till-close",
			"",
		},
		"gcs-no-permission": {
			"gs://cloud-samples-data/bigquery/us-states/us-states.parquet",
			"failed to open GCS object",
		},
		"azblob-invalid-uri1": {
			"wasbs://bad/url",
			"azure blob URI format:",
		},
		"azblob-invalid-uri2": {
			"wasbs://storageaccount.blob.core.windows.net//aa",
			"azure blob URI format:",
		},
		"azblob-good": {
			"wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/will-not-create-till-close",
			"",
		},
		"http-not-support": {
			"https://domain.tld/path/to/file",
			"writing to [https] endpoint is not currently supported",
		},
		"hdfs-failed": {
			"hdfs://localhost:1/temp/good.parquet",
			"connection refused",
		},
		"hdfs-with-user": {
			"hdfs://user@localhost:1/temp/good.parquet",
			"connection refused",
		},
	}

	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")
	t.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(uuid.New().NodeID()))
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pw, err := NewParquetFileWriter(tc.uri)
			defer func() {
				if pw != nil {
					_ = pw.Close()
				}
			}()
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateWriterColumnKeySchemaPaths(t *testing.T) {
	schemaHandler := testWriterNestedSchemaHandler(t)
	columnKey := testWriterKeyBase64(16)
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
	columnKey := testWriterKeyBase64(16)
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
	key16 := testWriterKeyBase64(16)

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
	columnKey := testWriterKeyBase64(16)

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

func TestNewCSVWriter(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "unit-test.parquet")
	wOpt := WriteOption{}
	testCases := map[string]struct {
		option WriteOption
		uri    string
		schema []string
		errMsg string
	}{
		"invalid-uri": {
			wOpt,
			"://uri",
			nil,
			"unable to parse file location",
		},
		"invalid-scheme": {
			wOpt,
			"invalid-scheme://something",
			nil,
			"unknown location scheme",
		},
		"invalid-schema1": {
			wOpt,
			tempFile,
			[]string{"invalid schema"},
			"expect 'key=value'",
		},
		"invalid-schema2": {
			wOpt,
			tempFile,
			[]string{"name=Id"},
			"not a valid Type string",
		},
		"invalid-schema3": {
			wOpt,
			tempFile,
			[]string{"name=Id, type=FOOBAR"},
			"field [Id] with type [FOOBAR]: not a valid Type string",
		},
		"invalid-codec": {
			WriteOption{CompressionCodec: "FOOBAR"},
			tempFile,
			[]string{"name=Id, type=INT64"},
			"not a valid CompressionCodec string",
		},
		"unsupported-codec": {
			WriteOption{CompressionCodec: "LZO"},
			tempFile,
			[]string{"name=Id, type=INT64"},
			"compression is not supported at this moment",
		},
		"supported-brotli": {
			WriteOption{CompressionCodec: "BROTLI"},
			tempFile,
			[]string{"name=Id, type=INT64"},
			"",
		},
		"compression-level": {
			WriteOption{CompressionCodec: "GZIP", CompressionLevel: []string{"GZIP=6"}},
			tempFile,
			[]string{"name=Id, type=INT64"},
			"",
		},
		"invalid-schema-decimal-float": {
			wOpt,
			tempFile,
			[]string{"name=val, type=FLOAT, convertedtype=DECIMAL, scale=2, precision=9"},
			"LogicalType DECIMAL can only be used",
		},
		"invalid-schema-date-int64": {
			wOpt,
			tempFile,
			[]string{"name=val, type=INT64, logicaltype=DATE"},
			"LogicalType DATE can only be used with INT32",
		},
		"invalid-schema-timestamp-int32": {
			wOpt,
			tempFile,
			[]string{"name=val, type=INT32, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"},
			"LogicalType TIMESTAMP can only be used with INT64",
		},
		"invalid-schema-float16-byte-array": {
			wOpt,
			tempFile,
			[]string{"name=val, type=BYTE_ARRAY, logicaltype=FLOAT16"},
			"LogicalType FLOAT16 can only be used with FIXED_LEN_BYTE_ARRAY",
		},
		"column-key-parse-error": {
			WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"bad"},
			},
			tempFile,
			[]string{"name=Id, type=INT64"},
			"invalid writer column key format",
		},
		"column-key-schema-invalid": {
			WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"Id=@footer-key"},
			},
			tempFile,
			[]string{"invalid schema"},
			"create schema from metadata",
		},
		"column-key-path-missing": {
			WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"Missing=" + testWriterKeyBase64(16)},
			},
			tempFile,
			[]string{"name=Id, type=INT64"},
			"writer column key path [Missing] not found in schema",
		},
		"column-key-path-valid": {
			WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"Id=@footer-key"},
			},
			tempFile,
			[]string{"name=Id, type=INT64"},
			"",
		},
		"encrypt-all-columns-valid": {
			WriteOption{
				WriterFooterKey:   testWriterKeyBase64(16),
				EncryptAllColumns: true,
			},
			tempFile,
			[]string{"name=Id, type=INT64"},
			"",
		},
		"hdfs-failed": {
			wOpt,
			"hdfs://localhost:1/temp/good.parquet",
			nil,
			"connection refused",
		},
		"all-good": {
			WriteOption{CompressionCodec: "SNAPPY"},
			tempFile,
			[]string{"name=Id, type=INT64"},
			"",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pw, err := NewCSVWriter(tc.uri, tc.option, tc.schema)
			defer func() {
				if pw != nil {
					_ = pw.PFile.Close()
				}
			}()
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, pw)
		})
	}
}

func TestNewJSONWriter(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "unit-test.parquet")
	validSchema := `{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=id, type=INT64"}]}`

	testCases := map[string]struct {
		uri    string
		schema string
		option WriteOption
		errMsg string
	}{
		"invalid-uri": {
			"://uri",
			"",
			WriteOption{CompressionCodec: "SNAPPY"},
			"unable to parse file location",
		},
		"invalid-schema1": {
			tempFile,
			"invalid schema",
			WriteOption{CompressionCodec: "SNAPPY"},
			"unmarshal json schema string",
		},
		"invalid-schema2": {
			tempFile,
			`{"Tag":"name=top","Fields":[{"Tag":"name=id, type=FOOBAR"}]}`,
			WriteOption{CompressionCodec: "SNAPPY"},
			"field [Id] with type [FOOBAR]: not a valid Type string",
		},
		"invalid-compression": {
			tempFile,
			validSchema,
			WriteOption{CompressionCodec: "INVALID"},
			"not a valid CompressionCodec",
		},
		"compression-level": {
			tempFile,
			validSchema,
			WriteOption{CompressionCodec: "GZIP", CompressionLevel: []string{"GZIP=6"}},
			"",
		},
		"column-key-parse-error": {
			tempFile,
			validSchema,
			WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"bad"},
			},
			"invalid writer column key format",
		},
		"column-key-schema-invalid": {
			tempFile,
			"invalid schema",
			WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"id=@footer-key"},
			},
			"create schema from JSON",
		},
		"column-key-path-missing": {
			tempFile,
			validSchema,
			WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"missing=" + testWriterKeyBase64(16)},
			},
			"writer column key path [missing] not found in schema",
		},
		"column-key-path-valid": {
			tempFile,
			validSchema,
			WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"id=@footer-key"},
			},
			"",
		},
		"encrypt-all-columns-valid": {
			tempFile,
			validSchema,
			WriteOption{
				WriterFooterKey:   testWriterKeyBase64(16),
				EncryptAllColumns: true,
			},
			"",
		},
		"all-good": {
			tempFile,
			validSchema,
			WriteOption{CompressionCodec: "SNAPPY"},
			"",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pw, err := NewJSONWriter(tc.uri, tc.option, tc.schema)
			defer func() {
				if pw != nil {
					_ = pw.PFile.Close()
				}
			}()
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, pw)
		})
	}
}

func TestNewGenericWriter(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "unit-test.parquet")
	schema := `{"Tag":"name=root","Fields":[{"Tag":"name=id, type=INT64"}]}`

	testCases := map[string]struct {
		uri    string
		option WriteOption
		schema string
		errMsg string
	}{
		"invalid-uri": {
			"://uri",
			WriteOption{},
			"",
			"unable to parse file location",
		},
		"schema-not-json": {
			tempFile,
			WriteOption{},
			"invalid schema",
			"unmarshal json schema string:",
		},
		"schema-invalid": {
			tempFile,
			WriteOption{},
			`{"Tag":"name=root","Fields":[{"Tag":"name=id, type=FOOBAR"}]}`,
			"field [Id] with type [FOOBAR]: not a valid Type string",
		},
		"invalid-codec": {
			tempFile,
			WriteOption{CompressionCodec: "FOOBAR"},
			schema,
			"not a valid CompressionCodec string",
		},
		"unsupported-codec": {
			tempFile,
			WriteOption{CompressionCodec: "LZO"},
			schema,
			"compression is not supported at this moment",
		},
		"supported-brotli": {
			tempFile,
			WriteOption{CompressionCodec: "BROTLI"},
			schema,
			"",
		},
		"all-good": {
			tempFile,
			WriteOption{CompressionCodec: "SNAPPY"},
			schema,
			"",
		},
		"compression-level-gzip": {
			tempFile,
			WriteOption{CompressionCodec: "GZIP", CompressionLevel: []string{"GZIP=6"}},
			schema,
			"",
		},
		"compression-level-zstd": {
			tempFile,
			WriteOption{CompressionCodec: "ZSTD", CompressionLevel: []string{"ZSTD=3"}},
			schema,
			"",
		},
		"compression-level-brotli": {
			tempFile,
			WriteOption{CompressionCodec: "BROTLI", CompressionLevel: []string{"BROTLI=5"}},
			schema,
			"",
		},
		"compression-level-lz4raw": {
			tempFile,
			WriteOption{CompressionCodec: "LZ4_RAW", CompressionLevel: []string{"LZ4_RAW=3"}},
			schema,
			"",
		},
		"compression-level-snappy-invalid": {
			tempFile,
			WriteOption{CompressionCodec: "SNAPPY", CompressionLevel: []string{"SNAPPY=3"}},
			schema,
			"does not support compression levels",
		},
		"compression-level-invalid-value": {
			tempFile,
			WriteOption{CompressionCodec: "GZIP", CompressionLevel: []string{"GZIP=99"}},
			schema,
			"out of range",
		},
		"compression-level-multi": {
			tempFile,
			WriteOption{CompressionCodec: "GZIP", CompressionLevel: []string{"GZIP=6,ZSTD=3"}},
			schema,
			"",
		},
		"compression-level-nil": {
			tempFile,
			WriteOption{CompressionCodec: "GZIP"},
			schema,
			"",
		},
		"schema-decimal-float": {
			tempFile,
			WriteOption{},
			`{"Tag":"name=root","Fields":[{"Tag":"name=val, type=FLOAT, convertedtype=DECIMAL, scale=2, precision=9"}]}`,
			"LogicalType DECIMAL can only be used",
		},
		"schema-date-int64": {
			tempFile,
			WriteOption{},
			`{"Tag":"name=root","Fields":[{"Tag":"name=val, type=INT64, logicaltype=DATE"}]}`,
			"LogicalType DATE can only be used with INT32",
		},
		"schema-timestamp-int32": {
			tempFile,
			WriteOption{},
			`{"Tag":"name=root","Fields":[{"Tag":"name=val, type=INT32, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"}]}`,
			"LogicalType TIMESTAMP can only be used with INT64",
		},
		"schema-float16-byte-array": {
			tempFile,
			WriteOption{},
			`{"Tag":"name=root","Fields":[{"Tag":"name=val, type=BYTE_ARRAY, logicaltype=FLOAT16"}]}`,
			"LogicalType FLOAT16 can only be used with FIXED_LEN_BYTE_ARRAY",
		},
		"column-key-parse-error": {
			tempFile,
			WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"bad"},
			},
			schema,
			"invalid writer column key format",
		},
		"column-key-schema-invalid": {
			tempFile,
			WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"id=@footer-key"},
			},
			"invalid schema",
			"create schema from JSON",
		},
		"column-key-path-missing": {
			tempFile,
			WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"missing=" + testWriterKeyBase64(16)},
			},
			schema,
			"writer column key path [missing] not found in schema",
		},
		"column-key-path-valid": {
			tempFile,
			WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"id=@footer-key"},
			},
			schema,
			"",
		},
		"encrypt-all-columns-valid": {
			tempFile,
			WriteOption{
				WriterFooterKey:   testWriterKeyBase64(16),
				EncryptAllColumns: true,
			},
			schema,
			"",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pw, err := NewGenericWriter(tc.uri, tc.option, tc.schema)
			defer func() {
				if pw != nil {
					_ = pw.PFile.Close()
				}
			}()
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Nil(t, pw)
			require.Contains(t, err.Error(), tc.errMsg)
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
			option:      WriteOption{WriterFooterKey: base64StdKey},
			expectedLen: 3,
		},
		"reject-url-safe-base64-footer-key": {
			option: WriteOption{WriterFooterKey: base64URLKey},
			errMsg: "invalid base64 writer footer key",
		},
		"reject-unpadded-base64-footer-key": {
			option: WriteOption{WriterFooterKey: base64RawKey},
			errMsg: "invalid base64 writer footer key",
		},
		"invalid-footer-key-base64": {
			option: WriteOption{WriterFooterKey: "not base64"},
			errMsg: "invalid base64 writer footer key",
		},
		"invalid-footer-key-size": {
			option: WriteOption{WriterFooterKey: testWriterKeyBase64(15)},
			errMsg: "writer footer key must be 16, 24, or 32 bytes",
		},
		"valid-column-key": {
			option: WriteOption{
				WriterFooterKey:  testWriterKeyBase64(16),
				WriterColumnKeys: []string{"name=" + testWriterKeyBase64(16)},
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
				WriterColumnKeys: []string{"name=" + testWriterKeyBase64(15)},
			},
			errMsg: "writer column key for [name] must be 16, 24, or 32 bytes",
		},
		"duplicate-column-key-path": {
			option: WriteOption{
				WriterFooterKey: testWriterKeyBase64(16),
				WriterColumnKeys: []string{
					"name=" + testWriterKeyBase64(16),
					"name=" + testWriterKeyBase64(24),
				},
			},
			errMsg: "duplicate writer column key path [name]",
		},
		"column-key-without-footer-key": {
			option: WriteOption{WriterColumnKeys: []string{"name=" + testWriterKeyBase64(16)}},
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
				WriterColumnKeys: []string{"name=" + testWriterKeyBase64(16)},
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
			option:   WriteOption{WriterColumnKeys: []string{"name=" + testWriterKeyBase64(16)}},
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
