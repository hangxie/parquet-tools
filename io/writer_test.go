package io

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	parquetschema "github.com/hangxie/parquet-go/v3/schema"
	"github.com/stretchr/testify/require"
)

func testWriterKeyBase64(size int) *string {
	s := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", size)))
	return &s
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
	explicitColonPath := "writer-" + uuid.NewString() + ":unit-test.parquet"
	implicitColonPath := "writer-" + uuid.NewString() + ":unit-test.parquet"
	t.Cleanup(func() {
		for _, path := range []string{explicitColonPath, implicitColonPath} {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				t.Errorf("remove test output %q: %v", path, err)
			}
		}
	})
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
		"new-local-file-with-colon-requires-scheme": {
			implicitColonPath,
			"unknown location scheme",
		},
		"new-local-file-with-colon-and-scheme": {
			"file://./" + explicitColonPath,
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
				WriterColumnKeys: []string{"Missing=" + *testWriterKeyBase64(16)},
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
		"writer-key-file-bad-path": {
			WriteOption{WriterKeyFile: new("../testdata/no-such-key-file.json")},
			tempFile,
			[]string{"name=Id, type=INT64"},
			"read key file:",
		},
		"writer-key-file-footer-key": {
			WriteOption{WriterKeyFile: new("../testdata/key-file-footer.json")},
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
				WriterColumnKeys: []string{"missing=" + *testWriterKeyBase64(16)},
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
		"writer-key-file-bad-path": {
			tempFile,
			validSchema,
			WriteOption{WriterKeyFile: new("../testdata/no-such-key-file.json")},
			"read key file:",
		},
		"writer-key-file-footer-key": {
			tempFile,
			validSchema,
			WriteOption{WriterKeyFile: new("../testdata/key-file-footer.json")},
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
				WriterColumnKeys: []string{"missing=" + *testWriterKeyBase64(16)},
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
		"writer-key-file-bad-path": {
			tempFile,
			WriteOption{WriterKeyFile: new("../testdata/no-such-key-file.json")},
			schema,
			"read key file:",
		},
		"writer-key-file-footer-key": {
			tempFile,
			WriteOption{WriterKeyFile: new("../testdata/key-file-footer.json")},
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

func TestWriterKeyFileReadBeforeOpeningDestination(t *testing.T) {
	validJSONSchema := `{"Tag":"name=root","Fields":[{"Tag":"name=id, type=INT64"}]}`
	constructors := map[string]func(string, WriteOption) error{
		"csv": func(uri string, option WriteOption) error {
			pw, err := NewCSVWriter(uri, option, []string{"name=Id, type=INT64"})
			if pw != nil {
				_ = pw.PFile.Close()
			}
			return err
		},
		"json": func(uri string, option WriteOption) error {
			pw, err := NewJSONWriter(uri, option, validJSONSchema)
			if pw != nil {
				_ = pw.PFile.Close()
			}
			return err
		},
		"generic": func(uri string, option WriteOption) error {
			pw, err := NewGenericWriter(uri, option, validJSONSchema)
			if pw != nil {
				_ = pw.PFile.Close()
			}
			return err
		},
	}

	for name, constructor := range constructors {
		t.Run(name, func(t *testing.T) {
			tempDir := t.TempDir()
			outputPath := filepath.Join(tempDir, "existing.parquet")
			original := []byte("preserve existing output")
			require.NoError(t, os.WriteFile(outputPath, original, 0o600))

			err := constructor(outputPath, WriteOption{WriterKeyFile: new(filepath.Join(tempDir, "missing-keys.json"))})
			require.Error(t, err)
			require.Contains(t, err.Error(), "read key file")

			got, readErr := os.ReadFile(outputPath)
			require.NoError(t, readErr)
			require.Equal(t, original, got)
		})
	}
}
