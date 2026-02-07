package io

import (
	"encoding/base64"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNewParquetFileWriter(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "unit-test.parquet")
	testCases := map[string]struct {
		uri    string
		errMsg string
	}{
		"invalid-uri":          {"://uri", "unable to parse file location"},
		"invalid-scheme":       {"invalid-scheme://something", "unknown location scheme"},
		"local-file-not-found": {"file://path/to/file", "no such file or directory"},
		"local-not-file":       {"../testdata/", "is a directory"},
		"local-file-good":      {tempFile, ""},
		"s3-bucket-not-found":  {"s3://bucket-does-not-exist" + uuid.NewString(), "not found"},
		"s3-good":              {"s3://daylight-openstreetmap/will-not-create-till-close", ""},
		"gcs-no-permission":    {"gs://cloud-samples-data/bigquery/us-states/us-states.parquet", "failed to open GCS object"},
		"azblob-invalid-uri1":  {"wasbs://bad/url", "azure blob URI format:"},
		"azblob-invalid-uri2":  {"wasbs://storageaccount.blob.core.windows.net//aa", "azure blob URI format:"},
		"azblob-good":          {"wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/will-not-create-till-close", ""},
		"http-not-support":     {"https://domain.tld/path/to/file", "writing to [https] endpoint is not currently supported"},
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
		"invalid-uri":       {wOpt, "://uri", nil, "unable to parse file location"},
		"invalid-scheme":    {wOpt, "invalid-scheme://something", nil, "unknown location scheme"},
		"invalid-schema1":   {wOpt, tempFile, []string{"invalid schema"}, "expect 'key=value'"},
		"invalid-schema2":   {wOpt, tempFile, []string{"name=Id"}, "not a valid Type string"},
		"invalid-schema3":   {wOpt, tempFile, []string{"name=Id, type=FOOBAR"}, "field [Id] with type [FOOBAR]: not a valid Type string"},
		"invalid-codec":     {WriteOption{Compression: "FOOBAR"}, tempFile, []string{"name=Id, type=INT64"}, "not a valid CompressionCodec string"},
		"unsupported-codec": {WriteOption{Compression: "LZO"}, tempFile, []string{"name=Id, type=INT64"}, "compression is not supported at this moment"},
		"supported-brotli":  {WriteOption{Compression: "BROTLI"}, tempFile, []string{"name=Id, type=INT64"}, ""},
		"hdfs-failed":       {wOpt, "hdfs://localhost:1/temp/good.parquet", nil, "connection refused"},
		"all-good":          {WriteOption{Compression: "SNAPPY"}, tempFile, []string{"name=Id, type=INT64"}, ""},
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
		uri         string
		schema      string
		compression string
		errMsg      string
	}{
		"invalid-uri":         {"://uri", "", "SNAPPY", "unable to parse file location"},
		"invalid-schema1":     {tempFile, "invalid schema", "SNAPPY", "unmarshal json schema string"},
		"invalid-schema2":     {tempFile, `{"Tag":"name=top","Fields":[{"Tag":"name=id, type=FOOBAR"}]}`, "SNAPPY", "field [Id] with type [FOOBAR]: not a valid Type string"},
		"invalid-compression": {tempFile, validSchema, "INVALID", "not a valid CompressionCodec"},
		"all-good":            {tempFile, validSchema, "SNAPPY", ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pw, err := NewJSONWriter(tc.uri, WriteOption{Compression: tc.compression}, tc.schema)
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
		"invalid-uri":       {"://uri", WriteOption{}, "", "unable to parse file location"},
		"schema-not-json":   {tempFile, WriteOption{}, "invalid schema", "unmarshal json schema string:"},
		"schema-invalid":    {tempFile, WriteOption{}, `{"Tag":"name=root","Fields":[{"Tag":"name=id, type=FOOBAR"}]}`, "field [Id] with type [FOOBAR]: not a valid Type string"},
		"invalid-codec":     {tempFile, WriteOption{Compression: "FOOBAR"}, schema, "not a valid CompressionCodec string"},
		"unsupported-codec": {tempFile, WriteOption{Compression: "LZO"}, schema, "compression is not supported at this moment"},
		"supported-brotli":  {tempFile, WriteOption{Compression: "BROTLI"}, schema, ""},
		"all-good":          {tempFile, WriteOption{Compression: "SNAPPY"}, schema, ""},
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
