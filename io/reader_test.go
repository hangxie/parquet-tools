package io

import (
	"encoding/base64"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// test files are from https://github.com/apache/parquet-testing/
// keys can be found at https://github.com/apache/parquet-testing/blob/master/data/README.md#encrypted-files
const (
	encryptedFooterURI  = "../testdata/encrypted-footer.parquet"   // renamed from encrypt_columns_and_footer.parquet.encrypted
	encryptedColumnURI  = "../testdata/encrypted-columns.parquet"  // renamed from encrypt_columns_plaintext_footer.parquet.encrypted
	encryptedAADURI     = "../testdata/encrypted-aad.parquet"      // renamed from encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted
	encryptedUniformURI = "../testdata/uniform-encryption.parquet" // renamed from uniform_encryption.parquet.encrypted

	testFooterKey      = "MDEyMzQ1Njc4OTAxMjM0NQ=="
	testDoubleFieldKey = "MTIzNDU2Nzg5MDEyMzQ1MA=="
	testFloatFieldKey  = "MTIzNDU2Nzg5MDEyMzQ1MQ=="
	testAADPrefix      = "dGVzdGVy"
	testWrongKey       = "d3Jvbmd3cm9uZ3dyb25nMQ=="
)

func TestBuildReaderOptions(t *testing.T) {
	testCases := map[string]struct {
		option ReadOption
		errMsg string
	}{
		"empty":                    {option: ReadOption{}},
		"invalid-footer-key":       {option: ReadOption{FooterKey: "!!!"}, errMsg: "invalid base64 footer key"},
		"invalid-aad-prefix":       {option: ReadOption{AADPrefix: "!!!"}, errMsg: "invalid base64 AAD prefix"},
		"column-key-missing-equal": {option: ReadOption{ColumnKeys: []string{"colpath"}}, errMsg: "invalid column key format"},
		"column-key-empty-path":    {option: ReadOption{ColumnKeys: []string{"=YWJj"}}, errMsg: "invalid column key format"},
		"column-key-empty-value":   {option: ReadOption{ColumnKeys: []string{"col.path="}}, errMsg: "invalid column key format"},
		"column-key-invalid-key":   {option: ReadOption{ColumnKeys: []string{"col.path=!!!"}}, errMsg: "invalid base64 column key"},
		"valid-footer-key-std":     {option: ReadOption{FooterKey: testFooterKey}},
		"valid-column-key":         {option: ReadOption{ColumnKeys: []string{"double_field=" + testDoubleFieldKey}}},
		"multiple-column-keys": {
			option: ReadOption{ColumnKeys: []string{
				"double_field=" + testDoubleFieldKey,
				"float_field=" + testFloatFieldKey,
			}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			opts, err := buildReaderOptions(tc.option)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Len(t, opts, len(tc.option.ColumnKeys)+boolToInt(tc.option.FooterKey != "")+boolToInt(tc.option.AADPrefix != ""))
		})
	}
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func TestNewParquetFileReader(t *testing.T) {
	rOpt := ReadOption{}
	s3URL := "s3://daylight-openstreetmap/parquet/osm_features/release=v1.58/type=way/20241112_191814_00139_grr7u_0041fe64-a5ba-4375-88bf-ef790dfedfff"
	gcsURL := "gs://cloud-samples-data/bigquery/us-states/us-states.parquet"
	azblobURL := "wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet"
	httpURL := "https://github.com/hangxie/parquet-tools/raw/refs/heads/main/testdata/good.parquet"
	testCases := map[string]struct {
		uri    string
		option ReadOption
		errMsg string
	}{
		"invalid-uri":            {"://uri", rOpt, "unable to parse file location"},
		"invalid-scheme":         {"invalid-scheme://something", rOpt, "unknown location scheme"},
		"local-file-not-found":   {"file://path/to/file", rOpt, "no such file or directory"},
		"local-file-not-parquet": {"../testdata/not-a-parquet-file", rOpt, "invalid argument"},
		"local-file-good":        {"../testdata/good.parquet", rOpt, ""},
		"s3-not-found":           {"s3://bucket-does-not-exist", rOpt, "not found"},
		"s3-good":                {s3URL, ReadOption{Anonymous: true}, ""},
		"s3-wrong-version":       {s3URL, ReadOption{ObjectVersion: "random-version-id", Anonymous: true}, "https response error StatusCode: 400"},
		"gcs-no-permission":      {gcsURL, rOpt, "failed to create GCS client"},
		"gcs-wrong-generation":   {gcsURL, ReadOption{Anonymous: true, ObjectVersion: "99999"}, "Error 404: No such object:"},
		"gcs-good":               {gcsURL, ReadOption{Anonymous: true}, ""},
		"gcs-good-with-gen":      {gcsURL, ReadOption{Anonymous: true, ObjectVersion: "-1"}, ""},
		"azblob-no-permission":   {azblobURL, rOpt, "Server failed to authenticate the request"},
		"azblob-bad-version":     {azblobURL, ReadOption{Anonymous: true, ObjectVersion: "foo-bar"}, "RESPONSE 400: 400"},
		"azblob-good":            {azblobURL, ReadOption{Anonymous: true}, ""},
		"http-bad-url":           {"https://.../", rOpt, "no such host"},
		"http-no-range-support":  {"https://www.google.com/", rOpt, "does not support range"},
		"http-good":              {httpURL, rOpt, ""},
		"hdfs-failed":            {"hdfs://localhost:1/temp/good.parquet", rOpt, "connection refused"},
		"azblob-invalid-uri1":    {"wasbs://bad/url", rOpt, "azure blob URI format:"},
		"azblob-invalid-uri2":    {"wasbs://storageaccount.blob.core.windows.net//aa", rOpt, "azure blob URI format:"},
	}

	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")
	t.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(uuid.New().NodeID()))
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := NewParquetFileReader(tc.uri, tc.option)
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestNewParquetFileReaderEncryption(t *testing.T) {
	testCases := map[string]struct {
		uri      string
		option   ReadOption
		readRows bool
		errMsg   string
		readErr  string
		rowCount int
	}{
		"plain-file-no-key": {
			uri: "../testdata/good.parquet",
		},
		"plain-file-key-provided": {
			uri:    "../testdata/good.parquet",
			option: ReadOption{FooterKey: testFooterKey},
			errMsg: "encryption keys provided but parquet file is not encrypted",
		},
		"encrypted-footer-correct-key": {
			uri:      encryptedFooterURI,
			option:   encryptedReadOption(),
			readRows: true,
			rowCount: 10,
		},
		"encrypted-footer-wrong-key": {
			uri:    encryptedFooterURI,
			option: ReadOption{FooterKey: testWrongKey},
			errMsg: "decrypt",
		},
		"encrypted-footer-no-key": {
			uri:    encryptedFooterURI,
			errMsg: "decryption key required for footer",
		},
		"encrypted-columns-footer-and-column-keys": {
			uri:      encryptedColumnURI,
			option:   encryptedReadOption(),
			readRows: true,
			rowCount: 10,
		},
		"encrypted-columns-column-keys-only": {
			uri: encryptedColumnURI,
			option: ReadOption{ColumnKeys: []string{
				"double_field=" + testDoubleFieldKey,
				"float_field=" + testFloatFieldKey,
			}},
			readRows: true,
			rowCount: 10,
		},
		"encrypted-columns-no-keys": {
			uri:      encryptedColumnURI,
			readRows: true,
			readErr:  "decryption key required for column",
		},
		"encrypted-columns-wrong-column-key": {
			uri:    encryptedColumnURI,
			option: encryptedReadOptionWithColumnKey("double_field=" + testWrongKey),
			errMsg: "decrypt",
		},
		"encrypted-columns-not-exists": {
			uri:    encryptedColumnURI,
			option: encryptedReadOptionWithColumnKey("Missing=" + testDoubleFieldKey),
			errMsg: "does not match any schema column",
		},
		"encrypted-columns-duplicate-column-key": {
			uri:      encryptedColumnURI,
			option:   encryptedReadOptionWithColumnKeyPrefix("double_field=" + testWrongKey),
			readRows: true,
			rowCount: 10,
		},
		"encrypted-footer-column-key-only": {
			uri: encryptedFooterURI,
			option: ReadOption{ColumnKeys: []string{
				"double_field=" + testDoubleFieldKey,
			}},
			errMsg: "decryption key required for footer",
		},
		"encrypted-aad-provided": {
			uri:      encryptedAADURI,
			option:   encryptedAADReadOption(),
			readRows: true,
			rowCount: 10,
		},
		"encrypted-aad-missing": {
			uri:    encryptedAADURI,
			option: ReadOption{FooterKey: testFooterKey},
			errMsg: "AAD",
		},
		"encrypted-uniform": {
			uri:      encryptedUniformURI,
			option:   ReadOption{FooterKey: testFooterKey},
			readRows: true,
			rowCount: 10,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pr, err := NewParquetFileReader(tc.uri, tc.option)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			defer func() { _ = pr.ReadStop() }()

			if !tc.readRows {
				return
			}
			rows, err := pr.ReadByNumber(10)
			if tc.readErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.readErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, rows, tc.rowCount)
		})
	}
}

func encryptedReadOption() ReadOption {
	return ReadOption{
		FooterKey: testFooterKey,
		ColumnKeys: []string{
			"double_field=" + testDoubleFieldKey,
			"float_field=" + testFloatFieldKey,
		},
	}
}

func encryptedReadOptionWithColumnKey(columnKey string) ReadOption {
	option := encryptedReadOption()
	option.ColumnKeys = append(option.ColumnKeys, columnKey)
	return option
}

func encryptedReadOptionWithColumnKeyPrefix(columnKey string) ReadOption {
	option := encryptedReadOption()
	option.ColumnKeys = append([]string{columnKey}, option.ColumnKeys...)
	return option
}

func encryptedAADReadOption() ReadOption {
	option := encryptedReadOption()
	option.AADPrefix = testAADPrefix
	return option
}
