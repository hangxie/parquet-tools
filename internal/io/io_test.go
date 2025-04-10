package internal

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"math"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func Test_azureAccessDetail_invalid_uri(t *testing.T) {
	u := url.URL{
		Host: "storageacconut",
	}
	_ = os.Unsetenv("AZURE_STORAGE_ACCESS_KEY")

	invalidPaths := []string{
		"",
		"missin/leading/slash",
		"/no-container",
		"/empty-blob/",
	}

	for _, path := range invalidPaths {
		t.Run(path, func(t *testing.T) {
			u.Path = path
			uri, cred, err := azureAccessDetail(u, false)
			require.NotNil(t, err)
			require.Contains(t, err.Error(), "azure blob URI format:")
			require.Equal(t, "", uri)
			require.Nil(t, cred)
		})
	}
}

func Test_azureAccessDetail_bad_shared_cred(t *testing.T) {
	u := url.URL{
		Host: "storageaccount",
		Path: "/container/path/to/object",
		User: url.User("container-name"),
	}

	_ = os.Setenv("AZURE_STORAGE_ACCESS_KEY", "bad-access-key")
	uri, cred, err := azureAccessDetail(u, false)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to create Azure credential")
	require.Equal(t, "", uri)
	require.Nil(t, cred)
}

func Test_azureAccessDetail_good_anonymous_cred(t *testing.T) {
	u := url.URL{
		Host: "storageaccount.blob.core.windows.net",
		Path: "/path/to/object",
		User: url.User("container"),
	}
	// anonymous access by lack of environment variable
	_ = os.Unsetenv("AZURE_STORAGE_ACCESS_KEY")
	uri, cred, err := azureAccessDetail(u, false)
	require.Nil(t, err)
	require.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object", uri)
	require.Nil(t, cred)

	_ = os.Setenv("AZURE_STORAGE_ACCESS_KEY", "")
	uri, cred, err = azureAccessDetail(u, false)
	require.Nil(t, err)
	require.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object", uri)
	require.Nil(t, cred)

	// anonymous access by explicit setting
	randBytes := make([]byte, 64)
	_, err = rand.Read(randBytes)
	if err != nil {
		t.Fatalf("failed to setup test: %s", err.Error())
	}
	_ = os.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(randBytes))
	uri, cred, err = azureAccessDetail(u, true)
	require.Nil(t, err)
	require.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object", uri)
	require.Nil(t, cred)
}

func Test_azureAccessDetail_good_shared_cred(t *testing.T) {
	u := url.URL{
		Host: "storageaccount.blob.core.windows.net",
		Path: "/path/to/object",
		User: url.User("container"),
	}

	randBytes := make([]byte, 64)
	_, err := rand.Read(randBytes)
	if err != nil {
		t.Fatalf("failed to setup test: %s", err.Error())
	}
	dummyKey := base64.StdEncoding.EncodeToString(randBytes)
	_ = os.Setenv("AZURE_STORAGE_ACCESS_KEY", dummyKey)
	uri, cred, err := azureAccessDetail(u, false)
	require.Nil(t, err)
	require.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object", uri)
	require.Equal(t, "*exported.SharedKeyCredential", reflect.TypeOf(cred).String())
}

func Test_getBucketRegion(t *testing.T) {
	testCases := map[string]struct {
		profile string
		bucket  string
		public  bool
		errMsg  string
	}{
		"non-existent-bucket":  {"", uuid.New().String(), true, "not found"},
		"unable-to-get-region": {"", "localhost/something/does/not/matter", true, "unable to get region for S3 bucket"},
		"bucket-name-with-dot": {"", "xiehang.com", false, ""},
		"private-bucket":       {"", "doc-example-bucket", true, "S3 bucket doc-example-bucket is not public"},
		"aws-error":            {"", "00", true, "unrecognized StatusCode from AWS: 400"},
		"missing-credential":   {uuid.New().String(), "daylight-openstreetmap", false, "failed to get shared config profile"},
	}

	_ = os.Setenv("AWS_CONFIG_FILE", "/dev/null")
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			_ = os.Setenv("AWS_PROFILE", tc.profile)
			_, err := getS3Client(tc.bucket, tc.public)
			if tc.errMsg == "" {
				require.Nil(t, err)
			} else {
				require.NotNil(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_parseURI(t *testing.T) {
	testCases := map[string]struct {
		uri    string
		scheme string
		host   string
		path   string
		errMsg string
	}{
		"invalid-uri":    {"://uri", "", "", "", "unable to parse file location"},
		"with-user":      {"scheme://username@path/to/file", "scheme", "path", "/to/file", ""},
		"with-file":      {"file://path/to/file", "file", "", "path/to/file", ""},
		"with-file-root": {"file:///path/to/file", "file", "", "/path/to/file", ""},
		"without-file":   {"path/to/file", "file", "", "path/to/file", ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			u, err := parseURI(tc.uri)
			if tc.errMsg != "" {
				require.NotNil(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.Nil(t, err)
			require.Equal(t, tc.scheme, u.Scheme)
			require.Equal(t, tc.host, u.Host)
			require.Equal(t, tc.path, u.Path)
		})
	}
}

func Test_NewParquetFileReader_invalid_uri(t *testing.T) {
	option := ReadOption{}
	uri := "://uri"
	_, err := NewParquetFileReader(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_NewParquetFileReader(t *testing.T) {
	rOpt := ReadOption{}
	s3URL := "s3://daylight-openstreetmap/parquet/osm_features/release=v1.46/type=way/20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0"
	gcsURL := "gs://cloud-samples-data/bigquery/us-states/us-states.parquet"
	azblobURL := "wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet"
	testCases := map[string]struct {
		uri    string
		option ReadOption
		errMsg string
	}{
		"invalid-uri":            {"://uri", rOpt, "unable to parse file location"},
		"invalid-scheme":         {"invalid-scheme://something", rOpt, "unknown location scheme"},
		"local-file-not-found":   {"file://path/to/file", rOpt, "no such file or directory"},
		"local-file-not-parquet": {"../../testdata/not-a-parquet-file", rOpt, "invalid argument"},
		"local-file-good":        {"../../testdata/good.parquet", rOpt, ""},
		"s3-not-found":           {"s3://bucket-does-not-exist", rOpt, "not found"},
		"s3-good":                {s3URL, ReadOption{Anonymous: true}, ""},
		"s3-wrong-version":       {s3URL, ReadOption{ObjectVersion: "random-version-id", Anonymous: true}, "https response error StatusCode: 400"},
		"gcs-no-permission":      {gcsURL, rOpt, "failed to create GCS client"},
		"gcs-good":               {gcsURL, ReadOption{Anonymous: true}, ""},
		"azblob-no-permission":   {azblobURL, rOpt, "failed to open Azure blob object"},
		"azblob-good":            {azblobURL, ReadOption{Anonymous: true}, ""},
	}

	_ = os.Setenv("AWS_CONFIG_FILE", "/dev/null")
	_ = os.Unsetenv("AWS_PROFILE")
	_ = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")
	_ = os.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(uuid.New().NodeID()))
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			_, err := NewParquetFileReader(tc.uri, tc.option)
			if tc.errMsg == "" {
				require.Nil(t, err)
				return
			}
			require.NotNil(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func Test_NewParquetFileWriter_invalid_uri(t *testing.T) {
	option := WriteOption{}
	uri := "://uri"
	_, err := NewParquetFileWriter(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_NewParquetFileWriter_invalid_uri_scheme(t *testing.T) {
	option := WriteOption{}
	uri := "invalid-scheme://something"
	_, err := NewParquetFileWriter(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unknown location scheme")
}

func Test_NewParquetFileWriter_local_not_a_file(t *testing.T) {
	option := WriteOption{}
	uri := "../../testdata/"
	_, err := NewParquetFileWriter(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "is a directory")
}

func Test_NewParquetFileWriter_local_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	option := WriteOption{}
	uri := filepath.Join(tempDir, "file-writer.parquet")
	fw, err := NewParquetFileWriter(uri, option)
	require.Nil(t, err)
	require.NotNil(t, fw)
	_ = fw.Close()
}

func Test_NewParquetFileWriter_s3_non_existent_bucket(t *testing.T) {
	option := WriteOption{}
	intVal, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	uri := fmt.Sprintf("s3://bucket-does-not-exist-%d", intVal.Int64())
	_, err := NewParquetFileWriter(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to access to")
}

func Test_NewParquetFileWriter_s3_good(t *testing.T) {
	// Make sure there is no AWS access
	_ = os.Setenv("AWS_CONFIG_FILE", "/dev/null")
	_ = os.Unsetenv("AWS_PROFILE")

	// parquet writer does not actually write to destination immediately
	option := WriteOption{}
	uri := "s3://daylight-openstreetmap/parquet/osm_features/release=v1.46/type=way/20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0"
	fw, err := NewParquetFileWriter(uri, option)
	require.Nil(t, err)
	require.NotNil(t, fw)
	_ = fw.Close()
}

func Test_NewParquetFileWriter_gcs_no_permission(t *testing.T) {
	// Make sure there is no GCS access
	_ = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")

	// parquet writer does not actually write to destination immediately
	option := WriteOption{}
	uri := "gs://cloud-samples-data/bigquery/us-states/us-states.parquet"
	_, err := NewParquetFileWriter(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open GCS object")
}

func Test_NewParquetFileWriter_azblob_invalid_url(t *testing.T) {
	// Make sure there is no Azure blob access
	randBytes := make([]byte, 64)
	_, err := rand.Read(randBytes)
	if err != nil {
		t.Fatalf("failed to setup test: %s", err.Error())
	}
	_ = os.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(randBytes))

	option := WriteOption{}
	uri := "wasbs://bad/url"
	_, err = NewParquetFileWriter(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "azure blob URI format:")

	uri = "wasbs://storageaccount.blob.core.windows.net//aa"
	_, err = NewParquetFileWriter(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "azure blob URI format:")
}

func Test_NewParquetFileWriter_azblob_good(t *testing.T) {
	// Make sure there is no Azure blob access
	randBytes := make([]byte, 64)
	_, err := rand.Read(randBytes)
	if err != nil {
		t.Fatalf("failed to setup test: %s", err.Error())
	}
	_ = os.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(randBytes))

	option := WriteOption{}
	uri := "wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/foobar.parquet"

	// permission will be checked at close/flush time
	_, err = NewParquetFileWriter(uri, option)
	require.Nil(t, err)
}

func Test_NewParquetFileWriter_http_not_supported(t *testing.T) {
	option := WriteOption{}
	uri := "https://domain.tld/path/to/file"
	_, err := NewParquetFileWriter(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "writing to https endpoint is not currently supported")
}

func Test_NewCSVWriter_invalid_uri(t *testing.T) {
	option := WriteOption{}
	uri := "://uri"
	_, err := NewCSVWriter(uri, option, []string{"name=Id, type=INT64"})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_NewCSVWriter_invalid_schema(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	option := WriteOption{}
	uri := filepath.Join(tempDir, "csv-writer.parquet")
	_, err := NewCSVWriter(uri, option, []string{"invalid schema"})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "expect 'key=value'")

	_, err = NewCSVWriter(uri, option, []string{"name=Id"})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "not a valid Type string")
}

func Test_NewCSVWriter_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	option := WriteOption{}
	option.Compression = "LZ4_RAW"
	uri := filepath.Join(tempDir, "csv-writer.parquet")
	pw, err := NewCSVWriter(uri, option, []string{"name=Id, type=INT64"})
	require.Nil(t, err)
	require.NotNil(t, pw)
	_ = pw.PFile.Close()
}

func Test_NewJSONWriter_invalid_uri(t *testing.T) {
	option := WriteOption{}
	uri := "://uri"
	_, err := NewJSONWriter(uri, option, "")
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_NewJSONWriter_invalid_schema(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	option := WriteOption{}
	uri := filepath.Join(tempDir, "json-writer.parquet")
	_, err := NewJSONWriter(uri, option, "invalid schema")
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "error in unmarshalling json schema string:")

	_, err = NewJSONWriter(uri, option, `{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=id, type=FOOBAR"}]}`)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "type FOOBAR: not a valid Type string")
}

func Test_NewJSONWriter_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	option := WriteOption{}
	option.Compression = "ZSTD"
	uri := filepath.Join(tempDir, "json-writer.parquet")
	pw, err := NewJSONWriter(uri, option, `{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=id, type=INT64"}]}`)
	require.Nil(t, err)
	require.NotNil(t, pw)
	_ = pw.PFile.Close()
}

func Test_NewGenericWriter_bad(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "generic-writer")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	testCases := map[string]struct {
		uri    string
		option WriteOption
		schema string
		errMsg string
	}{
		"invalud-uri":         {"://uri", WriteOption{}, "", "unable to parse file location"},
		"schema-not-json":     {filepath.Join(tempDir, "dummy"), WriteOption{}, "invalid schema", "error in unmarshalling json schema string:"},
		"schema-invalid":      {filepath.Join(tempDir, "dummy"), WriteOption{}, `{"Tag":"name=root","Fields":[{"Tag":"name=id, type=FOOBAR"}]}`, "type FOOBAR: not a valid Type string"},
		"invalid-compression": {filepath.Join(tempDir, "dummy"), WriteOption{Compression: "FOOBAR"}, `{"Tag":"name=root","Fields":[{"Tag":"name=id, type=INT64"}]}`, "not a valid CompressionCodec string"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pw, err := NewGenericWriter(tc.uri, tc.option, tc.schema)
			require.NotNil(t, err)
			require.Nil(t, pw)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func Test_NewGenericWriter_good(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	option := WriteOption{}
	option.Compression = "ZSTD"
	uri := filepath.Join(tempDir, "json-writer.parquet")
	pw, err := NewGenericWriter(uri, option, `{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=id, type=INT64"}]}`)
	require.Nil(t, err)
	require.NotNil(t, pw)
	defer func() {
		_ = pw.PFile.Close()
	}()
}

func Test_NewParquetFileReader_http_bad_url(t *testing.T) {
	option := ReadOption{}
	uri := "https://no-such-host.tld/"
	option.HTTPMultipleConnection = true
	option.HTTPIgnoreTLSError = true
	option.HTTPExtraHeaders = map[string]string{"key": "value"}
	_, err := NewParquetFileReader(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "no such host")
}

func Test_NewParquetFileReader_http_no_range_support(t *testing.T) {
	option := ReadOption{}
	uri := "https://www.google.com/"
	option.HTTPMultipleConnection = false
	option.HTTPIgnoreTLSError = true
	option.HTTPExtraHeaders = map[string]string{"key": "value"}
	_, err := NewParquetFileReader(uri, option)

	require.NotNil(t, err)
	require.Contains(t, err.Error(), "does not support range")
}

func Test_NewParquetFileReader_http_good(t *testing.T) {
	option := ReadOption{}
	uri := "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
	option.HTTPMultipleConnection = true
	option.HTTPIgnoreTLSError = false
	option.HTTPExtraHeaders = map[string]string{"key": "value"}
	_, err := NewParquetFileReader(uri, option)
	require.Nil(t, err)
}

func Test_NewParquetFileReader_hdfs_bad(t *testing.T) {
	option := ReadOption{}
	uri := "hdfs://localhost:1/temp/good.parquet"
	_, err := NewParquetFileReader(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "connection refused")
}

func Test_newParquetFileWriter_hdfs_bad(t *testing.T) {
	option := WriteOption{}
	uri := "hdfs://localhost:1/temp/good.parquet"
	_, err := NewParquetFileWriter(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "connection refused")
}

func Test_NewCSVWriter_invalid_compression_codec(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	option := WriteOption{}
	option.Compression = "foobar"
	uri := filepath.Join(tempDir, "csv-writer.parquet")
	pw, err := NewCSVWriter(uri, option, []string{"name=Id, type=INT64"})
	require.NotNil(t, err)
	require.Nil(t, pw)
	require.Contains(t, "not a valid CompressionCodec string", err.Error())
}

func Test_NewJSONWriter_invalid_compression_codec(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	option := WriteOption{}
	option.Compression = "random-dude"
	uri := filepath.Join(tempDir, "json-writer.parquet")
	pw, err := NewJSONWriter(uri, option, `{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=id, type=INT64"}]}`)
	require.NotNil(t, err)
	require.Nil(t, pw)
	require.Contains(t, "not a valid CompressionCodec string", err.Error())
}

var unsupportedCodec = []string{
	"BROTLI",
	"LZO",
}

func Test_NewCSVWriter_unsupported_compression_codec(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	option := WriteOption{}

	for _, codec := range unsupportedCodec {
		option.Compression = codec
		uri := filepath.Join(tempDir, "csv-writer.parquet")
		pw, err := NewCSVWriter(uri, option, []string{"name=Id, type=INT64"})
		require.NotNil(t, err)
		require.Nil(t, pw)
		require.Contains(t, err.Error(), "compression is not supported at this moment")
	}
}

func Test_NewJSONWriter_unsupported_compression_codec(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	option := WriteOption{}
	for _, codec := range unsupportedCodec {
		option.Compression = codec
		uri := filepath.Join(tempDir, "json-writer.parquet")
		pw, err := NewJSONWriter(uri, option, `{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=id, type=INT64"}]}`)
		require.NotNil(t, err)
		require.Nil(t, pw)
		require.Contains(t, err.Error(), "compression is not supported at this moment")
	}
}
