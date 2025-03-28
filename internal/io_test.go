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

func Test_getBucketRegion_s3_non_existent_bucket(t *testing.T) {
	intVal, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	_, err := getS3Client(fmt.Sprintf("bucket-does-not-exist-%d", intVal.Int64()), true)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "not found")
}

func Test_getBucketRegion_s3_unable_to_get_region(t *testing.T) {
	_, err := getS3Client("localhost/something/does/not/matter", true)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to get region for S3 bucket")
}

func Test_getBucketRegion_s3_bucket_name_with_dot(t *testing.T) {
	_, err := getS3Client("xiehang.com", false)
	require.Nil(t, err)
}

func Test_getBucketRegion_s3_private_bucket(t *testing.T) {
	_, err := getS3Client("doc-example-bucket", true)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "S3 bucket doc-example-bucket is not public")
}

func Test_getBucketRegion_s3_aws_error(t *testing.T) {
	// AWS bucket name needs to be between 3 and 63 characters
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
	_, err := getS3Client("00", true)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unrecognized StatusCode from AWS: 400")
}

func Test_getBucketRegion_s3_missing_credential(t *testing.T) {
	// AWS provides open access: https://registry.opendata.aws/daylight-osm/
	intVal, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	_ = os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", intVal.Int64()))
	_, err := getS3Client("daylight-openstreetmap", false)
	// since aws-go-sdk-v2/config 1.18.45, non-existent profile becomes an error
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to get shared config profile")
}

func Test_parseURI_invalid_uri(t *testing.T) {
	_, err := parseURI("://uri")
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_parseURI_good(t *testing.T) {
	u, err := parseURI("scheme://username@path/to/file")
	require.Nil(t, err)
	require.Equal(t, "scheme", u.Scheme)
	require.Equal(t, "path", u.Host)
	require.Equal(t, "/to/file", u.Path)

	u, err = parseURI("path/to/file")
	require.Nil(t, err)
	require.Equal(t, schemeLocal, u.Scheme)
	require.Equal(t, "", u.Host)
	require.Equal(t, "path/to/file", u.Path)

	u, err = parseURI("file://path/to/file")
	require.Nil(t, err)
	require.Equal(t, schemeLocal, u.Scheme)
	require.Equal(t, "", u.Host)
	require.Equal(t, "path/to/file", u.Path)
}

func Test_NewParquetFileReader_invalid_uri(t *testing.T) {
	option := ReadOption{}
	uri := "://uri"
	_, err := NewParquetFileReader(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_NewParquetFileReader_invalid_uri_scheme(t *testing.T) {
	option := ReadOption{}
	uri := "invalid-scheme://something"
	_, err := NewParquetFileReader(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unknown location scheme")
}

func Test_NewParquetFileReader_local_non_existent_file(t *testing.T) {
	option := ReadOption{}
	uri := "file/does/not/exist"
	_, err := NewParquetFileReader(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "no such file or directory")
}

func Test_NewParquetFileReader_local_not_parquet(t *testing.T) {
	option := ReadOption{}
	uri := "../testdata/not-a-parquet-file"
	_, err := NewParquetFileReader(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "invalid argument")
}

func Test_NewParquetFileReader_local_good(t *testing.T) {
	option := ReadOption{}
	uri := "../testdata/good.parquet"
	pr, err := NewParquetFileReader(uri, option)
	require.Nil(t, err)
	require.NotNil(t, pr)
	_ = pr.PFile.Close()
}

func Test_NewParquetFileReader_s3_non_existent(t *testing.T) {
	// Make sure there is no AWS access
	_ = os.Setenv("AWS_CONFIG_FILE", "/dev/null")
	_ = os.Unsetenv("AWS_PROFILE")

	option := ReadOption{Anonymous: true}
	intVal, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	uri := fmt.Sprintf("s3://bucket-does-not-exist-%d", intVal.Int64())
	_, err := NewParquetFileReader(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "not found")
}

func Test_NewParquetFileReader_s3_good(t *testing.T) {
	// Make sure there is no AWS access
	_ = os.Setenv("AWS_CONFIG_FILE", "/dev/null")
	_ = os.Unsetenv("AWS_PROFILE")

	option := ReadOption{Anonymous: true}
	uri := "s3://daylight-openstreetmap/parquet/osm_features/release=v1.46/type=way/20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0"
	_, err := NewParquetFileReader(uri, option)
	require.Nil(t, err)
}

func Test_NewParquetFileReader_s3_non_existent_versioned(t *testing.T) {
	// Make sure there is no AWS access
	_ = os.Setenv("AWS_CONFIG_FILE", "/dev/null")
	_ = os.Unsetenv("AWS_PROFILE")

	option := ReadOption{ObjectVersion: "random-version-id", Anonymous: true}
	uri := "s3://daylight-openstreetmap/parquet/osm_features/release=v1.46/type=way/20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0"
	_, err := NewParquetFileReader(uri, option)
	require.NotNil(t, err)
	// refer to https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
	// the sample data bucket does not have version enabled so it will return HTTP/400
	require.Contains(t, err.Error(), "https response error StatusCode: 400")
}

func Test_NewParquetFileReader_gcs_no_permission(t *testing.T) {
	// Make sure there is no GCS access
	_ = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")

	option := ReadOption{}
	uri := "gs://cloud-samples-data/bigquery/us-states/us-states.parquet"
	_, err := NewParquetFileReader(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to create GCS client")
}

func Test_NewParquetFileReader_gcs_good(t *testing.T) {
	// Make sure there is no GCS access
	_ = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")

	option := ReadOption{Anonymous: true}
	uri := "gs://cloud-samples-data/bigquery/us-states/us-states.parquet"
	_, err := NewParquetFileReader(uri, option)
	require.Nil(t, err)
}

func Test_NewParquetFileReader_azblob_no_permission(t *testing.T) {
	// Use a faked access key so anonymous access will fail
	randBytes := make([]byte, 64)
	_, err := rand.Read(randBytes)
	if err != nil {
		t.Fatalf("failed to setup test: %s", err.Error())
	}
	_ = os.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(randBytes))

	option := ReadOption{}
	uri := "wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet"
	_, err = NewParquetFileReader(uri, option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open Azure blob object")
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
	uri := "../testdata/"
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

func Test_NewGenericWriter_invalid_uri(t *testing.T) {
	option := WriteOption{}
	uri := "://uri"
	_, err := NewGenericWriter(uri, option, "")
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_NewGenericWriter_invalid_schema(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	option := WriteOption{}
	uri := filepath.Join(tempDir, "json-writer.parquet")
	_, err := NewGenericWriter(uri, option, "invalid schema")
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "error in unmarshalling json schema string:")

	_, err = NewGenericWriter(uri, option, `{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=id, type=FOOBAR"}]}`)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "type FOOBAR: not a valid Type string")
}

func Test_NewGenericWriter_invalid_compression(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	option := WriteOption{Compression: "dummy"}
	uri := filepath.Join(tempDir, "json-writer.parquet")
	_, err := NewGenericWriter(uri, option, `{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=id, type=INT64"}]}`)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "not a valid CompressionCodec string")
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
