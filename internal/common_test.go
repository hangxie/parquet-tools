package internal

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"math"
	"math/big"
	"net/url"
	"os"
	"reflect"
	"testing"

	"github.com/hangxie/parquet-tools/cmd"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go/types"
)

func Test_azureAccessDetail_invalid_uri(t *testing.T) {
	u := url.URL{
		Host: "",
		Path: "",
	}
	os.Unsetenv("AZURE_STORAGE_ACCESS_KEY")

	uri, cred, err := azureAccessDetail(u, false)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "azure blob URI format:")
	require.Equal(t, "", uri)
	require.Nil(t, cred)

	u.Host = "storageacconut"
	u.Path = "missin/leading/slash"
	_, _, err = azureAccessDetail(u, false)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "azure blob URI format:")

	u.Host = "storageacconut"
	u.Path = "/no-container"
	_, _, err = azureAccessDetail(u, false)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "azure blob URI format:")

	u.Host = "storageacconut"
	u.Path = "/empty-blob/"
	_, _, err = azureAccessDetail(u, false)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "azure blob URI format:")
}

func Test_azureAccessDetail_bad_shared_cred(t *testing.T) {
	u := url.URL{
		Host: "storageaccount",
		Path: "/container/path/to/object",
		User: url.User("container-name"),
	}

	os.Setenv("AZURE_STORAGE_ACCESS_KEY", "bad-access-key")
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
	os.Unsetenv("AZURE_STORAGE_ACCESS_KEY")
	uri, cred, err := azureAccessDetail(u, false)
	require.Nil(t, err)
	require.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object", uri)
	require.Nil(t, cred)

	os.Setenv("AZURE_STORAGE_ACCESS_KEY", "")
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
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(randBytes))
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
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", dummyKey)
	uri, cred, err := azureAccessDetail(u, false)
	require.Nil(t, err)
	require.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object", uri)
	require.Equal(t, "*azblob.SharedKeyCredential", reflect.TypeOf(cred).String())
}

func Test_getBucketRegion_s3_non_existent_bucket(t *testing.T) {
	intVal, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	_, err := getS3Client(fmt.Sprintf("bucket-does-not-exist-%d", intVal.Int64()), true)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to find region of bucket [bucket-does-not-exist-")
}

func Test_getBucketRegion_s3_missing_credential(t *testing.T) {
	intVal, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", intVal.Int64()))
	_, err := getS3Client("aws-roda-hcls-datalake", false)
	// private bucket error happens at reading time
	require.Nil(t, err)
}

func Test_getBucketRegion_aws_error(t *testing.T) {
	_, err := getS3Client("*&^%", true)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "AWS error:")
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

// newParquetFileReader
func Test_newParquetFileReader_invalid_uri(t *testing.T) {
	option := cmd.ReadOption{}
	option.URI = "://uri"
	_, err := newParquetFileReader(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_newParquetFileReader_invalid_uri_scheme(t *testing.T) {
	option := cmd.ReadOption{}
	option.URI = "invalid-scheme://something"
	_, err := newParquetFileReader(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unknown location scheme")
}

func Test_newParquetFileReader_local_non_existent_file(t *testing.T) {
	option := cmd.ReadOption{}
	option.URI = "file/does/not/exist"
	_, err := newParquetFileReader(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "no such file or directory")
}

func Test_newParquetFileReader_local_not_parquet(t *testing.T) {
	option := cmd.ReadOption{}
	option.URI = "../testdata/not-a-parquet-file"
	_, err := newParquetFileReader(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "invalid argument")
}

func Test_newParquetFileReader_local_good(t *testing.T) {
	option := cmd.ReadOption{}
	option.URI = "../testdata/good.parquet"
	pr, err := newParquetFileReader(option)
	require.Nil(t, err)
	require.NotNil(t, pr)
	pr.PFile.Close()
}

func Test_newParquetFileReader_s3_aws_error(t *testing.T) {
	// Make sure there is no AWS access
	intVal, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", intVal.Int64()))

	option := cmd.ReadOption{}
	option.URI = "s3:///path/to/object"
	_, err := newParquetFileReader(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "AWS error:")
}

func Test_newParquetFileReader_s3_good(t *testing.T) {
	// Make sure there is no AWS access
	intVal, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", intVal.Int64()))
	option := cmd.ReadOption{Anonymous: true}
	option.URI = "s3://aws-roda-hcls-datalake/gnomad/chrm/run-DataSink0-1-part-block-0-r-00000-snappy.parquet"
	_, err := newParquetFileReader(option)
	require.Nil(t, err)
}

func Test_newParquetFileReader_s3_non_existent_versioned(t *testing.T) {
	// Make sure there is no AWS access
	intVal, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", intVal.Int64()))

	option := cmd.ReadOption{ObjectVersion: "random-version-id", Anonymous: true}
	option.URI = "s3://aws-roda-hcls-datalake/gnomad/chrm/run-DataSink0-1-part-block-0-r-00000-snappy.parquet"
	_, err := newParquetFileReader(option)
	require.NotNil(t, err)
	// refer to https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
	// access to non-existent object/version without ListBucket permission will get 403 instead of 404
	require.Contains(t, err.Error(), "https response error StatusCode: 403")
}

func Test_newParquetFileReader_gcs_no_permission(t *testing.T) {
	// Make sure there is no GCS access
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")

	option := cmd.ReadOption{}
	option.URI = "gs://cloud-samples-data/bigquery/us-states/us-states.parquet"
	_, err := newParquetFileReader(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open GCS object")
}

func Test_newParquetFileReader_azblob_no_permission(t *testing.T) {
	// Use a faked access key so anonymous access will fail
	randBytes := make([]byte, 64)
	_, err := rand.Read(randBytes)
	if err != nil {
		t.Fatalf("failed to setup test: %s", err.Error())
	}
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(randBytes))

	option := cmd.ReadOption{}
	option.URI = "wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet"
	_, err = newParquetFileReader(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open Azure blob object")
}

// newFileWriter
func Test_newFileWriter_invalid_uri(t *testing.T) {
	option := cmd.WriteOption{}
	option.URI = "://uri"
	_, err := newParquetFileWriter(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_newFileWriter_invalid_uri_scheme(t *testing.T) {
	option := cmd.WriteOption{}
	option.URI = "invalid-scheme://something"
	_, err := newParquetFileWriter(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unknown location scheme")
}

func Test_newFileWriter_local_not_a_file(t *testing.T) {
	option := cmd.WriteOption{}
	option.URI = "../testdata/"
	_, err := newParquetFileWriter(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "is a directory")
}

func Test_newFileWriter_local_good(t *testing.T) {
	option := cmd.WriteOption{}
	option.URI = os.TempDir() + "/file-writer.parquet"
	fw, err := newParquetFileWriter(option)
	require.Nil(t, err)
	require.NotNil(t, fw)
	fw.Close()
}

func Test_newFileWriter_s3_non_existent_bucket(t *testing.T) {
	option := cmd.WriteOption{}
	intVal, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	option.URI = fmt.Sprintf("s3://bucket-does-not-exist-%d", intVal.Int64())
	_, err := newParquetFileWriter(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to find region of bucket [bucket-does-not-exist-")
}

func Test_newFileWriter_s3_good(t *testing.T) {
	// Make sure there is no AWS access
	intVal, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", intVal.Int64()))

	// parquet writer does not actually write to destination immediately
	option := cmd.WriteOption{}
	option.URI = "s3://aws-roda-hcls-datalake/gnomad/chrm/run-DataSink0-1-part-block-0-r-00000-snappy.parquet"
	fw, err := newParquetFileWriter(option)
	require.Nil(t, err)
	require.NotNil(t, fw)
	fw.Close()
}

func Test_newFileWriter_gcs_no_permission(t *testing.T) {
	// Make sure there is no GCS access
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")

	// parquet writer does not actually write to destination immediately
	option := cmd.WriteOption{}
	option.URI = "gs://cloud-samples-data/bigquery/us-states/us-states.parquet"
	_, err := newParquetFileWriter(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open GCS object")
}

func Test_newFileWriter_azblob_invalid_url(t *testing.T) {
	// Make sure there is no Azure blob access
	randBytes := make([]byte, 64)
	_, err := rand.Read(randBytes)
	if err != nil {
		t.Fatalf("failed to setup test: %s", err.Error())
	}
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(randBytes))

	option := cmd.WriteOption{}
	option.URI = "wasbs://bad/url"
	_, err = newParquetFileWriter(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "azure blob URI format:")
}

func Test_newFileWriter_http_not_supported(t *testing.T) {
	option := cmd.WriteOption{}
	option.URI = "https://domain.tld/path/to/file"
	_, err := newParquetFileWriter(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "writing to https endpoint is not currently supported")
}

// newCSVWriter
func Test_newCSVWriter_invalid_uri(t *testing.T) {
	option := cmd.WriteOption{}
	option.URI = "://uri"
	_, err := NewCSVWriter(option, []string{"name=Id, type=INT64"})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unable to parse file location")
}

func Test_newCSVWriter_invalid_schema(t *testing.T) {
	option := cmd.WriteOption{}
	option.URI = os.TempDir() + "/csv-writer.parquet"
	_, err := NewCSVWriter(option, []string{"invalid schema"})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "expect 'key=value'")

	_, err = NewCSVWriter(option, []string{"name=Id"})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "not a valid Type string")
}

func Test_newCSVWriter_good(t *testing.T) {
	option := cmd.WriteOption{}
	option.URI = os.TempDir() + "/csv-writer.parquet"
	pw, err := NewCSVWriter(option, []string{"name=Id, type=INT64"})
	require.NotNil(t, pw)
	require.Nil(t, err)
}

func Test_decimalToFloat_nil(t *testing.T) {
	f64, err := decimalToFloat(ReinterpretField{}, nil)
	require.Nil(t, err)
	require.Nil(t, f64)
}

func Test_decimalToFloat_int32(t *testing.T) {
	fieldAttr := ReinterpretField{
		scale: 2,
	}
	f64, err := decimalToFloat(fieldAttr, int32(0))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 0.0, *f64)

	f64, err = decimalToFloat(fieldAttr, int32(11))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 0.11, *f64)

	f64, err = decimalToFloat(fieldAttr, int32(222))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 2.22, *f64)

	f64, err = decimalToFloat(fieldAttr, int32(-11))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, -0.11, *f64)

	f64, err = decimalToFloat(fieldAttr, int32(-222))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, -2.22, *f64)
}

func Test_decimalToFloat_int64(t *testing.T) {
	fieldAttr := ReinterpretField{
		scale: 2,
	}
	f64, err := decimalToFloat(fieldAttr, int64(0))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 0.0, *f64)

	f64, err = decimalToFloat(fieldAttr, int64(11))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 0.11, *f64)

	f64, err = decimalToFloat(fieldAttr, int64(222))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 2.22, *f64)

	f64, err = decimalToFloat(fieldAttr, int64(-11))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, -0.11, *f64)

	f64, err = decimalToFloat(fieldAttr, int64(-222))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, -2.22, *f64)
}

func Test_decimalToFloat_string(t *testing.T) {
	fieldAttr := ReinterpretField{
		scale:     2,
		precision: 10,
	}

	f64, err := decimalToFloat(fieldAttr, types.StrIntToBinary("000", "BigEndian", 0, true))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 0.0, *f64)

	f64, err = decimalToFloat(fieldAttr, types.StrIntToBinary("011", "BigEndian", 0, true))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 0.11, *f64)

	f64, err = decimalToFloat(fieldAttr, types.StrIntToBinary("222", "BigEndian", 0, true))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, 2.22, *f64)

	f64, err = decimalToFloat(fieldAttr, types.StrIntToBinary("-011", "BigEndian", 0, true))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, -0.11, *f64)

	f64, err = decimalToFloat(fieldAttr, types.StrIntToBinary("-222", "BigEndian", 0, true))
	require.Nil(t, err)
	require.NotNil(t, f64)
	require.Equal(t, -2.22, *f64)
}

func Test_decimalToFloat_invalid_type(t *testing.T) {
	fieldAttr := ReinterpretField{}

	f64, err := decimalToFloat(fieldAttr, int(0))
	require.NotNil(t, err)
	require.Equal(t, "unknown type: int", err.Error())
	require.Nil(t, f64)

	f64, err = decimalToFloat(fieldAttr, float32(0.0))
	require.NotNil(t, err)
	require.Equal(t, "unknown type: float32", err.Error())
	require.Nil(t, f64)

	f64, err = decimalToFloat(fieldAttr, float64(0.0))
	require.NotNil(t, err)
	require.Equal(t, "unknown type: float64", err.Error())
	require.Nil(t, f64)
}

func Test_newParquetFileReader_http_bad_url(t *testing.T) {
	option := cmd.ReadOption{}
	option.URI = "https://no-such-host.tld/"
	option.HTTPMultipleConnection = true
	option.HTTPIgnoreTLSError = true
	option.HTTPExtraHeaders = map[string]string{"key": "value"}
	_, err := newParquetFileReader(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "no such host")
}

func Test_newParquetFileReader_http_no_range_support(t *testing.T) {
	option := cmd.ReadOption{}
	option.URI = "https://www.google.com/"
	option.HTTPMultipleConnection = false
	option.HTTPIgnoreTLSError = true
	option.HTTPExtraHeaders = map[string]string{"key": "value"}
	_, err := newParquetFileReader(option)

	require.NotNil(t, err)
	require.Contains(t, err.Error(), "does not support range")
}

func Test_newParquetFileReader_http_good(t *testing.T) {
	option := cmd.ReadOption{}
	option.URI = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
	option.HTTPMultipleConnection = true
	option.HTTPIgnoreTLSError = false
	option.HTTPExtraHeaders = map[string]string{"key": "value"}
	_, err := newParquetFileReader(option)
	require.Nil(t, err)
}

func Test_newParquetFileReader_hdfs_bad(t *testing.T) {
	option := cmd.ReadOption{}
	option.URI = "hdfs://localhost:1/temp/good.parquet"
	_, err := newParquetFileReader(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "connection refused")
}

func Test_newParquetFileWriter_hdfs_bad(t *testing.T) {
	option := cmd.WriteOption{}
	option.URI = "hdfs://localhost:1/temp/good.parquet"
	_, err := newParquetFileWriter(option)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "connection refused")
}
