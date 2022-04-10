package cmd

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go/types"
)

// this for unit test only
func captureStdoutStderr(f func()) (string, string) {
	savedStdout := os.Stdout
	savedStderr := os.Stderr

	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()
	os.Stdout = wOut
	os.Stderr = wErr
	f()
	wOut.Close()
	wErr.Close()
	stdout, _ := ioutil.ReadAll(rOut)
	stderr, _ := ioutil.ReadAll(rErr)
	rOut.Close()
	rErr.Close()

	os.Stdout = savedStdout
	os.Stderr = savedStderr

	return string(stdout), string(stderr)
}

// this for unit test only
func loadExpected(t *testing.T, fileName string) string {
	fd, err := os.Open(fileName)
	if err != nil {
		t.Fatal("cannot open golden file:", fileName, "because of:", err.Error())
	}
	buf, err := ioutil.ReadAll(fd)
	if err != nil {
		t.Fatal("cannot load golden file:", fileName, "because of:", err.Error())
	}
	if !strings.HasSuffix(fileName, ".json") && !strings.HasSuffix(fileName, ".jsonl") {
		return string(buf)
	}

	// JSON and JSONL golden files are formatted by jq
	var result string
	currentBuf := []byte{}
	for _, line := range bytes.Split(buf, []byte("\n")) {
		// in jq format, if the first character is not space than it's
		// start (when currentBuf is empty) or end of an object (when
		// currentBuf is not empty)
		endOfObject := len(line) > 0 && line[0] != ' ' && len(currentBuf) != 0
		currentBuf = append(currentBuf, line...)
		if endOfObject {
			dst := new(bytes.Buffer)
			if err := json.Compact(dst, currentBuf); err != nil {
				t.Fatal("cannot parse golden file:", fileName, "because of:", err.Error())
			}
			result += dst.String() + "\n"
			currentBuf = []byte{}
		}
	}
	return result
}

func Test_common_azureAccessDetail_invalid_uri(t *testing.T) {
	u := url.URL{
		Host: "",
		Path: "",
	}
	os.Unsetenv("AZURE_STORAGE_ACCESS_KEY")

	uri, cred, err := azureAccessDetail(u)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "azure blob URI format:")
	assert.Equal(t, "", uri)
	assert.Nil(t, cred)

	u.Host = "storageacconut"
	u.Path = "missin/leading/slash"
	_, _, err = azureAccessDetail(u)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "azure blob URI format:")

	u.Host = "storageacconut"
	u.Path = "/no-container"
	_, _, err = azureAccessDetail(u)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "azure blob URI format:")

	u.Host = "storageacconut"
	u.Path = "/empty-blob/"
	_, _, err = azureAccessDetail(u)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "azure blob URI format:")
}

func Test_common_azureAccessDetail_bad_shared_cred(t *testing.T) {
	u := url.URL{
		Host: "storageaccount",
		Path: "/container/path/to/object",
		User: url.User("container-name"),
	}

	os.Setenv("AZURE_STORAGE_ACCESS_KEY", "bad-access-key")
	uri, cred, err := azureAccessDetail(u)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to create Azure credential")
	assert.Equal(t, "", uri)
	assert.Nil(t, cred)
}

func Test_common_azureAccessDetail_good_anonymous_cred(t *testing.T) {
	u := url.URL{
		Host: "storageaccount.blob.core.windows.net",
		Path: "/path/to/object",
		User: url.User("container"),
	}
	os.Unsetenv("AZURE_STORAGE_ACCESS_KEY")
	uri, cred, err := azureAccessDetail(u)
	assert.Nil(t, err)
	assert.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object", uri)
	assert.Equal(t, "*azblob.anonymousCredentialPolicyFactory", reflect.TypeOf(cred).String())

	os.Setenv("AZURE_STORAGE_ACCESS_KEY", "")
	uri, cred, err = azureAccessDetail(u)
	assert.Nil(t, err)
	assert.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object", uri)
	assert.Equal(t, "*azblob.anonymousCredentialPolicyFactory", reflect.TypeOf(cred).String())
}

func Test_common_azureAccessDetail_good_shared_cred(t *testing.T) {
	u := url.URL{
		Host: "storageaccount.blob.core.windows.net",
		Path: "/path/to/object",
		User: url.User("container"),
	}

	randBytes := make([]byte, 64)
	rand.Read(randBytes)
	dummyKey := base64.StdEncoding.EncodeToString(randBytes)
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", dummyKey)
	uri, cred, err := azureAccessDetail(u)
	assert.Nil(t, err)
	assert.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object", uri)
	assert.Equal(t, "*azblob.SharedKeyCredential", reflect.TypeOf(cred).String())
}

func Test_common_getBucketRegion_s3_non_existent_bucket(t *testing.T) {
	_, err := getS3Client(fmt.Sprintf("bucket-does-not-exist-%d", rand.Int63()), true)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to find region of bucket [bucket-does-not-exist-")
}

func Test_common_getBucketRegion_s3_missing_credential(t *testing.T) {
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))
	t.Logf("dummy AWS_PROFILE: %s\n", os.Getenv("AWS_PROFILE"))
	_, err := getS3Client("aws-roda-hcls-datalake", false)
	// private bucket error happens at reading time
	assert.Nil(t, err)
}

func Test_common_getBucketRegion_aws_error(t *testing.T) {
	_, err := getS3Client("*&^%", true)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "AWS error:")
}

func Test_common_parseURI_invalid_uri(t *testing.T) {
	_, err := parseURI("://uri")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_common_parseURI_good(t *testing.T) {
	u, err := parseURI("scheme://path/to/file")
	assert.Nil(t, err)
	assert.Equal(t, "scheme", u.Scheme)
	assert.Equal(t, "path", u.Host)
	assert.Equal(t, "/to/file", u.Path)

	u, err = parseURI("path/to/file")
	assert.Nil(t, err)
	assert.Equal(t, "file", u.Scheme)
	assert.Equal(t, "", u.Host)
	assert.Equal(t, "path/to/file", u.Path)

	u, err = parseURI("file://path/to/file")
	assert.Nil(t, err)
	assert.Equal(t, "file", u.Scheme)
	assert.Equal(t, "", u.Host)
	assert.Equal(t, "path/to/file", u.Path)
}

// newParquetFileReader
func Test_common_newParquetFileReader_invalid_uri(t *testing.T) {
	option := ReadOption{}
	option.URI = "://uri"
	_, err := newParquetFileReader(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_common_newParquetFileReader_invalid_uri_scheme(t *testing.T) {
	option := ReadOption{}
	option.URI = "invalid-scheme://something"
	_, err := newParquetFileReader(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unknown location scheme")
}

func Test_common_newParquetFileReader_local_non_existent_file(t *testing.T) {
	option := ReadOption{}
	option.URI = "file/does/not/exist"
	_, err := newParquetFileReader(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "no such file or directory")
}

func Test_common_newParquetFileReader_local_not_parquet(t *testing.T) {
	option := ReadOption{}
	option.URI = "testdata/not-a-parquet-file"
	_, err := newParquetFileReader(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid argument")
}

func Test_common_newParquetFileReader_local_good(t *testing.T) {
	option := ReadOption{}
	option.URI = "testdata/good.parquet"
	pr, err := newParquetFileReader(option)
	assert.Nil(t, err)
	assert.NotNil(t, pr)
	pr.PFile.Close()
}

func Test_common_newParquetFileReader_s3_aws_error(t *testing.T) {
	// Make sure there is no AWS access
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))

	option := ReadOption{}
	option.URI = "s3:///path/to/object"
	_, err := newParquetFileReader(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "AWS error:")
}

func Test_common_newParquetFileReader_s3_good(t *testing.T) {
	// Make sure there is no AWS access
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))

	option := ReadOption{IsPublic: true}
	option.URI = "s3://aws-roda-hcls-datalake/gnomad/chrm/run-DataSink0-1-part-block-0-r-00000-snappy.parquet"
	_, err := newParquetFileReader(option)
	assert.Nil(t, err)
}

func Test_common_newParquetFileReader_s3_non_existent_versioned(t *testing.T) {
	// Make sure there is no AWS access
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))

	option := ReadOption{ObjectVersion: "random-version-id", IsPublic: true}
	option.URI = "s3://aws-roda-hcls-datalake/gnomad/chrm/run-DataSink0-1-part-block-0-r-00000-snappy.parquet"
	_, err := newParquetFileReader(option)
	assert.NotNil(t, err)
	// refer to https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
	// access to non-existent object/version without ListBucket permission will get 403 instead of 404
	assert.Contains(t, err.Error(), "https response error StatusCode: 403")
}

func Test_common_newParquetFileReader_gcs_no_permission(t *testing.T) {
	// Make sure there is no GCS access
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")

	option := ReadOption{}
	option.URI = "gs://cloud-samples-data/bigquery/us-states/us-states.parquet"
	_, err := newParquetFileReader(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open GCS object")
}

func Test_common_newParquetFileReader_azblob_no_permission(t *testing.T) {
	// Use a faked access key so anonymous access will fail
	randBytes := make([]byte, 64)
	rand.Read(randBytes)
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(randBytes))

	option := ReadOption{}
	option.URI = "wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/puYear=2021/puMonth=9/part-00005-tid-8898858832658823408-a1de80bd-eed3-4d11-b9d4-fa74bfbd47bc-426324-135.c000.snappy.parquet"
	_, err := newParquetFileReader(option)
	assert.NotNil(t, err)
	// This is returned from parquet-go-source, which does not help too much
	assert.Contains(t, err.Error(), "Server failed to authenticate the request")
}

// newFileWriter
func Test_common_newFileWriter_invalid_uri(t *testing.T) {
	option := CommonOption{URI: "://uri"}
	_, err := newFileWriter(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_common_newFileWriter_invalid_uri_scheme(t *testing.T) {
	option := CommonOption{URI: "invalid-scheme://something"}
	_, err := newFileWriter(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unknown location scheme")
}

func Test_common_newFileWriter_local_not_a_file(t *testing.T) {
	option := CommonOption{URI: "testdata/"}
	_, err := newFileWriter(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "is a directory")
}

func Test_common_newFileWriter_local_good(t *testing.T) {
	option := CommonOption{
		URI: os.TempDir() + "/file-writer.parquet",
	}
	fw, err := newFileWriter(option)
	assert.Nil(t, err)
	assert.NotNil(t, fw)
	fw.Close()
}

func Test_common_newFileWriter_s3_non_existent_bucket(t *testing.T) {
	option := CommonOption{
		URI: fmt.Sprintf("s3://bucket-does-not-exist-%d", rand.Int63()),
	}
	_, err := newFileWriter(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to find region of bucket [bucket-does-not-exist-")
}

func Test_common_newFileWriter_s3_good(t *testing.T) {
	// Make sure there is no AWS access
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))

	// parquet writer does not actually write to destination immediately
	option := CommonOption{
		URI: "s3://aws-roda-hcls-datalake/gnomad/chrm/run-DataSink0-1-part-block-0-r-00000-snappy.parquet",
	}
	fw, err := newFileWriter(option)
	assert.Nil(t, err)
	assert.NotNil(t, fw)
	fw.Close()
}

func Test_common_newFileWriter_gcs_no_permission(t *testing.T) {
	// Make sure there is no GCS access
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")

	// parquet writer does not actually write to destination immediately
	option := CommonOption{
		URI: "gs://cloud-samples-data/bigquery/us-states/us-states.parquet",
	}
	_, err := newFileWriter(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open GCS object")
}

func Test_common_newFileWriter_azblob_invalid_url(t *testing.T) {
	// Make sure there is no Azure blob access
	randBytes := make([]byte, 64)
	rand.Read(randBytes)
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(randBytes))

	option := CommonOption{URI: "wasbs://bad/url"}
	_, err := newFileWriter(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "azure blob URI format:")
}

func Test_common_newFileWriter_http_not_supported(t *testing.T) {
	option := CommonOption{URI: "https://domain.tld/path/to/file"}
	_, err := newFileWriter(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "writing to https endpoint is not currently supported")
}

// newCSVWriter
func Test_common_newCSVWriter_invalid_uri(t *testing.T) {
	option := CommonOption{URI: "://uri"}
	_, err := newCSVWriter(option, []string{"name=Id, type=INT64"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_common_newCSVWriter_invalid_schema(t *testing.T) {
	// invalid schema will cause panic
	option := CommonOption{
		URI: os.TempDir() + "/csv-writer.parquet",
	}
	_, err := newCSVWriter(option, []string{"invalid schema"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "expect 'key=value'")

	_, err = newCSVWriter(option, []string{"name=Id"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "not a valid Type string")
}

func Test_common_newCSVWriter_good(t *testing.T) {
	option := CommonOption{
		URI: os.TempDir() + "/csv-writer.parquet",
	}
	pw, err := newCSVWriter(option, []string{"name=Id, type=INT64"})
	assert.NotNil(t, pw)
	assert.Nil(t, err)
}

func Test_common_decimalToFloat_nil(t *testing.T) {
	f64, err := decimalToFloat(ReinterpretField{}, nil)
	assert.Nil(t, err)
	assert.Nil(t, f64)
}

func Test_common_decimalToFloat_int32(t *testing.T) {
	fieldAttr := ReinterpretField{
		scale: 2,
	}
	f64, err := decimalToFloat(fieldAttr, int32(0))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, 0.0, *f64)

	f64, err = decimalToFloat(fieldAttr, int32(11))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, 0.11, *f64)

	f64, err = decimalToFloat(fieldAttr, int32(222))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, 2.22, *f64)

	f64, err = decimalToFloat(fieldAttr, int32(-11))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, -0.11, *f64)

	f64, err = decimalToFloat(fieldAttr, int32(-222))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, -2.22, *f64)
}

func Test_common_decimalToFloat_int64(t *testing.T) {
	fieldAttr := ReinterpretField{
		scale: 2,
	}
	f64, err := decimalToFloat(fieldAttr, int64(0))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, 0.0, *f64)

	f64, err = decimalToFloat(fieldAttr, int64(11))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, 0.11, *f64)

	f64, err = decimalToFloat(fieldAttr, int64(222))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, 2.22, *f64)

	f64, err = decimalToFloat(fieldAttr, int64(-11))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, -0.11, *f64)

	f64, err = decimalToFloat(fieldAttr, int64(-222))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, -2.22, *f64)
}

func Test_common_decimalToFloat_string(t *testing.T) {
	fieldAttr := ReinterpretField{
		scale:     2,
		precision: 10,
	}

	f64, err := decimalToFloat(fieldAttr, types.StrIntToBinary("000", "BigEndian", 0, true))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, 0.0, *f64)

	f64, err = decimalToFloat(fieldAttr, types.StrIntToBinary("011", "BigEndian", 0, true))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, 0.11, *f64)

	f64, err = decimalToFloat(fieldAttr, types.StrIntToBinary("222", "BigEndian", 0, true))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, 2.22, *f64)

	f64, err = decimalToFloat(fieldAttr, types.StrIntToBinary("-011", "BigEndian", 0, true))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, -0.11, *f64)

	f64, err = decimalToFloat(fieldAttr, types.StrIntToBinary("-222", "BigEndian", 0, true))
	assert.Nil(t, err)
	assert.NotNil(t, f64)
	assert.Equal(t, -2.22, *f64)
}

func Test_common_decimalToFloat_invalid_type(t *testing.T) {
	fieldAttr := ReinterpretField{}

	f64, err := decimalToFloat(fieldAttr, int(0))
	assert.NotNil(t, err)
	assert.Equal(t, "unknown type: int", err.Error())
	assert.Nil(t, f64)

	f64, err = decimalToFloat(fieldAttr, float32(0.0))
	assert.NotNil(t, err)
	assert.Equal(t, "unknown type: float32", err.Error())
	assert.Nil(t, f64)

	f64, err = decimalToFloat(fieldAttr, float64(0.0))
	assert.NotNil(t, err)
	assert.Equal(t, "unknown type: float64", err.Error())
	assert.Nil(t, f64)
}

func Test_common_newParquetFileReader_http_bad_url(t *testing.T) {
	option := ReadOption{}
	option.URI = "https://no-such-host.tld/"
	option.HTTPMultipleConnection = true
	option.HTTPIgnoreTLSError = true
	option.HTTPExtraHeaders = map[string]string{"key": "value"}
	_, err := newParquetFileReader(option)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "no such host")
}

func Test_common_newParquetFileReader_http_no_range_support(t *testing.T) {
	option := ReadOption{}
	option.URI = "https://www.google.com/"
	option.HTTPMultipleConnection = false
	option.HTTPIgnoreTLSError = true
	option.HTTPExtraHeaders = map[string]string{"key": "value"}
	_, err := newParquetFileReader(option)

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "does not support range")
}

func Test_common_newParquetFileReader_http_good(t *testing.T) {
	option := ReadOption{}
	option.URI = "https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/bing_covid-19_data/latest/bing_covid-19_data.parquet"
	option.HTTPMultipleConnection = true
	option.HTTPIgnoreTLSError = false
	option.HTTPExtraHeaders = map[string]string{"key": "value"}
	_, err := newParquetFileReader(option)
	assert.Nil(t, err)
}
