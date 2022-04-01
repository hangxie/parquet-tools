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
	_, err := getBucketRegion(fmt.Sprintf("bucket-does-not-exist-%d", rand.Int63()))
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "AWS error: bucket not found")
}

func Test_common_getBucketRegion_bad_request(t *testing.T) {
	_, err := getBucketRegion("*&^%")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Bad Request")
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
	_, err := newParquetFileReader(CommonOption{URI: "://uri"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_common_newParquetFileReader_invalid_uri_scheme(t *testing.T) {
	_, err := newParquetFileReader(CommonOption{URI: "invalid-scheme://something"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unknown location scheme")
}

func Test_common_newParquetFileReader_local_non_existent_file(t *testing.T) {
	_, err := newParquetFileReader(CommonOption{URI: "file/does/not/exist"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "no such file or directory")
}

func Test_common_newParquetFileReader_local_not_parquet(t *testing.T) {
	_, err := newParquetFileReader(CommonOption{URI: "testdata/not-a-parquet-file"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid argument")
}

func Test_common_newParquetFileReader_local_good(t *testing.T) {
	pr, err := newParquetFileReader(CommonOption{URI: "testdata/good.parquet"})
	assert.Nil(t, err)
	assert.NotNil(t, pr)
	pr.PFile.Close()
}

func Test_common_newParquetFileReader_s3_aws_error(t *testing.T) {
	// Make sure there is no AWS access
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))
	t.Logf("dummy AWS_PROFILE: %s\n", os.Getenv("AWS_PROFILE"))

	_, err := newParquetFileReader(CommonOption{URI: "s3:///path/to/object"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "AWS error:")
}

func Test_common_newParquetFileReader_s3_good(t *testing.T) {
	// Make sure there is no AWS access
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))
	t.Logf("dummy AWS_PROFILE: %s\n", os.Getenv("AWS_PROFILE"))

	_, err := newParquetFileReader(CommonOption{
		URI: "s3://dpla-provider-export/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet",
	})
	assert.Nil(t, err)
}

func Test_common_newParquetFileReader_gcs_no_permission(t *testing.T) {
	// Make sure there is no GCS access
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")

	_, err := newParquetFileReader(CommonOption{URI: "gs://cloud-samples-data/bigquery/us-states/us-states.parquet"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open GCS object")
}

func Test_common_newParquetFileReader_azblob_no_permission(t *testing.T) {
	// Use a faked access key so anonymous access will fail
	randBytes := make([]byte, 64)
	rand.Read(randBytes)
	dummyKey := base64.StdEncoding.EncodeToString(randBytes)
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", dummyKey)
	t.Logf("dummyKey is [%s]", dummyKey)

	_, err := newParquetFileReader(CommonOption{
		URI: "wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/puYear=2021/puMonth=9/part-00005-tid-8898858832658823408-a1de80bd-eed3-4d11-b9d4-fa74bfbd47bc-426324-135.c000.snappy.parquet",
	})
	assert.NotNil(t, err)
	// This is returned from parquet-go-source, which does not help too much
	assert.Contains(t, err.Error(), "Server failed to authenticate the request")
}

// newFileWriter
func Test_common_newFileWriter_invalid_uri(t *testing.T) {
	_, err := newFileWriter(CommonOption{URI: "://uri"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_common_newFileWriter_invalid_uri_scheme(t *testing.T) {
	_, err := newFileWriter(CommonOption{URI: "invalid-scheme://something"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unknown location scheme")
}

func Test_common_newFileWriter_local_not_a_file(t *testing.T) {
	_, err := newFileWriter(CommonOption{URI: "testdata/"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "is a directory")
}

func Test_common_newFileWriter_local_good(t *testing.T) {
	fw, err := newFileWriter(CommonOption{URI: os.TempDir() + "/file-writer.parquet"})
	assert.Nil(t, err)
	assert.NotNil(t, fw)
	fw.Close()
}

func Test_common_newFileWriter_s3_non_existent_bucket(t *testing.T) {
	_, err := newFileWriter(CommonOption{URI: fmt.Sprintf("s3://bucket-does-not-exist-%d", rand.Int63())})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "AWS error: bucket not found")
}

func Test_common_newFileWriter_s3_good(t *testing.T) {
	// Make sure there is no AWS access
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))
	t.Logf("dummy AWS_PROFILE: %s\n", os.Getenv("AWS_PROFILE"))

	// parquet writer does not actually write to destination immediately
	fw, err := newFileWriter(CommonOption{URI: "s3://dpla-provider-export/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet"})
	assert.Nil(t, err)
	assert.NotNil(t, fw)
	fw.Close()
}

func Test_common_newFileWriter_gcs_no_permission(t *testing.T) {
	// Make sure there is no GCS access
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")

	// parquet writer does not actually write to destination immediately
	_, err := newFileWriter(CommonOption{URI: "gs://cloud-samples-data/bigquery/us-states/us-states.parquet"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open GCS object")
}

func Test_common_newFileWriter_azblob_invalid_url(t *testing.T) {
	// Make sure there is no Azure blob access
	randBytes := make([]byte, 64)
	rand.Read(randBytes)
	dummyKey := base64.StdEncoding.EncodeToString(randBytes)
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", dummyKey)

	_, err := newFileWriter(CommonOption{URI: "wasbs://bad/url"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "zure blob URI format:")
}

// newCSVWriter
func Test_common_newCSVWriter_invalid_uri(t *testing.T) {
	_, err := newCSVWriter(CommonOption{URI: "://uri"}, []string{"name=Id, type=INT64"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_common_newCSVWriter_invalid_schema(t *testing.T) {
	// invalid schema will cause panic
	testFile := os.TempDir() + "/csv-writer.parquet"
	_, err := newCSVWriter(CommonOption{URI: testFile}, []string{"invalid schema"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "expect 'key=value'")

	_, err = newCSVWriter(CommonOption{URI: testFile}, []string{"name=Id"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "not a valid Type string")
}

func Test_common_newCSVWriter_good(t *testing.T) {
	pw, err := newCSVWriter(CommonOption{URI: os.TempDir() + "/csv-writer.parquet"}, []string{"name=Id, type=INT64"})
	assert.NotNil(t, pw)
	assert.Nil(t, err)
}

func Test_common_getAllDecimalFields_good(t *testing.T) {
	// TODO
	// this is currently covered by high level test cases but eventually needs unit test cases
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
	_, err := newParquetFileReader(
		CommonOption{
			URI:                    "https://no-such-host.tld/",
			HttpMultipleConnection: true,
			HttpIgnoreTLSError:     true,
			HttpExtraHeaders:       map[string]string{"key": "value"},
		},
	)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "no such host")
}

func Test_common_newParquetFileReader_http_no_range_support(t *testing.T) {
	_, err := newParquetFileReader(
		CommonOption{
			URI:                    "https://www.google.com/",
			HttpMultipleConnection: false,
			HttpIgnoreTLSError:     true,
			HttpExtraHeaders:       map[string]string{"key": "value"},
		},
	)

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "does not support range")
}

func Test_common_newParquetFileReader_http_good(t *testing.T) {
	_, err := newParquetFileReader(
		CommonOption{
			URI:                    "https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/bing_covid-19_data/latest/bing_covid-19_data.parquet",
			HttpMultipleConnection: true,
			HttpIgnoreTLSError:     false,
			HttpExtraHeaders:       map[string]string{"key": "value"},
		},
	)
	assert.Nil(t, err)
}
