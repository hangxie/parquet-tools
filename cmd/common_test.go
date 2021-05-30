package cmd

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
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

// help functions
func Test_common_azureAccessDetail_invalid_uri(t *testing.T) {
	u := url.URL{
		Host: "",
		Path: "",
	}
	os.Unsetenv("AZURE_STORAGE_ACCESS_KEY")

	uri, cred, err := azureAccessDetail(u)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "azure blob URI format:")
	assert.Equal(t, uri, "")
	assert.Equal(t, cred, nil)

	u.Host = "storageacconut"
	u.Path = "missin/leading/slash"
	_, _, err = azureAccessDetail(u)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "azure blob URI format:")

	u.Host = "storageacconut"
	u.Path = "/no-blob"
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
	}

	os.Setenv("AZURE_STORAGE_ACCESS_KEY", "bad-access-key")
	uri, cred, err := azureAccessDetail(u)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to create Azure credential")
	assert.Equal(t, uri, "")
	assert.Equal(t, cred, nil)
}

func Test_common_azureAccessDetail_good_anonymous_cred(t *testing.T) {
	u := url.URL{
		Host: "storageaccount",
		Path: "/container/path/to/object",
	}
	os.Unsetenv("AZURE_STORAGE_ACCESS_KEY")
	uri, cred, err := azureAccessDetail(u)
	assert.Nil(t, err)
	assert.Equal(t, uri, "https://storageaccount.blob.core.windows.net/container/path/to/object")
	assert.Equal(t, reflect.TypeOf(cred).String(), "*azblob.anonymousCredentialPolicyFactory")

	os.Setenv("AZURE_STORAGE_ACCESS_KEY", "")
	uri, cred, err = azureAccessDetail(u)
	assert.Nil(t, err)
	assert.Equal(t, uri, "https://storageaccount.blob.core.windows.net/container/path/to/object")
	assert.Equal(t, reflect.TypeOf(cred).String(), "*azblob.anonymousCredentialPolicyFactory")
}

func Test_common_azureAccessDetail_good_shared_cred(t *testing.T) {
	u := url.URL{
		Host: "storageaccount",
		Path: "/container/path/to/object",
	}

	randBytes := make([]byte, 64)
	rand.Read(randBytes)
	dummyKey := base64.StdEncoding.EncodeToString(randBytes)
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", dummyKey)
	uri, cred, err := azureAccessDetail(u)
	assert.Nil(t, err)
	assert.Equal(t, uri, "https://storageaccount.blob.core.windows.net/container/path/to/object")
	assert.Equal(t, reflect.TypeOf(cred).String(), "*azblob.SharedKeyCredential")
}

func Test_common_getBucketRegion_s3_non_existent_bucket(t *testing.T) {
	_, err := getBucketRegion(fmt.Sprintf("bucket-does-not-exist-%d", rand.Int63()))
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to find")
}

func Test_common_getBucketRegion_aws_error(t *testing.T) {
	_, err := getBucketRegion("")
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
	assert.Equal(t, u.Scheme, "scheme")
	assert.Equal(t, u.Host, "path")
	assert.Equal(t, u.Path, "/to/file")

	u, err = parseURI("path/to/file")
	assert.Nil(t, err)
	assert.Equal(t, u.Scheme, "file")
	assert.Equal(t, u.Host, "")
	assert.Equal(t, u.Path, "path/to/file")

	u, err = parseURI("file://path/to/file")
	assert.Nil(t, err)
	assert.Equal(t, u.Scheme, "file")
	assert.Equal(t, u.Host, "")
	assert.Equal(t, u.Path, "path/to/file")
}

func Test_common_toNumber_bad(t *testing.T) {
	badValues := []interface{}{
		string("8"),
		[]uint{9, 10},
		nil,
	}

	for _, iface := range badValues {
		_, ok := toNumber(interface{}(iface))
		assert.False(t, ok)
	}
}

func Test_common_toNumber_good(t *testing.T) {
	badValues := []interface{}{
		uint(1),
		uint8(2),
		uint16(3),
		uint32(4),
		uint64(5),
		int(6),
		int8(7),
		int16(8),
		int32(9),
		int64(10),
		float32(11),
		float64(12),
	}

	for i, iface := range badValues {
		v, ok := toNumber(interface{}(iface))
		assert.True(t, ok)
		assert.Equal(t, v, float64(i+1))
	}
}

// newParquetFileReader
func Test_common_newParquetFileReader_invalid_uri(t *testing.T) {
	_, err := newParquetFileReader("://uri")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_common_newParquetFileReader_invalid_uri_scheme(t *testing.T) {
	_, err := newParquetFileReader("invalid-scheme://something")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unknown location scheme")
}

func Test_common_newParquetFileReader_local_non_existent_file(t *testing.T) {
	_, err := newParquetFileReader("file/does/not/exist")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "no such file or directory")
}

func Test_common_newParquetFileReader_local_not_parquet(t *testing.T) {
	_, err := newParquetFileReader("testdata/not-a-parquet-file")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid argument")
}

func Test_common_newParquetFileReader_local_good(t *testing.T) {
	pr, err := newParquetFileReader("testdata/good.parquet")
	assert.Nil(t, err)
	assert.NotNil(t, pr)
	pr.PFile.Close()
}

func Test_common_newParquetFileReader_s3_aws_error(t *testing.T) {
	// Make sure there is no AWS access
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))
	t.Logf("dummy AWS_PROFILE: %s\n", os.Getenv("AWS_PROFILE"))

	_, err := newParquetFileReader("s3:///path/to/object")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "AWS error:")
}

func Test_common_newParquetFileReader_s3_no_permission(t *testing.T) {
	// Make sure there is no AWS access
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))
	t.Logf("dummy AWS_PROFILE: %s\n", os.Getenv("AWS_PROFILE"))

	_, err := newParquetFileReader("s3://dpla-provider-export/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open S3 object")
}

func Test_common_newParquetFileReader_gcs_no_permission(t *testing.T) {
	// Make sure there is no GCS access
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")

	_, err := newParquetFileReader("gs://cloud-samples-data/bigquery/us-states/us-states.parquet")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open GCS object")
}

func Test_common_newParquetFileReader_azblob_access_fail(t *testing.T) {
	// Make sure there is no Azure blob access
	randBytes := make([]byte, 64)
	rand.Read(randBytes)
	dummyKey := base64.StdEncoding.EncodeToString(randBytes)
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", dummyKey)

	_, err := newParquetFileReader("azblob://bad/uri")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "azure blob URI format:")
}

func Test_common_newParquetFileReader_azblob_no_permission(t *testing.T) {
	// Make sure there is no Azure blob access
	randBytes := make([]byte, 64)
	rand.Read(randBytes)
	dummyKey := base64.StdEncoding.EncodeToString(randBytes)
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", dummyKey)

	_, err := newParquetFileReader("azblob://azureopendatastorage/censusdatacontainer/release/us_population_zip/year=2010/part-00178-tid-5434563040420806442-84b5e4ab-8ab1-4e28-beb1-81caf32ca312-1919656.c000.snappy.parquet")
	assert.NotNil(t, err)
	// This is returned from parquet-go-source, which does not help too much
	assert.Contains(t, err.Error(), "Seek: invalid offset")
}

// newFileWriter
func Test_common_newFileWriter_invalid_uri(t *testing.T) {
	_, err := newFileWriter("://uri")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_common_newFileWriter_invalid_uri_scheme(t *testing.T) {
	_, err := newFileWriter("invalid-scheme://something")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unknown location scheme")
}

func Test_common_newFileWriter_local_not_a_file(t *testing.T) {
	_, err := newFileWriter("testdata/")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "is a directory")
}

func Test_common_newFileWriter_local_good(t *testing.T) {
	fw, err := newFileWriter(os.TempDir() + "/file-writer.parquet")
	assert.Nil(t, err)
	assert.NotNil(t, fw)
	fw.Close()
}

func Test_common_newFileWriter_s3_non_existent_bucket(t *testing.T) {
	_, err := newFileWriter(fmt.Sprintf("s3://bucket-does-not-exist-%d", rand.Int63()))
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to find")
}

func Test_common_newFileWriter_s3_good(t *testing.T) {
	// Make sure there is no AWS access
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))
	t.Logf("dummy AWS_PROFILE: %s\n", os.Getenv("AWS_PROFILE"))

	// parquet writer does not actually write to destination immediately
	fw, err := newFileWriter("s3://dpla-provider-export/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet")
	assert.Nil(t, err)
	assert.NotNil(t, fw)
	fw.Close()
}

func Test_common_newFileWriter_gcs_no_permission(t *testing.T) {
	// Make sure there is no GCS access
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")

	// parquet writer does not actually write to destination immediately
	_, err := newFileWriter("gs://cloud-samples-data/bigquery/us-states/us-states.parquet")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open GCS object")
}

func Test_common_newFileWriter_azblob_no_permission(t *testing.T) {
	// Make sure there is no Azure blob access
	randBytes := make([]byte, 64)
	rand.Read(randBytes)
	dummyKey := base64.StdEncoding.EncodeToString(randBytes)
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", dummyKey)

	_, err := newFileWriter("azblob://azureopendatastorage/censusdatacontainer/release/us_population_zip/year=2010/part-00178-tid-5434563040420806442-84b5e4ab-8ab1-4e28-beb1-81caf32ca312-1919656.c000.snappy.parquet")
	assert.NotNil(t, err)
	// This is returned from parquet-go-source, which does not help too much
	assert.Contains(t, err.Error(), "failed to open Azure blob object")
}

// newParquetFileWriter
func Test_common_newParquetFileWriter_invalid_uri(t *testing.T) {
	_, err := newParquetFileWriter("://uri", &struct{}{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_common_newParquetFileWriter_good(t *testing.T) {
	pw, err := newParquetFileWriter(os.TempDir()+"/parquet-writer.parquet", &struct{}{})
	assert.NotNil(t, pw)
	assert.Nil(t, err)
	pw.PFile.Close()
}

// newCSVWriter
func Test_common_newCSVWriter_invalid_uri(t *testing.T) {
	_, err := newCSVWriter("://uri", []string{"name=Id, type=INT64"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_common_newCSVWriter_invalid_schema(t *testing.T) {
	// invalid schema will cause panic
	testFile := os.TempDir() + "/csv-writer.parquet"
	assert.Panics(t, func() {
		_, err := newCSVWriter(testFile, []string{"invalid schema"})
		assert.NotNil(t, err)
	})
	assert.Panics(t, func() {
		_, err := newCSVWriter(testFile, []string{"name=Id"})
		assert.NotNil(t, err)
	})
}

func Test_common_newCSVWriter_good(t *testing.T) {
	pw, err := newCSVWriter(os.TempDir()+"/csv-writer.parquet", []string{"name=Id, type=INT64"})
	assert.NotNil(t, pw)
	assert.Nil(t, err)
}
