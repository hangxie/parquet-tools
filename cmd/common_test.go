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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go/parquet"
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
		assert.Equal(t, float64(i+1), v)
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

func Test_common_newParquetFileReader_azblob_no_permission(t *testing.T) {
	// Use a faked access key so anonymous access will fail
	randBytes := make([]byte, 64)
	rand.Read(randBytes)
	dummyKey := base64.StdEncoding.EncodeToString(randBytes)
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", dummyKey)
	t.Logf("dummyKey is [%s]", dummyKey)

	_, err := newParquetFileReader("wasbs://censusdatacontainer@azureopendatastorage.blob.core.windows.net/release/us_population_zip/year=2010/part-00178-tid-5434563040420806442-84b5e4ab-8ab1-4e28-beb1-81caf32ca312-1919656.c000.snappy.parquet")
	assert.NotNil(t, err)
	// This is returned from parquet-go-source, which does not help too much
	assert.Contains(t, err.Error(), "Server failed to authenticate the request")
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

func Test_common_newFileWriter_azblob_invalid_url(t *testing.T) {
	// Make sure there is no Azure blob access
	randBytes := make([]byte, 64)
	rand.Read(randBytes)
	dummyKey := base64.StdEncoding.EncodeToString(randBytes)
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", dummyKey)

	_, err := newFileWriter("wasbs://bad/url")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "zure blob URI format:")
}

func Test_common_newFileWriter_azblob_no_permission(t *testing.T) {
	// Make sure there is no Azure blob access
	randBytes := make([]byte, 64)
	rand.Read(randBytes)
	dummyKey := base64.StdEncoding.EncodeToString(randBytes)
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", dummyKey)

	_, err := newFileWriter("wasbs://censusdatacontainer@azureopendatastorage.blob.core.windows.net/release/us_population_zip/year=2010/part-00178-tid-5434563040420806442-84b5e4ab-8ab1-4e28-beb1-81caf32ca312-1919656.c000.snappy.parquet")
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
	_, err := newCSVWriter(testFile, []string{"invalid schema"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "expect 'key=value'")

	_, err = newCSVWriter(testFile, []string{"name=Id"})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "not a valid Type string")
}

func Test_common_newCSVWriter_good(t *testing.T) {
	pw, err := newCSVWriter(os.TempDir()+"/csv-writer.parquet", []string{"name=Id, type=INT64"})
	assert.NotNil(t, pw)
	assert.Nil(t, err)
}

func Test_common_getAllDecimalFields_good(t *testing.T) {
	// TODO
	// this is currently covered by high level test cases but eventually needs unit test cases
}

func Test_reformatStringDecimalValue_good_decimal(t *testing.T) {
	fieldAttr := ReinterpretField{
		parquetType:   parquet.Type_BYTE_ARRAY,
		convertedType: parquet.ConvertedType_DECIMAL,
		scale:         2,
		precision:     10,
	}

	decimalValue := types.StrIntToBinary("-011", "BigEndian", 0, true)
	reformatStringValue(fieldAttr, reflect.ValueOf(&decimalValue).Elem())
	assert.Equal(t, "-0.11", decimalValue)

	decimalPtr := new(string)
	*decimalPtr = types.StrIntToBinary("222", "BigEndian", 0, true)
	reformatStringValue(fieldAttr, reflect.ValueOf(&decimalPtr).Elem())
	assert.Equal(t, "2.22", *decimalPtr)

	var nilPtr *string
	reformatStringValue(fieldAttr, reflect.ValueOf(&nilPtr).Elem())
	assert.Nil(t, nilPtr)
}

func Test_reformatStringDecimalValue_good_interval(t *testing.T) {
	fieldAttr := ReinterpretField{
		parquetType:   parquet.Type_BYTE_ARRAY,
		convertedType: parquet.ConvertedType_INTERVAL,
		scale:         0,
		precision:     10,
	}

	intervalValue := types.StrIntToBinary("54321", "LittleEndian", 10, false)
	assert.NotEqual(t, "54321", intervalValue)

	reformatStringValue(fieldAttr, reflect.ValueOf(&intervalValue).Elem())
	assert.Equal(t, "54321", intervalValue)
}

func Test_reformatStringDecimalValue_good_int96(t *testing.T) {
	fieldAttr := ReinterpretField{
		parquetType:   parquet.Type_INT96,
		convertedType: parquet.ConvertedType_TIMESTAMP_MICROS,
		scale:         0,
		precision:     0,
	}

	timeValue, _ := time.Parse("2006-01-02", "2022-01-01")
	int96Value := types.TimeToINT96(timeValue)
	assert.NotEqual(t, "2022-01-01T00:00:00Z", int96Value)

	reformatStringValue(fieldAttr, reflect.ValueOf(&int96Value).Elem())
	assert.Equal(t, "2022-01-01T00:00:00Z", int96Value)
}

func Test_decimalToFloat_nil(t *testing.T) {
	f64, err := decimalToFloat(ReinterpretField{}, nil)
	assert.Nil(t, err)
	assert.Nil(t, f64)
}

func Test_decimalToFloat_int32(t *testing.T) {
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

func Test_decimalToFloat_int64(t *testing.T) {
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

func Test_decimalToFloat_string(t *testing.T) {
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

func Test_decimalToFloat_invalid_type(t *testing.T) {
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
