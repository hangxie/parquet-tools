package cmd

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func Test_common_parseURI_invalid_uri(t *testing.T) {
	_, err := parseURI("://uri")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse file location")
}

func Test_common_getBucketRegion_s3_non_existent_bucket(t *testing.T) {
	_, err := newParquetFileReader(fmt.Sprintf("s3://bucket-does-not-exist-%d", rand.Int63()))
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to find")
}

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

func Test_common_newParquetFileReader_s3_no_permission(t *testing.T) {
	// Make sure there is no AWS access
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))
	t.Logf("dummy AWS_PROFILE: %s\n", os.Getenv("AWS_PROFILE"))

	_, err := newParquetFileReader("s3://dpla-provider-export/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open S3 object")
}

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

func Test_common_newFileWriter_s3_non_existent_bucket(t *testing.T) {
	_, err := newFileWriter(fmt.Sprintf("s3://bucket-does-not-exist-%d", rand.Int63()))
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to find")
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
