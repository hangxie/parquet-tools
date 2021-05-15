package cmd

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_newParquetFileReader(t *testing.T) {
	testCases := []struct {
		uri           string
		expectedError error
	}{
		{
			uri:           "://uri",
			expectedError: fmt.Errorf("unable to parse file location"),
		},
		{
			uri:           "non-existent-local-file",
			expectedError: fmt.Errorf("no such file or directory"),
		},
		{
			uri:           "invalid-scheme://something",
			expectedError: fmt.Errorf("unknown location scheme"),
		},
		{
			uri:           "file://./testdata/not-a-parquet-file",
			expectedError: fmt.Errorf("invalid argument"),
		},
		{
			uri:           "file://testdata/not-a-parquet-file",
			expectedError: fmt.Errorf("invalid argument"),
		},
		{
			uri:           "testdata/not-a-parquet-file",
			expectedError: fmt.Errorf("invalid argument"),
		},
		{
			uri:           "testdata/good.parquet",
			expectedError: nil,
		},
		{
			uri:           "s3://somebucket-not-exists",
			expectedError: fmt.Errorf("unable to find"),
		},
		{
			// https://pro.dp.la/developers/bulk-download
			uri:           "s3://dpla-provider-export/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet",
			expectedError: fmt.Errorf("failed to open S3 object"),
		},
	}

	// Make sure there is no AWS access
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))
	t.Logf("dummy AWS_PROFILE: %s\n", os.Getenv("AWS_PROFILE"))

	for _, tc := range testCases {
		r, err := newParquetFileReader(tc.uri)
		if tc.expectedError != nil {
			// expect error
			assert.NotEqual(t, err, nil)
			assert.Contains(t, err.Error(), tc.expectedError.Error())
			continue
		}

		// expect good result
		assert.Equal(t, err, nil)
		assert.NotEqual(t, r, nil)
	}
}

func Test_newFileWriter(t *testing.T) {
	testCases := []struct {
		uri           string
		expectedError error
	}{
		{
			uri:           "://uri",
			expectedError: fmt.Errorf("unable to parse file location"),
		},
		{
			uri:           "invalid-scheme://something",
			expectedError: fmt.Errorf("unknown location scheme"),
		},
		{
			uri:           "testdata/",
			expectedError: fmt.Errorf("is a directory"),
		},
		{
			uri:           os.TempDir() + "/file-writer.parquet",
			expectedError: nil,
		},
		{
			uri:           "s3://somebucket-not-exists",
			expectedError: fmt.Errorf("unable to find"),
		},
		{
			// https://pro.dp.la/developers/bulk-download
			uri:           "s3://dpla-provider-export/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet",
			expectedError: nil,
		},
	}

	// Make sure there is no AWS access
	os.Setenv("AWS_PROFILE", fmt.Sprintf("%d", rand.Int63()))
	t.Logf("dummy AWS_PROFILE: %s\n", os.Getenv("AWS_PROFILE"))

	for _, tc := range testCases {
		r, err := newFileWriter(tc.uri)
		if tc.expectedError != nil {
			// expect error
			assert.NotEqual(t, err, nil)
			assert.Contains(t, err.Error(), tc.expectedError.Error())
			continue
		}

		// expect good result
		assert.Equal(t, err, nil)
		assert.NotEqual(t, r, nil)
		os.Remove(tc.uri)
	}
}

func Test_newParquetFileWriter(t *testing.T) {
	dummySchema := struct{}{}
	testFile := os.TempDir() + "/parquet-writer.parquet"
	pw, err := newParquetFileWriter(testFile, &dummySchema)
	assert.NotNil(t, pw)
	assert.Nil(t, err)

	// invalid URI
	_, err = newParquetFileWriter("invalid://uri", dummySchema)
	assert.Contains(t, err.Error(), "unknown location scheme")

	// invalid schema
	_, err = newParquetFileWriter(testFile, "")
	assert.Contains(t, err.Error(), "error in unmarshalling json schema string")
}

func Test_newCSVWriter(t *testing.T) {
	dummySchema := []string{"name=Id, type=INT64"}
	testFile := os.TempDir() + "/csv-writer.parquet"
	pw, err := newCSVWriter(testFile, dummySchema)
	assert.NotNil(t, pw)
	assert.Nil(t, err)

	// invalid URI
	_, err = newCSVWriter("invalid://uri", dummySchema)
	assert.Contains(t, err.Error(), "unknown location scheme")

	// invalid schema will cause panic
	assert.Panics(t, func() { newCSVWriter(testFile, []string{"invalid schema"}) })
	assert.Panics(t, func() { newCSVWriter(testFile, []string{"name=Id"}) })
}

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
	os.Stdout = savedStdout
	os.Stderr = savedStderr

	return string(stdout), string(stderr)
}
