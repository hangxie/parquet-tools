package cmd

import (
	"fmt"
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
