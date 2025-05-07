package io

import (
	"crypto/rand"
	"encoding/base64"
	"net/url"
	"os"
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
