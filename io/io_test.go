package io

import (
	"crypto/rand"
	"encoding/base64"
	"net/url"
	"reflect"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/google/uuid"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/stretchr/testify/require"
)

func TestAzureAccessDetail(t *testing.T) {
	t.Run("invalid-uri", func(t *testing.T) {
		t.Setenv("AZURE_STORAGE_ACCESS_KEY", "")

		invalidPaths := []string{
			"",
			"missing/leading/slash",
			"/no-container",
			"/empty-blob/",
		}

		for _, path := range invalidPaths {
			t.Run(path, func(t *testing.T) {
				// Cannot use t.Parallel() with t.Setenv() from parent test
				// Create separate URL instance to avoid race conditions
				u := url.URL{
					Host: "storageaccount",
					Path: path,
				}
				uri, cred, err := azureAccessDetail(u, false, "")
				require.Error(t, err)
				require.Contains(t, err.Error(), "azure blob URI format:")
				require.Equal(t, "", uri)
				require.Nil(t, cred)
			})
		}
	})

	t.Run("bad-shared-cred", func(t *testing.T) {
		u := url.URL{
			Host: "storageaccount",
			Path: "/container/path/to/object",
			User: url.User("container-name"),
		}

		t.Setenv("AZURE_STORAGE_ACCESS_KEY", "bad-access-key")
		uri, cred, err := azureAccessDetail(u, false, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to create Azure credential")
		require.Equal(t, "", uri)
		require.Nil(t, cred)
	})

	t.Run("good-anonymous-cred", func(t *testing.T) {
		u := url.URL{
			Host: "storageaccount.blob.core.windows.net",
			Path: "/path/to/object",
			User: url.User("container"),
		}
		// anonymous access by lack of environment variable
		t.Setenv("AZURE_STORAGE_ACCESS_KEY", "")
		uri, cred, err := azureAccessDetail(u, false, "")
		require.NoError(t, err)
		require.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object", uri)
		require.Nil(t, cred)

		t.Setenv("AZURE_STORAGE_ACCESS_KEY", "")
		uri, cred, err = azureAccessDetail(u, false, "")
		require.NoError(t, err)
		require.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object", uri)
		require.Nil(t, cred)

		// anonymous access by explicit setting
		randBytes := make([]byte, 64)
		_, err = rand.Read(randBytes)
		if err != nil {
			t.Fatalf("failed to setup test: %s", err.Error())
		}
		t.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(randBytes))
		uri, cred, err = azureAccessDetail(u, true, "")
		require.NoError(t, err)
		require.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object", uri)
		require.Nil(t, cred)

		// with version id
		t.Setenv("AZURE_STORAGE_ACCESS_KEY", "")
		uri, cred, err = azureAccessDetail(u, false, "foo-bar")
		require.NoError(t, err)
		require.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object?versionid=foo-bar", uri)
		require.Nil(t, cred)
	})

	t.Run("good-shared-cred", func(t *testing.T) {
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
		t.Setenv("AZURE_STORAGE_ACCESS_KEY", dummyKey)
		uri, _, err := azureAccessDetail(u, false, "")
		require.NoError(t, err)
		require.Equal(t, "https://storageaccount.blob.core.windows.net/container/path/to/object", uri)
		require.Equal(t, "*exported.SharedKeyCredential", reflect.TypeFor[*azblob.SharedKeyCredential]().String())
	})
}

func TestGetBucketRegion(t *testing.T) {
	testCases := map[string]struct {
		profile   string
		bucket    string
		public    bool
		ignoreTLS bool
		errMsg    string
	}{
		"non-existent-bucket":            {"", uuid.New().String(), true, false, "not found"},
		"unable-to-get-region":           {"", "localhost/something/does/not/matter", true, false, "unable to get region for S3 bucket"},
		"bucket-name-with-dot":           {"", "xiehang.com", false, true, ""},
		"bucket-name-with-dot-no-ignore": {"", "xiehang.com", false, false, "unable to get region for S3 bucket"},
		"private-bucket":                 {"", "doc-example-bucket", true, false, "S3 bucket doc-example-bucket is not public"},
		"aws-error":                      {"", "00", true, false, "unrecognized StatusCode from AWS: 400"},
		"missing-credential":             {uuid.New().String(), "daylight-openstreetmap", false, false, "failed to get shared config profile"},
	}

	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("AWS_PROFILE", tc.profile)
			_, err := getS3Client(tc.bucket, tc.public, tc.ignoreTLS)
			if tc.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func TestValidCompressionCodecs(t *testing.T) {
	require.NotEmpty(t, ValidCompressionCodecs)
	for _, codec := range ValidCompressionCodecs {
		t.Run(codec, func(t *testing.T) {
			t.Parallel()
			_, err := parquet.CompressionCodecFromString(codec)
			require.NoError(t, err, "codec %s should be recognized by parquet library", codec)
		})
	}
}

func TestParseURI(t *testing.T) {
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
			t.Parallel()
			u, err := parseURI(tc.uri)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.scheme, u.Scheme)
			require.Equal(t, tc.host, u.Host)
			require.Equal(t, tc.path, u.Path)
		})
	}
}
