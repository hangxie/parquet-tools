package io

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

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

func TestAzureAccessDetailSchemeVariants(t *testing.T) {
	testCases := map[string]struct {
		scheme    string
		host      string
		container string
		path      string
		account   string
		expected  string
		errMsg    string
	}{
		"wasbs": {
			scheme:    schemeAzureStorageBlob,
			host:      "storageaccount.blob.core.windows.net",
			container: "container",
			path:      "/path/to/object",
			expected:  "https://storageaccount.blob.core.windows.net/container/path/to/object",
		},
		"wasb-alias-stays-on-https": {
			scheme:    schemeAzureStorageBlobAlias,
			host:      "storageaccount.blob.core.windows.net",
			container: "container",
			path:      "/path/to/object",
			expected:  "https://storageaccount.blob.core.windows.net/container/path/to/object",
		},
		"abfss-dfs-host-translated": {
			scheme:    schemeAzureDataLake,
			host:      "storageaccount.dfs.core.windows.net",
			container: "container",
			path:      "/path/to/object",
			expected:  "https://storageaccount.blob.core.windows.net/container/path/to/object",
		},
		"abfs-alias-stays-on-https": {
			scheme:    schemeAzureDataLakeAlias,
			host:      "storageaccount.dfs.core.windows.net",
			container: "container",
			path:      "/path/to/object",
			expected:  "https://storageaccount.blob.core.windows.net/container/path/to/object",
		},
		"abfss-blob-host-untouched": {
			scheme:    schemeAzureDataLake,
			host:      "storageaccount.blob.core.windows.net",
			container: "container",
			path:      "/path/to/object",
			expected:  "https://storageaccount.blob.core.windows.net/container/path/to/object",
		},
		"abfss-non-azure-host-untouched": {
			scheme:    schemeAzureDataLake,
			host:      "storageaccount.dfs.example.com",
			container: "container",
			path:      "/path/to/object",
			expected:  "https://storageaccount.dfs.example.com/container/path/to/object",
		},
		"abfss-missing-container": {
			scheme: schemeAzureDataLake,
			host:   "storageaccount.dfs.core.windows.net",
			path:   "/path/to/object",
			errMsg: "azure blob URI format: abfss://container@storageaccount.dfs.core.windows.net/path/to/blob",
		},
		"az-account-from-env": {
			scheme:   schemeAzureShorthand,
			host:     "container",
			path:     "/path/to/object",
			account:  "storageaccount",
			expected: "https://storageaccount.blob.core.windows.net/container/path/to/object",
		},
		"az-without-account": {
			scheme: schemeAzureShorthand,
			host:   "container",
			path:   "/path/to/object",
			errMsg: "AZURE_STORAGE_ACCOUNT_NAME",
		},
		"az-missing-blob": {
			scheme:  schemeAzureShorthand,
			host:    "container",
			account: "storageaccount",
			errMsg:  "azure blob URI format: az://container/path/to/blob",
		},
		"az-trailing-slash": {
			scheme:  schemeAzureShorthand,
			host:    "container",
			path:    "/path/to/",
			account: "storageaccount",
			errMsg:  "azure blob URI format: az://container/path/to/blob",
		},
		"az-with-blob-host-in-uri": {
			scheme:    schemeAzureShorthand,
			host:      "storageaccount.blob.core.windows.net",
			container: "container",
			path:      "/path/to/object",
			expected:  "https://storageaccount.blob.core.windows.net/container/path/to/object",
		},
		"az-with-dfs-host-in-uri": {
			scheme:    schemeAzureShorthand,
			host:      "storageaccount.dfs.core.windows.net",
			container: "container",
			path:      "/path/to/object",
			expected:  "https://storageaccount.blob.core.windows.net/container/path/to/object",
		},
		"az-uri-account-beats-env": {
			scheme:    schemeAzureShorthand,
			host:      "uriaccount.blob.core.windows.net",
			container: "container",
			path:      "/path/to/object",
			account:   "envaccount",
			expected:  "https://uriaccount.blob.core.windows.net/container/path/to/object",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// Cannot use t.Parallel() with t.Setenv()
			t.Setenv("AZURE_STORAGE_ACCESS_KEY", "")
			t.Setenv("AZURE_STORAGE_ACCOUNT_NAME", tc.account)

			u := url.URL{Scheme: tc.scheme, Host: tc.host, Path: tc.path}
			if tc.container != "" {
				u.User = url.User(tc.container)
			}
			uri, cred, err := azureAccessDetail(u, false, "")
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				require.Equal(t, "", uri)
				require.Nil(t, cred)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, uri)
			require.Nil(t, cred)
		})
	}
}

func TestNormalizeFieldPath(t *testing.T) {
	testCases := []struct {
		name      string
		path      string
		delimiter string
		expected  string
	}{
		{
			name:      "default-dot-delimiter",
			path:      "parent.child",
			delimiter: "",
			expected:  "parent\x01child",
		},
		{
			name:      "custom-slash-delimiter",
			path:      "parent/child",
			delimiter: "/",
			expected:  "parent\x01child",
		},
		{
			name:      "dot-is-literal-with-custom-delimiter",
			path:      "parent.child/leaf.name",
			delimiter: "/",
			expected:  "parent.child\x01leaf.name",
		},
		{
			name:      "multi-character-delimiter",
			path:      "parent::child",
			delimiter: "::",
			expected:  "parent\x01child",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, NormalizeFieldPath(tc.path, tc.delimiter))
		})
	}
}

func TestValidateFieldDelimiter(t *testing.T) {
	testCases := []struct {
		name      string
		delimiter string
		wantErr   bool
	}{
		{
			name:      "valid-dot-delimiter",
			delimiter: ".",
			wantErr:   false,
		},
		{
			name:      "valid-slash-delimiter",
			delimiter: "/",
			wantErr:   false,
		},
		{
			name:      "valid-empty-delimiter-defaults-to-dot",
			delimiter: "",
			wantErr:   false,
		},
		{
			name:      "invalid-multi-char-delimiter",
			delimiter: "::",
			wantErr:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateFieldDelimiter(tc.delimiter)
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "field delimiter must be a single character")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetBucketRegion(t *testing.T) {
	testCases := map[string]struct {
		profile   string
		bucket    string
		public    bool
		ignoreTLS bool
		errMsg    string
	}{
		"non-existent-bucket": {
			"",
			uuid.New().String(),
			true,
			false,
			"not found",
		},
		"unable-to-get-region": {
			"",
			"localhost/something/does/not/matter",
			true,
			false,
			"unable to get region for S3 bucket",
		},
		"bucket-name-with-dot": {
			"",
			"xiehang.com",
			false,
			true,
			"",
		},
		"bucket-name-with-dot-no-ignore": {
			"",
			"xiehang.com",
			false,
			false,
			"unable to get region for S3 bucket",
		},
		"private-bucket": {
			"",
			"doc-example-bucket",
			true,
			false,
			"S3 bucket doc-example-bucket is not public",
		},
		"aws-error": {
			"",
			"00",
			true,
			false,
			"unrecognized StatusCode from AWS: 400",
		},
		"missing-credential": {
			uuid.New().String(),
			"daylight-openstreetmap",
			false,
			false,
			"failed to get shared config profile",
		},
	}

	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("AWS_PROFILE", tc.profile)
			_, err := getS3Client(context.Background(), tc.bucket, tc.public, tc.ignoreTLS)
			if tc.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func TestGetS3BucketRegionUsesHead(t *testing.T) {
	originalClient := http.DefaultClient
	t.Cleanup(func() {
		http.DefaultClient = originalClient
	})

	var method string
	http.DefaultClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			method = req.Method
			return &http.Response{
				StatusCode: http.StatusOK,
				Header: http.Header{
					"X-Amz-Bucket-Region": []string{"us-west-2"},
				},
				Body: io.NopCloser(strings.NewReader("unused bucket listing")),
			}, nil
		}),
	}

	region, err := getS3BucketRegion(context.Background(), "example-bucket", true, false)
	require.NoError(t, err)
	require.Equal(t, "us-west-2", region)
	require.Equal(t, http.MethodHead, method)
}

func TestGetS3ClientUsesConfiguredEndpoint(t *testing.T) {
	originalClient := http.DefaultClient
	t.Cleanup(func() {
		http.DefaultClient = originalClient
	})
	http.DefaultClient = &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("unexpected Amazon S3 region probe")
		}),
	}

	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")

	testCases := []struct {
		name        string
		environment string
		anonymous   bool
		ignoreTLS   bool
	}{
		{name: "global-endpoint", environment: "AWS_ENDPOINT_URL"},
		{name: "s3-endpoint", environment: "AWS_ENDPOINT_URL_S3"},
		{name: "anonymous", environment: "AWS_ENDPOINT_URL_S3", anonymous: true},
		{name: "insecure-tls", environment: "AWS_ENDPOINT_URL_S3", ignoreTLS: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AWS_ENDPOINT_URL", "")
			t.Setenv("AWS_ENDPOINT_URL_S3", "")
			t.Setenv(tc.environment, "http://localhost:9000")

			client, err := getS3Client(context.Background(), "test-bucket", tc.anonymous, tc.ignoreTLS)
			require.NoError(t, err)

			options := client.Options()
			require.Equal(t, "us-west-2", options.Region)
			require.Equal(t, aws.String("http://localhost:9000"), options.BaseEndpoint)
			require.True(t, options.UsePathStyle)

			if tc.anonymous {
				require.Nil(t, options.Credentials)
			}
			if tc.ignoreTLS {
				httpClient, ok := options.HTTPClient.(*http.Client)
				require.True(t, ok)
				transport, ok := httpClient.Transport.(*http.Transport)
				require.True(t, ok)
				require.NotNil(t, transport.TLSClientConfig)
				require.True(t, transport.TLSClientConfig.InsecureSkipVerify)
			}
		})
	}
}

func TestGetS3ClientRegionProbeUsesContext(t *testing.T) {
	type contextKey string
	const key contextKey = "region-probe"

	originalClient := http.DefaultClient
	t.Cleanup(func() {
		http.DefaultClient = originalClient
	})

	var contextValue any
	http.DefaultClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			contextValue = req.Context().Value(key)
			return nil, errors.New("stop region probe")
		}),
	}

	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")

	ctx := context.WithValue(context.Background(), key, "caller context")
	_, err := getS3Client(ctx, "test-bucket", false, false)
	require.ErrorContains(t, err, "stop region probe")
	require.Equal(t, "caller context", contextValue)
}

func TestGetS3ClientUsesSharedConfigEndpoint(t *testing.T) {
	originalClient := http.DefaultClient
	t.Cleanup(func() {
		http.DefaultClient = originalClient
	})
	http.DefaultClient = &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("unexpected Amazon S3 region probe")
		}),
	}

	configPath := filepath.Join(t.TempDir(), "config")
	configData := `[profile custom-endpoint]
region = us-east-2
services = local-s3

[services local-s3]
s3 =
  endpoint_url = http://localhost:9001
`
	require.NoError(t, os.WriteFile(configPath, []byte(configData), 0o600))
	t.Setenv("AWS_CONFIG_FILE", configPath)
	t.Setenv("AWS_PROFILE", "custom-endpoint")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")

	client, err := getS3Client(context.Background(), "test-bucket", false, false)
	require.NoError(t, err)

	options := client.Options()
	require.Equal(t, "us-east-2", options.Region)
	require.Equal(t, aws.String("http://localhost:9001"), options.BaseEndpoint)
	require.True(t, options.UsePathStyle)
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
	colonPath := "parse-uri-" + uuid.NewString() + ":bar.parquet"
	require.NoError(t, os.WriteFile(colonPath, nil, 0o600))
	timestampPath := "2023-01-01T00:00-" + uuid.NewString() + ".parquet"
	require.NoError(t, os.WriteFile(timestampPath, nil, 0o600))
	nonOpaqueDir := "parse-uri-" + uuid.NewString() + ":"
	nonOpaquePath := nonOpaqueDir + "/bar.parquet"
	require.NoError(t, os.Mkdir(nonOpaqueDir, 0o700))
	require.NoError(t, os.WriteFile(nonOpaquePath, nil, 0o600))
	knownSchemePath := "s3:" + uuid.NewString()
	require.NoError(t, os.WriteFile(knownSchemePath, nil, 0o600))
	malformedS3Name := "parse-uri-" + uuid.NewString()
	malformedS3Dir := "s3:/" + malformedS3Name
	malformedS3URI := "s3://" + malformedS3Name + "/%zz"
	require.NoError(t, os.MkdirAll(malformedS3Dir, 0o700))
	require.NoError(t, os.WriteFile(malformedS3URI, nil, 0o600))
	malformedUpperS3Name := "parse-uri-" + uuid.NewString()
	malformedUpperS3Dir := "S3:/" + malformedUpperS3Name
	malformedUpperS3URI := "S3://" + malformedUpperS3Name + "/%zz"
	require.NoError(t, os.MkdirAll(malformedUpperS3Dir, 0o700))
	require.NoError(t, os.WriteFile(malformedUpperS3URI, nil, 0o600))
	t.Cleanup(func() {
		require.NoError(t, os.Remove(colonPath))
		require.NoError(t, os.Remove(timestampPath))
		require.NoError(t, os.Remove(nonOpaquePath))
		require.NoError(t, os.Remove(nonOpaqueDir))
		require.NoError(t, os.Remove(knownSchemePath))
		require.NoError(t, os.Remove(malformedS3URI))
		require.NoError(t, os.Remove(malformedUpperS3URI))
		require.NoError(t, os.Remove(malformedS3Dir))
		require.NoError(t, os.Remove(malformedUpperS3Dir))
		for _, root := range []string{"s3:", "S3:"} {
			if err := os.Remove(root); err != nil && !os.IsNotExist(err) {
				t.Errorf("remove test directory %q: %v", root, err)
			}
		}
	})

	testCases := map[string]struct {
		uri    string
		scheme string
		host   string
		path   string
		errMsg string
	}{
		"invalid-uri": {
			"://uri",
			"",
			"",
			"",
			"unable to parse file location",
		},
		"with-user": {
			"scheme://username@path/to/file",
			"scheme",
			"path",
			"/to/file",
			"",
		},
		"with-file": {
			"file://path/to/file",
			"file",
			"",
			"path/to/file",
			"",
		},
		"with-file-root": {
			"file:///path/to/file",
			"file",
			"",
			"/path/to/file",
			"",
		},
		"without-file": {
			"path/to/file",
			"file",
			"",
			"path/to/file",
			"",
		},
		"existing-local-file-with-colon": {
			colonPath,
			"file",
			"",
			colonPath,
			"",
		},
		"existing-timestamp-file-with-colon": {
			timestampPath,
			"file",
			"",
			timestampPath,
			"",
		},
		"existing-non-opaque-path-with-colon": {
			nonOpaquePath,
			"file",
			"",
			nonOpaquePath,
			"",
		},
		"nonexistent-path-with-colon": {
			"foo:bar.parquet",
			"foo",
			"",
			"",
			"",
		},
		"existing-path-with-known-scheme": {
			knownSchemePath,
			"s3",
			"",
			"",
			"",
		},
		"malformed-known-scheme-existing-locally": {
			malformedS3URI,
			"",
			"",
			"",
			"invalid URL escape",
		},
		"malformed-uppercase-known-scheme-existing-locally": {
			malformedUpperS3URI,
			"",
			"",
			"",
			"invalid URL escape",
		},
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
