package internal

import (
	"crypto/rand"
	"encoding/base64"
	"net/url"
	"os"
	"path/filepath"
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

func Test_NewParquetFileReader(t *testing.T) {
	rOpt := ReadOption{}
	s3URL := "s3://daylight-openstreetmap/parquet/osm_features/release=v1.46/type=way/20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0"
	gcsURL := "gs://cloud-samples-data/bigquery/us-states/us-states.parquet"
	azblobURL := "wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet"
	testCases := map[string]struct {
		uri    string
		option ReadOption
		errMsg string
	}{
		"invalid-uri":            {"://uri", rOpt, "unable to parse file location"},
		"invalid-scheme":         {"invalid-scheme://something", rOpt, "unknown location scheme"},
		"local-file-not-found":   {"file://path/to/file", rOpt, "no such file or directory"},
		"local-file-not-parquet": {"../../testdata/not-a-parquet-file", rOpt, "invalid argument"},
		"local-file-good":        {"../../testdata/good.parquet", rOpt, ""},
		"s3-not-found":           {"s3://bucket-does-not-exist", rOpt, "not found"},
		"s3-good":                {s3URL, ReadOption{Anonymous: true}, ""},
		"s3-wrong-version":       {s3URL, ReadOption{ObjectVersion: "random-version-id", Anonymous: true}, "https response error StatusCode: 400"},
		"gcs-no-permission":      {gcsURL, rOpt, "failed to create GCS client"},
		"gcs-good":               {gcsURL, ReadOption{Anonymous: true}, ""},
		"azblob-no-permission":   {azblobURL, rOpt, "failed to open Azure blob object"},
		"azblob-good":            {azblobURL, ReadOption{Anonymous: true}, ""},
		"http-bad-url":           {"https://no-such-host.tld/", rOpt, "no such host"},
		"http-no-range-support":  {"https://www.google.com/", rOpt, "does not support range"},
		"http-good":              {"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet", rOpt, ""},
		"hdfs-failed":            {"hdfs://localhost:1/temp/good.parquet", rOpt, "connection refused"},
	}

	_ = os.Setenv("AWS_CONFIG_FILE", "/dev/null")
	_ = os.Unsetenv("AWS_PROFILE")
	_ = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")
	_ = os.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(uuid.New().NodeID()))
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			_, err := NewParquetFileReader(tc.uri, tc.option)
			if tc.errMsg == "" {
				require.Nil(t, err)
				return
			}
			require.NotNil(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func Test_NewParquetFileWriter(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "unit-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()
	tempFile := filepath.Join(tempDir, "unit-test.parquet")
	testCases := map[string]struct {
		uri    string
		errMsg string
	}{
		"invalid-uri":          {"://uri", "unable to parse file location"},
		"invalid-scheme":       {"invalid-scheme://something", "unknown location scheme"},
		"local-file-not-found": {"file://path/to/file", "no such file or directory"},
		"local-not-file":       {"../../testdata/", "is a directory"},
		"local-file-good":      {tempFile, ""},
		"s3-bucket-not-found":  {"s3://bucket-does-not-exist" + uuid.NewString(), "not found"},
		"s3-good":              {"s3://daylight-openstreetmap/will-not-create-till-close", ""},
		"gcs-no-permission":    {"gs://cloud-samples-data/bigquery/us-states/us-states.parquet", "failed to open GCS object"},
		"azblob-invalid-uri1":  {"wasbs://bad/url", "azure blob URI format:"},
		"azblob-invalid-uri2":  {"wasbs://storageaccount.blob.core.windows.net//aa", "azure blob URI format:"},
		"azblob-good":          {"wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/will-not-create-till-close", ""},
		"http-not-support":     {"https://domain.tld/path/to/file", "writing to https endpoint is not currently supported"},
	}

	_ = os.Setenv("AWS_CONFIG_FILE", "/dev/null")
	_ = os.Unsetenv("AWS_PROFILE")
	_ = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")
	_ = os.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(uuid.New().NodeID()))
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pw, err := NewParquetFileWriter(tc.uri, WriteOption{})
			defer func() {
				if pw != nil {
					_ = pw.Close()
				}
			}()
			if tc.errMsg != "" {
				require.NotNil(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.Nil(t, err)
		})
	}
}

func Test_NewCSVWriter(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "unit-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()
	tempFile := filepath.Join(tempDir, "unit-test.parquet")
	wOpt := WriteOption{}
	testCases := map[string]struct {
		option WriteOption
		uri    string
		schema []string
		errMsg string
	}{
		"invalid-uri":        {wOpt, "://uri", nil, "unable to parse file location"},
		"invalid-scheme":     {wOpt, "invalid-scheme://something", nil, "unknown location scheme"},
		"invalid-schema1":    {wOpt, tempFile, []string{"invalid schema"}, "expect 'key=value'"},
		"invalid-schema2":    {wOpt, tempFile, []string{"name=Id"}, "not a valid Type string"},
		"invalid-schema3":    {wOpt, tempFile, []string{"name=Id, type=FOOBAR"}, "type FOOBAR: not a valid Type string"},
		"invalid-codec":      {WriteOption{Compression: "FOOBAR"}, tempFile, []string{"name=Id, type=INT64"}, "not a valid CompressionCodec string"},
		"unsupported-codec1": {WriteOption{Compression: "BROTLI"}, tempFile, []string{"name=Id, type=INT64"}, "compression is not supported at this moment"},
		"unsupported-codec2": {WriteOption{Compression: "LZO"}, tempFile, []string{"name=Id, type=INT64"}, "compression is not supported at this moment"},
		"hdfs-failed":        {wOpt, "hdfs://localhost:1/temp/good.parquet", nil, "connection refused"},
		"all-good":           {WriteOption{Compression: "SNAPPY"}, tempFile, []string{"name=Id, type=INT64"}, ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pw, err := NewCSVWriter(tc.uri, tc.option, tc.schema)
			defer func() {
				if pw != nil {
					_ = pw.PFile.Close()
				}
			}()
			if tc.errMsg != "" {
				require.NotNil(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.Nil(t, err)
			require.NotNil(t, pw)
		})
	}
}

func Test_NewJSONWriter(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "split-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()
	tempFile := filepath.Join(tempDir, "unit-test.parquet")

	testCases := map[string]struct {
		uri    string
		schema string
		errMsg string
	}{
		"invalid-uri":     {"://uri", "", "unable to parse file location"},
		"invalid-schema1": {tempFile, "invalid schema", "error in unmarshalling json schema string"},
		"invalid-schema2": {tempFile, `{"Tag":"name=top","Fields":[{"Tag":"name=id, type=FOOBAR"}]}`, "type FOOBAR: not a valid Type string"},
		"all-good":        {tempFile, `{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=id, type=INT64"}]}`, ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pw, err := NewJSONWriter(tc.uri, WriteOption{Compression: "SNAPPY"}, tc.schema)
			defer func() {
				if pw != nil {
					_ = pw.PFile.Close()
				}
			}()
			if tc.errMsg != "" {
				require.NotNil(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.Nil(t, err)
			require.NotNil(t, pw)
		})
	}
}

func Test_NewGenericWriter(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "unit-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()
	tempFile := filepath.Join(tempDir, "unit-test.parquet")
	schema := `{"Tag":"name=root","Fields":[{"Tag":"name=id, type=INT64"}]}`

	testCases := map[string]struct {
		uri    string
		option WriteOption
		schema string
		errMsg string
	}{
		"invalud-uri":        {"://uri", WriteOption{}, "", "unable to parse file location"},
		"schema-not-json":    {tempFile, WriteOption{}, "invalid schema", "error in unmarshalling json schema string:"},
		"schema-invalid":     {tempFile, WriteOption{}, `{"Tag":"name=root","Fields":[{"Tag":"name=id, type=FOOBAR"}]}`, "type FOOBAR: not a valid Type string"},
		"invalid-codec":      {tempFile, WriteOption{Compression: "FOOBAR"}, schema, "not a valid CompressionCodec string"},
		"unsupported-codec1": {tempFile, WriteOption{Compression: "BROTLI"}, schema, "compression is not supported at this moment"},
		"unsupported-codec2": {tempFile, WriteOption{Compression: "LZO"}, schema, "compression is not supported at this moment"},
		"all-good":           {tempFile, WriteOption{Compression: "SNAPPY"}, schema, ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pw, err := NewGenericWriter(tc.uri, tc.option, tc.schema)
			defer func() {
				if pw != nil {
					_ = pw.PFile.Close()
				}
			}()
			if tc.errMsg == "" {
				require.Nil(t, err)
				return
			}
			require.NotNil(t, err)
			require.Nil(t, pw)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}
