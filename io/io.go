package io

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

const (
	schemeLocal              string = "file"
	schemeGoogleCloudStorage string = "gs"
	schemeHDFS               string = "hdfs"
	schemeHTTP               string = "http"
	schemeHTTPS              string = "https"
	schemeAWSS3              string = "s3"
	schemeAzureStorageBlob   string = "wasbs"
	// wasb and abfs nominally mean plain HTTP in Hadoop, but they are accepted here
	// as naming aliases only: every Azure scheme talks to the Blob endpoint over HTTPS.
	schemeAzureStorageBlobAlias string = "wasb"
	schemeAzureDataLake         string = "abfss"
	schemeAzureDataLakeAlias    string = "abfs"
	schemeAzureShorthand        string = "az"

	azureBlobHostSuffix string = ".blob.core.windows.net"
	azureDFSHostSuffix  string = ".dfs.core.windows.net"
	azureAccountEnvVar  string = "AZURE_STORAGE_ACCOUNT_NAME"
)

// ValidateFieldDelimiter rejects values that conflict with field assignments.
func ValidateFieldDelimiter(delimiter string) error {
	if delimiter == "" {
		return nil
	}
	if len(delimiter) != 1 {
		return fmt.Errorf("field delimiter must be a single character")
	}
	return nil
}

// NormalizeFieldPath splits path by delimiter (defaulting to "." when delimiter is empty)
// and joins the segments with the internal ParGoPathDelimiter.
func NormalizeFieldPath(path, delimiter string) string {
	if delimiter == "" {
		delimiter = "."
	}
	return common.PathToStr(strings.Split(path, delimiter))
}

func parseURI(uri string) (*url.URL, error) {
	u, err := url.Parse(uri)
	if err != nil {
		scheme, _, hasScheme := strings.Cut(uri, ":")
		if !hasScheme || !knownLocationScheme(strings.ToLower(scheme)) {
			if localURI, found := existingLocalURI(uri); found {
				return localURI, nil
			}
		}
		return nil, fmt.Errorf("unable to parse file location [%s]: %w", uri, err)
	}

	if u.Scheme != "" && !knownLocationScheme(u.Scheme) {
		if localURI, found := existingLocalURI(uri); found {
			u = localURI
		}
	}

	if u.Scheme == "" {
		u.Scheme = schemeLocal
	}

	if u.Scheme == schemeLocal {
		u.Path = filepath.Join(u.Host, u.Path)
		u.Host = ""
	}

	return u, nil
}

func existingLocalURI(uri string) (*url.URL, bool) {
	if _, err := os.Stat(uri); err != nil {
		return nil, false
	}
	return &url.URL{Scheme: schemeLocal, Path: uri}, true
}

func knownLocationScheme(scheme string) bool {
	switch scheme {
	case schemeLocal,
		schemeGoogleCloudStorage,
		schemeHDFS,
		schemeHTTP,
		schemeHTTPS,
		schemeAWSS3,
		schemeAzureStorageBlob,
		schemeAzureStorageBlobAlias,
		schemeAzureDataLake,
		schemeAzureDataLakeAlias,
		schemeAzureShorthand:
		return true
	default:
		return false
	}
}

func newTLSInsecureHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
}

func getS3BucketRegion(ctx context.Context, bucket string, isPublic, ignoreTLS bool) (string, error) {
	client := http.DefaultClient
	if strings.Contains(bucket, ".") && ignoreTLS {
		// AWS' wildcard cert covers *.s3.amazonaws.com, so if the bucket name
		// contains a dot the cert will be invalid. Use a dedicated client with
		// TLS verification disabled instead of mutating http.DefaultTransport.
		client = newTLSInsecureHTTPClient()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, fmt.Sprintf("https://%s.s3.amazonaws.com", bucket), nil)
	if err != nil {
		return "", fmt.Errorf("unable to get region for S3 bucket %s: %w", bucket, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("unable to get region for S3 bucket %s: %w", bucket, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		return resp.Header.Get("X-Amz-Bucket-Region"), nil
	case http.StatusNotFound:
		return "", fmt.Errorf("S3 bucket %s not found", bucket)
	case http.StatusForbidden:
		if isPublic {
			return "", fmt.Errorf("S3 bucket %s is not public", bucket)
		}
		return resp.Header.Get("X-Amz-Bucket-Region"), nil
	default:
		return "", fmt.Errorf("unrecognized StatusCode from AWS: %d", resp.StatusCode)
	}
}

func getS3Client(ctx context.Context, bucket string, isPublic, ignoreTLS bool) (*s3.Client, error) {
	needCustomHTTP := strings.Contains(bucket, ".") && ignoreTLS
	opts := []func(*config.LoadOptions) error{config.WithDefaultRegion("us-east-1")}
	if isPublic {
		opts = append(opts, config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	}
	if needCustomHTTP {
		opts = append(opts, config.WithHTTPClient(newTLSInsecureHTTPClient()))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config for S3 bucket %s: %w", bucket, err)
	}

	// Service-specific endpoints such as AWS_ENDPOINT_URL_S3 are resolved while
	// constructing an S3 client rather than being exposed on aws.Config.
	configuredClient := s3.NewFromConfig(cfg)
	if configuredClient.Options().BaseEndpoint != nil {
		if ignoreTLS && !needCustomHTTP {
			cfg.HTTPClient = newTLSInsecureHTTPClient()
		}
		return s3.NewFromConfig(cfg, func(options *s3.Options) {
			options.UsePathStyle = true
		}), nil
	}

	region, err := getS3BucketRegion(ctx, bucket, isPublic, ignoreTLS)
	if err != nil {
		return nil, fmt.Errorf("unable to access to [%s]: %w", bucket, err)
	}
	cfg.Region = region
	return s3.NewFromConfig(cfg), nil
}

// azureBlobLocation maps any of the accepted Azure URI spellings to the Blob REST
// endpoint host and container that the azblob SDK talks to.
func azureBlobLocation(azURL url.URL) (string, string, error) {
	host, container := azURL.Host, azURL.User.Username()
	if azURL.Scheme == schemeAzureShorthand && container == "" {
		// adlfs shorthand az://container/path carries no account, so it has to
		// come from the environment; the account-bearing spelling falls through.
		account := os.Getenv(azureAccountEnvVar)
		if account == "" {
			return "", "", fmt.Errorf("%s:// URI without a storage account requires environment variable %s to name it", schemeAzureShorthand, azureAccountEnvVar)
		}
		host, container = account+azureBlobHostSuffix, azURL.Host
	}

	// ADLS Gen2 serves the same data on both endpoints, so a DFS host only needs
	// renaming to reach the Blob API.
	if account, found := strings.CutSuffix(host, azureDFSHostSuffix); found {
		host = account + azureBlobHostSuffix
	}
	return host, container, nil
}

// azureURIFormat spells out the expected URI layout for the scheme the user typed.
func azureURIFormat(scheme string) string {
	switch scheme {
	case schemeAzureShorthand:
		return schemeAzureShorthand + "://container/path/to/blob"
	case schemeAzureDataLake, schemeAzureDataLakeAlias:
		return scheme + "://container@storageaccount" + azureDFSHostSuffix + "/path/to/blob"
	case schemeAzureStorageBlobAlias:
		return scheme + "://container@storageaccount" + azureBlobHostSuffix + "/path/to/blob"
	default:
		return schemeAzureStorageBlob + "://container@storageaccount" + azureBlobHostSuffix + "/path/to/blob"
	}
}

func azureAccessDetail(azURL url.URL, anonymous bool, versionId string) (string, *azblob.SharedKeyCredential, error) {
	host, container, err := azureBlobLocation(azURL)
	if err != nil {
		return "", nil, err
	}
	if host == "" || container == "" || azURL.Path == "" || strings.HasSuffix(azURL.Path, "/") {
		return "", nil, fmt.Errorf("azure blob URI format: %s", azureURIFormat(azURL.Scheme))
	}
	httpURL := fmt.Sprintf("https://%s/%s%s", host, container, azURL.Path)
	if versionId != "" {
		httpURL = fmt.Sprintf("%s?versionid=%s", httpURL, versionId)
	}

	accessKey := os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	if anonymous || accessKey == "" {
		// anonymous access
		return httpURL, nil, nil
	}

	credential, err := azblob.NewSharedKeyCredential(strings.Split(host, ".")[0], accessKey)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create Azure credential: %w", err)
	}

	return httpURL, credential, nil
}

// ValidCompressionCodecs lists the compression codecs supported for writing.
var ValidCompressionCodecs = []string{
	"UNCOMPRESSED", "SNAPPY", "GZIP", "LZ4", "LZ4_RAW", "ZSTD", "BROTLI",
}

func compressionCodec(codecName string) (parquet.CompressionCodec, error) {
	// Normalize the codec name to uppercase
	codecName = strings.ToUpper(codecName)

	// Validate the codec name
	codec, err := parquet.CompressionCodecFromString(codecName)
	if err != nil {
		return parquet.CompressionCodec_UNCOMPRESSED, fmt.Errorf("invalid compression codec [%s]: %w, valid codecs: %s", codecName, err, strings.Join(ValidCompressionCodecs, ", "))
	}

	// Check for unsupported codecs
	switch codec {
	case parquet.CompressionCodec_LZO:
		return parquet.CompressionCodec_UNCOMPRESSED, fmt.Errorf("[%s] compression is not supported at this moment", codec.String())
	}

	return codec, nil
}
