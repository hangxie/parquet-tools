package internal

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
	"github.com/hangxie/parquet-go/parquet"
)

const (
	schemeLocal              string = "file"
	schemeGoogleCloudStorage string = "gs"
	schemeHDFS               string = "hdfs"
	schemeHTTP               string = "http"
	schemeHTTPS              string = "https"
	schemeAWSS3              string = "s3"
	schemeAzureStorageBlob   string = "wasbs"
)

func parseURI(uri string) (*url.URL, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("unable to parse file location [%s]: %w", uri, err)
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

func getS3BucketRegion(bucket string, isPublic bool) (string, error) {
	if strings.Contains(bucket, ".") {
		// AWS' wildcard cert covers *.s3.amazonaws.com, so if the bucket name contains dot the cert will be invalid
		http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	resp, err := http.Get(fmt.Sprintf("https://%s.s3.amazonaws.com", bucket))
	if err != nil {
		return "", fmt.Errorf("unable to get region for S3 bucket %s: %w", bucket, err)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return resp.Header.Get("x-amz-bucket-region"), nil
	case http.StatusNotFound:
		return "", fmt.Errorf("S3 bucket %s not found", bucket)
	case http.StatusForbidden:
		if isPublic {
			return "", fmt.Errorf("S3 bucket %s is not public", bucket)
		}
		return resp.Header.Get("x-amz-bucket-region"), nil
	default:
		return "", fmt.Errorf("unrecognized StatusCode from AWS: %d", resp.StatusCode)
	}
}

func getS3Client(bucket string, isPublic bool) (*s3.Client, error) {
	region, err := getS3BucketRegion(bucket, isPublic)
	if err != nil {
		return nil, fmt.Errorf("unable to access to [%s]: %w", bucket, err)
	}
	if isPublic {
		return s3.NewFromConfig(aws.Config{Region: region}), nil
	}

	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithDefaultRegion("us-east-1"))
	if err != nil {
		return nil, fmt.Errorf("failed to load config to determine bucket region: %w", err)
	}
	cfg.Region = region
	return s3.NewFromConfig(cfg), nil
}

func azureAccessDetail(azURL url.URL, anonymous bool) (string, *azblob.SharedKeyCredential, error) {
	container := azURL.User.Username()
	if azURL.Host == "" || container == "" || strings.HasSuffix(azURL.Path, "/") {
		return "", nil, fmt.Errorf("azure blob URI format: wasbs://container@storageaccount.blob.core.windows.net/path/to/blob")
	}
	httpURL := fmt.Sprintf("https://%s/%s%s", azURL.Host, container, azURL.Path)

	accessKey := os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	if anonymous || accessKey == "" {
		// anonymous access
		return httpURL, nil, nil
	}

	credential, err := azblob.NewSharedKeyCredential(strings.Split(azURL.Host, ".")[0], accessKey)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create Azure credential: %w", err)
	}

	return httpURL, credential, nil
}

func compressionCodec(codecName string) (parquet.CompressionCodec, error) {
	codec, err := parquet.CompressionCodecFromString(codecName)
	if err != nil {
		return parquet.CompressionCodec_UNCOMPRESSED, err
	}
	switch codec {
	case parquet.CompressionCodec_BROTLI, parquet.CompressionCodec_LZO:
		return parquet.CompressionCodec_UNCOMPRESSED, fmt.Errorf("%s compression is not supported at this moment", codec.String())
	}
	return codec, nil
}
