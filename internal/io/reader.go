package io

import (
	"context"
	"fmt"
	"net/url"
	"os/user"
	"runtime"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source"
	pqazblob "github.com/hangxie/parquet-go/v2/source/azblob"
	"github.com/hangxie/parquet-go/v2/source/gcs"
	"github.com/hangxie/parquet-go/v2/source/hdfs"
	pqhttp "github.com/hangxie/parquet-go/v2/source/http"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/source/s3v2"
	googleoption "google.golang.org/api/option"
)

// ReadOption includes options for read operation
type ReadOption struct {
	Anonymous              bool              `help:"(S3, GCS, and Azure only) object is publicly accessible." default:"false"`
	HTTPExtraHeaders       map[string]string `mapsep:"," help:"(HTTP URI only) extra HTTP headers." default:""`
	HTTPIgnoreTLSError     bool              `help:"(HTTP URI only) ignore TLS error." default:"false"`
	HTTPMultipleConnection bool              `help:"(HTTP URI only) use multiple HTTP connection." default:"false"`
	ObjectVersion          string            `help:"(S3, GCS, and Azure only) object version." default:""`
}

func newLocalReader(u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	return local.NewLocalFileReader(u.Path)
}

func newAWSS3Reader(u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	s3Client, err := getS3Client(u.Host, option.Anonymous)
	if err != nil {
		return nil, err
	}

	var objVersion *string = nil
	if option.ObjectVersion != "" {
		objVersion = &option.ObjectVersion
	}
	return s3v2.NewS3FileReaderWithClient(context.Background(), s3Client, u.Host, strings.TrimLeft(u.Path, "/"), objVersion)
}

func newAzureStorageBlobReader(u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	azURL, cred, err := azureAccessDetail(*u, option.Anonymous, option.ObjectVersion)
	if err != nil {
		return nil, err
	}

	return pqazblob.NewAzBlobFileReader(context.Background(), azURL, cred, blockblob.ClientOptions{})
}

func newGoogleCloudStorageReader(u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	generation := int64(-1)
	if option.ObjectVersion != "" {
		var err error
		generation, err = strconv.ParseInt(option.ObjectVersion, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid GCS generation [%s]: %w", option.ObjectVersion, err)
		}
	}
	ctx := context.Background()

	options := []googleoption.ClientOption{}
	if option.Anonymous {
		options = append(options, googleoption.WithoutAuthentication())
	}
	client, err := storage.NewClient(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return gcs.NewGcsFileReaderWithClient(ctx, client, "", u.Host, strings.TrimLeft(u.Path, "/"), generation)
}

func newHTTPReader(u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	return pqhttp.NewHttpReader(u.String(), option.HTTPMultipleConnection, option.HTTPIgnoreTLSError, option.HTTPExtraHeaders)
}

func newHDFSReader(u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	userName := u.User.Username()
	if userName == "" {
		osUser, err := user.Current()
		if err == nil && osUser != nil {
			userName = osUser.Username
		}
	}

	return hdfs.NewHdfsFileReader([]string{u.Host}, userName, u.Path)
}

func NewParquetFileReader(URI string, option ReadOption) (*reader.ParquetReader, error) {
	readerFuncTable := map[string]func(*url.URL, ReadOption) (source.ParquetFileReader, error){
		schemeLocal:              newLocalReader,
		schemeAWSS3:              newAWSS3Reader,
		schemeGoogleCloudStorage: newGoogleCloudStorageReader,
		schemeAzureStorageBlob:   newAzureStorageBlobReader,
		schemeHTTP:               newHTTPReader,
		schemeHTTPS:              newHTTPReader,
		schemeHDFS:               newHDFSReader,
	}

	u, err := parseURI(URI)
	if err != nil {
		return nil, err
	}
	readerFunc, found := readerFuncTable[u.Scheme]
	if !found {
		return nil, fmt.Errorf("unknown location scheme [%s]", u.Scheme)
	}

	fileReader, err := readerFunc(u, option)
	if err != nil {
		return nil, fmt.Errorf("unable to open file [%s]: %w", u.String(), err)
	}

	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}
