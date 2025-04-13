package internal

import (
	"context"
	"fmt"
	"net/url"
	"os/user"
	"runtime"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	pqtazblob "github.com/hangxie/parquet-go-source/azblob"
	"github.com/hangxie/parquet-go-source/gcs"
	"github.com/hangxie/parquet-go-source/hdfs"
	pqhttp "github.com/hangxie/parquet-go-source/http"
	"github.com/hangxie/parquet-go-source/local"
	"github.com/hangxie/parquet-go-source/s3v2"
	"github.com/hangxie/parquet-go/reader"
	googleoption "google.golang.org/api/option"
)

// ReadOption includes options for read operation
type ReadOption struct {
	HTTPMultipleConnection bool              `help:"(HTTP URI only) use multiple HTTP connection." default:"false"`
	HTTPIgnoreTLSError     bool              `help:"(HTTP URI only) ignore TLS error." default:"false"`
	HTTPExtraHeaders       map[string]string `mapsep:"," help:"(HTTP URI only) extra HTTP headers." default:""`
	ObjectVersion          string            `help:"(S3 URI only) object version." default:""`
	Anonymous              bool              `help:"(S3, GCS, and Azure only) object is publicly accessible." default:"false"`
}

func newLocalReader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	fileReader, err := local.NewLocalFileReader(u.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open local file [%s]: %w", u.Path, err)
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newAWSS3Reader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	s3Client, err := getS3Client(u.Host, option.Anonymous)
	if err != nil {
		return nil, err
	}

	var objVersion *string = nil
	if option.ObjectVersion != "" {
		objVersion = &option.ObjectVersion
	}
	fileReader, err := s3v2.NewS3FileReaderWithClientVersioned(context.Background(), s3Client, u.Host, strings.TrimLeft(u.Path, "/"), objVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to open S3 object [%s] version [%s]: %w", u.String(), option.ObjectVersion, err)
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newAzureStorageBlobReader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	azURL, cred, err := azureAccessDetail(*u, option.Anonymous)
	if err != nil {
		return nil, err
	}

	fileReader, err := pqtazblob.NewAzBlobFileReaderWithSharedKey(context.Background(), azURL, cred, blockblob.ClientOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to open Azure blob object [%s]: %w", u.String(), err)
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newGoogleCloudStorageReader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	ctx := context.Background()

	options := []googleoption.ClientOption{}
	if option.Anonymous {
		options = append(options, googleoption.WithoutAuthentication())
	}
	client, err := storage.NewClient(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	fileReader, err := gcs.NewGcsFileReaderWithClient(ctx, client, "", u.Host, strings.TrimLeft(u.Path, "/"))
	if err != nil {
		return nil, fmt.Errorf("failed to open GCS object [%s]: %w", u.String(), err)
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newHTTPReader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	fileReader, err := pqhttp.NewHttpReader(u.String(), option.HTTPMultipleConnection, option.HTTPIgnoreTLSError, option.HTTPExtraHeaders)
	if err != nil {
		return nil, fmt.Errorf("failed to open HTTP source [%s]: %w", u.String(), err)
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newHDFSReader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	userName := u.User.Username()
	if userName == "" {
		osUser, err := user.Current()
		if err == nil && osUser != nil {
			userName = osUser.Username
		}
	}

	fileReader, err := hdfs.NewHdfsFileReader([]string{u.Host}, userName, u.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open HDFS source [%s]: %w", u.String(), err)
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func NewParquetFileReader(URI string, option ReadOption) (*reader.ParquetReader, error) {
	readerFuncTable := map[string]func(*url.URL, ReadOption) (*reader.ParquetReader, error){
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
	if readerFunc, found := readerFuncTable[u.Scheme]; found {
		return readerFunc(u, option)
	}

	return nil, fmt.Errorf("unknown location scheme [%s]", u.Scheme)
}
