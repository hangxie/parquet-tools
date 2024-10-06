package internal

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	pqtazblob "github.com/xitongsys/parquet-go-source/azblob"
	"github.com/xitongsys/parquet-go-source/gcs"
	"github.com/xitongsys/parquet-go-source/hdfs"
	pqhttp "github.com/xitongsys/parquet-go-source/http"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

const (
	schemeLocal              string = "file"
	schemeAWSS3              string = "s3"
	schemeGoogleCloudStorage string = "gs"
	schemeAzureStorageBlob   string = "wasbs"
	schemeHTTP               string = "http"
	schemeHTTPS              string = "https"
	schemeHDFS               string = "hdfs"
)

// ReadOption includes options for read operation
type ReadOption struct {
	HTTPMultipleConnection bool              `help:"(HTTP URI only) use multiple HTTP connection." default:"false"`
	HTTPIgnoreTLSError     bool              `help:"(HTTP URI only) ignore TLS error." default:"false"`
	HTTPExtraHeaders       map[string]string `mapsep:"," help:"(HTTP URI only) extra HTTP headers." default:""`
	ObjectVersion          string            `help:"(S3 URI only) object version." default:""`
	Anonymous              bool              `help:"(S3 and Azure only) object is publicly accessible." default:"false"`
}

// WriteOption includes options for write operation
type WriteOption struct {
	Compression string `short:"z" help:"compression codec (UNCOMPRESSED/SNAPPY/GZIP/LZ4/LZ4_RAW/ZSTD)" enum:"UNCOMPRESSED,SNAPPY,GZIP,LZ4,LZ4_RAW,ZSTD" default:"SNAPPY"`
}

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
		return "", fmt.Errorf("unable to get region for S3 bucket %s: %s", bucket, err)
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
		return nil, fmt.Errorf("unable to find region of bucket [%s]: %s", bucket, err)
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
	fileReader, err := gcs.NewGcsFileReader(context.Background(), "", u.Host, strings.TrimLeft(u.Path, "/"))
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

func newLocalWriter(u *url.URL, option WriteOption) (source.ParquetFile, error) {
	fileWriter, err := local.NewLocalFileWriter(u.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open local file [%s]: %w", u.Path, err)
	}
	return fileWriter, nil
}

func newAWSS3Writer(u *url.URL, option WriteOption) (source.ParquetFile, error) {
	s3Client, err := getS3Client(u.Host, false)
	if err != nil {
		return nil, err
	}

	fileWriter, err := s3v2.NewS3FileWriterWithClient(context.Background(), s3Client, u.Host, strings.TrimLeft(u.Path, "/"), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open S3 object [%s]: %w", u.String(), err)
	}
	return fileWriter, nil
}

func newGoogleCloudStorageWriter(u *url.URL, option WriteOption) (source.ParquetFile, error) {
	fileWriter, err := gcs.NewGcsFileWriter(context.Background(), "", u.Host, strings.TrimLeft(u.Path, "/"))
	if err != nil {
		return nil, fmt.Errorf("failed to open GCS object [%s]: %w", u.String(), err)
	}
	return fileWriter, nil
}

func newAzureStorageBlobWriter(u *url.URL, option WriteOption) (source.ParquetFile, error) {
	// write operation cannot be with anonymous access
	azURL, cred, err := azureAccessDetail(*u, false)
	if err != nil {
		return nil, err
	}

	fileWriter, err := pqtazblob.NewAzBlobFileWriterWithSharedKey(context.Background(), azURL, cred, blockblob.ClientOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to open Azure blob object [%s]: %w", u.String(), err)
	}
	return fileWriter, nil
}

func newHTTPWriter(u *url.URL, option WriteOption) (source.ParquetFile, error) {
	return nil, fmt.Errorf("writing to %s endpoint is not currently supported", u.Scheme)
}

func newHDFSWriter(u *url.URL, option WriteOption) (source.ParquetFile, error) {
	userName := u.User.Username()
	if userName == "" {
		osUser, err := user.Current()
		if err == nil && osUser != nil {
			userName = osUser.Username
		}
	}
	fileWriter, err := hdfs.NewHdfsFileWriter([]string{u.Host}, userName, u.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open HDFS source [%s]: %w", u.String(), err)
	}
	return fileWriter, nil
}

func NewParquetFileWriter(uri string, option WriteOption) (source.ParquetFile, error) {
	writerFuncTable := map[string]func(*url.URL, WriteOption) (source.ParquetFile, error){
		schemeLocal:              newLocalWriter,
		schemeAWSS3:              newAWSS3Writer,
		schemeGoogleCloudStorage: newGoogleCloudStorageWriter,
		schemeAzureStorageBlob:   newAzureStorageBlobWriter,
		schemeHTTP:               newHTTPWriter,
		schemeHTTPS:              newHTTPWriter,
		schemeHDFS:               newHDFSWriter,
	}

	u, err := parseURI(uri)
	if err != nil {
		return nil, err
	}
	if writerFunc, found := writerFuncTable[u.Scheme]; found {
		return writerFunc(u, option)
	}
	return nil, fmt.Errorf("unknown location scheme [%s]", u.Scheme)
}

func NewCSVWriter(uri string, option WriteOption, schema []string) (*writer.CSVWriter, error) {
	fileWriter, err := NewParquetFileWriter(uri, option)
	if err != nil {
		return nil, err
	}

	pw, err := writer.NewCSVWriter(schema, fileWriter, int64(runtime.NumCPU()))
	if err != nil {
		fileWriter.Close()
		return nil, err
	}
	codec, err := compressionCodec(option.Compression)
	if err != nil {
		fileWriter.Close()
		return nil, err
	}
	pw.CompressionType = codec
	return pw, nil
}

func NewJSONWriter(uri string, option WriteOption, schema string) (*writer.JSONWriter, error) {
	fileWriter, err := NewParquetFileWriter(uri, option)
	if err != nil {
		return nil, err
	}

	pw, err := writer.NewJSONWriter(schema, fileWriter, int64(runtime.NumCPU()))
	if err != nil {
		fileWriter.Close()
		return nil, err
	}
	codec, err := compressionCodec(option.Compression)
	if err != nil {
		fileWriter.Close()
		return nil, err
	}
	pw.CompressionType = codec
	return pw, nil
}

func NewGenericWriter(uri string, option WriteOption, schema string) (*writer.ParquetWriter, error) {
	fileWriter, err := NewParquetFileWriter(uri, option)
	if err != nil {
		return nil, err
	}

	pw, err := writer.NewParquetWriter(fileWriter, schema, int64(runtime.NumCPU()))
	if err != nil {
		fileWriter.Close()
		return nil, err
	}
	codec, err := compressionCodec(option.Compression)
	if err != nil {
		fileWriter.Close()
		return nil, err
	}
	pw.CompressionType = codec
	return pw, nil
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
