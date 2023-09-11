package internal

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	pqtazblob "github.com/xitongsys/parquet-go-source/azblob"
	"github.com/xitongsys/parquet-go-source/gcs"
	"github.com/xitongsys/parquet-go-source/hdfs"
	"github.com/xitongsys/parquet-go-source/http"
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

// CommonOption represents common options across most commands
type CommonOption struct {
	URI string `arg:"" predictor:"file" help:"URI of Parquet file."`
}

// ReadOption includes options for read operation
type ReadOption struct {
	CommonOption
	HTTPMultipleConnection bool              `help:"(HTTP URI only) use multiple HTTP connection." default:"false"`
	HTTPIgnoreTLSError     bool              `help:"(HTTP URI only) ignore TLS error." default:"false"`
	HTTPExtraHeaders       map[string]string `mapsep:"," help:"(HTTP URI only) extra HTTP headers." default:""`
	ObjectVersion          string            `help:"(S3 URI only) object version." default:""`
	Anonymous              bool              `help:"(S3 and Azure only) object is publicly accessible." default:"false"`
}

// WriteOption includes options for write operation
type WriteOption struct {
	CommonOption
	Compression string `short:"z" help:"compression codec (UNCOMPRESSED/SNAPPY/GZIP/LZ4/ZSTD)" enum:"UNCOMPRESSED,SNAPPY,GZIP,LZ4,ZSTD" default:"SNAPPY"`
}

func parseURI(uri string) (*url.URL, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("unable to parse file location [%s]: %s", uri, err.Error())
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

func getS3Client(bucket string, isPublic bool) (*s3.Client, error) {
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithDefaultRegion("us-east-1"))
	if err != nil {
		return nil, fmt.Errorf("failed to load config to determine bucket region: %s", err.Error())
	}
	region, err := manager.GetBucketRegion(ctx, s3.NewFromConfig(cfg), bucket)
	if err != nil {
		var apiErr manager.BucketNotFound
		if errors.As(err, &apiErr) {
			return nil, fmt.Errorf("unable to find region of bucket [%s]", bucket)
		}
		return nil, fmt.Errorf("AWS error: %s", err.Error())
	}

	if isPublic {
		return s3.NewFromConfig(aws.Config{Region: region}), nil
	}
	cfg.Region = region
	return s3.NewFromConfig(cfg), nil
}

func newLocalReader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	fileReader, err := local.NewLocalFileReader(u.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open local file [%s]: %s", u.Path, err.Error())
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
		return nil, fmt.Errorf("failed to open S3 object [%s] version [%s]: %s", u.String(), option.ObjectVersion, err.Error())
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
		return nil, fmt.Errorf("failed to open Azure blob object [%s]: %s", u.String(), err.Error())
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newGoogleCloudStorageReader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	fileReader, err := gcs.NewGcsFileReader(context.Background(), "", u.Host, strings.TrimLeft(u.Path, "/"))
	if err != nil {
		return nil, fmt.Errorf("failed to open GCS object [%s]: %s", u.String(), err.Error())
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newHTTPReader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	fileReader, err := http.NewHttpReader(u.String(), option.HTTPMultipleConnection, option.HTTPIgnoreTLSError, option.HTTPExtraHeaders)
	if err != nil {
		return nil, fmt.Errorf("failed to open HTTP source [%s]: %s", u.String(), err.Error())
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
		return nil, fmt.Errorf("failed to open HDFS source [%s]: %s", u.String(), err.Error())
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func NewParquetFileReader(option ReadOption) (*reader.ParquetReader, error) {
	readerFuncTable := map[string]func(*url.URL, ReadOption) (*reader.ParquetReader, error){
		schemeLocal:              newLocalReader,
		schemeAWSS3:              newAWSS3Reader,
		schemeGoogleCloudStorage: newGoogleCloudStorageReader,
		schemeAzureStorageBlob:   newAzureStorageBlobReader,
		schemeHTTP:               newHTTPReader,
		schemeHTTPS:              newHTTPReader,
		schemeHDFS:               newHDFSReader,
	}

	u, err := parseURI(option.URI)
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
		return nil, fmt.Errorf("failed to open local file [%s]: %s", u.Path, err.Error())
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
		return nil, fmt.Errorf("failed to open S3 object [%s]: %s", u.String(), err.Error())
	}
	return fileWriter, nil
}

func newGoogleCloudStorageWriter(u *url.URL, option WriteOption) (source.ParquetFile, error) {
	fileWriter, err := gcs.NewGcsFileWriter(context.Background(), "", u.Host, strings.TrimLeft(u.Path, "/"))
	if err != nil {
		return nil, fmt.Errorf("failed to open GCS object [%s]: %s", u.String(), err.Error())
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
		return nil, fmt.Errorf("failed to open Azure blob object [%s]: %s", u.String(), err.Error())
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
		return nil, fmt.Errorf("failed to open HDFS source [%s]: %s", u.String(), err.Error())
	}
	return fileWriter, nil
}

func NewParquetFileWriter(option WriteOption) (source.ParquetFile, error) {
	writerFuncTable := map[string]func(*url.URL, WriteOption) (source.ParquetFile, error){
		schemeLocal:              newLocalWriter,
		schemeAWSS3:              newAWSS3Writer,
		schemeGoogleCloudStorage: newGoogleCloudStorageWriter,
		schemeAzureStorageBlob:   newAzureStorageBlobWriter,
		schemeHTTP:               newHTTPWriter,
		schemeHTTPS:              newHTTPWriter,
		schemeHDFS:               newHDFSWriter,
	}

	u, err := parseURI(option.URI)
	if err != nil {
		return nil, err
	}
	if writerFunc, found := writerFuncTable[u.Scheme]; found {
		return writerFunc(u, option)
	}
	return nil, fmt.Errorf("unknown location scheme [%s]", u.Scheme)
}

func NewCSVWriter(option WriteOption, schema []string) (*writer.CSVWriter, error) {
	fileWriter, err := NewParquetFileWriter(option)
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

func NewJSONWriter(option WriteOption, schema string) (*writer.JSONWriter, error) {
	fileWriter, err := NewParquetFileWriter(option)
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
		return "", nil, fmt.Errorf("failed to create Azure credential: %v", err)
	}

	return httpURL, credential, nil
}

func compressionCodec(codecName string) (parquet.CompressionCodec, error) {
	codec, err := parquet.CompressionCodecFromString(codecName)
	if err != nil {
		return parquet.CompressionCodec_UNCOMPRESSED, err
	}
	switch codec {
	case parquet.CompressionCodec_BROTLI, parquet.CompressionCodec_LZO, parquet.CompressionCodec_LZ4_RAW:
		return parquet.CompressionCodec_UNCOMPRESSED, fmt.Errorf("%s compression is not supported at this moment", codec.String())
	}
	return codec, nil
}
