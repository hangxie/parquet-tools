package io

import (
	"context"
	"fmt"
	"net/url"
	"os/user"
	"runtime"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/azblob"
	"github.com/hangxie/parquet-go/v3/source/gcs"
	"github.com/hangxie/parquet-go/v3/source/hdfs"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/source/s3v2"
	"github.com/hangxie/parquet-go/v3/writer"
)

// WriteOption includes options for write operation
type WriteOption struct {
	Compression      string   `short:"z" help:"compression codec (UNCOMPRESSED/SNAPPY/GZIP/LZ4/LZ4_RAW/ZSTD/BROTLI)" default:"SNAPPY"`
	CompressionLevel []string `help:"Compression level setting." placeholder:"CODEC=LEVEL"`
	DataPageVersion  int32    `help:"Data page version (1 or 2). Use 1 for legacy DATA_PAGE format." enum:"1,2" default:"2"`
	PageSize         int64    `help:"Page size in bytes." default:"1048576"`
	RowGroupSize     int64    `help:"Row group size in bytes." default:"134217728"`
	ParallelNumber   int64    `help:"Number of parallel writer goroutines, 0 means number of cores." default:"0"`
}

func newLocalWriter(u *url.URL) (source.ParquetFileWriter, error) {
	fileWriter, err := local.NewLocalFileWriter(u.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open local file [%s]: %w", u.Path, err)
	}
	return fileWriter, nil
}

func newAWSS3Writer(u *url.URL) (source.ParquetFileWriter, error) {
	s3Client, err := getS3Client(u.Host, false, false)
	if err != nil {
		return nil, err
	}

	fileWriter, err := s3v2.NewS3FileWriterWithClient(context.Background(), s3Client, u.Host, strings.TrimLeft(u.Path, "/"), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open S3 object [%s]: %w", u.String(), err)
	}
	return fileWriter, nil
}

func newGoogleCloudStorageWriter(u *url.URL) (source.ParquetFileWriter, error) {
	fileWriter, err := gcs.NewGcsFileWriter(context.Background(), "", u.Host, strings.TrimLeft(u.Path, "/"))
	if err != nil {
		return nil, fmt.Errorf("failed to open GCS object [%s]: %w", u.String(), err)
	}
	return fileWriter, nil
}

func newAzureStorageBlobWriter(u *url.URL) (source.ParquetFileWriter, error) {
	// write operation cannot be with anonymous access
	azURL, cred, err := azureAccessDetail(*u, false, "")
	if err != nil {
		return nil, err
	}

	fileWriter, err := azblob.NewAzBlobFileWriter(context.Background(), azURL, cred, blockblob.ClientOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to open Azure blob object [%s]: %w", u.String(), err)
	}
	return fileWriter, nil
}

func newHTTPWriter(u *url.URL) (source.ParquetFileWriter, error) {
	return nil, fmt.Errorf("writing to [%s] endpoint is not currently supported", u.Scheme)
}

func newHDFSWriter(u *url.URL) (source.ParquetFileWriter, error) {
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

func NewParquetFileWriter(uri string) (source.ParquetFileWriter, error) {
	writerFuncTable := map[string]func(*url.URL) (source.ParquetFileWriter, error){
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
		return writerFunc(u)
	}
	return nil, fmt.Errorf("unknown location scheme [%s]", u.Scheme)
}

func writerOpts(option WriteOption) ([]writer.WriterOption, error) {
	var opts []writer.WriterOption
	if option.Compression != "" {
		codec, err := compressionCodec(option.Compression)
		if err != nil {
			return nil, err
		}
		opts = append(opts, writer.WithCompressionCodec(codec))
	}
	compressionLevelOpts, err := ParseCompressionLevels(option.CompressionLevel)
	if err != nil {
		return nil, err
	}
	opts = append(opts, compressionLevelOpts...)
	dpv := option.DataPageVersion
	if dpv == 0 {
		dpv = 2 // match CLI default
	}
	opts = append(opts, writer.WithDataPageVersion(dpv))
	if option.PageSize > 0 {
		opts = append(opts, writer.WithPageSize(option.PageSize))
	}
	if option.RowGroupSize > 0 {
		opts = append(opts, writer.WithRowGroupSize(option.RowGroupSize))
	}
	np := option.ParallelNumber
	if np == 0 {
		np = int64(runtime.NumCPU())
	}
	opts = append(opts, writer.WithNP(np))
	return opts, nil
}

func NewCSVWriter(uri string, option WriteOption, schema []string) (*writer.CSVWriter, error) {
	fileWriter, err := NewParquetFileWriter(uri)
	if err != nil {
		return nil, err
	}

	opts, err := writerOpts(option)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	pw, err := writer.NewCSVWriter(schema, fileWriter, opts...)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	return pw, nil
}

func NewJSONWriter(uri string, option WriteOption, schema string) (*writer.JSONWriter, error) {
	fileWriter, err := NewParquetFileWriter(uri)
	if err != nil {
		return nil, err
	}

	opts, err := writerOpts(option)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	pw, err := writer.NewJSONWriter(schema, fileWriter, opts...)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	return pw, nil
}

func NewGenericWriter(uri string, option WriteOption, schema string) (*writer.ParquetWriter, error) {
	fileWriter, err := NewParquetFileWriter(uri)
	if err != nil {
		return nil, err
	}

	opts, err := writerOpts(option)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	pw, err := writer.NewParquetWriter(fileWriter, schema, opts...)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	return pw, nil
}
