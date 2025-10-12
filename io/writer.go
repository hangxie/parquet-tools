package io

import (
	"context"
	"fmt"
	"net/url"
	"os/user"
	"runtime"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/hangxie/parquet-go/v2/source"
	"github.com/hangxie/parquet-go/v2/source/azblob"
	"github.com/hangxie/parquet-go/v2/source/gcs"
	"github.com/hangxie/parquet-go/v2/source/hdfs"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/source/s3v2"
	"github.com/hangxie/parquet-go/v2/writer"
)

// WriteOption includes options for write operation
type WriteOption struct {
	Compression string `short:"z" help:"compression codec (UNCOMPRESSED/SNAPPY/GZIP/LZ4/LZ4_RAW/ZSTD)" enum:"UNCOMPRESSED,SNAPPY,GZIP,LZ4,LZ4_RAW,ZSTD" default:"SNAPPY"`
}

func newLocalWriter(u *url.URL) (source.ParquetFileWriter, error) {
	fileWriter, err := local.NewLocalFileWriter(u.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open local file [%s]: %w", u.Path, err)
	}
	return fileWriter, nil
}

func newAWSS3Writer(u *url.URL) (source.ParquetFileWriter, error) {
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
	return nil, fmt.Errorf("writing to %s endpoint is not currently supported", u.Scheme)
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

func NewCSVWriter(uri string, option WriteOption, schema []string) (*writer.CSVWriter, error) {
	fileWriter, err := NewParquetFileWriter(uri)
	if err != nil {
		return nil, err
	}

	pw, err := writer.NewCSVWriter(schema, fileWriter, int64(runtime.NumCPU()))
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	codec, err := compressionCodec(option.Compression)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	pw.CompressionType = codec
	return pw, nil
}

func NewJSONWriter(uri string, option WriteOption, schema string) (*writer.JSONWriter, error) {
	fileWriter, err := NewParquetFileWriter(uri)
	if err != nil {
		return nil, err
	}

	pw, err := writer.NewJSONWriter(schema, fileWriter, int64(runtime.NumCPU()))
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	codec, err := compressionCodec(option.Compression)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	pw.CompressionType = codec
	return pw, nil
}

func NewGenericWriter(uri string, option WriteOption, schema string) (*writer.ParquetWriter, error) {
	fileWriter, err := NewParquetFileWriter(uri)
	if err != nil {
		return nil, err
	}

	pw, err := writer.NewParquetWriter(fileWriter, schema, int64(runtime.NumCPU()))
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	codec, err := compressionCodec(option.Compression)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	pw.CompressionType = codec
	return pw, nil
}
