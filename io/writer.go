package io

import (
	"context"
	"fmt"
	"net/url"
	"os/user"
	"runtime"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	parquetschema "github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/azblob"
	"github.com/hangxie/parquet-go/v3/source/gcs"
	"github.com/hangxie/parquet-go/v3/source/hdfs"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/source/s3v2"
	"github.com/hangxie/parquet-go/v3/writer"
)

const (
	// writerColumnKeyFooterSentinel is the --writer-column-key VALUE that
	// selects ENCRYPTION_WITH_FOOTER_KEY for a column. The leading "@" is
	// outside the base64 alphabet, so the sentinel cannot collide with any
	// valid base64-encoded key value.
	writerColumnKeyFooterSentinel = "@footer-key"
)

// WriterEncryptionAlgorithms lists the algorithm values accepted by the
// --encryption-algorithm flag.
var WriterEncryptionAlgorithms = []string{
	writerEncryptionAlgorithmAESGCMV1,
	writerEncryptionAlgorithmAESGCMCTRV1,
}

// WriteOption includes options for write operation
type WriteOption struct {
	CompressionCodec    string   `short:"z" name:"compression" help:"compression codec (UNCOMPRESSED/SNAPPY/GZIP/LZ4/LZ4_RAW/ZSTD/BROTLI)" default:"SNAPPY"`
	CompressionLevel    []string `help:"Compression level setting." placeholder:"CODEC=LEVEL"`
	DataPageVersion     int32    `help:"Data page version (1 or 2). Use 1 for legacy DATA_PAGE format." enum:"1,2" default:"2"`
	PageSize            int64    `help:"Page size in bytes." default:"1048576"`
	RowGroupSize        int64    `help:"Row group size in bytes." default:"134217728"`
	WriterFooterKey     *string  `name:"writer-footer-key" group:"Encryption" help:"base64-encoded AES-128/192/256 key. Encrypts the footer; also used for columns marked '=@footer-key' and for unlisted columns when --encrypt-all-columns is set. With --plaintext-footer the key signs the footer instead of encrypting it."`
	WriterColumnKeys    []string `name:"writer-column-key" group:"Encryption" help:"per-column encryption directive 'column.path=VALUE'; repeatable. column.path is the dotted file-schema path of a leaf column without the schema root (e.g. Parent.Child, not parquet_go_root.Parent.Child). VALUE is a base64-encoded AES key, or the literal '@footer-key' to encrypt the column with --writer-footer-key. Columns not listed are plaintext unless --encrypt-all-columns is set." placeholder:"column.path=base64key"`
	EncryptAllColumns   bool     `name:"encrypt-all-columns" group:"Encryption" help:"encrypt every leaf column. Columns not listed in --writer-column-key use --writer-footer-key. Default: false (only columns listed in --writer-column-key are encrypted)." default:"false"`
	PlaintextFooter     bool     `name:"plaintext-footer" group:"Encryption" help:"write a PAR1 file with a plaintext footer signed by --writer-footer-key instead of an encrypted PARE footer. Without --encrypt-all-columns or --writer-column-key the footer is signed for integrity only (columns remain plaintext)." default:"false"`
	EncryptionAlgorithm string   `name:"encryption-algorithm" group:"Encryption" help:"encryption algorithm used when --writer-footer-key is set. AES-GCM-V1 authenticates every module; AES-GCM-CTR-V1 uses AES-CTR for page bodies (lower overhead, page body tampering not detected). Has no effect without --writer-footer-key." enum:"${writer_encryption_algorithms}" default:"AES-GCM-V1"`
	WriterKeyFile       *string  `name:"writer-key-file" group:"Encryption" help:"path to a JSON file containing encryption keys ({footer_key, column_keys}); CLI flags override file values; --writer-column-key flags merge with file column_keys, CLI wins per path. Recommend chmod 600 on the file."`
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

func writerOpts(option WriteOption, columnKeys []writerColumnKey) ([]writer.WriterOption, error) {
	var opts []writer.WriterOption
	if option.CompressionCodec != "" {
		codec, err := compressionCodec(option.CompressionCodec)
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
		dpv = 2 // match CLI default and preserve existing writer behavior
	}
	opts = append(opts, writer.WithDataPageVersion(dpv))
	if option.PageSize > 0 {
		opts = append(opts, writer.WithPageSize(option.PageSize))
	}
	if option.RowGroupSize > 0 {
		opts = append(opts, writer.WithRowGroupSize(option.RowGroupSize))
	}
	encryptionOpts, err := writerEncryptionOpts(option, columnKeys)
	if err != nil {
		return nil, err
	}
	opts = append(opts, encryptionOpts...)
	opts = append(opts, writer.WithNP(int64(runtime.NumCPU())))
	return opts, nil
}

func NewCSVWriter(uri string, option WriteOption, schema []string) (*writer.CSVWriter, error) {
	option, err := applyWriterKeyFileOption(option)
	if err != nil {
		return nil, err
	}

	fileWriter, err := NewParquetFileWriter(uri)
	if err != nil {
		return nil, err
	}

	columnKeys, err := parseWriterColumnKeys(option.WriterColumnKeys)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	opts, err := writerOpts(option, columnKeys)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	encOpts, err := writerColumnEncryptionSchemaOpts(option, columnKeys, func() (*parquetschema.SchemaHandler, error) {
		return parquetschema.NewSchemaHandlerFromMetadata(schema)
	}, "create schema from metadata")
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	opts = append(opts, encOpts...)
	pw, err := writer.NewCSVWriter(schema, fileWriter, opts...)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	return pw, nil
}

func NewJSONWriter(uri string, option WriteOption, schema string) (*writer.JSONWriter, error) {
	option, err := applyWriterKeyFileOption(option)
	if err != nil {
		return nil, err
	}

	fileWriter, err := NewParquetFileWriter(uri)
	if err != nil {
		return nil, err
	}

	columnKeys, err := parseWriterColumnKeys(option.WriterColumnKeys)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	opts, err := writerOpts(option, columnKeys)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	encOpts, err := writerColumnEncryptionSchemaOpts(option, columnKeys, func() (*parquetschema.SchemaHandler, error) {
		return parquetschema.NewSchemaHandlerFromJSON(schema)
	}, "create schema from JSON")
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	opts = append(opts, encOpts...)
	pw, err := writer.NewJSONWriter(schema, fileWriter, opts...)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	return pw, nil
}

func NewGenericWriter(uri string, option WriteOption, schema string) (*writer.ParquetWriter, error) {
	option, err := applyWriterKeyFileOption(option)
	if err != nil {
		return nil, err
	}

	fileWriter, err := NewParquetFileWriter(uri)
	if err != nil {
		return nil, err
	}

	columnKeys, err := parseWriterColumnKeys(option.WriterColumnKeys)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	opts, err := writerOpts(option, columnKeys)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	encOpts, err := writerColumnEncryptionSchemaOpts(option, columnKeys, func() (*parquetschema.SchemaHandler, error) {
		return parquetschema.NewSchemaHandlerFromJSON(schema)
	}, "create schema from JSON")
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	opts = append(opts, encOpts...)
	pw, err := writer.NewParquetWriter(fileWriter, schema, opts...)
	if err != nil {
		_ = fileWriter.Close()
		return nil, err
	}
	return pw, nil
}
