package io

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os/user"
	"runtime"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source"
	pqazblob "github.com/hangxie/parquet-go/v3/source/azblob"
	"github.com/hangxie/parquet-go/v3/source/gcs"
	"github.com/hangxie/parquet-go/v3/source/hdfs"
	pqhttp "github.com/hangxie/parquet-go/v3/source/http"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/source/s3v2"
	googleoption "google.golang.org/api/option"
)

// ReadOption includes options for read operation
type ReadOption struct {
	Anonymous              bool              `help:"(S3, GCS, and Azure only) object is publicly accessible." default:"false"`
	HTTPExtraHeaders       map[string]string `mapsep:"," help:"(HTTP URI only) extra HTTP headers." default:""`
	HTTPIgnoreTLSError     bool              `help:"(HTTP and S3 URI) ignore TLS error." default:"false"`
	HTTPMultipleConnection bool              `help:"(HTTP URI only) use multiple HTTP connection." default:"false"`
	ObjectVersion          *string           `help:"(S3, GCS, and Azure only) object version."`
	FooterKey              *string           `name:"footer-key" group:"Encryption" help:"(encrypted files only) base64-encoded AES-128/192/256 key to decrypt the footer. KMS is not directly supported; retrieve the key manually first."`
	ColumnKeys             []string          `name:"column-key" group:"Encryption" help:"(encrypted files only) column decryption key as 'column.path=base64key'; repeatable. KMS is not directly supported; retrieve the key manually first." placeholder:"column.path=base64key"`
	AADPrefix              *string           `name:"aad-prefix" group:"Encryption" help:"(encrypted files only) base64-encoded AAD prefix (if not stored in file)."`
	KeyFile                *string           `name:"key-file" group:"Encryption" help:"path to a JSON file containing decryption keys ({footer_key, aad_prefix, column_keys}); CLI flags override file values."`
}

// decodeBase64 accepts only standard base64 with padding (RFC 4648 §4).
// URL-safe and unpadded variants are rejected so that sentinels like
// "@footer-key" remain the only path to special-cased values and so that
// inputs cannot be silently reinterpreted between encodings.
func decodeBase64(s string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("not valid base64")
	}
	return b, nil
}

func buildReaderOptions(option ReadOption) ([]reader.ReaderOption, error) {
	var opts []reader.ReaderOption

	if option.FooterKey != nil {
		key, err := decodeBase64(*option.FooterKey)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 footer key: %w", err)
		}
		opts = append(opts, reader.WithFooterKey(key))
	}

	if option.AADPrefix != nil {
		prefix, err := decodeBase64(*option.AADPrefix)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 AAD prefix: %w", err)
		}
		opts = append(opts, reader.WithAADPrefix(prefix))
	}

	for _, ck := range option.ColumnKeys {
		parts := strings.SplitN(ck, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid column key format [%s], expected 'column.path=base64key'", ck)
		}
		key, err := decodeBase64(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid base64 column key for [%s]: %w", parts[0], err)
		}
		opts = append(opts, reader.WithColumnKey(parts[0], key))
	}

	return opts, nil
}

func newLocalReader(u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	return local.NewLocalFileReader(u.Path)
}

func newAWSS3Reader(u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	s3Client, err := getS3Client(u.Host, option.Anonymous, option.HTTPIgnoreTLSError)
	if err != nil {
		return nil, err
	}

	return s3v2.NewS3FileReaderWithClient(context.Background(), s3Client, u.Host, strings.TrimLeft(u.Path, "/"), option.ObjectVersion)
}

func newAzureStorageBlobReader(u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	objectVersion := ""
	if option.ObjectVersion != nil {
		objectVersion = *option.ObjectVersion
	}
	azURL, cred, err := azureAccessDetail(*u, option.Anonymous, objectVersion)
	if err != nil {
		return nil, err
	}

	return pqazblob.NewAzBlobFileReader(context.Background(), azURL, cred, blockblob.ClientOptions{})
}

func newGoogleCloudStorageReader(u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	generation := int64(-1)
	if option.ObjectVersion != nil {
		var err error
		generation, err = strconv.ParseInt(*option.ObjectVersion, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid GCS generation [%s]: %w", *option.ObjectVersion, err)
		}
	}
	ctx := context.Background()

	var options []googleoption.ClientOption
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

func newSourceReader(URI string, option ReadOption) (source.ParquetFileReader, error) {
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
	src, err := readerFunc(u, option)
	if err != nil {
		return nil, fmt.Errorf("unable to open file [%s]: %w", u.String(), err)
	}
	return src, nil
}

func NewParquetFileReader(URI string, option ReadOption) (*reader.ParquetReader, error) {
	if option.KeyFile != nil {
		if err := loadKeyFile(*option.KeyFile, &option); err != nil {
			return nil, err
		}
	}

	fileReader, err := newSourceReader(URI, option)
	if err != nil {
		return nil, err
	}

	encOpts, err := buildReaderOptions(option)
	if err != nil {
		_ = fileReader.Close()
		return nil, err
	}

	readerOpts := append(encOpts, reader.WithNP(int64(runtime.NumCPU())))
	pr, err := reader.NewParquetReader(fileReader, nil, readerOpts...)
	if err != nil {
		_ = fileReader.Close()
		return nil, err
	}

	hasEncryptionOptions := option.FooterKey != nil || len(option.ColumnKeys) > 0 || option.AADPrefix != nil
	isEncrypted := pr.FileCrypto != nil || (pr.Footer != nil && pr.Footer.IsSetEncryptionAlgorithm())
	if hasEncryptionOptions && !isEncrypted {
		_ = fileReader.Close()
		return nil, fmt.Errorf("encryption keys provided but parquet file is not encrypted")
	}

	return pr, nil
}
