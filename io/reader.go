package io

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os/user"
	"runtime"
	"sort"
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
	AADPrefix              *string           `name:"aad-prefix" group:"Encryption" help:"(encrypted files only) base64-encoded AAD prefix (if not stored in file)."`
	Anonymous              bool              `help:"(S3, GCS, and Azure only) object is publicly accessible." default:"false"`
	ColumnKeys             []string          `name:"column-key" group:"Encryption" help:"(encrypted files only) column decryption key as 'column.path=base64key'; repeatable. KMS is not directly supported; retrieve the key manually first." placeholder:"column.path=base64key"`
	FieldDelimiter         string            `kong:"-"`
	FooterKey              *string           `name:"footer-key" group:"Encryption" help:"(encrypted files only) base64-encoded AES-128/192/256 key to decrypt the footer. KMS is not directly supported; retrieve the key manually first."`
	HTTPExtraHeaders       map[string]string `mapsep:"," help:"(HTTP URI only) extra HTTP headers." default:""`
	HTTPIgnoreTLSError     bool              `help:"(HTTP and S3 URI) ignore TLS error." default:"false"`
	HTTPMultipleConnection bool              `help:"(HTTP URI only) use multiple HTTP connection." default:"false"`
	ObjectVersion          *string           `help:"(S3, GCS, and Azure only) object version."`
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

func buildReaderOptions(opt ReadOption) ([]reader.ReaderOption, error) {
	var opts []reader.ReaderOption

	if opt.FooterKey != nil {
		key, err := decodeBase64(*opt.FooterKey)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 footer key: %w", err)
		}
		opts = append(opts, reader.WithFooterKey(key))
	}

	if opt.AADPrefix != nil {
		prefix, err := decodeBase64(*opt.AADPrefix)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 AAD prefix: %w", err)
		}
		opts = append(opts, reader.WithAADPrefix(prefix))
	}

	for _, ck := range opt.ColumnKeys {
		path, encodedKey, found := strings.Cut(ck, "=")
		if !found || path == "" || encodedKey == "" {
			return nil, fmt.Errorf("invalid column key format [%s], expected 'column.path=base64key'", ck)
		}
		key, err := decodeBase64(encodedKey)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 column key for [%s]: %w", path, err)
		}
		opts = append(opts, reader.WithColumnKey(NormalizeFieldPath(path, opt.FieldDelimiter), key))
	}

	return opts, nil
}

func applyKeyFile(kf keyFileSchema, opt *ReadOption) {
	if opt.FooterKey == nil && kf.FooterKey != "" {
		opt.FooterKey = &kf.FooterKey
	}
	if opt.AADPrefix == nil && kf.AADPrefix != "" {
		opt.AADPrefix = &kf.AADPrefix
	}
	existing := make(map[string]struct{}, len(opt.ColumnKeys))
	for _, ck := range opt.ColumnKeys {
		if path, _, found := strings.Cut(ck, "="); found && path != "" {
			existing[NormalizeFieldPath(path, opt.FieldDelimiter)] = struct{}{}
		}
	}
	paths := make([]string, 0, len(kf.ColumnKeys))
	for p := range kf.ColumnKeys {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		if _, ok := existing[NormalizeFieldPath(p, opt.FieldDelimiter)]; !ok {
			opt.ColumnKeys = append(opt.ColumnKeys, p+"="+kf.ColumnKeys[p])
		}
	}
}

func newLocalReader(_ context.Context, u *url.URL, _ ReadOption) (source.ParquetFileReader, error) {
	return local.NewLocalFileReader(u.Path)
}

func newAWSS3Reader(ctx context.Context, u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	s3Client, err := getS3Client(u.Host, option.Anonymous, option.HTTPIgnoreTLSError)
	if err != nil {
		return nil, err
	}

	return s3v2.NewS3FileReaderWithClient(ctx, s3Client, u.Host, strings.TrimLeft(u.Path, "/"), option.ObjectVersion)
}

func newAzureStorageBlobReader(ctx context.Context, u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	objectVersion := ""
	if option.ObjectVersion != nil {
		objectVersion = *option.ObjectVersion
	}
	azURL, cred, err := azureAccessDetail(*u, option.Anonymous, objectVersion)
	if err != nil {
		return nil, err
	}

	return pqazblob.NewAzBlobFileReader(ctx, azURL, cred, blockblob.ClientOptions{})
}

func newGoogleCloudStorageReader(ctx context.Context, u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	generation := int64(-1)
	if option.ObjectVersion != nil {
		var err error
		generation, err = strconv.ParseInt(*option.ObjectVersion, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid GCS generation [%s]: %w", *option.ObjectVersion, err)
		}
	}

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

func newHTTPReader(ctx context.Context, u *url.URL, option ReadOption) (source.ParquetFileReader, error) {
	return pqhttp.NewHttpReaderWithContext(ctx, u.String(), option.HTTPMultipleConnection, option.HTTPIgnoreTLSError, option.HTTPExtraHeaders)
}

func newHDFSReader(_ context.Context, u *url.URL, _ ReadOption) (source.ParquetFileReader, error) {
	userName := u.User.Username()
	if userName == "" {
		osUser, err := user.Current()
		if err == nil && osUser != nil {
			userName = osUser.Username
		}
	}

	return hdfs.NewHdfsFileReader([]string{u.Host}, userName, u.Path)
}

func newSourceReader(ctx context.Context, URI string, option ReadOption) (source.ParquetFileReader, error) {
	readerFuncTable := map[string]func(context.Context, *url.URL, ReadOption) (source.ParquetFileReader, error){
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
	src, err := readerFunc(ctx, u, option)
	if err != nil {
		return nil, fmt.Errorf("unable to open file [%s]: %w", u.String(), err)
	}
	return src, nil
}

func NewParquetFileReader(ctx context.Context, URI string, option ReadOption) (*reader.ParquetReader, error) {
	if option.KeyFile != nil {
		kf, err := parseKeyFile(*option.KeyFile)
		if err != nil {
			return nil, err
		}
		applyKeyFile(kf, &option)
	}

	fileReader, err := newSourceReader(ctx, URI, option)
	if err != nil {
		return nil, err
	}

	encOpts, err := buildReaderOptions(option)
	if err != nil {
		_ = fileReader.Close()
		return nil, err
	}

	readerOpts := append(encOpts, reader.WithNP(int64(runtime.NumCPU())))
	pr, err := reader.NewParquetReaderWithContext(ctx, fileReader, nil, readerOpts...)
	if err != nil {
		_ = fileReader.Close()
		return nil, err
	}
	internalFooter, err := pr.InternalFooter()
	if err != nil {
		_ = fileReader.Close()
		return nil, fmt.Errorf("translate footer to internal names: %w", err)
	}
	if internalFooter != nil {
		pr.Footer = internalFooter
		pr.SchemaHandler.SchemaElements = internalFooter.Schema
	}

	hasEncryptionOptions := option.FooterKey != nil || len(option.ColumnKeys) > 0 || option.AADPrefix != nil
	isEncrypted := pr.FileCrypto != nil || (pr.Footer != nil && pr.Footer.IsSetEncryptionAlgorithm())
	if hasEncryptionOptions && !isEncrypted {
		_ = fileReader.Close()
		return nil, fmt.Errorf("encryption keys provided but parquet file is not encrypted")
	}

	return pr, nil
}
