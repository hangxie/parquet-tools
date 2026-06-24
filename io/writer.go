package io

import (
	"context"
	"fmt"
	"net/url"
	"os/user"
	"runtime"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/hangxie/parquet-go/v3/common"
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
	writerEncryptionAlgorithmAESGCMV1    = "AES-GCM-V1"
	writerEncryptionAlgorithmAESGCMCTRV1 = "AES-GCM-CTR-V1"

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

func validateWriterAESKey(name string, key []byte) error {
	switch len(key) {
	case 16, 24, 32:
		return nil
	default:
		return fmt.Errorf("%s must be 16, 24, or 32 bytes after base64 decoding, got %d bytes", name, len(key))
	}
}

func writerEncryptionRequested(option WriteOption) bool {
	// --encryption-algorithm only selects an algorithm once encryption is
	// enabled by the writer key flags; it does not request encryption by itself.
	return option.WriterFooterKey != nil || len(option.WriterColumnKeys) > 0 || option.PlaintextFooter || option.EncryptAllColumns
}

// writerColumnKey is one parsed --writer-column-key directive. Path is the
// user-supplied path verbatim (used in error messages and as the argument
// to writer.WithColumnEncrypted); it must be the dotted file-schema path
// of a leaf column without the schema root (e.g. "Parent.Child", not
// "parquet_go_root.Parent.Child"). NormalizedPath is the ReformPathStr
// form used for schema lookup and duplicate detection. Value is the RHS
// — either the "@footer-key" sentinel or a base64-encoded AES key —
// interpreted by downstream callers.
type writerColumnKey struct {
	Path           string
	NormalizedPath string
	Value          string
}

// parseWriterColumnKeys normalizes the raw repeatable --writer-column-key
// strings into a single slice. Format validation and duplicate-path rejection
// happen here exactly once; downstream helpers consume the result without
// re-parsing or re-checking.
func parseWriterColumnKeys(rawKeys []string) ([]writerColumnKey, error) {
	if len(rawKeys) == 0 {
		return nil, nil
	}
	parsed := make([]writerColumnKey, 0, len(rawKeys))
	seen := make(map[string]struct{}, len(rawKeys))
	for _, raw := range rawKeys {
		parts := strings.SplitN(raw, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid writer column key format [%s], expected 'column.path=base64key'", raw)
		}
		normalized := common.ReformPathStr(parts[0])
		if _, ok := seen[normalized]; ok {
			return nil, fmt.Errorf("duplicate writer column key path [%s]", parts[0])
		}
		seen[normalized] = struct{}{}
		parsed = append(parsed, writerColumnKey{
			Path:           parts[0],
			NormalizedPath: normalized,
			Value:          parts[1],
		})
	}
	return parsed, nil
}

func writerEncryptionOpts(option WriteOption, columnKeys []writerColumnKey) ([]writer.WriterOption, error) {
	// Defense in depth: kong's enum constraint already rejects unknown
	// algorithms at parse time, but Go callers of writerEncryptionOpts (such
	// as the unit tests) bypass that check, so guard here too. The empty
	// string is accepted as "use the parquet-go default" so that zero-value
	// WriteOption values (no CLI parsing) keep working.
	switch option.EncryptionAlgorithm {
	case "", writerEncryptionAlgorithmAESGCMV1, writerEncryptionAlgorithmAESGCMCTRV1:
	default:
		return nil, fmt.Errorf("invalid encryption algorithm [%s]", option.EncryptionAlgorithm)
	}

	if !writerEncryptionRequested(option) {
		return nil, nil
	}
	if option.WriterFooterKey == nil {
		return nil, fmt.Errorf("--writer-footer-key is required for encryption")
	}

	footerKey, err := decodeBase64(*option.WriterFooterKey)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 writer footer key: %w", err)
	}
	if err := validateWriterAESKey("writer footer key", footerKey); err != nil {
		return nil, err
	}

	opts := []writer.WriterOption{writer.WithFooterKey(footerKey)}
	for _, ck := range columnKeys {
		if ck.Value == writerColumnKeyFooterSentinel {
			opts = append(opts, writer.WithColumnEncrypted(ck.Path, writer.ColumnFooterKey()))
			continue
		}

		key, err := decodeBase64(ck.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 writer column key for [%s]: %w", ck.Path, err)
		}
		if err := validateWriterAESKey("writer column key for ["+ck.Path+"]", key); err != nil {
			return nil, err
		}

		opts = append(opts, writer.WithColumnEncrypted(ck.Path, writer.ColumnKey(key)))
	}

	if option.PlaintextFooter {
		opts = append(opts, writer.WithPlaintextFooter(true))
	}
	if option.EncryptionAlgorithm == writerEncryptionAlgorithmAESGCMCTRV1 {
		opts = append(opts, writer.WithEncryptionAlgorithm(writer.EncryptionAESGCMCTRV1))
	}

	return opts, nil
}

func validateWriterColumnKeySchemaPaths(columnKeys []writerColumnKey, pathToLeaf map[string]string) error {
	if len(columnKeys) == 0 || pathToLeaf == nil {
		return nil
	}
	for _, ck := range columnKeys {
		if _, ok := pathToLeaf[ck.NormalizedPath]; !ok {
			return fmt.Errorf("writer column key path [%s] not found in schema (use the file-schema form without the schema root, e.g. Parent.Child)", ck.Path)
		}
	}
	return nil
}

// writerSchemaPathToLeaf maps the canonical user-facing form of a leaf
// column path — the external (Tag-name) path with the schema root
// stripped, in parquet-go's ParGoPathDelimiter-separated form — to the
// in-path used in schemaHandler.ValueColumns. That delimiter-separated
// form is what ReformPathStr produces from the dotted path the user
// supplies, which is why callers look up entries with
// ReformPathStr(userInput). Only one form is accepted on purpose:
// allowing multiple forms lets the same logical column appear twice
// in a --writer-column-key list under different spellings, which would
// produce two WithColumnEncrypted calls for the same leaf with
// conflicting keys.
func writerSchemaPathToLeaf(schemaHandler *parquetschema.SchemaHandler) map[string]string {
	m := make(map[string]string, len(schemaHandler.ValueColumns))
	for _, inPathStr := range schemaHandler.ValueColumns {
		exPathStr, ok := schemaHandler.InPathToExPath[inPathStr]
		if !ok {
			exPathStr = inPathStr
		}
		stripped := stripWriterSchemaRoot(exPathStr)
		if stripped == "" {
			continue
		}
		m[common.ReformPathStr(stripped)] = inPathStr
	}
	return m
}

// stripWriterSchemaRoot returns the leaf path with the schema root removed,
// in dot-separated form — matching both --writer-column-key syntax and
// parquet-go's documented WithColumnEncrypted("rootless, dot-separated")
// input contract. The empty return signals a path with no children below
// the root (i.e., nothing to encrypt).
func stripWriterSchemaRoot(path string) string {
	parts := common.StrToPath(path)
	if len(parts) <= 1 {
		return ""
	}
	return strings.Join(parts[1:], ".")
}

// writerEncryptAllColumnsOpts returns one WithColumnEncrypted(path,
// ColumnFooterKey()) option for every leaf column in schemaHandler that the
// user has not already listed in --writer-column-key. It is the CLI-level
// implementation of --encrypt-all-columns: parquet-go's default treatment of
// unlisted columns is plaintext, so to encrypt them with the footer key we
// must enumerate them explicitly.
//
// pathToLeaf must be writerSchemaPathToLeaf(schemaHandler); the caller
// passes it in so the orchestrator can share one map with
// validateWriterColumnKeySchemaPaths instead of rebuilding it here. Every
// entry in columnKeys must have a NormalizedPath that resolves in
// pathToLeaf — writerColumnEncryptionSchemaOpts enforces this by calling
// validateWriterColumnKeySchemaPaths first; callers must not invoke this
// helper without satisfying that contract.
func writerEncryptAllColumnsOpts(option WriteOption, columnKeys []writerColumnKey, schemaHandler *parquetschema.SchemaHandler, pathToLeaf map[string]string) []writer.WriterOption {
	if !option.EncryptAllColumns || schemaHandler == nil {
		return nil
	}
	listedLeaves := make(map[string]struct{}, len(columnKeys))
	for _, ck := range columnKeys {
		listedLeaves[pathToLeaf[ck.NormalizedPath]] = struct{}{}
	}
	var opts []writer.WriterOption
	for _, inPathStr := range schemaHandler.ValueColumns {
		if _, ok := listedLeaves[inPathStr]; ok {
			continue
		}
		exPathStr, ok := schemaHandler.InPathToExPath[inPathStr]
		if !ok {
			exPathStr = inPathStr
		}
		path := stripWriterSchemaRoot(exPathStr)
		if path == "" {
			continue
		}
		opts = append(opts, writer.WithColumnEncrypted(path, writer.ColumnFooterKey()))
	}
	return opts
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

// writerColumnEncryptionSchemaOpts validates --writer-column-key paths against
// the schema and returns the implicit options needed for --encrypt-all-columns.
// It returns nil opts (and nil error) when neither feature is requested, so
// callers can skip building a schema handler in the common unencrypted path.
// schemaErrPrefix is the message prefix used to wrap factory errors.
func writerColumnEncryptionSchemaOpts(
	option WriteOption,
	columnKeys []writerColumnKey,
	newSchemaHandler func() (*parquetschema.SchemaHandler, error),
	schemaErrPrefix string,
) ([]writer.WriterOption, error) {
	if len(columnKeys) == 0 && !option.EncryptAllColumns {
		return nil, nil
	}
	schemaHandler, err := newSchemaHandler()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", schemaErrPrefix, err)
	}
	pathToLeaf := writerSchemaPathToLeaf(schemaHandler)
	if err := validateWriterColumnKeySchemaPaths(columnKeys, pathToLeaf); err != nil {
		return nil, err
	}
	return writerEncryptAllColumnsOpts(option, columnKeys, schemaHandler, pathToLeaf), nil
}

func NewCSVWriter(uri string, option WriteOption, schema []string) (*writer.CSVWriter, error) {
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
