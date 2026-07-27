package io

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	parquetschema "github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/writer"
)

const (
	writerEncryptionAlgorithmAESGCMV1    = "AES-GCM-V1"
	writerEncryptionAlgorithmAESGCMCTRV1 = "AES-GCM-CTR-V1"
)

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

func applyWriterKeyFile(kf keyFileSchema, opt *WriteOption) {
	if opt.WriterFooterKey == nil && kf.FooterKey != "" {
		opt.WriterFooterKey = &kf.FooterKey
	}
	existing := make(map[string]struct{}, len(opt.WriterColumnKeys))
	for _, ck := range opt.WriterColumnKeys {
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
			opt.WriterColumnKeys = append(opt.WriterColumnKeys, p+"="+kf.ColumnKeys[p])
		}
	}
}

func applyWriterKeyFileOption(option WriteOption) (WriteOption, error) {
	if option.WriterKeyFile == nil {
		return option, nil
	}
	kf, err := parseKeyFile(*option.WriterKeyFile)
	if err != nil {
		return option, err
	}
	applyWriterKeyFile(kf, &option)
	return option, nil
}

// writerColumnKey is one parsed --writer-column-key directive. Path is the
// user-supplied path verbatim (used in error messages). It must be the
// file-schema path of a leaf column without the schema root (e.g. "Parent.Child", not
// "parquet_go_root.Parent.Child"). NormalizedPath uses parquet-go's internal
// delimiter and is passed to writer.WithColumnEncrypted as well as used for
// schema lookup and duplicate detection. Value is the RHS
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
func parseWriterColumnKeys(rawKeys []string, fieldDelimiter string) ([]writerColumnKey, error) {
	if len(rawKeys) == 0 {
		return nil, nil
	}
	parsed := make([]writerColumnKey, 0, len(rawKeys))
	seen := make(map[string]struct{}, len(rawKeys))
	for _, raw := range rawKeys {
		path, value, found := strings.Cut(raw, "=")
		if !found || path == "" || value == "" {
			return nil, fmt.Errorf("invalid writer column key format [%s], expected 'column.path=base64key'", raw)
		}
		normalized := NormalizeFieldPath(path, fieldDelimiter)
		if _, ok := seen[normalized]; ok {
			return nil, fmt.Errorf("duplicate writer column key path [%s]", path)
		}
		seen[normalized] = struct{}{}
		parsed = append(parsed, writerColumnKey{
			Path:           path,
			NormalizedPath: normalized,
			Value:          value,
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
			opts = append(opts, writer.WithColumnEncrypted(ck.NormalizedPath, writer.ColumnFooterKey()))
			continue
		}

		key, err := decodeBase64(ck.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 writer column key for [%s]: %w", ck.Path, err)
		}
		if err := validateWriterAESKey("writer column key for ["+ck.Path+"]", key); err != nil {
			return nil, err
		}

		opts = append(opts, writer.WithColumnEncrypted(ck.NormalizedPath, writer.ColumnKey(key)))
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
// in-path used in schemaHandler.ValueColumns. Only one form is accepted on
// purpose: allowing multiple forms lets the same logical column appear twice
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
		m[stripped] = inPathStr
	}
	return m
}

// stripWriterSchemaRoot returns the leaf path with the schema root removed,
// in parquet-go's internal delimiter-separated form. The empty return signals
// a path with no children below
// the root (i.e., nothing to encrypt).
func stripWriterSchemaRoot(path string) string {
	parts := common.StrToPath(path)
	if len(parts) <= 1 {
		return ""
	}
	return common.PathToStr(parts[1:])
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
