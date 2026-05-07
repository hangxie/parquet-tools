package io

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/hangxie/parquet-go/v3/parquet"
)

const (
	magicPAR1 = "PAR1"
	magicPARE = "PARE"
)

// ColumnKeyHint holds key_metadata for a column-key-encrypted column, readable
// from a plaintext-footer (PAR1) file without decryption keys.
type ColumnKeyHint struct {
	PathInSchema   []string
	EncryptionMode string
	KeyMetadata    string `json:",omitempty"`
}

// EncryptionKeyHints holds key_metadata fields that are readable from a parquet
// file without providing decryption keys.
//
// For PARE (encrypted footer) files: FooterKeyMetadata is set.
// For PAR1 (plaintext footer) files with encrypted columns: Columns is set.
type EncryptionKeyHints struct {
	FooterKeyMetadata string          `json:",omitempty"`
	Columns           []ColumnKeyHint `json:",omitempty"`
}

// ReadEncryptionKeyHints reads key_metadata fields from a parquet file by
// parsing raw bytes, requiring no decryption keys. Returns nil if the file is
// not encrypted or carries no key_metadata.
func ReadEncryptionKeyHints(URI string, option ReadOption) (*EncryptionKeyHints, error) {
	src, err := newSourceReader(URI, option)
	if err != nil {
		return nil, err
	}
	defer func() { _ = src.Close() }()

	fileSize, err := src.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek to end: %w", err)
	}
	if fileSize < 8 {
		return nil, fmt.Errorf("file too small to be a parquet file")
	}

	tail := make([]byte, 8)
	if _, err := src.Seek(fileSize-8, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to tail: %w", err)
	}
	if _, err := io.ReadFull(src, tail); err != nil {
		return nil, fmt.Errorf("read tail: %w", err)
	}

	magic := string(tail[4:8])
	footerLen := int64(binary.LittleEndian.Uint32(tail[:4]))
	if fileSize < footerLen+8 {
		return nil, fmt.Errorf("invalid parquet footer length")
	}

	footerBytes := make([]byte, footerLen)
	if _, err := src.Seek(fileSize-footerLen-8, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to footer: %w", err)
	}
	if _, err := io.ReadFull(src, footerBytes); err != nil {
		return nil, fmt.Errorf("read footer: %w", err)
	}

	switch magic {
	case magicPARE:
		return parsePAREKeyHints(footerBytes)
	case magicPAR1:
		return parsePAR1KeyHints(footerBytes)
	default:
		return nil, fmt.Errorf("not a parquet file (magic: %q)", magic)
	}
}

func parsePAREKeyHints(footerBytes []byte) (*EncryptionKeyHints, error) {
	mem := thrift.NewTMemoryBufferLen(len(footerBytes))
	if _, err := mem.Write(footerBytes); err != nil {
		return nil, fmt.Errorf("write footer to buffer: %w", err)
	}
	protocol := thrift.NewTCompactProtocolConf(mem, &thrift.TConfiguration{})
	fileCrypto := parquet.NewFileCryptoMetaData()
	if err := fileCrypto.Read(context.Background(), protocol); err != nil {
		return nil, fmt.Errorf("parse FileCryptoMetaData: %w", err)
	}
	km := fileCrypto.GetKeyMetadata()
	if len(km) == 0 {
		return nil, nil
	}
	return &EncryptionKeyHints{
		FooterKeyMetadata: base64.StdEncoding.EncodeToString(km),
	}, nil
}

func parsePAR1KeyHints(footerBytes []byte) (*EncryptionKeyHints, error) {
	protocol := thrift.NewTCompactProtocolConf(
		thrift.NewTBufferedTransport(
			thrift.NewStreamTransportR(bytes.NewReader(footerBytes)),
			len(footerBytes),
		),
		&thrift.TConfiguration{},
	)
	footer := parquet.NewFileMetaData()
	if err := footer.Read(context.Background(), protocol); err != nil {
		return nil, fmt.Errorf("parse FileMetaData: %w", err)
	}
	if !footer.IsSetEncryptionAlgorithm() {
		return nil, nil
	}

	hints := &EncryptionKeyHints{}
	if km := footer.GetFooterSigningKeyMetadata(); len(km) > 0 {
		hints.FooterKeyMetadata = base64.StdEncoding.EncodeToString(km)
	}

	for _, rg := range footer.GetRowGroups() {
		for _, col := range rg.GetColumns() {
			cm := col.GetCryptoMetadata()
			if cm == nil || !cm.IsSetENCRYPTION_WITH_COLUMN_KEY() {
				continue
			}
			eck := cm.GetENCRYPTION_WITH_COLUMN_KEY()
			colHint := ColumnKeyHint{
				PathInSchema:   eck.GetPathInSchema(),
				EncryptionMode: "COLUMN_KEY",
			}
			if km := eck.GetKeyMetadata(); len(km) > 0 {
				colHint.KeyMetadata = base64.StdEncoding.EncodeToString(km)
			}
			hints.Columns = append(hints.Columns, colHint)
		}
	}
	if hints.FooterKeyMetadata == "" && len(hints.Columns) == 0 {
		return nil, nil
	}
	return hints, nil
}
