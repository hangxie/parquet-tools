package io

import (
	"fmt"
	"runtime"

	"github.com/hangxie/parquet-go/v3/types"
	"github.com/hangxie/parquet-go/v3/writer"
)

// ParseValueMode resolves the CLI representation, including the default for direct callers.
func ParseValueMode(value string) (types.ValueMode, error) {
	switch value {
	case "", "interpreted":
		return types.ValueModeInterpreted, nil
	case "raw":
		return types.ValueModeRaw, nil
	default:
		return 0, fmt.Errorf("invalid value mode %q, expected interpreted or raw", value)
	}
}

func writerOpts(option WriteOption, columnKeys []writerColumnKey) ([]writer.WriterOption, error) {
	var opts []writer.WriterOption
	if option.BinaryMinMaxTruncateLength < 0 {
		return nil, fmt.Errorf("binary min/max truncate length must not be negative: %d", option.BinaryMinMaxTruncateLength)
	}
	if option.BinaryMinMaxTruncateLength > 0 {
		opts = append(opts, writer.WithBinaryMinMaxTruncateLength(option.BinaryMinMaxTruncateLength))
	}
	if option.MaxDictionarySize < 0 {
		return nil, fmt.Errorf("maximum dictionary size must not be negative: %d", option.MaxDictionarySize)
	}
	if option.MaxDictionarySize > 0 {
		opts = append(opts, writer.WithMaxDictionarySize(option.MaxDictionarySize))
	}
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
	if option.WriteCRC {
		opts = append(opts, writer.WithWriteCRC(true))
	}
	if option.EnforceUTF8 {
		opts = append(opts, writer.WithEnforceUTF8(true))
	}
	encryptionOpts, err := writerEncryptionOpts(option, columnKeys)
	if err != nil {
		return nil, err
	}
	opts = append(opts, encryptionOpts...)
	if option.ValueMode != types.ValueModeInterpreted {
		opts = append(opts, writer.WithValueMode(option.ValueMode))
	}
	opts = append(opts, writer.WithNP(int64(runtime.NumCPU())))
	return opts, nil
}
