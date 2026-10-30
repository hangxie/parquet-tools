package io

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/reader"
)

// noLimit is the flag value that disables a read limit.
const noLimit = -1

type readLimit struct {
	flag   string
	option string
	size   int64
	apply  func(int64) reader.ReaderOption
}

func readLimits(opt ReadOption) []readLimit {
	return []readLimit{
		{"--max-page-size", "reader.WithMaxPageSize", opt.MaxPageSize, reader.WithMaxPageSize},
		{"--max-decompressed-size", "reader.WithMaxDecompressedSize", opt.MaxDecompressedSize, reader.WithMaxDecompressedSize},
		{"--max-decoded-batch-bytes", "reader.WithMaxDecodedBatchBytes", opt.MaxDecodedBatchBytes, reader.WithMaxDecodedBatchBytes},
	}
}

// buildLimitOptions leaves a zero limit to the parquet-go default.
func buildLimitOptions(opt ReadOption) ([]reader.ReaderOption, error) {
	var opts []reader.ReaderOption
	for _, limit := range readLimits(opt) {
		switch {
		case limit.size == 0:
			continue
		case limit.size < noLimit:
			return nil, fmt.Errorf("invalid %s %d: must be positive, 0 for the default, or -1 for no limit", limit.flag, limit.size)
		}
		opts = append(opts, limit.apply(limit.size))
	}
	return opts, nil
}

type hintedError struct {
	msg string
	err error
}

func (e hintedError) Error() string { return e.msg }
func (e hintedError) Unwrap() error { return e.err }

// LimitFlagHint rewrites parquet-go's read-limit hints to name the CLI flags instead.
func LimitFlagHint(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	for _, limit := range readLimits(ReadOption{}) {
		msg = strings.ReplaceAll(msg, "(see "+limit.option+")", "(see "+limit.flag+")")
	}
	if msg == err.Error() {
		return err
	}
	return hintedError{msg: msg, err: err}
}
