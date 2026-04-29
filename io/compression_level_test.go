package io

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCompressionLevels(t *testing.T) {
	testCases := map[string]struct {
		input       []string
		expectedLen int
		errMsg      string
	}{
		// Valid cases
		"single-gzip":       {input: []string{"GZIP=1"}, expectedLen: 1},
		"single-zstd":       {input: []string{"ZSTD=22"}, expectedLen: 1},
		"single-brotli":     {input: []string{"BROTLI=0"}, expectedLen: 1},
		"mixed-case":        {input: []string{"gzip=9"}, expectedLen: 1},
		"comma-separated":   {input: []string{"GZIP=9,ZSTD=3"}, expectedLen: 2},
		"whitespace":        {input: []string{" gzip = 9 "}, expectedLen: 1},
		"whitespace-commas": {input: []string{" GZIP = 6 , ZSTD= 5 "}, expectedLen: 2},
		"empty-input":       {input: []string{}},
		"nil-input":         {},
		"empty-string":      {input: []string{""}},

		// Duplicate ordering: left-to-right, last wins
		"duplicate-across-flags": {input: []string{"GZIP=3", "ZSTD=2,GZIP=9"}, expectedLen: 2},

		// Invalid format
		"no-equals":       {input: []string{"GZIP9"}, errMsg: "invalid compression level format"},
		"equals-no-codec": {input: []string{"=9"}, errMsg: "empty codec"},
		"equals-no-level": {input: []string{"GZIP="}, errMsg: "empty level"},
		"just-equals":     {input: []string{"="}, errMsg: "empty codec"},

		// Invalid level
		"non-integer": {input: []string{"GZIP=abc"}, errMsg: "must be an integer"},

		// Unsupported codecs
		"snappy":       {input: []string{"SNAPPY=1"}, errMsg: "does not support compression levels"},
		"uncompressed": {input: []string{"UNCOMPRESSED=3"}, errMsg: "does not support compression levels"},

		// Unknown codec
		"unknown-codec": {input: []string{"FOO=1"}, errMsg: "unknown codec"},

		// Range error
		"gzip-out-of-range": {input: []string{"GZIP=99"}, errMsg: "out of range"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			opts, err := ParseCompressionLevels(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			if tc.expectedLen == 0 {
				require.Nil(t, opts)
				return
			}
			require.Len(t, opts, tc.expectedLen)
		})
	}
}
