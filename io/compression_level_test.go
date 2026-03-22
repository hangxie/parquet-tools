package io

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCompressionLevels(t *testing.T) {
	testCases := map[string]struct {
		input  []string
		errMsg string
	}{
		// Valid cases
		"single-gzip":       {[]string{"GZIP=1"}, ""},
		"single-zstd":       {[]string{"ZSTD=22"}, ""},
		"single-brotli":     {[]string{"BROTLI=0"}, ""},
		"mixed-case":        {[]string{"gzip=9"}, ""},
		"comma-separated":   {[]string{"GZIP=9,ZSTD=3"}, ""},
		"whitespace":        {[]string{" gzip = 9 "}, ""},
		"whitespace-commas": {[]string{" GZIP = 6 , ZSTD= 5 "}, ""},
		"empty-input":       {[]string{}, ""},
		"nil-input":         {nil, ""},
		"empty-string":      {[]string{""}, ""},

		// Duplicate ordering: left-to-right, last wins
		"duplicate-across-flags": {[]string{"GZIP=3", "ZSTD=2,GZIP=9"}, ""},

		// Invalid format
		"no-equals":       {[]string{"GZIP9"}, "invalid compression level format"},
		"equals-no-codec": {[]string{"=9"}, "empty codec"},
		"equals-no-level": {[]string{"GZIP="}, "empty level"},
		"just-equals":     {[]string{"="}, "empty codec"},

		// Invalid level
		"non-integer": {[]string{"GZIP=abc"}, "must be an integer"},

		// Unsupported codecs
		"snappy":       {[]string{"SNAPPY=1"}, "does not support compression levels"},
		"uncompressed": {[]string{"UNCOMPRESSED=3"}, "does not support compression levels"},

		// Unknown codec
		"unknown-codec": {[]string{"FOO=1"}, "unknown codec"},

		// Range error from upstream
		"gzip-out-of-range": {[]string{"GZIP=99"}, "out of range"},
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
			if len(tc.input) == 0 || tc.input[0] == "" {
				require.Nil(t, opts)
			}
		})
	}
}
