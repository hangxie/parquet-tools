package io

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriterAADPrefixOpts(t *testing.T) {
	testCases := map[string]struct {
		option WriteOption
		count  int
		errMsg string
	}{
		"none":                {option: WriteOption{}, count: 0},
		"stored":              {option: WriteOption{WriterAADPrefix: new("YWFk")}, count: 1},
		"supplied":            {option: WriteOption{WriterAADPrefix: new("YWFk"), WriterSupplyAADPrefix: true}, count: 2},
		"supply-without":      {option: WriteOption{WriterSupplyAADPrefix: true}, errMsg: "--writer-supply-aad-prefix requires an AAD prefix"},
		"not-base64":          {option: WriteOption{WriterAADPrefix: new("not base64!")}, errMsg: "invalid base64 writer AAD prefix"},
		"empty":               {option: WriteOption{WriterAADPrefix: new("")}, errMsg: "writer AAD prefix must not be empty"},
		"any-length-accepted": {option: WriteOption{WriterAADPrefix: new("YQ==")}, count: 1},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			opts, err := writerAADPrefixOpts(tc.option)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Len(t, opts, tc.count)
		})
	}
}
