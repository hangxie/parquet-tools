package io

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/writer"
)

// writerAADPrefixOpts turns the writer AAD prefix flags into writer options.
func writerAADPrefixOpts(option WriteOption) ([]writer.WriterOption, error) {
	if option.WriterAADPrefix == nil {
		if option.WriterSupplyAADPrefix {
			return nil, fmt.Errorf("--writer-supply-aad-prefix requires an AAD prefix (--writer-aad-prefix, --writer-aad-prefix-env, or aad_prefix in --writer-key-file)")
		}
		return nil, nil
	}
	prefix, err := decodeBase64(*option.WriterAADPrefix)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 writer AAD prefix: %w", err)
	}
	if len(prefix) == 0 {
		return nil, fmt.Errorf("writer AAD prefix must not be empty")
	}
	opts := []writer.WriterOption{writer.WithAADPrefix(prefix)}
	if option.WriterSupplyAADPrefix {
		opts = append(opts, writer.WithSupplyAADPrefix(true))
	}
	return opts, nil
}
