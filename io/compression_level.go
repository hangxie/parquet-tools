package io

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/writer"
)

// codecsWithLevels lists codecs that support compression levels.
var codecsWithLevels = map[string]parquet.CompressionCodec{
	"GZIP":    parquet.CompressionCodec_GZIP,
	"ZSTD":    parquet.CompressionCodec_ZSTD,
	"BROTLI":  parquet.CompressionCodec_BROTLI,
	"LZ4_RAW": parquet.CompressionCodec_LZ4_RAW,
	"LZ4":     parquet.CompressionCodec_LZ4,
}

// codecsWithoutLevels lists codecs that do not support levels.
var codecsWithoutLevels = map[string]bool{
	"SNAPPY":       true,
	"UNCOMPRESSED": true,
}

// parseCompressionLevel parses a single "CODEC=LEVEL" item and returns the
// normalized codec name and level. Returns an error for invalid format,
// unknown/unsupported codecs, or non-integer levels.
func parseCompressionLevel(item string) (string, int, error) {
	parts := strings.SplitN(item, "=", 2)
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid compression level format [%s], expected 'CODEC=LEVEL'", item)
	}

	codec := strings.TrimSpace(parts[0])
	levelStr := strings.TrimSpace(parts[1])

	if codec == "" {
		return "", 0, fmt.Errorf("empty codec in [%s]", item)
	}
	if levelStr == "" {
		return "", 0, fmt.Errorf("empty level in [%s]", item)
	}

	codec = strings.ToUpper(codec)

	if codecsWithoutLevels[codec] {
		return "", 0, fmt.Errorf("codec [%s] does not support compression levels; supported codecs with levels: GZIP, ZSTD, BROTLI, LZ4, LZ4_RAW", codec)
	}
	if _, ok := codecsWithLevels[codec]; !ok {
		return "", 0, fmt.Errorf("unknown codec [%s]; supported: GZIP, ZSTD, BROTLI, LZ4, LZ4_RAW (SNAPPY/UNCOMPRESSED do not accept levels)", codec)
	}

	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid compression level [%s] for codec [%s]: must be an integer", levelStr, codec)
	}

	return codec, level, nil
}

// ParseCompressionLevels parses compression level specifications and returns
// writer options for each. Input is a slice of strings, each containing one or
// more comma-separated CODEC=LEVEL pairs. Inputs are processed left-to-right;
// later values override earlier ones for the same codec.
func ParseCompressionLevels(levels []string) ([]writer.WriterOption, error) {
	if len(levels) == 0 {
		return nil, nil
	}

	parsed := make(map[string]int)
	var order []string
	for _, entry := range levels {
		for item := range strings.SplitSeq(entry, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}

			codec, level, err := parseCompressionLevel(item)
			if err != nil {
				return nil, err
			}
			if err := validateCompressionLevel(codec, level); err != nil {
				return nil, err
			}

			if _, exists := parsed[codec]; !exists {
				order = append(order, codec)
			}
			parsed[codec] = level
		}
	}

	if len(order) == 0 {
		return nil, nil
	}

	opts := make([]writer.WriterOption, 0, len(order))
	for _, codec := range order {
		opts = append(opts, writer.WithCompressionLevel(codecsWithLevels[codec], parsed[codec]))
	}
	return opts, nil
}

func validateCompressionLevel(codec string, level int) error {
	codec = strings.ToUpper(codec)
	minLevel, maxLevel := 0, 0
	switch codec {
	case "GZIP":
		minLevel, maxLevel = 1, 9
	case "ZSTD":
		minLevel, maxLevel = 1, 22
	case "BROTLI":
		minLevel, maxLevel = 0, 11
	case "LZ4", "LZ4_RAW":
		minLevel, maxLevel = 0, 9
	default:
		return fmt.Errorf("unknown codec [%s]", codec)
	}
	if level < minLevel || level > maxLevel {
		return fmt.Errorf("compression level [%d] out of range for [%s]: expected %d-%d", level, codec, minLevel, maxLevel)
	}
	return nil
}
