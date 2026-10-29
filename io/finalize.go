package io

import (
	"context"
	"fmt"
	"os"

	"github.com/hangxie/parquet-go/v3/writer"
)

// FinalizeWriter finishes a Parquet write. On success it writes the footer and
// closes the destination. On failure it closes the destination without a
// footer and discards the partial file, so an aborted command never leaves a
// truncated but readable Parquet file behind.
func FinalizeWriter(ctx context.Context, fileWriter *writer.ParquetWriter, target string, writeErr error) error {
	footerWritten := false
	if writeErr == nil {
		if err := fileWriter.WriteStopWithContext(ctx); err != nil {
			writeErr = fmt.Errorf("failed to end write [%s]: %w", target, err)
		} else {
			footerWritten = true
		}
	}
	if err := fileWriter.PFile.Close(); err != nil && writeErr == nil {
		writeErr = fmt.Errorf("failed to close [%s]: %w", target, err)
	}
	if !footerWritten {
		DiscardDestination(target)
	}
	return writeErr
}

// DiscardDestination deletes a partially written destination when the backend
// allows it. Remote object stores have no delete through the writer interface;
// their leftover object is incomplete without a footer.
func DiscardDestination(uri string) {
	u, err := parseURI(uri)
	if err != nil {
		return
	}
	if u.Scheme == schemeLocal {
		_ = os.Remove(u.Path)
	}
}
