package io

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/writer"
)

const hdfsCloseAttempts = 10

// hdfsCloseRetryDelay is a variable so tests can skip the wait.
var hdfsCloseRetryDelay = time.Second

// s3UploadOptions gives the multipart abort its own context; by default it reuses the cancelled upload context and fails.
var s3UploadOptions = []func(*transfermanager.Options){
	func(o *transfermanager.Options) { o.FailTimeout = 10 * time.Second },
}

// abortWriter lets a failed write cancel a streaming upload before it commits.
type abortWriter struct {
	source.ParquetFileWriter
	cancel context.CancelFunc
}

func (w *abortWriter) Close() error {
	err := w.ParquetFileWriter.Close()
	w.cancel()
	return err
}

func (w *abortWriter) Abort() {
	// cancel before closing, so the upload fails instead of seeing end-of-data
	w.cancel()
	_ = w.ParquetFileWriter.Close()
}

// hdfsRetryWriter retries Close while HDFS is still replicating the last block.
type hdfsRetryWriter struct {
	source.ParquetFileWriter
	ctx context.Context
}

func (w *hdfsRetryWriter) Close() error {
	// retry on particular errors according to https://github.com/colinmarc/hdfs/blob/v2.4.0/file_writer.go#L220-L226
	for attempt := 1; ; attempt++ {
		err := w.ParquetFileWriter.Close()
		if err == nil || attempt == hdfsCloseAttempts || !strings.Contains(err.Error(), "replication in progress") {
			return err
		}
		select {
		case <-w.ctx.Done():
			return err
		case <-time.After(hdfsCloseRetryDelay):
		}
	}
}

// FinishWrite writes the footer and closes; call it only on success, with AbortWrite deferred for every other exit.
func FinishWrite(ctx context.Context, pw *writer.ParquetWriter, uri string) error {
	if err := pw.WriteStopWithContext(ctx); err != nil {
		abortFile(pw.PFile)
		return fmt.Errorf("failed to end write [%s]: %w", uri, err)
	}
	if err := pw.PFile.Close(); err != nil {
		return fmt.Errorf("failed to close [%s]: %w", uri, err)
	}
	return nil
}

// AbortWrite closes without a footer, so a failed or panicking write never leaves a readable file.
func AbortWrite(pw *writer.ParquetWriter) {
	abortFile(pw.PFile)
}

// abortFile closes without a footer; local and HDFS leftovers stay where they are.
func abortFile(pf source.ParquetFileWriter) {
	if w, ok := pf.(interface{ Abort() }); ok {
		w.Abort()
		return
	}
	_ = pf.Close()
}
