package io

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/azblob"
	"github.com/hangxie/parquet-go/v3/source/gcs"
	"github.com/hangxie/parquet-go/v3/source/s3v2"
	"github.com/hangxie/parquet-go/v3/writer"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

const testAbortSchema = `{"Tag":"name=root","Fields":[{"Tag":"name=id, type=INT64"}]}`

type mockFileWriter struct {
	writes     int
	failWrites int // writes after this many fail; 0 never fails
	closeErr   error
	closes     int
	ctx        context.Context // when set, records whether it was cancelled by the first Close
	cancelled  bool
}

func (m *mockFileWriter) Write(p []byte) (int, error) {
	m.writes++
	if m.failWrites != 0 && m.writes > m.failWrites {
		return 0, errors.New("write failed")
	}
	return len(p), nil
}

func (m *mockFileWriter) Close() error {
	m.closes++
	if m.ctx != nil && m.closes == 1 {
		m.cancelled = m.ctx.Err() != nil
	}
	return m.closeErr
}

func (m *mockFileWriter) Create(string) (source.ParquetFileWriter, error) {
	return nil, errors.New("not implemented")
}

func newTestAbortWriter(inner *mockFileWriter) *abortWriter {
	ctx, cancel := context.WithCancel(context.Background())
	inner.ctx = ctx
	return &abortWriter{ParquetFileWriter: inner, cancel: cancel}
}

func TestAbortWriter(t *testing.T) {
	t.Run("close-commits-before-cancel", func(t *testing.T) {
		inner := &mockFileWriter{}
		w := newTestAbortWriter(inner)
		require.NoError(t, w.Close())
		require.Equal(t, 1, inner.closes)
		require.False(t, inner.cancelled)
		require.Error(t, inner.ctx.Err(), "context is released after close")
	})

	t.Run("close-error", func(t *testing.T) {
		w := newTestAbortWriter(&mockFileWriter{closeErr: errors.New("boom")})
		require.ErrorContains(t, w.Close(), "boom")
	})

	t.Run("abort-cancels-before-close", func(t *testing.T) {
		inner := &mockFileWriter{closeErr: errors.New("upload cancelled")}
		w := newTestAbortWriter(inner)
		w.Abort()
		require.Equal(t, 1, inner.closes)
		require.True(t, inner.cancelled)
	})
}

func TestHDFSRetryWriter(t *testing.T) {
	saved := hdfsCloseRetryDelay
	t.Cleanup(func() { hdfsCloseRetryDelay = saved })

	testCases := map[string]struct {
		errs      []error
		cancelled bool
		closes    int
		errMsg    string
	}{
		"success":            {errs: []error{nil}, closes: 1},
		"non-retryable":      {errs: []error{errors.New("some other error")}, closes: 1, errMsg: "some other error"},
		"retry-then-ok":      {errs: []error{errors.New("replication in progress"), nil}, closes: 2},
		"retry-exhausted":    {errs: []error{errors.New("replication in progress")}, closes: 10, errMsg: "replication in progress"},
		"cancelled-no-retry": {errs: []error{errors.New("replication in progress")}, cancelled: true, closes: 1, errMsg: "replication in progress"},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			hdfsCloseRetryDelay = 0
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelled {
				hdfsCloseRetryDelay = time.Hour
				cancel()
			}
			inner := &sequenceCloser{errs: tc.errs}
			err := (&hdfsRetryWriter{ParquetFileWriter: inner, ctx: ctx}).Close()
			require.Equal(t, tc.closes, inner.closes)
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.errMsg)
		})
	}
}

// sequenceCloser returns errs in order from Close, repeating the last one.
type sequenceCloser struct {
	mockFileWriter
	errs []error
}

func (s *sequenceCloser) Close() error {
	s.closes++
	return s.errs[min(s.closes, len(s.errs))-1]
}

func newTestJSONWriter(t *testing.T, pf source.ParquetFileWriter) *writer.ParquetWriter {
	t.Helper()
	jw, err := writer.NewJSONWriterWithContext(context.Background(), testAbortSchema, pf)
	require.NoError(t, err)
	require.NoError(t, jw.WriteWithContext(context.Background(), `{"id":1}`))
	return &jw.ParquetWriter
}

func TestFinishWrite(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "out.parquet")
		jw, err := NewJSONWriter(context.Background(), path, WriteOption{}, testAbortSchema)
		require.NoError(t, err)
		require.NoError(t, jw.WriteWithContext(context.Background(), `{"id":1}`))
		require.NoError(t, FinishWrite(context.Background(), &jw.ParquetWriter, path))

		pr, err := NewParquetFileReader(context.Background(), path, ReadOption{})
		require.NoError(t, err)
		defer func() { _ = pr.PFile.Close() }()
		require.Equal(t, int64(1), pr.GetNumRows())
	})

	testCases := map[string]struct {
		inner     *mockFileWriter
		errMsg    string
		cancelled bool
	}{
		"write-stop-fails": {inner: &mockFileWriter{failWrites: 1}, errMsg: "failed to end write [target]", cancelled: true},
		"close-fails":      {inner: &mockFileWriter{closeErr: errors.New("boom")}, errMsg: "failed to close [target]: boom"},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pw := newTestJSONWriter(t, newTestAbortWriter(tc.inner))
			require.ErrorContains(t, FinishWrite(context.Background(), pw, "target"), tc.errMsg)
			require.Equal(t, 1, tc.inner.closes)
			require.Equal(t, tc.cancelled, tc.inner.cancelled)
		})
	}
}

func TestAbortWrite(t *testing.T) {
	t.Run("abortable", func(t *testing.T) {
		inner := &mockFileWriter{}
		pw := newTestJSONWriter(t, newTestAbortWriter(inner))
		writes := inner.writes
		AbortWrite(pw)
		require.Equal(t, writes, inner.writes, "no footer after a failure")
		require.Equal(t, 1, inner.closes)
		require.True(t, inner.cancelled)
	})

	t.Run("not-abortable", func(t *testing.T) {
		inner := &mockFileWriter{}
		pw := newTestJSONWriter(t, inner)
		writes := inner.writes
		AbortWrite(pw)
		require.Equal(t, writes, inner.writes, "no footer after a failure")
		require.Equal(t, 1, inner.closes)
	})
}

// The commands' shape: FinishWrite only on success, AbortWrite deferred; a panic mid-write must leave no footer.
func TestPanicMidWriteLeavesNoFooter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.parquet")
	jw, err := NewJSONWriter(context.Background(), path, WriteOption{}, testAbortSchema)
	require.NoError(t, err)

	run := func() error {
		finished := false
		defer func() {
			if !finished {
				AbortWrite(&jw.ParquetWriter)
			}
		}()
		require.NoError(t, jw.WriteWithContext(context.Background(), `{"id":1}`))
		panic("writer bug")
	}
	require.PanicsWithValue(t, "writer bug", func() { _ = run() })
	require.FileExists(t, path)
	_, err = NewParquetFileReader(context.Background(), path, ReadOption{})
	require.Error(t, err, "a panic must not leave a readable file")
}

// HDFS is not a streaming upload, so a failed write still retries the close that releases the lease, unless interrupted.
func TestAbortFileRetriesHDFSClose(t *testing.T) {
	saved := hdfsCloseRetryDelay
	t.Cleanup(func() { hdfsCloseRetryDelay = saved })
	hdfsCloseRetryDelay = 0

	inner := &sequenceCloser{errs: []error{errors.New("replication in progress"), nil}}
	abortFile(&hdfsRetryWriter{ParquetFileWriter: inner, ctx: context.Background()})
	require.Equal(t, 2, inner.closes)
}

func TestHDFSRetryWriterCancelledWhileWaiting(t *testing.T) {
	saved := hdfsCloseRetryDelay
	t.Cleanup(func() { hdfsCloseRetryDelay = saved })
	hdfsCloseRetryDelay = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	inner := &sequenceCloser{errs: []error{errors.New("replication in progress")}}
	time.AfterFunc(10*time.Millisecond, cancel)
	err := (&hdfsRetryWriter{ParquetFileWriter: inner, ctx: ctx}).Close()
	require.ErrorContains(t, err, "replication in progress")
	require.Equal(t, 1, inner.closes)
}

// fakeS3 fails every call on a cancelled context, like the SDK does, and records whether the multipart abort got a live one.
type fakeS3 struct {
	transfermanager.S3APIClient
	mu         sync.Mutex
	partSent   chan struct{}
	sendOnce   sync.Once
	aborted    bool
	abortCtxOK bool
}

func (f *fakeS3) CreateMultipartUpload(ctx context.Context, _ *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("upload-id")}, nil
}

func (f *fakeS3) UploadPart(ctx context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_, _ = io.Copy(io.Discard, in.Body)
	f.sendOnce.Do(func() { close(f.partSent) })
	return &s3.UploadPartOutput{ETag: aws.String("etag")}, nil
}

func (f *fakeS3) CompleteMultipartUpload(ctx context.Context, _ *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (f *fakeS3) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_, _ = io.Copy(io.Discard, in.Body)
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) AbortMultipartUpload(ctx context.Context, _ *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.aborted = true
	f.abortCtxOK = ctx.Err() == nil
	return &s3.AbortMultipartUploadOutput{}, ctx.Err()
}

func TestS3AbortCleansUpMultipartUpload(t *testing.T) {
	client := &fakeS3{partSent: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	fileWriter, err := s3v2.NewS3FileWriterWithClient(ctx, client, "bucket", "key", s3UploadOptions)
	require.NoError(t, err)
	w := &abortWriter{ParquetFileWriter: fileWriter, cancel: cancel}

	// past two 8 MiB parts, so the upload is multipart and a part goes up before the abort
	_, err = w.Write(make([]byte, 17*1024*1024))
	require.NoError(t, err)
	<-client.partSent
	w.Abort()

	client.mu.Lock()
	defer client.mu.Unlock()
	require.True(t, client.aborted)
	require.True(t, client.abortCtxOK, "AbortMultipartUpload ran on the cancelled upload context")
}

func TestGCSAbortCreatesNoObject(t *testing.T) {
	testCases := map[string]struct {
		abort   bool
		uploads int
	}{
		"close-commits": {uploads: 1},
		"abort-cancels": {abort: true, uploads: 0},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			uploads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				uploads++
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"kind":"storage#object","bucket":"b","name":"o.parquet","size":"4"}`))
			}))
			defer server.Close()

			client, err := storage.NewClient(context.Background(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithoutAuthentication())
			require.NoError(t, err)
			defer func() { _ = client.Close() }()
			ctx, cancel := context.WithCancel(context.Background())
			fileWriter, err := gcs.NewGcsFileWriterWithClient(ctx, client, "", "b", "o.parquet")
			require.NoError(t, err)
			w := &abortWriter{ParquetFileWriter: fileWriter, cancel: cancel}

			_, err = w.Write([]byte("PAR1"))
			require.NoError(t, err)
			if tc.abort {
				w.Abort()
			} else {
				require.NoError(t, w.Close())
			}

			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, tc.uploads, uploads)
		})
	}
}

func TestAzureAbortCommitsNoBlob(t *testing.T) {
	testCases := map[string]struct {
		size        int
		abort       bool
		afterBlocks bool
		commits     int32
	}{
		"close-commits":      {size: 4, commits: 1},
		"abort-cancels":      {size: 4, abort: true},
		"abort-after-blocks": {size: 3 * 1024 * 1024, abort: true, afterBlocks: true},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// a block upload is only visible once the block list is committed, or for a single-shot PUT
			var commits atomic.Int32
			staged := make(chan struct{})
			var stagedOnce sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if r.URL.Query().Get("comp") == "block" {
					stagedOnce.Do(func() { close(staged) })
				} else {
					commits.Add(1)
				}
				w.Header().Set("ETag", `"etag"`)
				w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
				w.WriteHeader(http.StatusCreated)
			}))
			defer server.Close()

			ctx, cancel := context.WithCancel(context.Background())
			fileWriter, err := azblob.NewAzBlobFileWriter(ctx, server.URL+"/container/blob.parquet", nil, blockblob.ClientOptions{})
			require.NoError(t, err)
			w := &abortWriter{ParquetFileWriter: fileWriter, cancel: cancel}

			_, err = w.Write(make([]byte, tc.size))
			require.NoError(t, err)
			if tc.afterBlocks {
				<-staged
			}
			if tc.abort {
				w.Abort()
			} else {
				require.NoError(t, w.Close())
			}
			require.Equal(t, tc.commits, commits.Load())
		})
	}
}
