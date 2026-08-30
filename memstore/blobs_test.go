package memstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"testing"
	"testing/iotest"
	"time"

	"github.com/looprig/storage"
)

// This file holds only the memstore-SPECIFIC blob tests that the shared
// storetest conformance suite (run from conformance_test.go) deliberately does
// NOT cover: the reader-error pass-through branch and the independent-reader
// copy-out guarantee. The shared behaviors (put/get round-trip, byte-identical
// re-Put no-op, different-content BlobConflictError leaving the original,
// absent-key BlobNotFoundError, sorted/deduped/prefix-filtered List, idempotent
// Delete, invalid keys, and the large-blob round-trip) live in package
// storetest.

// getBytes reads the blob at key to completion and closes the reader, returning
// the bytes. It fails the test on any error so callers can assert on bytes.
func getBytes(t *testing.T, s *blobStore, key string) []byte {
	t.Helper()
	rc, err := s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q) unexpected error: %v", key, err)
	}
	defer func() {
		if cerr := rc.Close(); cerr != nil {
			t.Errorf("Close() = %v, want nil", cerr)
		}
	}()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(%q) unexpected error: %v", key, err)
	}
	return data
}

// TestBlobPutReaderError documents Put's current pass-through of a reader
// failure: when r returns an error mid-drain, Put propagates that error verbatim
// (no storage-typed wrapping — a BlobReadError is deliberately deferred to the
// streaming backends) and stores nothing, so the key stays absent.
func TestBlobPutReaderError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newBlobStore()
	const key = "blobs/readerror"

	boom := errors.New("boom")
	err := s.Put(ctx, key, iotest.ErrReader(boom))
	if !errors.Is(err, boom) {
		t.Fatalf("Put(failing reader) error = %v, want it to wrap %v", err, boom)
	}

	// Nothing must have been stored: the key stays absent.
	_, gerr := s.Get(ctx, key)
	var nf *storage.BlobNotFoundError
	if !errors.As(gerr, &nf) {
		t.Fatalf("Get after failed Put = %v, want *storage.BlobNotFoundError (nothing stored)", gerr)
	}
	if nf.Key != key {
		t.Errorf("BlobNotFoundError.Key = %q, want %q", nf.Key, key)
	}
}

func TestBlobGetIndependentReader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newBlobStore()
	const key = "blobs/independent"
	content := []byte("original")

	if err := s.Put(ctx, key, bytes.NewReader(content)); err != nil {
		t.Fatalf("Put() unexpected error: %v", err)
	}

	// Two Gets must yield two independent readers over equal bytes.
	first := getBytes(t, s, key)
	if !bytes.Equal(first, content) {
		t.Fatalf("first Get() = %q, want %q", first, content)
	}
	// Mutating the bytes read out of the first reader must not affect stored data.
	for i := range first {
		first[i] = 'Z'
	}

	second := getBytes(t, s, key)
	if !bytes.Equal(second, content) {
		t.Errorf("second Get() = %q, want %q (each Get is independent of stored bytes)", second, content)
	}
}

func TestBlobGetReaderClosePublishesClosedState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newBlobStore()
	const key = "blobs/close-state"
	content := []byte("independent immutable bytes")
	if err := s.Put(ctx, key, bytes.NewReader(content)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var prefix [4]byte
	if n, err := rc.Read(prefix[:]); n != len(prefix) || err != nil {
		t.Fatalf("Read prefix = %d, %v", n, err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("first Close = %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	var p [1]byte
	if n, err := rc.Read(p[:]); n != 0 || !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Read after Close = %d, %v; want 0, fs.ErrClosed", n, err)
	}
	if got := getBytes(t, s, key); !bytes.Equal(got, content) {
		t.Fatalf("closing one reader changed stored bytes: got %q want %q", got, content)
	}
}

func TestBlobGetReaderCloseBeforeRead(t *testing.T) {
	t.Parallel()

	s := newBlobStore()
	const key = "blobs/close-before-read"
	if err := s.Put(context.Background(), key, bytes.NewReader([]byte("unread"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, err := s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("first Close = %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	var p [1]byte
	if n, err := rc.Read(p[:]); n != 0 || !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Read after immediate Close = %d, %v; want 0, fs.ErrClosed", n, err)
	}
}

func TestBlobGetReaderBoundsWorkPerRead(t *testing.T) {
	t.Parallel()

	const expectedReadChunk = 64 << 10
	content := make([]byte, 3*expectedReadChunk+17)
	for i := range content {
		content[i] = byte(i * 31)
	}
	s := newBlobStore()
	const key = "blobs/bounded-read"
	if err := s.Put(context.Background(), key, bytes.NewReader(content)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, err := s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	p := make([]byte, 4*expectedReadChunk)
	n, err := rc.Read(p)
	if n != expectedReadChunk || err != nil {
		t.Fatalf("large-buffer Read = %d, %v; want bounded partial read %d, nil", n, err, expectedReadChunk)
	}
	tail, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll remainder: %v", err)
	}
	got := append(append([]byte(nil), p[:n]...), tail...)
	if !bytes.Equal(got, content) {
		t.Fatal("bounded partial reads did not preserve byte-for-byte content")
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if n, err := rc.Read(p[:1]); n != 0 || !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Read after Close = %d, %v; want 0, fs.ErrClosed", n, err)
	}
}

func TestBlobReaderCloseWaitsForActiveRead(t *testing.T) {
	t.Parallel()

	readLocked := make(chan struct{})
	releaseRead := make(chan struct{})
	closeEntered := make(chan struct{})
	r := &blobReader{
		data: []byte("active read"),
		testHooks: &blobReaderTestHooks{
			readLocked: func() {
				close(readLocked)
				<-releaseRead
			},
			closeEntered: func() { close(closeEntered) },
		},
	}

	readDone := make(chan readResult, 1)
	go func() {
		var p [1]byte
		n, err := r.Read(p[:])
		readDone <- readResult{n: n, err: err}
	}()
	<-readLocked

	closeResult := make(chan error, 1)
	go func() { closeResult <- r.Close() }()
	<-closeEntered
	select {
	case err := <-closeResult:
		t.Fatalf("Close returned while Read held the lifecycle lock: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(releaseRead)
	select {
	case got := <-readDone:
		if got.n != 1 || got.err != nil {
			t.Fatalf("active Read = %d, %v; want 1, nil", got.n, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("active Read did not return after release")
	}
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("Close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after active Read completed")
	}
}

type readResult struct {
	n   int
	err error
}
