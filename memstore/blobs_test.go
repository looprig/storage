package memstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"testing/iotest"

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
