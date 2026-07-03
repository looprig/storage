package memstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"testing/iotest"

	"github.com/ciram-co/storekit"
)

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

func TestBlobPutGetRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content []byte
	}{
		{name: "non-empty content", content: []byte("some bytes")},
		{name: "empty content", content: []byte{}},
		{name: "binary content", content: []byte{0x00, 0xff, 0x10, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newBlobStore()
			const key = "blobs/roundtrip"

			if err := s.Put(ctx, key, bytes.NewReader(tt.content)); err != nil {
				t.Fatalf("Put() unexpected error: %v", err)
			}
			if got := getBytes(t, s, key); !bytes.Equal(got, tt.content) {
				t.Errorf("Get() = %q, want %q", got, tt.content)
			}
		})
	}
}

// TestBlobPutReaderError documents Put's current pass-through of a reader
// failure: when r returns an error mid-drain, Put propagates that error verbatim
// (no storekit-typed wrapping — a BlobReadError is deliberately deferred to the
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
	var nf *storekit.BlobNotFoundError
	if !errors.As(gerr, &nf) {
		t.Fatalf("Get after failed Put = %v, want *storekit.BlobNotFoundError (nothing stored)", gerr)
	}
	if nf.Key != key {
		t.Errorf("BlobNotFoundError.Key = %q, want %q", nf.Key, key)
	}
}

func TestBlobRePutIdenticalIsNoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newBlobStore()
	const key = "blobs/identical"
	content := []byte("content-addressed bytes")

	if err := s.Put(ctx, key, bytes.NewReader(content)); err != nil {
		t.Fatalf("first Put() unexpected error: %v", err)
	}
	// Re-Put of byte-identical content is a success/no-op, not a conflict.
	if err := s.Put(ctx, key, bytes.NewReader(content)); err != nil {
		t.Fatalf("re-Put(identical) = %v, want nil (no-op)", err)
	}
	if got := getBytes(t, s, key); !bytes.Equal(got, content) {
		t.Errorf("Get() after identical re-Put = %q, want %q", got, content)
	}
}

func TestBlobRePutDifferentConflicts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		original []byte
		second   []byte
	}{
		{name: "different bytes same length", original: []byte("aaaa"), second: []byte("bbbb")},
		{name: "shorter is different", original: []byte("aaaa"), second: []byte("aa")},
		{name: "longer is different", original: []byte("aa"), second: []byte("aaaa")},
		{name: "non-empty vs empty", original: []byte("aa"), second: []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newBlobStore()
			const key = "blobs/conflict"

			if err := s.Put(ctx, key, bytes.NewReader(tt.original)); err != nil {
				t.Fatalf("first Put() unexpected error: %v", err)
			}

			err := s.Put(ctx, key, bytes.NewReader(tt.second))
			var bc *storekit.BlobConflictError
			if !errors.As(err, &bc) {
				t.Fatalf("re-Put(different) error = %v, want *storekit.BlobConflictError", err)
			}
			if bc.Key != key {
				t.Errorf("BlobConflictError.Key = %q, want %q", bc.Key, key)
			}

			// The ORIGINAL content must survive a rejected conflicting Put.
			if got := getBytes(t, s, key); !bytes.Equal(got, tt.original) {
				t.Errorf("Get() after rejected Put = %q, want original %q", got, tt.original)
			}
		})
	}
}

func TestBlobGetAbsent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newBlobStore()
	const key = "blobs/missing"

	_, err := s.Get(ctx, key)
	var nf *storekit.BlobNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("Get(absent) error = %v, want *storekit.BlobNotFoundError", err)
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

func TestBlobList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		seed   []string
		prefix string
		want   []string
	}{
		{
			name:   "empty prefix returns all sorted",
			seed:   []string{"blobs/c", "blobs/a", "snaps/z", "blobs/b"},
			prefix: "",
			want:   []string{"blobs/a", "blobs/b", "blobs/c", "snaps/z"},
		},
		{
			name:   "prefix filters and sorts",
			seed:   []string{"blobs/c", "blobs/a", "snaps/z", "blobs/b"},
			prefix: "blobs/",
			want:   []string{"blobs/a", "blobs/b", "blobs/c"},
		},
		{
			name:   "prefix matching nothing yields empty",
			seed:   []string{"blobs/a"},
			prefix: "snaps/",
			want:   nil,
		},
		{
			name:   "empty store yields empty",
			seed:   nil,
			prefix: "",
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newBlobStore()
			for _, k := range tt.seed {
				if err := s.Put(ctx, k, bytes.NewReader([]byte("v"))); err != nil {
					t.Fatalf("seed Put(%q) unexpected error: %v", k, err)
				}
			}

			got, err := s.List(ctx, tt.prefix)
			if err != nil {
				t.Fatalf("List() unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("List(%q) = %v, want %v", tt.prefix, got, tt.want)
			}
		})
	}
}

func TestBlobDeleteIdempotent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		seed    bool
		deletes int
	}{
		{name: "delete absent is nil", seed: false, deletes: 1},
		{name: "delete existing removes", seed: true, deletes: 1},
		{name: "double delete is nil", seed: true, deletes: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newBlobStore()
			const key = "blobs/del"

			if tt.seed {
				if err := s.Put(ctx, key, bytes.NewReader([]byte("v"))); err != nil {
					t.Fatalf("seed Put unexpected error: %v", err)
				}
			}
			for i := 0; i < tt.deletes; i++ {
				if err := s.Delete(ctx, key); err != nil {
					t.Fatalf("Delete() call %d = %v, want nil (idempotent)", i, err)
				}
			}

			// Absent after delete: Get reports not-found.
			if _, err := s.Get(ctx, key); !errors.As(err, new(*storekit.BlobNotFoundError)) {
				t.Errorf("Get after delete = %v, want *BlobNotFoundError", err)
			}
			// A deleted key is truly free: a fresh Put of new content succeeds
			// (no lingering conflict against the deleted bytes).
			if err := s.Put(ctx, key, bytes.NewReader([]byte("fresh"))); err != nil {
				t.Errorf("Put after delete = %v, want nil", err)
			}
		})
	}
}

func TestBlobInvalidName(t *testing.T) {
	t.Parallel()

	badNames := []struct {
		label string
		value string
	}{
		{label: "empty", value: ""},
		{label: "leading slash", value: "/leading"},
		{label: "trailing slash", value: "trailing/"},
		{label: "doubled slash", value: "a//b"},
		{label: "uppercase", value: "Upper"},
		{label: "space", value: "has space"},
		{label: "dot-dot segment", value: ".."},
	}

	methods := []struct {
		method string
		call   func(s *blobStore, key string) error
	}{
		{
			method: "Put",
			call: func(s *blobStore, key string) error {
				return s.Put(context.Background(), key, bytes.NewReader([]byte("x")))
			},
		},
		{
			method: "Get",
			call: func(s *blobStore, key string) error {
				_, err := s.Get(context.Background(), key)
				return err
			},
		},
		{
			method: "Delete",
			call: func(s *blobStore, key string) error {
				return s.Delete(context.Background(), key)
			},
		},
	}

	for _, m := range methods {
		for _, bad := range badNames {
			t.Run(m.method+"/"+bad.label, func(t *testing.T) {
				t.Parallel()
				s := newBlobStore()
				err := m.call(s, bad.value)
				var ine *storekit.InvalidNameError
				if !errors.As(err, &ine) {
					t.Fatalf("%s(%q) error = %v, want *storekit.InvalidNameError", m.method, bad.value, err)
				}
				if ine.Name != bad.value {
					t.Errorf("InvalidNameError.Name = %q, want %q", ine.Name, bad.value)
				}
			})
		}
	}
}
