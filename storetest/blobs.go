package storetest

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/looprig/storage"
)

// TestBlobs runs the Blobs conformance suite. newBackend must return a fresh,
// empty Blobs; register any cleanup via t.Cleanup inside newBackend.
func TestBlobs(t *testing.T, newBackend func(t *testing.T) storekit.Blobs) {
	ctx := context.Background()

	t.Run("put get round-trip", func(t *testing.T) {
		cases := []struct {
			name    string
			content []byte
		}{
			{name: "non-empty", content: []byte("some bytes")},
			{name: "empty", content: []byte{}},
			{name: "binary", content: []byte{0x00, 0xff, 0x10, 0x00}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				b := newBackend(t)
				const key = "blobs/roundtrip"
				if err := b.Put(ctx, key, bytes.NewReader(tc.content)); err != nil {
					t.Fatalf("Put: %v", err)
				}
				rc, err := b.Get(ctx, key)
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if got := readBlob(t, rc); !bytes.Equal(got, tc.content) {
					t.Errorf("Get = %q, want %q", got, tc.content)
				}
			})
		}
	})

	t.Run("byte-identical re-Put is a no-op success", func(t *testing.T) {
		b := newBackend(t)
		const key = "blobs/identical"
		content := []byte("content-addressed bytes")
		if err := b.Put(ctx, key, bytes.NewReader(content)); err != nil {
			t.Fatalf("first Put: %v", err)
		}
		if err := b.Put(ctx, key, bytes.NewReader(content)); err != nil {
			t.Fatalf("re-Put(identical) = %v, want nil (no-op)", err)
		}
		rc, err := b.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got := readBlob(t, rc); !bytes.Equal(got, content) {
			t.Errorf("Get after identical re-Put = %q, want %q", got, content)
		}
	})

	t.Run("different-content re-Put conflicts leaving original", func(t *testing.T) {
		cases := []struct {
			name     string
			original []byte
			second   []byte
		}{
			{name: "different bytes same length", original: []byte("aaaa"), second: []byte("bbbb")},
			{name: "shorter is different", original: []byte("aaaa"), second: []byte("aa")},
			{name: "longer is different", original: []byte("aa"), second: []byte("aaaa")},
			{name: "non-empty vs empty", original: []byte("aa"), second: []byte{}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				b := newBackend(t)
				const key = "blobs/conflict"
				if err := b.Put(ctx, key, bytes.NewReader(tc.original)); err != nil {
					t.Fatalf("first Put: %v", err)
				}
				err := b.Put(ctx, key, bytes.NewReader(tc.second))
				var bc *storekit.BlobConflictError
				if !errors.As(err, &bc) {
					t.Fatalf("re-Put(different) = %v, want *BlobConflictError", err)
				}
				if bc.Key != key {
					t.Errorf("BlobConflictError.Key = %q, want %q", bc.Key, key)
				}
				// The original content must survive a rejected conflicting Put.
				rc, gerr := b.Get(ctx, key)
				if gerr != nil {
					t.Fatalf("Get: %v", gerr)
				}
				if got := readBlob(t, rc); !bytes.Equal(got, tc.original) {
					t.Errorf("Get after rejected Put = %q, want original %q", got, tc.original)
				}
			})
		}
	})

	t.Run("get absent returns BlobNotFoundError", func(t *testing.T) {
		b := newBackend(t)
		const key = "blobs/missing"
		_, err := b.Get(ctx, key)
		var nf *storekit.BlobNotFoundError
		if !errors.As(err, &nf) {
			t.Fatalf("Get(absent) = %v, want *BlobNotFoundError", err)
		}
		if nf.Key != key {
			t.Errorf("BlobNotFoundError.Key = %q, want %q", nf.Key, key)
		}
	})

	t.Run("List sorted deduped prefix-filtered", func(t *testing.T) {
		cases := []struct {
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
				name:   "prefix matching nothing is empty",
				seed:   []string{"blobs/a"},
				prefix: "snaps/",
				want:   nil,
			},
			{
				name:   "empty store is empty",
				seed:   nil,
				prefix: "",
				want:   nil,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				b := newBackend(t)
				for _, k := range tc.seed {
					if err := b.Put(ctx, k, bytes.NewReader([]byte("v"))); err != nil {
						t.Fatalf("seed Put(%q): %v", k, err)
					}
				}
				got, err := b.List(ctx, tc.prefix)
				if err != nil {
					t.Fatalf("List: %v", err)
				}
				if !equalStringSlices(got, tc.want) {
					t.Errorf("List(%q) = %v, want %v", tc.prefix, got, tc.want)
				}
			})
		}
	})

	t.Run("Delete idempotent", func(t *testing.T) {
		cases := []struct {
			name    string
			seed    bool
			deletes int
		}{
			{name: "delete absent is nil", seed: false, deletes: 1},
			{name: "delete existing removes", seed: true, deletes: 1},
			{name: "double delete is nil", seed: true, deletes: 2},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				b := newBackend(t)
				const key = "blobs/del"
				if tc.seed {
					if err := b.Put(ctx, key, bytes.NewReader([]byte("v"))); err != nil {
						t.Fatalf("seed Put: %v", err)
					}
				}
				for i := 0; i < tc.deletes; i++ {
					if err := b.Delete(ctx, key); err != nil {
						t.Fatalf("Delete call %d = %v, want nil (idempotent)", i, err)
					}
				}

				if _, err := b.Get(ctx, key); !errors.As(err, new(*storekit.BlobNotFoundError)) {
					t.Errorf("Get after delete = %v, want *BlobNotFoundError", err)
				}
				// A deleted key is free: a fresh Put of new content succeeds with no
				// lingering conflict against the deleted bytes.
				if err := b.Put(ctx, key, bytes.NewReader([]byte("fresh"))); err != nil {
					t.Errorf("Put after delete = %v, want nil (key free)", err)
				}
			})
		}
	})

	t.Run("invalid key", func(t *testing.T) {
		methods := []struct {
			method string
			call   func(b storekit.Blobs, key string) error
		}{
			{"Put", func(b storekit.Blobs, key string) error { return b.Put(ctx, key, bytes.NewReader([]byte("x"))) }},
			{"Get", func(b storekit.Blobs, key string) error { _, err := b.Get(ctx, key); return err }},
			{"Delete", func(b storekit.Blobs, key string) error { return b.Delete(ctx, key) }},
		}
		for _, m := range methods {
			for _, bad := range invalidNames {
				t.Run(m.method+"/"+bad.label, func(t *testing.T) {
					b := newBackend(t)
					err := m.call(b, bad.value)
					var ine *storekit.InvalidNameError
					if !errors.As(err, &ine) {
						t.Fatalf("%s(%q) = %v, want *InvalidNameError", m.method, bad.value, err)
					}
					if ine.Name != bad.value {
						t.Errorf("InvalidNameError.Name = %q, want %q", ine.Name, bad.value)
					}
				})
			}
		}
	})

	t.Run("large blob round-trips byte-equal", func(t *testing.T) {
		b := newBackend(t)
		const key = "blobs/large"
		content := patternedBytes(payloadFloor)
		if err := b.Put(ctx, key, bytes.NewReader(content)); err != nil {
			t.Fatalf("Put(1 MiB): %v", err)
		}
		rc, err := b.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got := readBlob(t, rc); !bytes.Equal(got, content) {
			t.Errorf("1 MiB blob did not round-trip byte-equal")
		}
	})
}
