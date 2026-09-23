package storetest

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/looprig/storage"
)

// nestedCaseName is the subtest group both TestKV and TestBlobs register for
// the key-extension rule: a valid key and a valid key extending it with "/…"
// are distinct names and must coexist (storage package doc, KV, Blobs).
const nestedCaseName = "a key and a key extending it with '/' coexist"

// nestedCase is one row of the key-extension table. keys lists every key the
// case stores, in creation order; deleteKey is one of them; listings maps a
// prefix to the exact, sorted listing expected while every key is present.
// A sibling that merely shares a string prefix ("sessions/ab") rides along in
// every row so a backend that answers Keys/List by directory walk rather than
// by string prefix is caught too.
type nestedCase struct {
	name      string
	keys      []string
	deleteKey string
	listings  []nestedListing
}

type nestedListing struct {
	prefix string
	want   []string
}

const (
	nestedParent     = "sessions/a"
	nestedChild      = "sessions/a/b"
	nestedGrandchild = "sessions/a/b/c"
	nestedSibling    = "sessions/ab"
)

// nestedCases crosses creation order (key first / extension first) with which
// key is deleted, at one and two levels of extension.
func nestedCases() []nestedCase {
	full := []nestedListing{
		{prefix: "", want: []string{nestedParent, nestedChild, nestedSibling}},
		{prefix: "sessions/", want: []string{nestedParent, nestedChild, nestedSibling}},
		{prefix: nestedParent, want: []string{nestedParent, nestedChild, nestedSibling}},
		{prefix: nestedParent + "/", want: []string{nestedChild}},
		{prefix: nestedChild, want: []string{nestedChild}},
	}
	deep := []nestedListing{
		{prefix: nestedParent, want: []string{nestedParent, nestedGrandchild, nestedSibling}},
		{prefix: nestedParent + "/", want: []string{nestedGrandchild}},
		{prefix: nestedChild, want: []string{nestedGrandchild}},
		{prefix: nestedGrandchild, want: []string{nestedGrandchild}},
	}
	return []nestedCase{
		{name: "key first, delete key", keys: []string{nestedParent, nestedChild, nestedSibling}, deleteKey: nestedParent, listings: full},
		{name: "key first, delete extension", keys: []string{nestedParent, nestedChild, nestedSibling}, deleteKey: nestedChild, listings: full},
		{name: "extension first, delete key", keys: []string{nestedChild, nestedParent, nestedSibling}, deleteKey: nestedParent, listings: full},
		{name: "extension first, delete extension", keys: []string{nestedChild, nestedParent, nestedSibling}, deleteKey: nestedChild, listings: full},
		{name: "two-level extension first, delete key", keys: []string{nestedGrandchild, nestedParent, nestedSibling}, deleteKey: nestedParent, listings: deep},
		{name: "key first, delete two-level extension", keys: []string{nestedParent, nestedGrandchild, nestedSibling}, deleteKey: nestedGrandchild, listings: deep},
	}
}

// nestedValue is the distinct value/content stored under key, so a backend
// that aliases two keys onto one location is caught by a byte comparison.
func nestedValue(key string, generation string) []byte {
	return []byte("value of " + key + " (" + generation + ")")
}

// without returns keys minus drop, preserving order.
func without(keys []string, drop string) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if k != drop {
			out = append(out, k)
		}
	}
	return out
}

// runKVNested registers the KV key-extension group.
func runKVNested(t *testing.T, ctx context.Context, newBackend func(t *testing.T) storage.KV) {
	t.Run(nestedCaseName, func(t *testing.T) {
		for _, tc := range nestedCases() {
			t.Run(tc.name, func(t *testing.T) {
				kv := newBackend(t)
				revs := make(map[string]uint64, len(tc.keys))
				for _, k := range tc.keys {
					rev, err := kv.Put(ctx, k, 0, nestedValue(k, "v1"))
					if err != nil {
						t.Fatalf("create Put(%q) with %v already stored = %v, want nil", k, without(tc.keys, k), err)
					}
					revs[k] = rev
				}
				assertKVValues(t, ctx, kv, tc.keys, revs, "v1")
				assertKVListings(t, ctx, kv, tc.listings, "")

				for _, k := range tc.keys {
					rev, err := kv.Put(ctx, k, revs[k], nestedValue(k, "v2"))
					if err != nil {
						t.Fatalf("update Put(%q, rev %d) = %v, want nil", k, revs[k], err)
					}
					revs[k] = rev
				}
				assertKVValues(t, ctx, kv, tc.keys, revs, "v2")

				kvDeleteAndRecreate(t, ctx, kv, tc, revs)
			})
		}
	})
}

// kvDeleteAndRecreate deletes tc.deleteKey, checks every other key survives
// with its value and revision, then re-creates the deleted key create-only.
func kvDeleteAndRecreate(t *testing.T, ctx context.Context, kv storage.KV, tc nestedCase, revs map[string]uint64) {
	t.Helper()
	if err := kv.Delete(ctx, tc.deleteKey); err != nil {
		t.Fatalf("Delete(%q) = %v, want nil", tc.deleteKey, err)
	}
	if _, _, err := kv.Get(ctx, tc.deleteKey); !errors.As(err, new(*storage.KeyNotFoundError)) {
		t.Errorf("Get(%q) after its Delete = %v, want *KeyNotFoundError", tc.deleteKey, err)
	}
	survivors := without(tc.keys, tc.deleteKey)
	assertKVValues(t, ctx, kv, survivors, revs, "v2")
	assertKVListings(t, ctx, kv, tc.listings, tc.deleteKey)

	rev, err := kv.Put(ctx, tc.deleteKey, 0, nestedValue(tc.deleteKey, "v3"))
	if err != nil {
		t.Fatalf("re-create Put(%q) after Delete = %v, want nil (key free)", tc.deleteKey, err)
	}
	if got, gotRev, gerr := kv.Get(ctx, tc.deleteKey); gerr != nil || gotRev != rev || !bytes.Equal(got, nestedValue(tc.deleteKey, "v3")) {
		t.Errorf("Get(%q) after re-create = (%q, %d, %v), want (%q, %d, nil)", tc.deleteKey, got, gotRev, gerr, nestedValue(tc.deleteKey, "v3"), rev)
	}
	assertKVValues(t, ctx, kv, survivors, revs, "v2")
	assertKVListings(t, ctx, kv, tc.listings, "")
}

func assertKVValues(t *testing.T, ctx context.Context, kv storage.KV, keys []string, revs map[string]uint64, generation string) {
	t.Helper()
	for _, k := range keys {
		got, rev, err := kv.Get(ctx, k)
		if err != nil {
			t.Errorf("Get(%q) = %v, want nil", k, err)
			continue
		}
		if want := nestedValue(k, generation); !bytes.Equal(got, want) {
			t.Errorf("Get(%q) value = %q, want %q", k, got, want)
		}
		if rev != revs[k] {
			t.Errorf("Get(%q) rev = %d, want %d", k, rev, revs[k])
		}
	}
}

// assertKVListings checks every listing, with absent (if non-empty) removed
// from the expectation.
func assertKVListings(t *testing.T, ctx context.Context, kv storage.KV, listings []nestedListing, absent string) {
	t.Helper()
	for _, l := range listings {
		got, err := kv.Keys(ctx, l.prefix)
		if err != nil {
			t.Errorf("Keys(%q) = %v, want nil", l.prefix, err)
			continue
		}
		if want := without(l.want, absent); !equalStringSlices(got, want) {
			t.Errorf("Keys(%q) = %v, want %v", l.prefix, got, want)
		}
	}
}

// runBlobsNested registers the Blobs key-extension group.
func runBlobsNested(t *testing.T, ctx context.Context, newBackend func(t *testing.T) storage.Blobs) {
	t.Run(nestedCaseName, func(t *testing.T) {
		for _, tc := range nestedCases() {
			t.Run(tc.name, func(t *testing.T) {
				b := newBackend(t)
				for _, k := range tc.keys {
					if err := b.Put(ctx, k, bytes.NewReader(nestedValue(k, "v1"))); err != nil {
						t.Fatalf("Put(%q) with %v already stored = %v, want nil", k, without(tc.keys, k), err)
					}
				}
				assertBlobValues(t, ctx, b, tc.keys)
				assertBlobListings(t, ctx, b, tc.listings, "")

				// Byte-identical re-Put of every key stays a no-op success.
				for _, k := range tc.keys {
					if err := b.Put(ctx, k, bytes.NewReader(nestedValue(k, "v1"))); err != nil {
						t.Fatalf("identical re-Put(%q) = %v, want nil", k, err)
					}
				}

				if err := b.Delete(ctx, tc.deleteKey); err != nil {
					t.Fatalf("Delete(%q) = %v, want nil", tc.deleteKey, err)
				}
				if _, err := b.Get(ctx, tc.deleteKey); !errors.As(err, new(*storage.BlobNotFoundError)) {
					t.Errorf("Get(%q) after its Delete = %v, want *BlobNotFoundError", tc.deleteKey, err)
				}
				assertBlobValues(t, ctx, b, without(tc.keys, tc.deleteKey))
				assertBlobListings(t, ctx, b, tc.listings, tc.deleteKey)

				if err := b.Put(ctx, tc.deleteKey, bytes.NewReader(nestedValue(tc.deleteKey, "v1"))); err != nil {
					t.Fatalf("re-Put(%q) after Delete = %v, want nil (key free)", tc.deleteKey, err)
				}
				assertBlobValues(t, ctx, b, tc.keys)
				assertBlobListings(t, ctx, b, tc.listings, "")
			})
		}
	})
}

func assertBlobValues(t *testing.T, ctx context.Context, b storage.Blobs, keys []string) {
	t.Helper()
	for _, k := range keys {
		rc, err := b.Get(ctx, k)
		if err != nil {
			t.Errorf("Get(%q) = %v, want nil", k, err)
			continue
		}
		if got, want := readBlob(t, rc), nestedValue(k, "v1"); !bytes.Equal(got, want) {
			t.Errorf("Get(%q) = %q, want %q", k, got, want)
		}
	}
}

func assertBlobListings(t *testing.T, ctx context.Context, b storage.Blobs, listings []nestedListing, absent string) {
	t.Helper()
	for _, l := range listings {
		got, err := b.List(ctx, l.prefix)
		if err != nil {
			t.Errorf("List(%q) = %v, want nil", l.prefix, err)
			continue
		}
		if want := without(l.want, absent); !equalStringSlices(got, want) {
			t.Errorf("List(%q) = %v, want %v", l.prefix, got, want)
		}
	}
}
