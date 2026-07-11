package memstore

import (
	"bytes"
	"context"
	"testing"
)

// TestNewComposite asserts New() returns a fully-wired *storage.Composite: all
// four embedded providers are non-nil and each is independently callable through
// its named field. It is a wiring smoke test — the per-primitive semantics are
// pinned by the ledger/lease/kv/blobs tests.
func TestNewComposite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := New()
	if c == nil {
		t.Fatal("New() returned nil Composite")
	}
	if c.Ledger == nil || c.Leaser == nil || c.KV == nil || c.Blobs == nil {
		t.Fatalf("New() left a nil field: Ledger=%v Leaser=%v KV=%v Blobs=%v",
			c.Ledger == nil, c.Leaser == nil, c.KV == nil, c.Blobs == nil)
	}

	// Ledger: fresh ledger tip is 0, then an append advances it.
	if tip, err := c.Ledger.Tip(ctx, "sessions/s"); err != nil || tip != 0 {
		t.Fatalf("Ledger.Tip() = %d, %v; want 0, nil", tip, err)
	}
	if err := c.Ledger.Append(ctx, "sessions/s", 0, []byte("rec")); err != nil {
		t.Fatalf("Ledger.Append() unexpected error: %v", err)
	}

	// KV: create-only Put returns rev 1 and Get reads it back.
	rev, err := c.KV.Put(ctx, "sessions/s", 0, []byte("meta"))
	if err != nil || rev != 1 {
		t.Fatalf("KV.Put() = %d, %v; want 1, nil", rev, err)
	}
	if val, _, err := c.KV.Get(ctx, "sessions/s"); err != nil || !bytes.Equal(val, []byte("meta")) {
		t.Fatalf("KV.Get() = %q, %v; want \"meta\", nil", val, err)
	}

	// Blobs: Put then List sees the key.
	if err := c.Blobs.Put(ctx, "blobs/b", bytes.NewReader([]byte("bytes"))); err != nil {
		t.Fatalf("Blobs.Put() unexpected error: %v", err)
	}
	if keys, err := c.Blobs.List(ctx, ""); err != nil || len(keys) != 1 || keys[0] != "blobs/b" {
		t.Fatalf("Blobs.List() = %v, %v; want [blobs/b], nil", keys, err)
	}

	// Leaser: Acquire grants a live lease at epoch 1.
	lease, err := c.Leaser.Acquire(ctx, "sessions/s")
	if err != nil {
		t.Fatalf("Leaser.Acquire() unexpected error: %v", err)
	}
	if lease.Epoch() != 1 {
		t.Errorf("Leaser.Acquire() epoch = %d, want 1", lease.Epoch())
	}
	if err := lease.Release(ctx); err != nil {
		t.Errorf("Lease.Release() unexpected error: %v", err)
	}
}
