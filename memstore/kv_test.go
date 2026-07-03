package memstore

import (
	"bytes"
	"context"
	"testing"
)

// This file holds only the memstore-SPECIFIC KV tests that the shared storetest
// conformance suite (run from conformance_test.go) deliberately does NOT cover:
// value copy-in/copy-out ownership. The shared behaviors (absent-key
// KeyNotFoundError, create/get, revision bumps, wrong-rev conflicts with state
// unchanged, sorted/deduped/prefix-filtered Keys, idempotent Delete, invalid
// keys, and the 1 MiB value floor) live in package storetest.

func TestKVCopyIn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newKVStore()
	const key = "sessions/copyin"

	orig := []byte("hello")
	caller := make([]byte, len(orig))
	copy(caller, orig)

	if _, err := s.Put(ctx, key, 0, caller); err != nil {
		t.Fatalf("Put() unexpected error: %v", err)
	}
	// Mutating the caller's slice after Put must not reach stored data.
	caller[0] = 'Z'

	got, _, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get() unexpected error: %v", err)
	}
	if !bytes.Equal(got, orig) {
		t.Errorf("stored val = %q, want %q (Put must copy-in)", got, orig)
	}
}

func TestKVCopyOut(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newKVStore()
	const key = "sessions/copyout"

	orig := []byte("hello")
	if _, err := s.Put(ctx, key, 0, orig); err != nil {
		t.Fatalf("Put() unexpected error: %v", err)
	}

	got, _, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get() unexpected error: %v", err)
	}
	// Mutating the slice Get returned must not reach stored data.
	got[0] = 'Z'

	again, _, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get() unexpected error: %v", err)
	}
	if !bytes.Equal(again, orig) {
		t.Errorf("stored val = %q, want %q (Get must copy-out)", again, orig)
	}
}
