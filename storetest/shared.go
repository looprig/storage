package storetest

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/looprig/storage"
)

// payloadFloor is the minimum payload/value/blob size every storage backend
// must accept (1 MiB); the suites exercise it as the "payload floor". Larger
// payloads are the engine's responsibility to offload to Blobs.
const payloadFloor = 1 << 20

// invalidName pairs a name that violates the storage grammar with a human label
// used to name its subtest.
type invalidName struct {
	label string
	value string
}

// invalidNames is the shared table of grammar-violating names, reused by every
// suite's invalid-name case (Append/Read/Tip/Delete, Acquire, Get/Put/Delete).
var invalidNames = []invalidName{
	{label: "empty", value: ""},
	{label: "leading slash", value: "/leading"},
	{label: "trailing slash", value: "trailing/"},
	{label: "doubled slash", value: "a//b"},
	{label: "uppercase", value: "Upper"},
	{label: "space", value: "has space"},
	{label: "dot-dot segment", value: ".."},
}

// patternedBytes returns n bytes filled with a position-dependent pattern, so a
// byte-equality check on a large payload catches truncation or offset bugs a
// uniform fill would silently pass.
func patternedBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 31)
	}
	return b
}

// equalStringSlices reports whether a and b hold the same elements in the same
// order, treating nil and empty as equal (a backend may return either for an
// empty listing). Comparing a result against a sorted, duplicate-free want makes
// this assert sorted+dedup+correct-membership in one shot.
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// isClosed reports whether ch is closed without blocking. It is safe only on a
// channel that is open or closed (never one carrying live values) — exactly the
// contract of Lease.Lost().
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// readAll opens a cursor at from, drains it to exhaustion, and closes it,
// returning every record. It runs on the CALLING goroutine and fails the test on
// any error, so it must never be invoked from a spawned goroutine.
func readAll(t *testing.T, l storage.Ledger, name string, from uint64) []storage.Record {
	t.Helper()
	ctx := context.Background()
	cur, err := l.Read(ctx, name, from)
	if err != nil {
		t.Fatalf("Read(%q, %d): unexpected error: %v", name, from, err)
	}
	defer func() {
		if cerr := cur.Close(); cerr != nil {
			t.Errorf("cursor Close: %v, want nil", cerr)
		}
	}()
	var out []storage.Record
	for {
		rec, nerr := cur.Next(ctx)
		if errors.Is(nerr, io.EOF) {
			return out
		}
		if nerr != nil {
			t.Fatalf("cursor Next: unexpected error: %v", nerr)
		}
		out = append(out, rec)
	}
}

// readBlob reads rc to completion and closes it, returning the bytes; it fails
// the test on any error so callers can assert on the returned bytes directly.
func readBlob(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	defer func() {
		if err := rc.Close(); err != nil {
			t.Errorf("blob reader Close: %v, want nil", err)
		}
	}()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("blob ReadAll: unexpected error: %v", err)
	}
	return data
}
