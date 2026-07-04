package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/looprig/storage"
)

// TestLeaser runs the Leaser conformance suite. newBackend must return a fresh,
// empty Leaser; register any cleanup via t.Cleanup inside newBackend.
//
// Cross-process reclaim (a dead holder's lease being reclaimed by the backend's
// native mechanism) is "where testable" and left to each backend's own tests.
func TestLeaser(t *testing.T, newBackend func(t *testing.T) storekit.Leaser) {
	ctx := context.Background()

	t.Run("acquire on a free name grants a live lease", func(t *testing.T) {
		le := newBackend(t)
		lease, err := le.Acquire(ctx, "sessions/free")
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		if lease == nil {
			t.Fatal("Acquire returned a nil lease on success")
		}
		if isClosed(lease.Lost()) {
			t.Error("Lost() closed on a freshly-granted lease, want open")
		}
	})

	t.Run("acquire while held returns LeaseHeldError", func(t *testing.T) {
		le := newBackend(t)
		const name = "sessions/held"
		first, err := le.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("first Acquire: %v", err)
		}
		_, err = le.Acquire(ctx, name)
		var held *storekit.LeaseHeldError
		if !errors.As(err, &held) {
			t.Fatalf("second Acquire = %v, want *LeaseHeldError", err)
		}
		if held.Name != name {
			t.Errorf("LeaseHeldError.Name = %q, want %q", held.Name, name)
		}
		if held.HolderEpoch != first.Epoch() {
			t.Errorf("LeaseHeldError.HolderEpoch = %d, want %d (current holder's epoch)", held.HolderEpoch, first.Epoch())
		}
	})

	t.Run("epoch strictly increases across grant release grant", func(t *testing.T) {
		le := newBackend(t)
		const name = "sessions/epochs"
		var last uint64
		for i := 0; i < 4; i++ {
			lease, err := le.Acquire(ctx, name)
			if err != nil {
				t.Fatalf("Acquire iteration %d: %v", i, err)
			}
			if i > 0 && lease.Epoch() <= last {
				t.Errorf("grant %d Epoch() = %d, want strictly greater than %d", i, lease.Epoch(), last)
			}
			last = lease.Epoch()
			if err := lease.Release(ctx); err != nil {
				t.Fatalf("Release iteration %d: %v", i, err)
			}
		}
	})

	t.Run("epochs are independent per name", func(t *testing.T) {
		le := newBackend(t)
		// A lease on one name must not block acquiring a different name: the two
		// names are independent lease slots.
		leaseA, err := le.Acquire(ctx, "sessions/a")
		if err != nil {
			t.Fatalf("Acquire(a): %v", err)
		}
		leaseB, err := le.Acquire(ctx, "sessions/b")
		if err != nil {
			t.Fatalf("Acquire(b) while a is held = %v, want success (independent names)", err)
		}
		if isClosed(leaseA.Lost()) {
			t.Error("Lost(a) closed while a is held")
		}
		if isClosed(leaseB.Lost()) {
			t.Error("Lost(b) closed while b is held")
		}
		if err := leaseA.Release(ctx); err != nil {
			t.Errorf("Release(a): %v", err)
		}
		if err := leaseB.Release(ctx); err != nil {
			t.Errorf("Release(b): %v", err)
		}
	})

	t.Run("Release closes Lost", func(t *testing.T) {
		le := newBackend(t)
		const name = "sessions/release"
		lease, err := le.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		if isClosed(lease.Lost()) {
			t.Fatal("Lost() closed before Release")
		}
		if err := lease.Release(ctx); err != nil {
			t.Fatalf("Release: %v", err)
		}
		if !isClosed(lease.Lost()) {
			t.Error("Lost() not closed after Release, want closed")
		}
	})

	t.Run("double Release is nil", func(t *testing.T) {
		le := newBackend(t)
		lease, err := le.Acquire(ctx, "sessions/double")
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		for i := 0; i < 3; i++ {
			if err := lease.Release(ctx); err != nil {
				t.Fatalf("Release call %d = %v, want nil (idempotent)", i, err)
			}
		}
		if !isClosed(lease.Lost()) {
			t.Error("Lost() not closed after Release, want closed")
		}
	})

	t.Run("invalid name", func(t *testing.T) {
		for _, bad := range invalidNames {
			t.Run(bad.label, func(t *testing.T) {
				le := newBackend(t)
				_, err := le.Acquire(ctx, bad.value)
				var ine *storekit.InvalidNameError
				if !errors.As(err, &ine) {
					t.Fatalf("Acquire(%q) = %v, want *InvalidNameError", bad.value, err)
				}
				if ine.Name != bad.value {
					t.Errorf("InvalidNameError.Name = %q, want %q", ine.Name, bad.value)
				}
			})
		}
	})
}
