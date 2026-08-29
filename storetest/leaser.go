package storetest

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/looprig/storage"
)

// TestLeaser runs the baseline Leaser conformance suite. newBackend must return
// a fresh, empty Leaser; register any cleanup via t.Cleanup inside newBackend.
// Every provider also supplies a deterministic LeaserLifecycleHarness and runs
// TestLeaserLifecycle to cover renewal and expiry.
//
// Cross-process reclaim (a dead holder's lease being reclaimed by the backend's
// native mechanism) is "where testable" and left to each backend's own tests.
func TestLeaser(t *testing.T, newBackend func(t *testing.T) storage.Leaser) {
	ctx := conformanceContext(t)

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
		var held *storage.LeaseHeldError
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
				var ine *storage.InvalidNameError
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

// LeaserLifecycleHarness provides test-only, deterministic controls for a
// renewable Leaser implementation. Every LeaserLifecycleFactory call must
// return a fresh harness. Primary and each client returned by OpenIndependent
// must share the same backing state but be separate client views, so the suite
// can prove no operation depends on retaining one database connection or
// session. PrimaryViewID and each independent ViewID identify those test-only
// views. Renew must keep a live grant at its existing epoch; Expire must make
// that grant lose ownership and close Lost.
//
// These controls are intentionally not part of storage.Leaser or storage.Lease:
// providers choose their own renewal/expiry mechanism. External providers must
// supply their own deterministic harness when they run TestLeaserLifecycle.
type LeaserLifecycleHarness struct {
	Primary         storage.Leaser
	PrimaryViewID   uint64
	OpenIndependent func(t *testing.T) LeaserLifecycleClient
	Renew           func(t *testing.T, lease storage.Lease)
	Expire          func(t *testing.T, lease storage.Lease)
}

// LeaserLifecycleClient identifies one test-only client view over a Leaser.
// ViewID is an opaque nonzero identity supplied by the harness and must differ
// for distinct views; it never expands the production storage interfaces.
type LeaserLifecycleClient struct {
	Leaser storage.Leaser
	ViewID uint64
}

// LeaserLifecycleFactory returns a fresh lifecycle harness for one conformance
// subtest. It may register cleanup with t.Cleanup.
type LeaserLifecycleFactory func(t *testing.T) LeaserLifecycleHarness

// TestLeaserLifecycle runs conformance checks for renewable, monotonic epoch
// ownership. Run it alongside TestLeaser for every provider with a
// deterministic lifecycle harness.
func TestLeaserLifecycle(t *testing.T, newHarness LeaserLifecycleFactory) {
	t.Helper()
	ctx := conformanceContext(t)

	t.Run("renewal keeps the same epoch live", func(t *testing.T) {
		harness := requireLeaserLifecycleHarness(t, newHarness)
		const name = "sessions/renewal"

		lease, err := harness.Primary.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("primary Acquire: %v", err)
		}
		registerLifecycleLeaseCleanup(t, lease)
		epoch := lease.Epoch()

		harness.Renew(t, lease)
		if lease.Epoch() != epoch {
			t.Errorf("Epoch() after Renew = %d, want unchanged %d", lease.Epoch(), epoch)
		}
		if isClosed(lease.Lost()) {
			t.Error("Lost() closed after successful Renew")
		}

		independent := openIndependentLeaser(t, harness)
		_, err = independent.Acquire(ctx, name)
		requireLeaseHeld(t, err, name, epoch)
	})

	t.Run("expiry closes Lost and a later independent grant advances epoch", func(t *testing.T) {
		harness := requireLeaserLifecycleHarness(t, newHarness)
		const name = "sessions/expiry"

		first, err := harness.Primary.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("primary Acquire: %v", err)
		}
		registerLifecycleLeaseCleanup(t, first)
		firstEpoch := first.Epoch()

		harness.Expire(t, first)
		if !isClosed(first.Lost()) {
			t.Error("Lost() remained open after Expire")
		}

		second, err := openIndependentLeaser(t, harness).Acquire(ctx, name)
		if err != nil {
			t.Fatalf("independent Acquire after expiry: %v", err)
		}
		registerLifecycleLeaseCleanup(t, second)
		if second.Epoch() <= firstEpoch {
			t.Errorf("later grant Epoch() = %d, want greater than expired epoch %d", second.Epoch(), firstEpoch)
		}
		if isClosed(second.Lost()) {
			t.Error("Lost() closed on a newly acquired later grant")
		}
	})

	t.Run("stale Release cannot free a later holder", func(t *testing.T) {
		harness := requireLeaserLifecycleHarness(t, newHarness)
		const name = "sessions/stale-release"

		first, err := harness.Primary.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("primary Acquire: %v", err)
		}
		registerLifecycleLeaseCleanup(t, first)
		harness.Expire(t, first)

		second, err := openIndependentLeaser(t, harness).Acquire(ctx, name)
		if err != nil {
			t.Fatalf("independent Acquire after expiry: %v", err)
		}
		registerLifecycleLeaseCleanup(t, second)

		if err := first.Release(ctx); err != nil {
			t.Fatalf("stale Release: %v", err)
		}
		if isClosed(second.Lost()) {
			t.Error("stale Release closed the later holder's Lost()")
		}
		_, err = harness.Primary.Acquire(ctx, name)
		requireLeaseHeld(t, err, name, second.Epoch())
	})

	t.Run("independent views observe held and reclaimed state", func(t *testing.T) {
		harness := requireLeaserLifecycleHarness(t, newHarness)
		const name = "sessions/independent"

		first, err := harness.Primary.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("primary Acquire: %v", err)
		}
		registerLifecycleLeaseCleanup(t, first)

		independent := openIndependentLeaser(t, harness)
		_, err = independent.Acquire(ctx, name)
		requireLeaseHeld(t, err, name, first.Epoch())

		if err := first.Release(ctx); err != nil {
			t.Fatalf("primary Release: %v", err)
		}
		second, err := independent.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("independent Acquire after Release: %v", err)
		}
		registerLifecycleLeaseCleanup(t, second)
		if second.Epoch() <= first.Epoch() {
			t.Errorf("independent later grant Epoch() = %d, want greater than %d", second.Epoch(), first.Epoch())
		}

		_, err = harness.Primary.Acquire(ctx, name)
		requireLeaseHeld(t, err, name, second.Epoch())
	})
}

func requireLeaserLifecycleHarness(t *testing.T, newHarness LeaserLifecycleFactory) LeaserLifecycleHarness {
	t.Helper()
	if newHarness == nil {
		t.Fatal("LeaserLifecycleFactory is nil")
	}
	harness := newHarness(t)
	if harness.Primary == nil {
		t.Fatal("LeaserLifecycleHarness.Primary is nil")
	}
	if harness.PrimaryViewID == 0 {
		t.Fatal("LeaserLifecycleHarness.PrimaryViewID is zero")
	}
	if harness.OpenIndependent == nil {
		t.Fatal("LeaserLifecycleHarness.OpenIndependent is nil")
	}
	if harness.Renew == nil {
		t.Fatal("LeaserLifecycleHarness.Renew is nil")
	}
	if harness.Expire == nil {
		t.Fatal("LeaserLifecycleHarness.Expire is nil")
	}
	return harness
}

func openIndependentLeaser(t *testing.T, harness LeaserLifecycleHarness) storage.Leaser {
	t.Helper()
	independent := harness.OpenIndependent(t)
	if independent.Leaser == nil {
		t.Fatal("LeaserLifecycleHarness.OpenIndependent returned a nil Leaser")
	}
	if independent.ViewID == 0 {
		t.Fatal("LeaserLifecycleHarness.OpenIndependent returned a zero ViewID")
	}
	if independent.ViewID == harness.PrimaryViewID {
		t.Fatal("LeaserLifecycleHarness.OpenIndependent reused the primary ViewID")
	}
	if sameLeaserClient(independent.Leaser, harness.Primary) {
		t.Fatal("LeaserLifecycleHarness.OpenIndependent reused the primary Leaser client")
	}
	return independent.Leaser
}

func sameLeaserClient(left, right storage.Leaser) bool {
	leftType := reflect.TypeOf(left)
	return leftType != nil && leftType == reflect.TypeOf(right) && leftType.Comparable() && left == right
}

func registerLifecycleLeaseCleanup(t *testing.T, lease storage.Lease) {
	t.Helper()
	if lease == nil {
		t.Fatal("Acquire returned a nil lease on success")
	}
	t.Cleanup(func() {
		// Bound the release directly rather than through conformanceContext:
		// this runs during cleanup, which is too late to register another one.
		ctx, cancel := context.WithTimeout(context.Background(), conformanceTimeout)
		defer cancel()
		if err := lease.Release(ctx); err != nil {
			t.Errorf("Release cleanup: %v", err)
		}
	})
}

func requireLeaseHeld(t *testing.T, err error, name string, wantEpoch uint64) {
	t.Helper()
	var held *storage.LeaseHeldError
	if !errors.As(err, &held) {
		t.Fatalf("Acquire = %v, want *LeaseHeldError", err)
	}
	if held.Name != name {
		t.Errorf("LeaseHeldError.Name = %q, want %q", held.Name, name)
	}
	if held.HolderEpoch != wantEpoch {
		t.Errorf("LeaseHeldError.HolderEpoch = %d, want current holder epoch %d", held.HolderEpoch, wantEpoch)
	}
}
