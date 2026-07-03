package memstore

import (
	"context"
	"errors"
	"testing"

	"github.com/ciram-co/storekit"
)

// isClosed reports whether ch is closed, without blocking. It is only safe to
// call on a channel that is either open or closed (never one that carries live
// values), which is exactly the contract of Lease.Lost().
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestLeaserAcquireGrantsFreeName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		leaseName string
	}{
		{name: "single-segment name", leaseName: "primary"},
		{name: "multi-segment name", leaseName: "sessions/abc/primary"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newLeaserStore()

			lease, err := s.Acquire(ctx, tt.leaseName)
			if err != nil {
				t.Fatalf("Acquire(%q) unexpected error: %v", tt.leaseName, err)
			}
			if lease == nil {
				t.Fatal("Acquire() returned nil lease on success")
			}
			if lease.Epoch() != 1 {
				t.Errorf("first Epoch() = %d, want 1", lease.Epoch())
			}
			if isClosed(lease.Lost()) {
				t.Error("Lost() closed on a live lease, want open")
			}
		})
	}
}

func TestLeaserAcquireHeld(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newLeaserStore()
	const name = "sessions/held"

	first, err := s.Acquire(ctx, name)
	if err != nil {
		t.Fatalf("first Acquire() unexpected error: %v", err)
	}

	// A second Acquire while the first holder is live must be refused with a
	// *LeaseHeldError naming the live holder's epoch.
	_, err = s.Acquire(ctx, name)
	var held *storekit.LeaseHeldError
	if !errors.As(err, &held) {
		t.Fatalf("second Acquire() error = %v, want *storekit.LeaseHeldError", err)
	}
	if held.Name != name {
		t.Errorf("LeaseHeldError.Name = %q, want %q", held.Name, name)
	}
	if held.HolderEpoch != first.Epoch() {
		t.Errorf("LeaseHeldError.HolderEpoch = %d, want %d (current holder's epoch)", held.HolderEpoch, first.Epoch())
	}
}

func TestLeaserEpochStrictlyIncreases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newLeaserStore()
	const name = "sessions/epochs"

	var last uint64
	for i := 0; i < 4; i++ {
		lease, err := s.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("Acquire() iteration %d unexpected error: %v", i, err)
		}
		if lease.Epoch() <= last {
			t.Errorf("iteration %d Epoch() = %d, want strictly greater than %d", i, lease.Epoch(), last)
		}
		last = lease.Epoch()
		if err := lease.Release(ctx); err != nil {
			t.Fatalf("Release() iteration %d unexpected error: %v", i, err)
		}
	}
	if last != 4 {
		t.Errorf("final epoch after 4 grants = %d, want 4 (starts at 1, +1 per grant)", last)
	}
}

func TestLeaserEpochsIndependentPerName(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newLeaserStore()

	// Advance name "a" to epoch 3, leaving it released.
	for i := 0; i < 3; i++ {
		la, err := s.Acquire(ctx, "sessions/a")
		if err != nil {
			t.Fatalf("Acquire(a) iteration %d unexpected error: %v", i, err)
		}
		if err := la.Release(ctx); err != nil {
			t.Fatalf("Release(a) iteration %d unexpected error: %v", i, err)
		}
	}

	// A different name must start its own counter at 1, unaffected by "a".
	lb, err := s.Acquire(ctx, "sessions/b")
	if err != nil {
		t.Fatalf("Acquire(b) unexpected error: %v", err)
	}
	if lb.Epoch() != 1 {
		t.Errorf("independent name first Epoch() = %d, want 1", lb.Epoch())
	}
}

func TestLeaserReleaseClosesLostAndFreesName(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newLeaserStore()
	const name = "sessions/release"

	lease, err := s.Acquire(ctx, name)
	if err != nil {
		t.Fatalf("Acquire() unexpected error: %v", err)
	}
	if isClosed(lease.Lost()) {
		t.Fatal("Lost() closed before Release")
	}

	if err := lease.Release(ctx); err != nil {
		t.Fatalf("Release() unexpected error: %v", err)
	}
	if !isClosed(lease.Lost()) {
		t.Error("Lost() not closed after Release, want closed")
	}

	// Release frees the name so it can be re-acquired; the new grant advances
	// the epoch.
	next, err := s.Acquire(ctx, name)
	if err != nil {
		t.Fatalf("re-Acquire after Release unexpected error: %v", err)
	}
	if next.Epoch() != lease.Epoch()+1 {
		t.Errorf("re-Acquire Epoch() = %d, want %d", next.Epoch(), lease.Epoch()+1)
	}
}

func TestLeaserDoubleReleaseIsNoOp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		releases int
	}{
		{name: "single release", releases: 1},
		{name: "double release", releases: 2},
		{name: "triple release", releases: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newLeaserStore()

			lease, err := s.Acquire(ctx, "sessions/double")
			if err != nil {
				t.Fatalf("Acquire() unexpected error: %v", err)
			}
			for i := 0; i < tt.releases; i++ {
				if err := lease.Release(ctx); err != nil {
					t.Fatalf("Release() call %d = %v, want nil (idempotent)", i, err)
				}
			}
			if !isClosed(lease.Lost()) {
				t.Error("Lost() not closed after Release, want closed")
			}
		})
	}
}

func TestLeaserInvalidName(t *testing.T) {
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

	for _, bad := range badNames {
		t.Run(bad.label, func(t *testing.T) {
			t.Parallel()
			s := newLeaserStore()
			_, err := s.Acquire(context.Background(), bad.value)
			var ine *storekit.InvalidNameError
			if !errors.As(err, &ine) {
				t.Fatalf("Acquire(%q) error = %v, want *storekit.InvalidNameError", bad.value, err)
			}
			if ine.Name != bad.value {
				t.Errorf("InvalidNameError.Name = %q, want %q", ine.Name, bad.value)
			}
		})
	}
}

// TestLeaserAcquireContention races many goroutines to Acquire the SAME free
// name. Exactly one must win the grant; the rest must see a *LeaseHeldError.
// Per the concurrency test-hygiene rule, goroutines report only over a channel
// and every t.* assertion runs on the test goroutine.
func TestLeaserAcquireContention(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newLeaserStore()
	const name = "sessions/contention"
	const racers = 16

	type result struct {
		lease storekit.Lease
		err   error
	}
	results := make(chan result, racers)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		go func() {
			<-start // release all racers together
			lease, err := s.Acquire(ctx, name)
			results <- result{lease: lease, err: err}
		}()
	}
	close(start)

	var winners int
	var winnerEpoch uint64
	for i := 0; i < racers; i++ {
		r := <-results
		if r.err == nil {
			winners++
			winnerEpoch = r.lease.Epoch()
			continue
		}
		var held *storekit.LeaseHeldError
		if !errors.As(r.err, &held) {
			t.Errorf("loser Acquire error = %v, want *storekit.LeaseHeldError", r.err)
		}
	}
	if winners != 1 {
		t.Fatalf("winners = %d, want exactly 1", winners)
	}
	if winnerEpoch != 1 {
		t.Errorf("winner Epoch() = %d, want 1 (first grant of a fresh name)", winnerEpoch)
	}
}
