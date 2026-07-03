package memstore

import (
	"context"
	"errors"
	"testing"

	"github.com/ciram-co/storekit"
)

// This file holds only the memstore-SPECIFIC lease test that the shared
// storetest conformance suite (run from conformance_test.go) does NOT include:
// the acquire-contention single-winner race, kept here for its goroutine-hygiene
// concurrency coverage (it also pins the winner's first epoch to 1, a memstore
// detail). The shared behaviors (grant on a free name, LeaseHeldError while
// held, strictly-increasing epochs across grant/release/grant, per-name
// independence, Release closing Lost, idempotent double-Release, and invalid
// names) live in package storetest.

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
