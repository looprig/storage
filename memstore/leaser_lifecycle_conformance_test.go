package memstore

import (
	"context"
	"testing"

	"github.com/looprig/storage"
	"github.com/looprig/storage/storetest"
)

func TestLeaserLifecycleConformance(t *testing.T) {
	t.Parallel()

	storetest.TestLeaserLifecycle(t, func(t *testing.T) storetest.LeaserLifecycleHarness {
		t.Helper()

		store := newLeaserStore()
		return storetest.LeaserLifecycleHarness{
			Primary:       &leaserLifecycleClient{store: store},
			PrimaryViewID: 1,
			OpenIndependent: func(t *testing.T) storetest.LeaserLifecycleClient {
				t.Helper()
				return storetest.LeaserLifecycleClient{
					Leaser: &leaserLifecycleClient{store: store},
					ViewID: 2,
				}
			},
			Renew:  renewMemstoreLease,
			Expire: expireMemstoreLease,
		}
	})
}

// leaserLifecycleClient is a test-only client view over one leaserStore. Each
// instance is independent while the underlying lease state remains shared.
type leaserLifecycleClient struct {
	store *leaserStore
}

func (client *leaserLifecycleClient) Acquire(ctx context.Context, name string) (storage.Lease, error) {
	return client.store.Acquire(ctx, name)
}

// renewMemstoreLease exercises the no-expiry memstore oracle's successful
// renewal path. A live grant needs no state change to remain live.
func renewMemstoreLease(t *testing.T, lease storage.Lease) {
	t.Helper()

	memLease, ok := lease.(*memLease)
	if !ok {
		t.Fatalf("renew lease type = %T, want *memLease", lease)
	}
	memLease.store.mu.Lock()
	live := !memLease.ended && memLease.store.holders[memLease.name] == memLease
	memLease.store.mu.Unlock()
	if !live {
		t.Fatal("Renew called for a lease that is no longer live")
	}
}

// expireMemstoreLease is a test-only expiry control. It models a failed
// renewal/expiry: ownership ends and Lost() closes, but the holder never called
// Release, so its later stale Release must still be a safe no-op.
func expireMemstoreLease(t *testing.T, lease storage.Lease) {
	t.Helper()

	memLease, ok := lease.(*memLease)
	if !ok {
		t.Fatalf("expire lease type = %T, want *memLease", lease)
	}
	memLease.store.mu.Lock()
	defer memLease.store.mu.Unlock()
	if memLease.ended || memLease.store.holders[memLease.name] != memLease {
		t.Fatal("Expire called for a lease that is no longer live")
	}
	memLease.ended = true
	close(memLease.lost)
	delete(memLease.store.holders, memLease.name)
}
