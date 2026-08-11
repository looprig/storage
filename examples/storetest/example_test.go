package storetestexample_test

import (
	"testing"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
	"github.com/looprig/storage/storetest"
)

// TestBackendConformance shows how backend authors apply the reusable contract
// suites. Each factory must return a fresh, empty primitive.
func TestBackendConformance(t *testing.T) {
	t.Run("ledger", func(t *testing.T) {
		storetest.TestLedger(t, func(t *testing.T) storage.Ledger { return memstore.New().Ledger })
	})
	t.Run("leaser", func(t *testing.T) {
		storetest.TestLeaser(t, func(t *testing.T) storage.Leaser { return memstore.New().Leaser })
	})
	t.Run("kv", func(t *testing.T) {
		storetest.TestKV(t, func(t *testing.T) storage.KV { return memstore.New().KV })
	})
	t.Run("blobs", func(t *testing.T) {
		storetest.TestBlobs(t, func(t *testing.T) storage.Blobs { return memstore.New().Blobs })
	})
}
