package memstore_test

import (
	"testing"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
	"github.com/looprig/storage/storetest"
)

// This file runs the shared storetest backend-conformance suites against the
// in-memory reference backend. A fresh memstore.New() per factory call yields a
// fresh, empty primitive, so every suite subtest is fully independent. The
// memstore-SPECIFIC behaviors the suites deliberately skip (copy-in/copy-out,
// cursor boundedness, single-winner append contention, independent-reader, the
// reader-error branch, the acquire-contention hygiene test) live in the
// package-internal *_test.go files alongside the implementation.

func TestLedgerConformance(t *testing.T) {
	t.Parallel()
	storetest.TestLedger(t, func(t *testing.T) storekit.Ledger { return memstore.New().Ledger })
}

func TestLeaserConformance(t *testing.T) {
	t.Parallel()
	storetest.TestLeaser(t, func(t *testing.T) storekit.Leaser { return memstore.New().Leaser })
}

func TestKVConformance(t *testing.T) {
	t.Parallel()
	storetest.TestKV(t, func(t *testing.T) storekit.KV { return memstore.New().KV })
}

func TestBlobsConformance(t *testing.T) {
	t.Parallel()
	storetest.TestBlobs(t, func(t *testing.T) storekit.Blobs { return memstore.New().Blobs })
}
