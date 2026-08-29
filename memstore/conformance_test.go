package memstore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
	"github.com/looprig/storage/storetest"
)

// This file runs the shared storetest backend-conformance suites against the
// in-memory reference backend. A fresh memstore.New() per factory call yields a
// fresh, empty primitive, so every suite subtest is fully independent. The
// memstore-specific behaviors the shared suites deliberately skip (cursor
// boundedness, single-winner append contention, independent-reader, the
// reader-error branch, the acquire-contention hygiene test) live in the
// package-internal *_test.go files alongside the implementation. OrderedIndex
// copy-in/copy-out ownership is covered by its shared conformance suite.

func TestLedgerConformance(t *testing.T) {
	t.Parallel()
	storetest.TestLedger(t, func(t *testing.T) storage.Ledger { return memstore.New().Ledger })
}

func TestLeaserConformance(t *testing.T) {
	t.Parallel()
	storetest.TestLeaser(t, func(t *testing.T) storage.Leaser { return memstore.New().Leaser })
}

func TestKVConformance(t *testing.T) {
	t.Parallel()
	storetest.TestKV(t, func(t *testing.T) storage.KV { return memstore.New().KV })
}

func TestBlobsConformance(t *testing.T) {
	t.Parallel()
	storetest.TestBlobs(t, func(t *testing.T) storage.Blobs { return memstore.New().Blobs })
}

func TestOrderedIndexConformance(t *testing.T) {
	t.Parallel()
	storetest.TestOrderedIndex(t, func(t *testing.T) storage.OrderedIndex {
		return orderedCursorProbeIndex{OrderedIndex: memstore.New().OrderedIndex}
	})
}

// TestOrderedIndexConformanceAllowsUndisclosedActualRevision adapts memstore to
// verify the contract-valid undisclosed ActualRevision path. Ordinary providers
// may report either the current revision or the undisclosed zero sentinel.
func TestOrderedIndexConformanceAllowsUndisclosedActualRevision(t *testing.T) {
	t.Parallel()
	storetest.TestOrderedIndex(t, func(t *testing.T) storage.OrderedIndex {
		return orderedCursorProbeIndex{OrderedIndex: undisclosedActualRevisionIndex{OrderedIndex: memstore.New().OrderedIndex}}
	})
}

type undisclosedActualRevisionIndex struct{ storage.OrderedIndex }

func (index undisclosedActualRevisionIndex) Update(ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	record, err := index.OrderedIndex.Update(ctx, id, expectedRevision, value, rank, due)
	return record, undiscloseActualRevision(err)
}

func (index undisclosedActualRevisionIndex) Delete(ctx context.Context, id storage.OrderedID, expectedRevision uint64) (storage.OrderedRecord, error) {
	record, err := index.OrderedIndex.Delete(ctx, id, expectedRevision)
	return record, undiscloseActualRevision(err)
}

func undiscloseActualRevision(err error) error {
	var conflict *storage.OrderedRevisionConflictError
	if !errors.As(err, &conflict) {
		return err
	}
	undisclosed := *conflict
	undisclosed.ActualRevision = 0
	return &undisclosed
}

// orderedCursorProbeIndex supplies the fail-closed cursor inputs whose shape
// belongs to this provider's opaque encoding rather than to the shared
// contract. memstore tokens are "v<version>:<kind letter>:<encoded payload>".
type orderedCursorProbeIndex struct {
	storage.OrderedIndex
}

var _ storetest.OrderedCursorProbe = orderedCursorProbeIndex{}

// MalformedCursor returns a token with no recognizable memstore header, which
// this provider could never have issued.
func (orderedCursorProbeIndex) MalformedCursor(t *testing.T, kind storage.OrderedCursorKind) string {
	t.Helper()
	return "memstore-" + kind.String() + "-token-secret"
}

// UnknownVersionCursor returns a token whose header is syntactically a memstore
// cursor of a version this build does not implement.
func (orderedCursorProbeIndex) UnknownVersionCursor(t *testing.T, kind storage.OrderedCursorKind) string {
	t.Helper()
	if kind == storage.RankedCursorKind {
		return "v2:r:opaque"
	}
	return "v2:d:opaque"
}
