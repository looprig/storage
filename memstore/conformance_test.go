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
		return memstore.New().OrderedIndex
	}, memstoreCursorProbe{})
}

// TestOrderedIndexConformanceAllowsUndisclosedActualRevision adapts memstore to
// verify the contract-valid undisclosed ActualRevision path. Ordinary providers
// may report either the current revision or the undisclosed zero sentinel.
func TestOrderedIndexConformanceAllowsUndisclosedActualRevision(t *testing.T) {
	t.Parallel()
	storetest.TestOrderedIndexRevisionConflicts(t, func(t *testing.T) storage.OrderedIndex {
		return undisclosedActualRevisionIndex{OrderedIndex: memstore.New().OrderedIndex}
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

// memstoreCursorProbe supplies the fail-closed cursor inputs whose shape
// belongs to this provider's opaque encoding rather than to the shared
// contract. memstore tokens are "v<version>:<kind letter>:<encoded payload>".
type memstoreCursorProbe struct{}

var _ storetest.OrderedCursorProbe = memstoreCursorProbe{}

// MalformedCursor returns a token with no recognizable memstore header, which
// this provider could never have issued.
func (memstoreCursorProbe) MalformedCursor(t *testing.T, kind storage.OrderedCursorKind) string {
	t.Helper()
	return "memstore-" + kind.String() + "-token-secret"
}

// UnknownVersionCursor returns a token whose header is syntactically a memstore
// cursor of a version this build does not implement.
func (memstoreCursorProbe) UnknownVersionCursor(t *testing.T, kind storage.OrderedCursorKind) string {
	t.Helper()
	if kind == storage.RankedCursorKind {
		return "v2:r:opaque"
	}
	return "v2:d:opaque"
}

// TestOrderedIndexConformanceAllowsSparseOrders is the regression guard for the
// suite's independence from dense order allocation. The contract promises only
// that Order is nonzero, immutable, strictly increasing within its order scope,
// and never reused; it does not promise 1-based or contiguous values, because a
// provider may allocate from a JetStream stream sequence or a shared SQL
// sequence. sparseOrderIndex allocates deliberately sparse orders, so this test
// fails the moment the shared suite reintroduces a density assumption.
func TestOrderedIndexConformanceAllowsSparseOrders(t *testing.T) {
	t.Parallel()
	storetest.TestOrderedIndex(t, func(t *testing.T) storage.OrderedIndex {
		return sparseOrderIndex{OrderedIndex: memstore.New().OrderedIndex}
	}, memstoreCursorProbe{})
}

// sparseOrderStride spaces this adapter's acceptance orders apart. Multiplying
// memstore's dense 1..N allocation by a constant stride is an order-preserving
// bijection over the positive integers, so it stays strictly increasing,
// nonzero, immutable and never reused while never producing 1, 2, 3, ....
const sparseOrderStride = 7

// sparseOrderIndex adapts a dense provider into a sparse one, purely for the
// conformance test above. Every Order the wrapped index emits is scaled up on
// the way out and every afterOrder the caller supplies is scaled back down on
// the way in, which is exact because the caller can only ever pass back an
// order this adapter issued (or zero).
type sparseOrderIndex struct {
	storage.OrderedIndex
}

func (index sparseOrderIndex) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	record, err := index.OrderedIndex.Get(ctx, id)
	return sparseOrderRecord(record), err
}

func (index sparseOrderIndex) Create(ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	record, created, err := index.OrderedIndex.Create(ctx, id, rankingScope, value, rank, due)
	return sparseOrderRecord(record), created, err
}

func (index sparseOrderIndex) Update(ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	record, err := index.OrderedIndex.Update(ctx, id, expectedRevision, value, rank, due)
	return sparseOrderRecord(record), err
}

func (index sparseOrderIndex) Delete(ctx context.Context, id storage.OrderedID, expectedRevision uint64) (storage.OrderedRecord, error) {
	record, err := index.OrderedIndex.Delete(ctx, id, expectedRevision)
	return sparseOrderRecord(record), err
}

func (index sparseOrderIndex) ListOrdered(ctx context.Context, namespace string, orderingScope string, afterOrder uint64, limit int) (storage.OrderedPage, error) {
	page, err := index.OrderedIndex.ListOrdered(ctx, namespace, orderingScope, afterOrder/sparseOrderStride, limit)
	sparseOrderRecords(page.Records)
	if len(page.Records) > 0 {
		page.NextAfterOrder = page.Records[len(page.Records)-1].Order
	}
	return page, err
}

func (index sparseOrderIndex) ListRanked(ctx context.Context, namespace string, rankingScope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	page, err := index.OrderedIndex.ListRanked(ctx, namespace, rankingScope, after, limit)
	sparseOrderRecords(page.Records)
	return page, err
}

func (index sparseOrderIndex) ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after storage.DueCursor, limit int) (storage.DuePage, error) {
	page, err := index.OrderedIndex.ListDue(ctx, namespace, dueAtOrBefore, after, limit)
	sparseOrderRecords(page.Records)
	return page, err
}

func sparseOrderRecord(record storage.OrderedRecord) storage.OrderedRecord {
	record.Order *= sparseOrderStride
	return record
}

func sparseOrderRecords(records []storage.OrderedRecord) {
	for i := range records {
		records[i] = sparseOrderRecord(records[i])
	}
}
