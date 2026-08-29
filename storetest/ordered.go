package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/looprig/storage"
)

// OrderedIndexFactory returns a fresh, empty OrderedIndex for one conformance
// subtest.
type OrderedIndexFactory func(t *testing.T) storage.OrderedIndex

// OrderedCursorProbe supplies the fail-closed cursor inputs this suite cannot
// invent for itself. Cursor bytes are opaque and versioned, and each provider
// owns its own encoding, so a literal token written here would test one
// provider's grammar rather than the contract: a provider whose cursors are a
// signed blob or a stream sequence tuple would fail a test about its own
// format. A factory returns a provider that also implements this interface,
// usually by wrapping its OrderedIndex in its own conformance test.
//
// The remaining fail-closed rules need no probe: cross-query and cross-kind
// inputs are derived from real cursors the provider just issued.
type OrderedCursorProbe interface {
	// MalformedCursor returns a nonempty token the provider must reject for
	// kind with OrderedCursorMalformed. It must be a token the provider could
	// never have issued.
	MalformedCursor(t *testing.T, kind storage.OrderedCursorKind) string

	// UnknownVersionCursor returns a nonempty token that is well formed for the
	// provider's encoding but carries a token version it does not support, and
	// which it must therefore reject for kind with
	// OrderedCursorUnknownVersion.
	UnknownVersionCursor(t *testing.T, kind storage.OrderedCursorKind) string
}

// OrderedIndexCounters optionally asserts provider-specific query-work
// counters against a fresh OrderedIndex under the suite's bounded context. The
// concrete measurements are intentionally provider-owned: semantic conformance
// must not prescribe a filesystem walk, a JetStream scan, or a database
// buffer-read unit.
type OrderedIndexCounters interface {
	Assert(t *testing.T, ctx context.Context, index storage.OrderedIndex)
}

// OrderedIndexCounterFunc adapts an assertion function for
// OrderedIndexCounters, so a provider can keep its instrumentation seam local
// to its conformance test.
type OrderedIndexCounterFunc func(t *testing.T, ctx context.Context, index storage.OrderedIndex)

// Assert invokes f as an OrderedIndexCounters assertion.
func (f OrderedIndexCounterFunc) Assert(t *testing.T, ctx context.Context, index storage.OrderedIndex) {
	if f == nil {
		t.Fatal("nil OrderedIndexCounterFunc")
	}
	f(t, ctx, index)
}

// TestOrderedIndex runs the provider-neutral OrderedIndex conformance suite.
//
// newBackend must return a fresh, empty provider and may register cleanup with
// t.Cleanup. Every test receives a bounded context and reports record identity
// on failures so remote providers can use the same suite safely.
//
// probe is REQUIRED. Malformed and unknown-version cursor tokens are the two
// fail-closed inputs the suite cannot invent, because cursor bytes are opaque
// and each provider owns its encoding; a provider supplies them through
// OrderedCursorProbe. It is a parameter rather than an optional interface the
// provider might also implement so that omitting it is a compile error at the
// conformance test rather than a failure from inside one subtest.
//
// counters is optional and, when provided, runs one provider-defined
// bounded-work check on another fresh provider.
func TestOrderedIndex(t *testing.T, newBackend OrderedIndexFactory, probe OrderedCursorProbe, counters ...OrderedIndexCounters) {
	if newBackend == nil {
		t.Fatal("TestOrderedIndex requires a non-nil factory")
	}
	if probe == nil {
		t.Fatal("TestOrderedIndex requires a non-nil OrderedCursorProbe: only the provider knows which opaque tokens are malformed or carry an unsupported version")
	}
	if len(counters) > 1 {
		t.Fatal("TestOrderedIndex accepts at most one optional counter assertion")
	}

	t.Run("TestOrderedIndexCreateAssignsImmutableOrder", func(t *testing.T) {
		testOrderedIndexCreateAssignsImmutableOrder(t, newBackend)
	})
	t.Run("TestOrderedIndexDuplicateReturnsOriginal", func(t *testing.T) {
		testOrderedIndexDuplicateReturnsOriginal(t, newBackend)
	})
	t.Run("TestOrderedIndexScopesIdentity", func(t *testing.T) {
		testOrderedIndexScopesIdentity(t, newBackend)
	})
	t.Run("TestOrderedIndexCASUpdateChangesValueRankAndDueAtomically", func(t *testing.T) {
		testOrderedIndexCASUpdateChangesValueRankAndDueAtomically(t, newBackend)
	})
	t.Run("TestOrderedIndexWrongRevisionLeavesStateUntouched", func(t *testing.T) {
		testOrderedIndexWrongRevisionLeavesStateUntouched(t, newBackend)
	})
	t.Run("TestOrderedIndexRejectsInvalidListArguments", func(t *testing.T) {
		testOrderedIndexRejectsInvalidListArguments(t, newBackend)
	})
	t.Run("TestOrderedIndexListOrderedPagesInAcceptanceOrder", func(t *testing.T) {
		testOrderedIndexListOrderedPagesInAcceptanceOrder(t, newBackend)
	})
	t.Run("TestOrderedIndexListRankedPagesByRankAndStableKey", func(t *testing.T) {
		testOrderedIndexListRankedPagesByRankAndStableKey(t, newBackend)
	})
	t.Run("TestOrderedIndexRankMoveUpdatesAuthoritativePage", func(t *testing.T) {
		testOrderedIndexRankMoveUpdatesAuthoritativePage(t, newBackend)
	})
	t.Run("TestOrderedIndexListDuePagesByDeadlineAndStableKey", func(t *testing.T) {
		testOrderedIndexListDuePagesByDeadlineAndStableKey(t, newBackend)
	})
	t.Run("TestOrderedIndexNotDueNeverAppearsInDuePages", func(t *testing.T) {
		testOrderedIndexNotDueNeverAppearsInDuePages(t, newBackend)
	})
	t.Run("TestOrderedIndexTerminalRecordsDoNotFillPages", func(t *testing.T) {
		testOrderedIndexTerminalRecordsDoNotFillPages(t, newBackend)
	})
	t.Run("TestOrderedIndexConcurrentDuplicateHasOneWinner", func(t *testing.T) {
		testOrderedIndexConcurrentDuplicateHasOneWinner(t, newBackend)
	})
	t.Run("TestOrderedIndexConcurrentDistinctCreatesAreMonotonic", func(t *testing.T) {
		testOrderedIndexConcurrentDistinctCreatesAreMonotonic(t, newBackend)
	})
	t.Run("TestOrderedIndexOpaqueStableKeys", func(t *testing.T) {
		testOrderedIndexOpaqueStableKeys(t, newBackend)
	})
	t.Run("TestOrderedIndexInvalidCursorFailsClosed", func(t *testing.T) {
		testOrderedIndexInvalidCursorFailsClosed(t, newBackend, probe)
	})
	t.Run("TestOrderedIndexLargeValueRoundTrip", func(t *testing.T) {
		testOrderedIndexLargeValueRoundTrip(t, newBackend)
	})
	t.Run("TestOrderedIndexDeletePreventsReuse", func(t *testing.T) {
		testOrderedIndexDeletePreventsReuse(t, newBackend)
	})
	if len(counters) == 1 && counters[0] != nil {
		t.Run("TestOrderedIndexProviderCounters", func(t *testing.T) {
			counters[0].Assert(t, orderedIndexContext(t), freshOrderedIndex(t, newBackend))
		})
	}
}

// TestOrderedIndexRevisionConflicts runs only the CAS-conflict conformance
// case. It exists for the provider adapter that varies one narrow reporting
// choice the contract leaves open — whether *OrderedRevisionConflictError
// discloses ActualRevision or reports the zero sentinel — so verifying that
// branch costs one subtest instead of a second full suite run. It needs no
// cursor probe because it never lists.
func TestOrderedIndexRevisionConflicts(t *testing.T, newBackend OrderedIndexFactory) {
	if newBackend == nil {
		t.Fatal("TestOrderedIndexRevisionConflicts requires a non-nil factory")
	}
	t.Run("TestOrderedIndexWrongRevisionLeavesStateUntouched", func(t *testing.T) {
		testOrderedIndexWrongRevisionLeavesStateUntouched(t, newBackend)
	})
}

func testOrderedIndexCreateAssignsImmutableOrder(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	first := mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "first"), "workers", []byte("first"), storage.Rank{}, storage.Due{State: storage.NotDue})
	second := mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "second"), "workers", []byte("second"), storage.Rank{}, storage.Due{State: storage.NotDue})
	if first.Revision != 1 || second.Revision != 1 {
		t.Errorf("created revisions = (%d, %d), want (1, 1)", first.Revision, second.Revision)
	}
	// The contract promises a nonzero, strictly increasing order within an order
	// scope, never density: a provider allocating from a JetStream stream
	// sequence or a shared SQL sequence is conforming.
	if first.Order == 0 || second.Order <= first.Order {
		t.Errorf("created orders = (%d, %d), want nonzero and strictly increasing", first.Order, second.Order)
	}

	updated, err := index.Update(ctx, first.ID, first.Revision, []byte("first-updated"), storage.Rank{Ranked: true, Value: 4}, storage.Due{State: storage.DueAt, UnixMillis: 4})
	if err != nil {
		t.Fatalf("Update(%s): %v", orderedIndexIDLabel(first.ID), err)
	}
	if updated.Order != first.Order || updated.ID != first.ID || updated.RankingScope != first.RankingScope {
		t.Errorf("Update(%s) changed immutable fields: %s", orderedIndexIDLabel(first.ID), orderedIndexRecordSummary(updated))
	}

	otherScope := mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("other", "first"), "workers", []byte("other"), storage.Rank{}, storage.Due{State: storage.NotDue})
	// A fresh order scope gets its own stream, but its first order is not
	// required to restart at 1: a provider may serve every scope from one shared
	// sequence, so the only portable assertion is that the order is nonzero.
	if otherScope.Order == 0 {
		t.Errorf("other ordering scope first order = %d, want nonzero", otherScope.Order)
	}
}

func testOrderedIndexDuplicateReturnsOriginal(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	id := orderedIndexID("acceptance", "duplicate")
	input := []byte("original-value")
	first := mustCreateOrderedIndexRecord(t, ctx, index, id, "workers", input, storage.Rank{Ranked: true, Value: 8}, storage.Due{State: storage.DueAt, UnixMillis: 80})
	wantFirst := copyOrderedIndexRecord(first)

	input[0] = 'X'
	first.Value[0] = 'Y'
	duplicate, created, err := index.Create(ctx, id, "Workers", make([]byte, storage.MaxOrderedValueBytes+1), storage.Rank{}, storage.Due{State: storage.NotDue, UnixMillis: 1})
	if err != nil || created {
		t.Fatalf("duplicate Create(%s) = created %v, err %v; want false, nil", orderedIndexIDLabel(id), created, err)
	}
	requireOrderedIndexRecordEqual(t, "duplicate Create canonical record", duplicate, wantFirst)
	duplicate.Value[0] = 'Z'
	got := mustGetOrderedIndexRecord(t, ctx, index, id)
	requireOrderedIndexRecordEqual(t, "Get after caller mutations of Create input and outputs", got, wantFirst)
	got.Value[0] = 'Q'
	requireOrderedIndexRecordEqual(t, "Get after caller mutation of Get output", mustGetOrderedIndexRecord(t, ctx, index, id), wantFirst)

	invalidID := storage.OrderedID{Namespace: "Sessions", OrderingScope: "acceptance", StableKey: "duplicate"}
	_, created, err = index.Create(ctx, invalidID, "workers", []byte("value"), storage.Rank{}, storage.Due{State: storage.NotDue})
	var invalidName *storage.InvalidNameError
	if !errors.As(err, &invalidName) || created {
		t.Errorf("Create(invalid ID) = created %v, err %T %v; want false, *InvalidNameError", created, err, err)
	}
	for _, method := range []struct {
		name string
		call func() error
	}{
		{name: "Get", call: func() error { _, err := index.Get(ctx, invalidID); return err }},
		{name: "Update", call: func() error {
			_, err := index.Update(ctx, invalidID, 1, make([]byte, storage.MaxOrderedValueBytes+1), storage.Rank{}, storage.Due{State: storage.NotDue, UnixMillis: 1})
			return err
		}},
		{name: "Delete", call: func() error { _, err := index.Delete(ctx, invalidID, 1); return err }},
	} {
		err := method.call()
		if !errors.As(err, &invalidName) {
			t.Errorf("%s(invalid ID) = %T %v, want *InvalidNameError before lookup or candidate validation", method.name, err, err)
		}
	}

	for number, test := range []struct {
		name         string
		rankingScope string
		value        []byte
		due          storage.Due
		matches      func(error) bool
		want         string
	}{
		{
			name:         "invalid ranking scope",
			rankingScope: "Workers",
			value:        []byte("value"),
			due:          storage.Due{State: storage.NotDue},
			matches: func(err error) bool {
				var target *storage.InvalidNameError
				return errors.As(err, &target)
			},
			want: "*InvalidNameError",
		},
		{
			name:         "oversized value",
			rankingScope: "workers",
			value:        make([]byte, storage.MaxOrderedValueBytes+1),
			due:          storage.Due{State: storage.NotDue},
			matches: func(err error) bool {
				var target *storage.OrderedValueTooLargeError
				return errors.As(err, &target)
			},
			want: "*OrderedValueTooLargeError",
		},
		{
			name:         "noncanonical not due",
			rankingScope: "workers",
			value:        []byte("value"),
			due:          storage.Due{State: storage.NotDue, UnixMillis: 1},
			matches: func(err error) bool {
				var target *storage.InvalidDueError
				return errors.As(err, &target)
			},
			want: "*InvalidDueError",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidateID := orderedIndexID("acceptance", storage.StableKey(fmt.Sprintf("invalid-%d", number)))
			_, created, err := index.Create(ctx, candidateID, test.rankingScope, test.value, storage.Rank{}, test.due)
			if created || !test.matches(err) {
				t.Errorf("Create(absent %s) = created %v, err %T %v; want false, %s", orderedIndexIDLabel(candidateID), created, err, err, test.want)
			}
			_, getErr := index.Get(ctx, candidateID)
			var notFound *storage.OrderedRecordNotFoundError
			if !errors.As(getErr, &notFound) {
				t.Errorf("Get(%s) after rejected Create = %T %v, want *OrderedRecordNotFoundError", orderedIndexIDLabel(candidateID), getErr, getErr)
			}
		})
	}
}

func testOrderedIndexScopesIdentity(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	first := mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("scope-a", "shared"), "workers", []byte("a"), storage.Rank{}, storage.Due{State: storage.NotDue})
	second := mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("scope-b", "shared"), "workers", []byte("b"), storage.Rank{}, storage.Due{State: storage.NotDue})
	thirdID := storage.OrderedID{Namespace: "other", OrderingScope: "scope-a", StableKey: "shared"}
	third := mustCreateOrderedIndexRecord(t, ctx, index, thirdID, "workers", []byte("c"), storage.Rank{}, storage.Due{State: storage.NotDue})
	// Order values are not comparable across order scopes and are not required
	// to restart at 1 in each, so identity scoping is asserted through the
	// records themselves below; only nonzero order is portable here.
	if first.Order == 0 || second.Order == 0 || third.Order == 0 {
		t.Errorf("independent identity-scope orders = (%d, %d, %d), want all nonzero", first.Order, second.Order, third.Order)
	}
	for _, want := range []storage.OrderedRecord{first, second, third} {
		got := mustGetOrderedIndexRecord(t, ctx, index, want.ID)
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("Get(%s).Value = %q, want %q", orderedIndexIDLabel(want.ID), got.Value, want.Value)
		}
	}
}

func testOrderedIndexCASUpdateChangesValueRankAndDueAtomically(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	id := orderedIndexID("acceptance", "atomic")
	created := mustCreateOrderedIndexRecord(t, ctx, index, id, "workers", []byte("before"), storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.DueAt, UnixMillis: 10})
	updateInput := []byte("after")
	wantValue := append([]byte(nil), updateInput...)
	wantRank := storage.Rank{Ranked: true, Value: 99}
	wantDue := storage.Due{State: storage.DueAt, UnixMillis: -5}
	updated, err := index.Update(ctx, id, created.Revision, updateInput, wantRank, wantDue)
	if err != nil {
		t.Fatalf("Update(%s): %v", orderedIndexIDLabel(id), err)
	}
	wantUpdated := copyOrderedIndexRecord(created)
	wantUpdated.Revision++
	wantUpdated.Value = wantValue
	wantUpdated.Rank = wantRank
	wantUpdated.Due = wantDue
	updateInput[0] = 'X'
	requireOrderedIndexRecordEqual(t, "Update atomic result", updated, wantUpdated)
	got := mustGetOrderedIndexRecord(t, ctx, index, id)
	requireOrderedIndexRecordEqual(t, "Get after caller mutation of Update input", got, wantUpdated)

	tooLarge := make([]byte, storage.MaxOrderedValueBytes+1)
	_, err = index.Update(ctx, id, created.Revision, tooLarge, storage.Rank{}, storage.Due{State: storage.NotDue})
	var tooLargeErr *storage.OrderedValueTooLargeError
	if !errors.As(err, &tooLargeErr) {
		t.Errorf("Update(%s, stale+invalid candidate) = %T %v, want *OrderedValueTooLargeError before revision conflict", orderedIndexIDLabel(id), err, err)
	}
	requireOrderedIndexRecordEqual(t, "Get after rejected stale+invalid Update", mustGetOrderedIndexRecord(t, ctx, index, id), wantUpdated)

	updated.Value[0] = 'X'
	got = mustGetOrderedIndexRecord(t, ctx, index, id)
	requireOrderedIndexRecordEqual(t, "Get after caller mutation of Update output", got, wantUpdated)
	got.Value[0] = 'Y'
	requireOrderedIndexRecordEqual(t, "Get after caller mutation of Get output", mustGetOrderedIndexRecord(t, ctx, index, id), wantUpdated)
}

func testOrderedIndexWrongRevisionLeavesStateUntouched(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	id := orderedIndexID("acceptance", "revision")
	created := mustCreateOrderedIndexRecord(t, ctx, index, id, "workers", []byte("before"), storage.Rank{Ranked: true, Value: 7}, storage.Due{State: storage.DueAt, UnixMillis: 7})

	_, err := index.Update(ctx, id, created.Revision+1, []byte("after"), storage.Rank{Ranked: true, Value: 8}, storage.Due{State: storage.DueAt, UnixMillis: 8})
	requireOrderedRevisionConflict(t, "Update", err, id, created.Revision+1, created.Revision)
	requireOrderedIndexRecordEqual(t, "Get after stale Update", mustGetOrderedIndexRecord(t, ctx, index, id), created)

	_, err = index.Delete(ctx, id, created.Revision+1)
	requireOrderedRevisionConflict(t, "Delete", err, id, created.Revision+1, created.Revision)
	requireOrderedIndexRecordEqual(t, "Get after stale Delete", mustGetOrderedIndexRecord(t, ctx, index, id), created)

	absent := orderedIndexID("acceptance", "absent")
	_, err = index.Update(ctx, absent, 99, make([]byte, storage.MaxOrderedValueBytes+1), storage.Rank{}, storage.Due{State: storage.NotDue, UnixMillis: 1})
	var notFound *storage.OrderedRecordNotFoundError
	if !errors.As(err, &notFound) || notFound.ID != absent {
		t.Errorf("Update(absent %s) = %T %v, want *OrderedRecordNotFoundError before candidate validation", orderedIndexIDLabel(absent), err, err)
	}
	_, err = index.Delete(ctx, absent, 99)
	if !errors.As(err, &notFound) || notFound.ID != absent {
		t.Errorf("Delete(absent %s) = %T %v, want *OrderedRecordNotFoundError", orderedIndexIDLabel(absent), err, err)
	}
}

// testOrderedIndexRejectsInvalidListArguments covers the listing side of
// boundary validation. ValidateOrderedLimit and ValidateName are documented
// rules of all three List methods, and a provider that clamped limit 0 to a
// default, accepted a limit past MaxOrderedPageLimit, or let an ungrammatical
// namespace through to its backend would otherwise pass the entire suite.
func testOrderedIndexRejectsInvalidListArguments(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "listed"), "workers", []byte("listed"), storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.DueAt, UnixMillis: 1})

	// Each listing is reduced to the same shape — record count, continuation,
	// error — so one table can cover all three methods.
	listings := []struct {
		name string
		call func(namespace string, limit int) (int, bool, error)
	}{
		{name: "ListOrdered", call: func(namespace string, limit int) (int, bool, error) {
			page, err := index.ListOrdered(ctx, namespace, "acceptance", 0, limit)
			return len(page.Records), page.NextAfterOrder != 0, err
		}},
		{name: "ListRanked", call: func(namespace string, limit int) (int, bool, error) {
			page, err := index.ListRanked(ctx, namespace, "workers", "", limit)
			return len(page.Records), page.NextCursor != "", err
		}},
		{name: "ListDue", call: func(namespace string, limit int) (int, bool, error) {
			page, err := index.ListDue(ctx, namespace, 1, "", limit)
			return len(page.Records), page.NextCursor != "", err
		}},
	}
	limits := []struct {
		label string
		limit int
	}{
		{label: "zero", limit: 0},
		{label: "negative", limit: -1},
		{label: "above maximum", limit: storage.MaxOrderedPageLimit + 1},
	}
	for _, listing := range listings {
		for _, limit := range limits {
			t.Run(listing.name+" limit "+limit.label, func(t *testing.T) {
				records, hasNext, err := listing.call("sessions", limit.limit)
				var invalidLimit *storage.InvalidOrderedLimitError
				if !errors.As(err, &invalidLimit) {
					t.Fatalf("%s(limit=%d) = %T %v, want *InvalidOrderedLimitError", listing.name, limit.limit, err, err)
				}
				if invalidLimit.Limit != limit.limit {
					t.Errorf("%s(limit=%d) reported limit %d", listing.name, limit.limit, invalidLimit.Limit)
				}
				if records != 0 || hasNext {
					t.Errorf("%s(limit=%d) returned %d records with continuation %v, want a fail-closed empty page", listing.name, limit.limit, records, hasNext)
				}
			})
		}
		for _, name := range invalidNames {
			t.Run(listing.name+" namespace "+name.label, func(t *testing.T) {
				records, hasNext, err := listing.call(name.value, 1)
				var invalidName *storage.InvalidNameError
				if !errors.As(err, &invalidName) {
					t.Fatalf("%s(namespace=%q) = %T %v, want *InvalidNameError", listing.name, name.value, err, err)
				}
				if records != 0 || hasNext {
					t.Errorf("%s(namespace=%q) returned %d records with continuation %v, want a fail-closed empty page", listing.name, name.value, records, hasNext)
				}
			})
		}
	}

	// The scope arguments carry the same grammar as the namespace, and only
	// ListOrdered and ListRanked take one.
	for _, scope := range []struct {
		name string
		call func(scope string) (int, bool, error)
	}{
		{name: "ListOrdered orderingScope", call: func(scope string) (int, bool, error) {
			page, err := index.ListOrdered(ctx, "sessions", scope, 0, 1)
			return len(page.Records), page.NextAfterOrder != 0, err
		}},
		{name: "ListRanked rankingScope", call: func(scope string) (int, bool, error) {
			page, err := index.ListRanked(ctx, "sessions", scope, "", 1)
			return len(page.Records), page.NextCursor != "", err
		}},
	} {
		for _, name := range invalidNames {
			t.Run(scope.name+" "+name.label, func(t *testing.T) {
				records, hasNext, err := scope.call(name.value)
				var invalidName *storage.InvalidNameError
				if !errors.As(err, &invalidName) {
					t.Fatalf("%s(%q) = %T %v, want *InvalidNameError", scope.name, name.value, err, err)
				}
				if records != 0 || hasNext {
					t.Errorf("%s(%q) returned %d records with continuation %v, want a fail-closed empty page", scope.name, name.value, records, hasNext)
				}
			})
		}
	}
}

func testOrderedIndexListOrderedPagesInAcceptanceOrder(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	const limit = 2
	ids := []storage.OrderedID{
		orderedIndexID("acceptance", "first"),
		orderedIndexID("acceptance", "second"),
		orderedIndexID("acceptance", "third"),
		orderedIndexID("acceptance", "fourth"),
		orderedIndexID("acceptance", "fifth"),
	}
	for _, id := range ids {
		mustCreateOrderedIndexRecord(t, ctx, index, id, "workers", []byte(id.StableKey), storage.Rank{}, storage.Due{State: storage.NotDue})
	}
	middle := mustGetOrderedIndexRecord(t, ctx, index, ids[2])
	if _, err := index.Delete(ctx, middle.ID, middle.Revision); err != nil {
		t.Fatalf("Delete(%s): %v", orderedIndexIDLabel(middle.ID), err)
	}

	var all []storage.OrderedRecord
	after := uint64(0)
	for pageNumber := 0; pageNumber < 4; pageNumber++ {
		page, err := index.ListOrdered(ctx, "sessions", "acceptance", after, limit)
		if err != nil {
			t.Fatalf("ListOrdered(after=%d): %v", after, err)
		}
		if len(page.Records) > limit {
			t.Errorf("ListOrdered(after=%d, limit=%d) returned %d records, want no more than limit", after, limit, len(page.Records))
		}
		if len(page.Records) == 0 {
			if page.NextAfterOrder != 0 {
				t.Errorf("empty ListOrdered page next order = %d, want 0", page.NextAfterOrder)
			}
			break
		}
		if page.NextAfterOrder != page.Records[len(page.Records)-1].Order {
			t.Errorf("ListOrdered(after=%d) next order = %d, want final page order %d", after, page.NextAfterOrder, page.Records[len(page.Records)-1].Order)
		}
		for _, record := range page.Records {
			if record.Order <= after {
				t.Errorf("ListOrdered(after=%d) returned nonexclusive order %d for %s", after, record.Order, orderedIndexIDLabel(record.ID))
			}
		}
		all = append(all, page.Records...)
		after = page.NextAfterOrder
	}
	if len(all) != len(ids) {
		t.Fatalf("ListOrdered all pages returned %d records, want %d", len(all), len(ids))
	}
	if got, want := orderedIndexRecordLabels(all), []string{"acceptance/first", "acceptance/second", "acceptance/third", "acceptance/fourth", "acceptance/fifth"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListOrdered labels = %v, want %v", got, want)
	}
	requireStrictlyIncreasingOrders(t, "ListOrdered acceptance stream", all)
	if !all[2].Deleted {
		t.Errorf("ListOrdered terminal record %s omitted its tombstone state", orderedIndexIDLabel(all[2].ID))
	}
}

func testOrderedIndexListRankedPagesByRankAndStableKey(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "high"), "workers", []byte("high"), storage.Rank{Ranked: true, Value: 20}, storage.Due{State: storage.NotDue})
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "zeta"), "workers", []byte("zeta"), storage.Rank{Ranked: true, Value: 10}, storage.Due{State: storage.NotDue})
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("a", "same"), "workers", []byte("tie-a"), storage.Rank{Ranked: true, Value: 10}, storage.Due{State: storage.NotDue})
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("b", "same"), "workers", []byte("tie-b"), storage.Rank{Ranked: true, Value: 10}, storage.Due{State: storage.NotDue})
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "alpha"), "workers", []byte("alpha"), storage.Rank{Ranked: true, Value: 10}, storage.Due{State: storage.NotDue})
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("c", "low"), "workers", []byte("low"), storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.NotDue})
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "unranked"), "workers", []byte("unranked"), storage.Rank{}, storage.Due{State: storage.NotDue})

	first, err := index.ListRanked(ctx, "sessions", "workers", "", 3)
	if err != nil {
		t.Fatalf("ListRanked(first page): %v", err)
	}
	if got, want := orderedIndexRecordLabels(first.Records), []string{"acceptance/high", "acceptance/zeta", "b/same"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked(first page) = %v, want %v", got, want)
	}
	if first.NextCursor == "" {
		t.Fatal("ListRanked(first page) returned an empty cursor before exhaustion")
	}
	second, err := index.ListRanked(ctx, "sessions", "workers", first.NextCursor, 3)
	if err != nil {
		t.Fatalf("ListRanked(second page): %v", err)
	}
	if got, want := orderedIndexRecordLabels(second.Records), []string{"a/same", "acceptance/alpha", "c/low"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked(second page) = %v, want %v", got, want)
	}
	if second.NextCursor != "" {
		t.Errorf("ListRanked(final page) next cursor = %q, want empty", second.NextCursor)
	}
}

func testOrderedIndexRankMoveUpdatesAuthoritativePage(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	high := mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "high"), "workers", []byte("high"), storage.Rank{Ranked: true, Value: 20}, storage.Due{State: storage.NotDue})
	low := mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "low"), "workers", []byte("low"), storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.NotDue})

	moved, err := index.Update(ctx, low.ID, low.Revision, []byte("low-moved"), storage.Rank{Ranked: true, Value: 30}, storage.Due{State: storage.NotDue})
	if err != nil {
		t.Fatalf("Update(rank move %s): %v", orderedIndexIDLabel(low.ID), err)
	}
	page, err := index.ListRanked(ctx, "sessions", "workers", "", 10)
	if err != nil {
		t.Fatalf("ListRanked(after move): %v", err)
	}
	if got, want := orderedIndexRecordLabels(page.Records), []string{"acceptance/low", "acceptance/high"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked(after move) = %v, want %v", got, want)
	}
	if len(page.Records) != 2 || page.Records[0].Revision != moved.Revision || !bytes.Equal(page.Records[0].Value, []byte("low-moved")) || page.Records[1].ID != high.ID {
		t.Errorf("ListRanked(after move) did not use current authoritative records: %v", orderedIndexRecordSummaries(page.Records))
	}
}

func testOrderedIndexListDuePagesByDeadlineAndStableKey(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "past"), "workers", []byte("past"), storage.Rank{}, storage.Due{State: storage.DueAt, UnixMillis: 5})
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "alpha"), "workers", []byte("alpha"), storage.Rank{}, storage.Due{State: storage.DueAt, UnixMillis: 10})
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("b", "same"), "workers", []byte("tie-b"), storage.Rank{}, storage.Due{State: storage.DueAt, UnixMillis: 10})
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("a", "same"), "workers", []byte("tie-a"), storage.Rank{}, storage.Due{State: storage.DueAt, UnixMillis: 10})
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "zeta"), "workers", []byte("zeta"), storage.Rank{}, storage.Due{State: storage.DueAt, UnixMillis: 10})
	later := mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "later"), "workers", []byte("later"), storage.Rank{}, storage.Due{State: storage.DueAt, UnixMillis: 20})
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "not-due"), "workers", []byte("not-due"), storage.Rank{}, storage.Due{State: storage.NotDue})

	first, err := index.ListDue(ctx, "sessions", 10, "", 2)
	if err != nil {
		t.Fatalf("ListDue(first page): %v", err)
	}
	if got, want := orderedIndexRecordLabels(first.Records), []string{"acceptance/past", "acceptance/alpha"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(first page) = %v, want %v", got, want)
	}
	if first.NextCursor == "" {
		t.Fatal("ListDue(first page) returned an empty cursor before exhaustion")
	}
	second, err := index.ListDue(ctx, "sessions", 10, first.NextCursor, 2)
	if err != nil {
		t.Fatalf("ListDue(second page): %v", err)
	}
	if got, want := orderedIndexRecordLabels(second.Records), []string{"a/same", "b/same"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(second page) = %v, want %v", got, want)
	}
	if second.NextCursor == "" {
		t.Fatal("ListDue(second page) returned an empty cursor before zeta")
	}
	third, err := index.ListDue(ctx, "sessions", 10, second.NextCursor, 2)
	if err != nil {
		t.Fatalf("ListDue(third page): %v", err)
	}
	if got, want := orderedIndexRecordLabels(third.Records), []string{"acceptance/zeta"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(third page) = %v, want %v", got, want)
	}
	if third.NextCursor != "" {
		t.Errorf("ListDue(final page) next cursor = %q, want empty", third.NextCursor)
	}

	moved, err := index.Update(ctx, later.ID, later.Revision, []byte("later-moved"), storage.Rank{}, storage.Due{State: storage.DueAt, UnixMillis: 1})
	if err != nil {
		t.Fatalf("Update(due move %s): %v", orderedIndexIDLabel(later.ID), err)
	}
	afterMove, err := index.ListDue(ctx, "sessions", 100, "", 10)
	if err != nil {
		t.Fatalf("ListDue(after due move): %v", err)
	}
	if got, want := orderedIndexRecordLabels(afterMove.Records), []string{"acceptance/later", "acceptance/past", "acceptance/alpha", "a/same", "b/same", "acceptance/zeta"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(after due move) = %v, want %v", got, want)
	}
	if len(afterMove.Records) == 0 || afterMove.Records[0].Revision != moved.Revision || !bytes.Equal(afterMove.Records[0].Value, []byte("later-moved")) {
		t.Errorf("ListDue(after due move) did not return the moved current record: %v", orderedIndexRecordSummaries(afterMove.Records))
	}
}

func testOrderedIndexNotDueNeverAppearsInDuePages(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	due := mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "due"), "workers", []byte("due"), storage.Rank{}, storage.Due{State: storage.DueAt, UnixMillis: 1})
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "never-due"), "workers", []byte("never"), storage.Rank{}, storage.Due{State: storage.NotDue})

	updated, err := index.Update(ctx, due.ID, due.Revision, []byte("not-due"), storage.Rank{}, storage.Due{State: storage.NotDue})
	if err != nil {
		t.Fatalf("Update(not due %s): %v", orderedIndexIDLabel(due.ID), err)
	}
	if updated.Due != (storage.Due{State: storage.NotDue}) {
		t.Errorf("Update(not due %s).Due = %#v, want canonical NotDue", orderedIndexIDLabel(due.ID), updated.Due)
	}
	page, err := index.ListDue(ctx, "sessions", 100, "", 10)
	if err != nil {
		t.Fatalf("ListDue(after not-due move): %v", err)
	}
	if len(page.Records) != 0 || page.NextCursor != "" {
		t.Errorf("ListDue(after not-due move) = %#v, want empty page", page)
	}
}

func testOrderedIndexTerminalRecordsDoNotFillPages(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	deleteInput := []byte("terminal")
	target := mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "tombstone"), "workers", deleteInput, storage.Rank{Ranked: true, Value: 100}, storage.Due{State: storage.DueAt, UnixMillis: 1})
	wantLive := copyOrderedIndexRecord(target)
	deleteInput[0] = 'X'
	target.Value[0] = 'Y'
	liveGet := mustGetOrderedIndexRecord(t, ctx, index, target.ID)
	requireOrderedIndexRecordEqual(t, "Get after caller mutations of Create input and output", liveGet, wantLive)
	liveGet.Value[0] = 'Z'
	requireOrderedIndexRecordEqual(t, "Get after caller mutation of live Get output", mustGetOrderedIndexRecord(t, ctx, index, target.ID), wantLive)
	firstLive := mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "first-live"), "workers", []byte("first"), storage.Rank{Ranked: true, Value: 20}, storage.Due{State: storage.DueAt, UnixMillis: 2})
	mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "second-live"), "workers", []byte("second"), storage.Rank{Ranked: true, Value: 10}, storage.Due{State: storage.DueAt, UnixMillis: 3})

	deleted, err := index.Delete(ctx, target.ID, target.Revision)
	if err != nil {
		t.Fatalf("Delete(%s): %v", orderedIndexIDLabel(target.ID), err)
	}
	wantDeleted := copyOrderedIndexRecord(wantLive)
	wantDeleted.Revision++
	wantDeleted.Deleted = true
	wantDeleted.Rank = storage.Rank{}
	wantDeleted.Due = storage.Due{State: storage.NotDue}
	requireOrderedIndexRecordEqual(t, "Delete canonical tombstone", deleted, wantDeleted)
	deleted.Value[0] = 'D'
	deletedGet := mustGetOrderedIndexRecord(t, ctx, index, target.ID)
	requireOrderedIndexRecordEqual(t, "Get after caller mutation of Delete output", deletedGet, wantDeleted)
	deletedGet.Value[0] = 'G'
	requireOrderedIndexRecordEqual(t, "Get after caller mutation of tombstone Get output", mustGetOrderedIndexRecord(t, ctx, index, target.ID), wantDeleted)
	retry, err := index.Delete(ctx, target.ID, target.Revision)
	if err != nil {
		t.Fatalf("Delete retry(%s): %v", orderedIndexIDLabel(target.ID), err)
	}
	requireOrderedIndexRecordEqual(t, "Delete retry", retry, wantDeleted)
	_, err = index.Update(ctx, target.ID, 0, make([]byte, storage.MaxOrderedValueBytes+1), storage.Rank{}, storage.Due{State: storage.NotDue, UnixMillis: 1})
	var deletedErr *storage.OrderedDeletedError
	if !errors.As(err, &deletedErr) || deletedErr.ID != target.ID {
		t.Errorf("Update(tombstone %s) = %T %v, want *OrderedDeletedError before candidate validation", orderedIndexIDLabel(target.ID), err, err)
	}

	ranked, err := index.ListRanked(ctx, "sessions", "workers", "", 1)
	if err != nil {
		t.Fatalf("ListRanked(after Delete): %v", err)
	}
	if got, want := orderedIndexRecordLabels(ranked.Records), []string{"acceptance/first-live"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked(after Delete, limit 1) = %v, want %v", got, want)
	}
	due, err := index.ListDue(ctx, "sessions", 10, "", 1)
	if err != nil {
		t.Fatalf("ListDue(after Delete): %v", err)
	}
	if got, want := orderedIndexRecordLabels(due.Records), []string{"acceptance/first-live"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(after Delete, limit 1) = %v, want %v", got, want)
	}
	if len(ranked.Records) == 1 && ranked.Records[0].ID != firstLive.ID {
		t.Errorf("ListRanked(after Delete) returned %s, want %s", orderedIndexIDLabel(ranked.Records[0].ID), orderedIndexIDLabel(firstLive.ID))
	}
}

func testOrderedIndexConcurrentDuplicateHasOneWinner(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	const writers = 100
	id := orderedIndexID("acceptance", "contended")
	type result struct {
		record  storage.OrderedRecord
		created bool
		err     error
	}
	results := make(chan result, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			record, created, err := index.Create(ctx, id, "workers", []byte(fmt.Sprintf("writer-%03d", i)), storage.Rank{}, storage.Due{State: storage.NotDue})
			results <- result{record: record, created: created, err: err}
		}(i)
	}
	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("concurrent duplicate creates did not finish before deadline: %v", ctx.Err())
	}
	close(results)

	var canonical storage.OrderedRecord
	winners := 0
	allResults := make([]result, 0, writers)
	for result := range results {
		if result.err != nil {
			t.Errorf("concurrent Create(%s) error: %v", orderedIndexIDLabel(id), result.err)
			continue
		}
		allResults = append(allResults, result)
		if result.created {
			winners++
			canonical = result.record
		}
	}
	if winners != 1 {
		t.Fatalf("concurrent duplicate Create(%s) winners = %d, want 1", orderedIndexIDLabel(id), winners)
	}
	if canonical.Order == 0 || canonical.Revision != 1 {
		t.Errorf("concurrent duplicate winner %s has order/revision (%d, %d), want nonzero order and revision 1", orderedIndexIDLabel(id), canonical.Order, canonical.Revision)
	}
	for _, result := range allResults {
		requireOrderedIndexRecordEqual(t, "concurrent duplicate canonical result", result.record, canonical)
	}
}

func testOrderedIndexConcurrentDistinctCreatesAreMonotonic(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	const writers = 100
	type result struct {
		attemptedID storage.OrderedID
		record      storage.OrderedRecord
		created     bool
		err         error
	}
	results := make(chan result, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			id := orderedIndexID("acceptance", storage.StableKey(fmt.Sprintf("key-%03d", i)))
			record, created, err := index.Create(ctx, id, "workers", []byte("value"), storage.Rank{}, storage.Due{State: storage.NotDue})
			results <- result{attemptedID: id, record: record, created: created, err: err}
		}(i)
	}
	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("concurrent distinct creates did not finish before deadline: %v", ctx.Err())
	}
	close(results)

	orders := make([]uint64, 0, writers)
	for result := range results {
		if result.err != nil || !result.created {
			t.Errorf("concurrent distinct Create(%s) = created %v, err %v; want true, nil", orderedIndexIDLabel(result.attemptedID), result.created, result.err)
			continue
		}
		if result.record.ID != result.attemptedID {
			t.Errorf("concurrent distinct Create(%s) returned record for %s", orderedIndexIDLabel(result.attemptedID), orderedIndexIDLabel(result.record.ID))
			continue
		}
		orders = append(orders, result.record.Order)
	}
	if len(orders) != writers {
		t.Fatalf("concurrent distinct creates completed %d records, want %d", len(orders), writers)
	}
	// Sorting first turns "every concurrent create got its own order" into a
	// strict-increase check that holds for sparse allocation too: any repeated
	// or zero order survives the sort and fails here.
	sort.Slice(orders, func(i int, j int) bool { return orders[i] < orders[j] })
	for i, order := range orders {
		if order == 0 {
			t.Errorf("concurrent distinct order[%d] = 0, want nonzero", i)
			continue
		}
		if i > 0 && order <= orders[i-1] {
			t.Errorf("concurrent distinct order[%d] = %d, want strictly greater than the preceding %d", i, order, orders[i-1])
		}
	}
}

func testOrderedIndexOpaqueStableKeys(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	keys := []storage.StableKey{
		"v1:ABC_def-09",
		"slash/value",
		"UPPERCASE",
		"snowman-☃/世界",
		storage.StableKey(strings.Repeat("k", storage.MaxStableKeyBytes)),
	}
	for _, key := range keys {
		id := orderedIndexID("opaque", key)
		created := mustCreateOrderedIndexRecord(t, ctx, index, id, "workers", []byte("value:"+string(key)), storage.Rank{}, storage.Due{State: storage.NotDue})
		got := mustGetOrderedIndexRecord(t, ctx, index, id)
		requireOrderedIndexRecordEqual(t, "opaque stable key round trip", got, created)
	}
	page, err := index.ListOrdered(ctx, "sessions", "opaque", 0, len(keys))
	if err != nil {
		t.Fatalf("ListOrdered(opaque stable keys): %v", err)
	}
	gotKeys := make([]storage.StableKey, len(page.Records))
	for i, record := range page.Records {
		gotKeys[i] = record.ID.StableKey
	}
	if !reflect.DeepEqual(gotKeys, keys) {
		t.Errorf("ListOrdered(opaque stable keys) = %q, want creation-order keys %q", gotKeys, keys)
	}
	for _, key := range []storage.StableKey{"", storage.StableKey(strings.Repeat("k", storage.MaxStableKeyBytes+1)), storage.StableKey(string([]byte{0xff}))} {
		id := orderedIndexID("opaque", key)
		_, created, err := index.Create(ctx, id, "workers", []byte("value"), storage.Rank{}, storage.Due{State: storage.NotDue})
		var invalid *storage.InvalidStableKeyError
		if !errors.As(err, &invalid) || created {
			t.Errorf("Create(invalid stable key length %d) = created %v, err %T %v; want false, *InvalidStableKeyError", len(key), created, err, err)
		}
	}
}

func testOrderedIndexInvalidCursorFailsClosed(t *testing.T, newBackend OrderedIndexFactory, probe OrderedCursorProbe) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	for _, key := range []storage.StableKey{"first", "second", "third"} {
		mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", key), "workers", []byte(key), storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.DueAt, UnixMillis: 1})
	}

	ranked, err := index.ListRanked(ctx, "sessions", "workers", "", 1)
	if err != nil || ranked.NextCursor == "" {
		t.Fatalf("ListRanked(first page) = %d records, continuation %v, err %v; want continuation cursor", len(ranked.Records), ranked.NextCursor != "", err)
	}
	due, err := index.ListDue(ctx, "sessions", 1, "", 1)
	if err != nil || due.NextCursor == "" {
		t.Fatalf("ListDue(first page) = %d records, continuation %v, err %v; want continuation cursor", len(due.Records), due.NextCursor != "", err)
	}

	// Query mismatch: a real cursor replayed against another ranking scope,
	// namespace, or due bound.
	requireInvalidOrderedCursor(t, func() (int, bool, error) {
		page, err := index.ListRanked(ctx, "sessions", "other", ranked.NextCursor, 1)
		return len(page.Records), page.NextCursor != "", err
	}, storage.RankedCursorKind, storage.OrderedCursorQueryMismatch, string(ranked.NextCursor))
	requireInvalidOrderedCursor(t, func() (int, bool, error) {
		page, err := index.ListRanked(ctx, "other", "workers", ranked.NextCursor, 1)
		return len(page.Records), page.NextCursor != "", err
	}, storage.RankedCursorKind, storage.OrderedCursorQueryMismatch, string(ranked.NextCursor))
	requireInvalidOrderedCursor(t, func() (int, bool, error) {
		page, err := index.ListDue(ctx, "sessions", 2, due.NextCursor, 1)
		return len(page.Records), page.NextCursor != "", err
	}, storage.DueCursorKind, storage.OrderedCursorQueryMismatch, string(due.NextCursor))
	requireInvalidOrderedCursor(t, func() (int, bool, error) {
		page, err := index.ListDue(ctx, "other", 1, due.NextCursor, 1)
		return len(page.Records), page.NextCursor != "", err
	}, storage.DueCursorKind, storage.OrderedCursorQueryMismatch, string(due.NextCursor))

	// Wrong kind: a genuine cursor of the other listing family.
	requireInvalidOrderedCursor(t, func() (int, bool, error) {
		page, err := index.ListRanked(ctx, "sessions", "workers", storage.RankedCursor(due.NextCursor), 1)
		return len(page.Records), page.NextCursor != "", err
	}, storage.RankedCursorKind, storage.OrderedCursorWrongKind, string(due.NextCursor))
	requireInvalidOrderedCursor(t, func() (int, bool, error) {
		page, err := index.ListDue(ctx, "sessions", 1, storage.DueCursor(ranked.NextCursor), 1)
		return len(page.Records), page.NextCursor != "", err
	}, storage.DueCursorKind, storage.OrderedCursorWrongKind, string(ranked.NextCursor))

	// Malformed and unknown-version tokens are provider-supplied: their shape
	// is part of the provider's opaque encoding, not of this contract.
	malformedRanked := requireProbeCursor(t, probe.MalformedCursor(t, storage.RankedCursorKind), "MalformedCursor", storage.RankedCursorKind)
	requireInvalidOrderedCursor(t, func() (int, bool, error) {
		page, err := index.ListRanked(ctx, "sessions", "workers", storage.RankedCursor(malformedRanked), 1)
		return len(page.Records), page.NextCursor != "", err
	}, storage.RankedCursorKind, storage.OrderedCursorMalformed, malformedRanked)
	malformedDue := requireProbeCursor(t, probe.MalformedCursor(t, storage.DueCursorKind), "MalformedCursor", storage.DueCursorKind)
	requireInvalidOrderedCursor(t, func() (int, bool, error) {
		page, err := index.ListDue(ctx, "sessions", 1, storage.DueCursor(malformedDue), 1)
		return len(page.Records), page.NextCursor != "", err
	}, storage.DueCursorKind, storage.OrderedCursorMalformed, malformedDue)

	unknownRanked := requireProbeCursor(t, probe.UnknownVersionCursor(t, storage.RankedCursorKind), "UnknownVersionCursor", storage.RankedCursorKind)
	requireInvalidOrderedCursor(t, func() (int, bool, error) {
		page, err := index.ListRanked(ctx, "sessions", "workers", storage.RankedCursor(unknownRanked), 1)
		return len(page.Records), page.NextCursor != "", err
	}, storage.RankedCursorKind, storage.OrderedCursorUnknownVersion, unknownRanked)
	unknownDue := requireProbeCursor(t, probe.UnknownVersionCursor(t, storage.DueCursorKind), "UnknownVersionCursor", storage.DueCursorKind)
	requireInvalidOrderedCursor(t, func() (int, bool, error) {
		page, err := index.ListDue(ctx, "sessions", 1, storage.DueCursor(unknownDue), 1)
		return len(page.Records), page.NextCursor != "", err
	}, storage.DueCursorKind, storage.OrderedCursorUnknownVersion, unknownDue)
}

// requireProbeCursor rejects a probe that returns no token: an empty cursor
// means "start from the beginning" and would make its fail-closed case vacuous.
func requireProbeCursor(t *testing.T, cursor string, method string, kind storage.OrderedCursorKind) string {
	t.Helper()
	if cursor == "" {
		t.Fatalf("OrderedCursorProbe.%s(%s) returned an empty token, which the contract reads as an absent cursor", method, kind)
	}
	return cursor
}

func testOrderedIndexLargeValueRoundTrip(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	id := orderedIndexID("acceptance", "large")
	input := patternedBytes(storage.MaxOrderedValueBytes)
	want := append([]byte(nil), input...)
	created := mustCreateOrderedIndexRecord(t, ctx, index, id, "workers", input, storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.DueAt, UnixMillis: 1})
	input[0] ^= 0xff
	created.Value[1] ^= 0xff
	if got := mustGetOrderedIndexRecord(t, ctx, index, id); !bytes.Equal(got.Value, want) {
		t.Errorf("Get(%s) did not preserve a 1 MiB caller-owned value", orderedIndexIDLabel(id))
	}

	ordered, err := index.ListOrdered(ctx, "sessions", "acceptance", 0, 1)
	if err != nil || len(ordered.Records) != 1 {
		t.Fatalf("ListOrdered(large) = %d records, next order %d, err %v; want one record, nil", len(ordered.Records), ordered.NextAfterOrder, err)
	}
	ranked, err := index.ListRanked(ctx, "sessions", "workers", "", 1)
	if err != nil || len(ranked.Records) != 1 {
		t.Fatalf("ListRanked(large) = %d records, continuation %v, err %v; want one record, nil", len(ranked.Records), ranked.NextCursor != "", err)
	}
	due, err := index.ListDue(ctx, "sessions", 1, "", 1)
	if err != nil || len(due.Records) != 1 {
		t.Fatalf("ListDue(large) = %d records, continuation %v, err %v; want one record, nil", len(due.Records), due.NextCursor != "", err)
	}
	ordered.Records[0].Value[2] ^= 0xff
	ranked.Records[0].Value[3] ^= 0xff
	due.Records[0].Value[4] ^= 0xff
	if got := mustGetOrderedIndexRecord(t, ctx, index, id); !bytes.Equal(got.Value, want) {
		t.Errorf("listing snapshots alias stored 1 MiB value for %s", orderedIndexIDLabel(id))
	}
}

func testOrderedIndexDeletePreventsReuse(t *testing.T, newBackend OrderedIndexFactory) {
	ctx := orderedIndexContext(t)
	index := freshOrderedIndex(t, newBackend)
	id := orderedIndexID("acceptance", "terminal")
	created := mustCreateOrderedIndexRecord(t, ctx, index, id, "workers", []byte("original"), storage.Rank{}, storage.Due{State: storage.NotDue})
	deleted, err := index.Delete(ctx, id, created.Revision)
	if err != nil {
		t.Fatalf("Delete(%s): %v", orderedIndexIDLabel(id), err)
	}
	duplicate, createdAgain, err := index.Create(ctx, id, "Workers", make([]byte, storage.MaxOrderedValueBytes+1), storage.Rank{}, storage.Due{State: storage.NotDue, UnixMillis: 1})
	if err != nil || createdAgain {
		t.Fatalf("Create(tombstone %s) = created %v, err %v; want false, nil", orderedIndexIDLabel(id), createdAgain, err)
	}
	requireOrderedIndexRecordEqual(t, "Create tombstone retry", duplicate, deleted)
	requireOrderedIndexRecordEqual(t, "Get tombstone", mustGetOrderedIndexRecord(t, ctx, index, id), deleted)

	next := mustCreateOrderedIndexRecord(t, ctx, index, orderedIndexID("acceptance", "next"), "workers", []byte("next"), storage.Rank{}, storage.Due{State: storage.NotDue})
	// The tombstone keeps its order forever, so the next create must advance
	// past it. How far it advances is the provider's business.
	if next.Order <= created.Order {
		t.Errorf("Create(after tombstone) order = %d, want strictly greater than the tombstone order %d without reuse", next.Order, created.Order)
	}
}

func orderedIndexContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), conformanceTimeout)
	t.Cleanup(cancel)
	return ctx
}

func freshOrderedIndex(t *testing.T, newBackend OrderedIndexFactory) storage.OrderedIndex {
	t.Helper()
	index := newBackend(t)
	if index == nil {
		t.Fatal("ordered index factory returned nil")
	}
	return index
}

func orderedIndexID(orderingScope string, stableKey storage.StableKey) storage.OrderedID {
	return storage.OrderedID{Namespace: "sessions", OrderingScope: orderingScope, StableKey: stableKey}
}

func mustCreateOrderedIndexRecord(t *testing.T, ctx context.Context, index storage.OrderedIndex, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) storage.OrderedRecord {
	t.Helper()
	record, created, err := index.Create(ctx, id, rankingScope, value, rank, due)
	if err != nil || !created {
		t.Fatalf("Create(%s) = %s, created %v, err %v; want record, true, nil", orderedIndexIDLabel(id), orderedIndexRecordSummary(record), created, err)
	}
	return record
}

func mustGetOrderedIndexRecord(t *testing.T, ctx context.Context, index storage.OrderedIndex, id storage.OrderedID) storage.OrderedRecord {
	t.Helper()
	record, err := index.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get(%s): %v", orderedIndexIDLabel(id), err)
	}
	return record
}

func requireOrderedRevisionConflict(t *testing.T, operation string, err error, id storage.OrderedID, expectedRevision uint64, currentRevision uint64) {
	t.Helper()
	var conflict *storage.OrderedRevisionConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("%s(%s, stale revision) = %T %v, want *OrderedRevisionConflictError", operation, orderedIndexIDLabel(id), err, err)
	}
	if conflict.ID != id || conflict.ExpectedRevision != expectedRevision {
		t.Errorf("%s(%s, stale revision) conflict = %#v, want ID %s and expected revision %d", operation, orderedIndexIDLabel(id), conflict, orderedIndexIDLabel(id), expectedRevision)
	}
	if conflict.ActualRevision != 0 && conflict.ActualRevision != currentRevision {
		t.Errorf("%s(%s, stale revision) actual revision = %d, want undisclosed 0 or current revision %d", operation, orderedIndexIDLabel(id), conflict.ActualRevision, currentRevision)
	}
}

// requireOrderedIndexRecordEqual is the suite's most-used assertion, and its
// audience is a provider author debugging their own backend. Nearly every call
// compares a record with its own expected form, so naming the two IDs says
// nothing: the message reports which field diverged, then both full summaries
// for context.
func requireOrderedIndexRecordEqual(t *testing.T, label string, got storage.OrderedRecord, want storage.OrderedRecord) {
	t.Helper()
	if reflect.DeepEqual(got, want) {
		return
	}
	difference := orderedIndexRecordDifference(got, want)
	if difference == "" {
		// DeepEqual saw a divergence the field walk did not, which means the
		// record grew a field the walk has not been taught about.
		difference = "records are not deeply equal but no field difference was identified; orderedIndexRecordDifference is out of date with storage.OrderedRecord"
	}
	t.Errorf("%s: %s\n  got:  %s\n  want: %s", label, difference, orderedIndexRecordSummary(got), orderedIndexRecordSummary(want))
}

// orderedIndexRecordDifference describes how got diverges from want, field by
// field, and returns "" when they match. Value gets byte-level treatment
// because the record summary prints only its length, which is exactly the
// divergence a copy-in/copy-out aliasing bug produces.
func orderedIndexRecordDifference(got storage.OrderedRecord, want storage.OrderedRecord) string {
	var differences []string
	if got.ID != want.ID {
		differences = append(differences, fmt.Sprintf("ID %s != %s", orderedIndexIDLabel(got.ID), orderedIndexIDLabel(want.ID)))
	}
	if got.RankingScope != want.RankingScope {
		differences = append(differences, fmt.Sprintf("RankingScope %q != %q", got.RankingScope, want.RankingScope))
	}
	if got.Revision != want.Revision {
		differences = append(differences, fmt.Sprintf("Revision %d != %d", got.Revision, want.Revision))
	}
	if got.Order != want.Order {
		differences = append(differences, fmt.Sprintf("Order %d != %d", got.Order, want.Order))
	}
	if got.Due != want.Due {
		differences = append(differences, fmt.Sprintf("Due %d/%d != %d/%d", got.Due.State, got.Due.UnixMillis, want.Due.State, want.Due.UnixMillis))
	}
	if got.Rank != want.Rank {
		differences = append(differences, fmt.Sprintf("Rank %t/%d != %t/%d", got.Rank.Ranked, got.Rank.Value, want.Rank.Ranked, want.Rank.Value))
	}
	if got.Deleted != want.Deleted {
		differences = append(differences, fmt.Sprintf("Deleted %t != %t", got.Deleted, want.Deleted))
	}
	if value := orderedIndexValueDifference(got.Value, want.Value); value != "" {
		differences = append(differences, value)
	}
	return strings.Join(differences, "; ")
}

func orderedIndexValueDifference(got []byte, want []byte) string {
	if bytes.Equal(got, want) {
		// reflect.DeepEqual, which gates this report, separates a nil Value from
		// an empty one; bytes.Equal does not. Name that case rather than let it
		// fall through as an unidentified divergence.
		if (got == nil) != (want == nil) {
			return fmt.Sprintf("Value nil %t != nil %t (both empty)", got == nil, want == nil)
		}
		return ""
	}
	for i := 0; i < len(got) && i < len(want); i++ {
		if got[i] != want[i] {
			return fmt.Sprintf("Value differs at byte %d: %#x != %#x (lengths %d and %d)", i, got[i], want[i], len(got), len(want))
		}
	}
	return fmt.Sprintf("Value length %d != %d with a common prefix", len(got), len(want))
}

// requireStrictlyIncreasingOrders asserts the whole of what the contract says
// about a sequence of orders read out of one order scope in acceptance order:
// each is nonzero and strictly greater than its predecessor. It deliberately
// does not assert density — orders may be sparse and need not start at 1.
func requireStrictlyIncreasingOrders(t *testing.T, label string, records []storage.OrderedRecord) {
	t.Helper()
	for i, record := range records {
		if record.Order == 0 {
			t.Errorf("%s record %s order = 0, want nonzero", label, orderedIndexIDLabel(record.ID))
			continue
		}
		if i > 0 && record.Order <= records[i-1].Order {
			t.Errorf("%s record %s order = %d, want strictly greater than the preceding %s order %d", label, orderedIndexIDLabel(record.ID), record.Order, orderedIndexIDLabel(records[i-1].ID), records[i-1].Order)
		}
	}
}

func copyOrderedIndexRecord(record storage.OrderedRecord) storage.OrderedRecord {
	record.Value = append([]byte(nil), record.Value...)
	return record
}

func orderedIndexRecordLabels(records []storage.OrderedRecord) []string {
	labels := make([]string, len(records))
	for i, record := range records {
		labels[i] = record.ID.OrderingScope + "/" + string(record.ID.StableKey)
	}
	return labels
}

func orderedIndexRecordSummaries(records []storage.OrderedRecord) []string {
	summaries := make([]string, len(records))
	for i, record := range records {
		summaries[i] = orderedIndexRecordSummary(record)
	}
	return summaries
}

func orderedIndexRecordSummary(record storage.OrderedRecord) string {
	return fmt.Sprintf("%s rev=%d order=%d rank=%t/%d due=%d/%d deleted=%t valueBytes=%d", orderedIndexIDLabel(record.ID), record.Revision, record.Order, record.Rank.Ranked, record.Rank.Value, record.Due.State, record.Due.UnixMillis, record.Deleted, len(record.Value))
}

func orderedIndexIDLabel(id storage.OrderedID) string {
	return id.Namespace + "/" + id.OrderingScope + "/" + string(id.StableKey)
}

func requireInvalidOrderedCursor(t *testing.T, call func() (records int, hasNext bool, err error), kind storage.OrderedCursorKind, rule storage.OrderedCursorRule, raw string) {
	t.Helper()
	records, hasNext, err := call()
	var invalid *storage.InvalidOrderedCursorError
	if !errors.As(err, &invalid) {
		t.Fatalf("cursor error = %T %v, want *InvalidOrderedCursorError", err, err)
	}
	if invalid.Kind != kind || invalid.Rule != rule {
		t.Errorf("cursor error = %#v, want kind %q rule %q", invalid, kind, rule)
	}
	if strings.Contains(err.Error(), raw) {
		t.Errorf("cursor error %q leaked opaque token %q", err, raw)
	}
	if records != 0 || hasNext {
		t.Errorf("invalid cursor returned %d records with continuation %v, want fail-closed empty page", records, hasNext)
	}
	wantLength := len(raw)
	if wantLength > 1<<16-1 {
		wantLength = 1<<16 - 1
	}
	if invalid.CursorLength != uint16(wantLength) {
		t.Errorf("cursor error length = %d, want bounded raw length %d", invalid.CursorLength, wantLength)
	}
}
