package memstore

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

func TestOrderedCreateAssignsImmutableOrder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := New().OrderedIndex
	if index == nil {
		t.Fatal("New() left OrderedIndex nil")
	}

	firstID := storage.OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: "first"}
	secondID := storage.OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: "second"}
	first, created, err := index.Create(ctx, firstID, "workers", []byte("first"), storage.Rank{}, storage.Due{State: storage.NotDue})
	if err != nil || !created {
		t.Fatalf("Create(first) = %#v, %v, %v; want created record, true, nil", first, created, err)
	}
	second, created, err := index.Create(ctx, secondID, "workers", []byte("second"), storage.Rank{}, storage.Due{State: storage.NotDue})
	if err != nil || !created {
		t.Fatalf("Create(second) = %#v, %v, %v; want created record, true, nil", second, created, err)
	}
	if first.Order != 1 || second.Order != 2 {
		t.Errorf("orders = (%d, %d), want (1, 2)", first.Order, second.Order)
	}
}

func TestOrderedCreateDuplicatePrecedenceAndByteOwnership(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := orderedTestIndex(t)
	id := orderedTestID("acceptance", "same")
	input := []byte("original")
	wantValue := append([]byte(nil), input...)
	wantRank := storage.Rank{Ranked: true, Value: 9}
	wantDue := storage.Due{State: storage.DueAt, UnixMillis: 123}

	first, created, err := index.Create(ctx, id, "workers", input, wantRank, wantDue)
	if err != nil || !created {
		t.Fatalf("Create() = %#v, %v, %v; want created record, true, nil", first, created, err)
	}
	if first.Revision != 1 || first.Order != 1 {
		t.Errorf("Create() revision/order = (%d, %d), want (1, 1)", first.Revision, first.Order)
	}
	input[0] = 'X'
	first.Value[0] = 'Y'

	duplicate, created, err := index.Create(ctx, id, "Workers", make([]byte, storage.MaxOrderedValueBytes+1), storage.Rank{}, storage.Due{State: storage.NotDue, UnixMillis: 1})
	if err != nil || created {
		t.Fatalf("duplicate Create() = %#v, %v, %v; want canonical record, false, nil", duplicate, created, err)
	}
	if !bytes.Equal(duplicate.Value, wantValue) || duplicate.Revision != 1 || duplicate.Order != 1 || duplicate.Rank != wantRank || duplicate.Due != wantDue {
		t.Errorf("duplicate Create() = %#v, want original canonical state", duplicate)
	}
	duplicate.Value[0] = 'Z'

	got, err := index.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get() unexpected error: %v", err)
	}
	if !bytes.Equal(got.Value, wantValue) {
		t.Errorf("Get().Value = %q, want %q after caller mutations", got.Value, wantValue)
	}
}

func TestOrderedCreateValidatesAbsentCandidateAndScopesIdentity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := orderedTestIndex(t)
	id := orderedTestID("acceptance", "key")

	if _, created, err := index.Create(ctx, id, "Workers", []byte("value"), storage.Rank{}, storage.Due{State: storage.NotDue}); err == nil || created {
		t.Fatalf("Create(invalid ranking scope) = created %v, err %v; want false, validation error", created, err)
	} else {
		var target *storage.InvalidNameError
		if !errors.As(err, &target) {
			t.Errorf("Create(invalid ranking scope) error = %T %v, want *storage.InvalidNameError", err, err)
		}
	}
	if _, err := index.Get(ctx, id); err == nil {
		t.Fatal("Get() after rejected Create returned nil error, want not found")
	} else {
		var target *storage.OrderedRecordNotFoundError
		if !errors.As(err, &target) {
			t.Errorf("Get() error = %T %v, want *storage.OrderedRecordNotFoundError", err, err)
		}
	}

	first, created, err := index.Create(ctx, id, "workers", []byte("first"), storage.Rank{}, storage.Due{State: storage.NotDue})
	if err != nil || !created {
		t.Fatalf("Create(first scope) = %#v, %v, %v; want record, true, nil", first, created, err)
	}
	otherID := orderedTestID("other", "key")
	second, created, err := index.Create(ctx, otherID, "workers", []byte("second"), storage.Rank{}, storage.Due{State: storage.NotDue})
	if err != nil || !created {
		t.Fatalf("Create(other scope) = %#v, %v, %v; want record, true, nil", second, created, err)
	}
	if first.Order != 1 || second.Order != 1 {
		t.Errorf("scope-local orders = (%d, %d), want (1, 1)", first.Order, second.Order)
	}

	invalidID := storage.OrderedID{Namespace: "Sessions", OrderingScope: id.OrderingScope, StableKey: id.StableKey}
	if _, created, err := index.Create(ctx, invalidID, "Workers", make([]byte, storage.MaxOrderedValueBytes+1), storage.Rank{}, storage.Due{State: storage.NotDue, UnixMillis: 1}); err == nil || created {
		t.Fatalf("Create(invalid ID) = created %v, err %v; want false, ID validation error", created, err)
	} else {
		var target *storage.InvalidNameError
		if !errors.As(err, &target) {
			t.Errorf("Create(invalid ID) error = %T %v, want *storage.InvalidNameError", err, err)
		}
	}
}

func TestOrderedListOrderedPagesIncludeTombstones(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := orderedTestIndex(t)
	ids := []storage.OrderedID{
		orderedTestID("acceptance", "first"),
		orderedTestID("acceptance", "second"),
		orderedTestID("acceptance", "third"),
	}
	for _, id := range ids {
		mustCreateOrdered(t, index, id, "workers", []byte(id.StableKey), storage.Rank{}, storage.Due{State: storage.NotDue})
	}
	second, err := index.Get(ctx, ids[1])
	if err != nil {
		t.Fatalf("Get(second) unexpected error: %v", err)
	}
	if _, err := index.Delete(ctx, ids[1], second.Revision); err != nil {
		t.Fatalf("Delete(second) unexpected error: %v", err)
	}

	page, err := index.ListOrdered(ctx, "sessions", "acceptance", 0, 2)
	if err != nil {
		t.Fatalf("ListOrdered(first page) unexpected error: %v", err)
	}
	if got, want := orderedStableKeys(page.Records), []string{"first", "second"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListOrdered(first page) keys = %v, want %v", got, want)
	}
	if page.NextAfterOrder != 2 || !page.Records[1].Deleted {
		t.Errorf("first page = %#v, want NextAfterOrder 2 with a tombstone", page)
	}

	page, err = index.ListOrdered(ctx, "sessions", "acceptance", page.NextAfterOrder, 2)
	if err != nil {
		t.Fatalf("ListOrdered(second page) unexpected error: %v", err)
	}
	if got, want := orderedStableKeys(page.Records), []string{"third"}; !reflect.DeepEqual(got, want) || page.NextAfterOrder != 3 {
		t.Errorf("ListOrdered(second page) = %#v, want [third] after 3", page)
	}

	empty, err := index.ListOrdered(ctx, "sessions", "acceptance", 3, 2)
	if err != nil {
		t.Fatalf("ListOrdered(exhausted) unexpected error: %v", err)
	}
	if len(empty.Records) != 0 || empty.NextAfterOrder != 0 {
		t.Errorf("ListOrdered(exhausted) = %#v, want empty page with zero cursor", empty)
	}
}

func TestOrderedUpdateCASPrecedenceAndTombstone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := orderedTestIndex(t)
	absent := orderedTestID("acceptance", "absent")
	tooLarge := make([]byte, storage.MaxOrderedValueBytes+1)
	if _, err := index.Update(ctx, absent, 99, tooLarge, storage.Rank{}, storage.Due{State: storage.NotDue, UnixMillis: 1}); err == nil {
		t.Fatal("Update(absent) returned nil error, want not found")
	} else {
		var target *storage.OrderedRecordNotFoundError
		if !errors.As(err, &target) {
			t.Errorf("Update(absent) error = %T %v, want *storage.OrderedRecordNotFoundError", err, err)
		}
	}

	id := orderedTestID("acceptance", "live")
	created := mustCreateOrdered(t, index, id, "workers", []byte("before"), storage.Rank{}, storage.Due{State: storage.NotDue})
	if _, err := index.Update(ctx, id, created.Revision+1, []byte("wrong"), storage.Rank{Ranked: true, Value: 3}, storage.Due{State: storage.DueAt, UnixMillis: 7}); err == nil {
		t.Fatal("Update(stale revision) returned nil error, want conflict")
	} else {
		var target *storage.OrderedRevisionConflictError
		if !errors.As(err, &target) || target.ActualRevision != created.Revision {
			t.Errorf("Update(stale revision) error = %#v, want conflict at actual revision %d", err, created.Revision)
		}
	}
	unchanged, err := index.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get(after stale update) unexpected error: %v", err)
	}
	if !bytes.Equal(unchanged.Value, []byte("before")) || unchanged.Revision != created.Revision {
		t.Errorf("stale Update changed state to %#v", unchanged)
	}

	if _, err := index.Update(ctx, id, created.Revision+1, tooLarge, storage.Rank{}, storage.Due{State: storage.NotDue}); err == nil {
		t.Fatal("Update(invalid candidate) returned nil error, want validation error")
	} else {
		var target *storage.OrderedValueTooLargeError
		if !errors.As(err, &target) {
			t.Errorf("Update(invalid candidate) error = %T %v, want *storage.OrderedValueTooLargeError before conflict", err, err)
		}
	}

	updated, err := index.Update(ctx, id, created.Revision, []byte("after"), storage.Rank{Ranked: true, Value: 3}, storage.Due{State: storage.DueAt, UnixMillis: 7})
	if err != nil {
		t.Fatalf("Update() unexpected error: %v", err)
	}
	if updated.Revision != created.Revision+1 || updated.Order != created.Order || updated.RankingScope != created.RankingScope {
		t.Errorf("Update() = %#v, want one revision advance with immutable fields", updated)
	}

	tombstone, err := index.Delete(ctx, id, updated.Revision)
	if err != nil {
		t.Fatalf("Delete() unexpected error: %v", err)
	}
	if !tombstone.Deleted || tombstone.Revision != updated.Revision+1 || tombstone.Rank != (storage.Rank{}) || tombstone.Due != (storage.Due{State: storage.NotDue}) || !bytes.Equal(tombstone.Value, []byte("after")) {
		t.Errorf("Delete() = %#v, want canonical tombstone preserving value", tombstone)
	}
	retry, err := index.Delete(ctx, id, updated.Revision)
	if err != nil {
		t.Fatalf("Delete(retry with pre-delete revision) unexpected error: %v", err)
	}
	if !reflect.DeepEqual(retry, tombstone) {
		t.Errorf("Delete(retry) = %#v, want canonical tombstone %#v", retry, tombstone)
	}
	if _, err := index.Update(ctx, id, 0, tooLarge, storage.Rank{}, storage.Due{State: storage.NotDue, UnixMillis: 1}); err == nil {
		t.Fatal("Update(tombstone) returned nil error, want deleted")
	} else {
		var target *storage.OrderedDeletedError
		if !errors.As(err, &target) {
			t.Errorf("Update(tombstone) error = %T %v, want *storage.OrderedDeletedError", err, err)
		}
	}
	duplicate, createdAgain, err := index.Create(ctx, id, "Workers", tooLarge, storage.Rank{}, storage.Due{State: storage.NotDue, UnixMillis: 1})
	if err != nil || createdAgain || !reflect.DeepEqual(duplicate, tombstone) {
		t.Errorf("Create(tombstone duplicate) = %#v, %v, %v; want canonical tombstone, false, nil", duplicate, createdAgain, err)
	}
}

func TestOrderedRankedViewMovesAndBindsCursors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := orderedTestIndex(t)
	high := mustCreateOrdered(t, index, orderedTestID("acceptance", "high"), "workers", []byte("high"), storage.Rank{Ranked: true, Value: 20}, storage.Due{State: storage.NotDue})
	tieA := mustCreateOrdered(t, index, orderedTestID("a", "same"), "workers", []byte("tie-a"), storage.Rank{Ranked: true, Value: 10}, storage.Due{State: storage.NotDue})
	tieB := mustCreateOrdered(t, index, orderedTestID("b", "same"), "workers", []byte("tie-b"), storage.Rank{Ranked: true, Value: 10}, storage.Due{State: storage.NotDue})
	low := mustCreateOrdered(t, index, orderedTestID("c", "low"), "workers", []byte("low"), storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.NotDue})
	mustCreateOrdered(t, index, orderedTestID("d", "unranked"), "workers", []byte("unranked"), storage.Rank{}, storage.Due{State: storage.NotDue})

	page, err := index.ListRanked(ctx, "sessions", "workers", "", 2)
	if err != nil {
		t.Fatalf("ListRanked(first page) unexpected error: %v", err)
	}
	if got, want := orderedScopesAndKeys(page.Records), []string{"acceptance/high", "b/same"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked(first page) = %v, want %v", got, want)
	}
	if page.NextCursor == "" {
		t.Fatal("ListRanked(first page) returned empty cursor with more rows")
	}
	secondPage, err := index.ListRanked(ctx, "sessions", "workers", page.NextCursor, 2)
	if err != nil {
		t.Fatalf("ListRanked(second page) unexpected error: %v", err)
	}
	if got, want := orderedScopesAndKeys(secondPage.Records), []string{"a/same", "c/low"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked(second page) = %v, want %v", got, want)
	}

	if _, err := index.Update(ctx, low.ID, low.Revision, []byte("low-moved"), storage.Rank{Ranked: true, Value: 30}, storage.Due{State: storage.NotDue}); err != nil {
		t.Fatalf("Update(rank move) unexpected error: %v", err)
	}
	moved, err := index.ListRanked(ctx, "sessions", "workers", "", 10)
	if err != nil {
		t.Fatalf("ListRanked(after move) unexpected error: %v", err)
	}
	if got, want := orderedScopesAndKeys(moved.Records), []string{"c/low", "acceptance/high", "b/same", "a/same"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked(after move) = %v, want %v", got, want)
	}
	if _, err := index.Get(ctx, high.ID); err != nil {
		t.Errorf("Get(high) unexpected error: %v", err)
	}
	if _, err := index.Get(ctx, tieA.ID); err != nil {
		t.Errorf("Get(tieA) unexpected error: %v", err)
	}
	if _, err := index.Get(ctx, tieB.ID); err != nil {
		t.Errorf("Get(tieB) unexpected error: %v", err)
	}

	if _, err := index.ListRanked(ctx, "sessions", "other", page.NextCursor, 2); err == nil {
		t.Fatal("ListRanked(cross query cursor) returned nil error, want query mismatch")
	} else {
		requireCursorError(t, err, storage.RankedCursorKind, storage.OrderedCursorQueryMismatch)
	}
	assertRankedCursorRejectedWithoutLeak(t, index, storage.RankedCursor("ranked-token-secret"), storage.OrderedCursorMalformed)
	for _, cursor := range []storage.RankedCursor{"v2:r:opaque", "v13:r:opaque"} {
		if _, err := index.ListRanked(ctx, "sessions", "workers", cursor, 2); err == nil {
			t.Errorf("ListRanked(%q) returned nil error", cursor)
		} else {
			requireCursorError(t, err, storage.RankedCursorKind, storage.OrderedCursorUnknownVersion)
		}
	}
}

func TestOrderedDueViewMovesExcludesNotDueAndBindsCursors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := orderedTestIndex(t)
	past := mustCreateOrdered(t, index, orderedTestID("acceptance", "past"), "workers", []byte("past"), storage.Rank{}, storage.Due{State: storage.DueAt, UnixMillis: 5})
	mustCreateOrdered(t, index, orderedTestID("b", "same"), "workers", []byte("tie-b"), storage.Rank{}, storage.Due{State: storage.DueAt, UnixMillis: 10})
	tieA := mustCreateOrdered(t, index, orderedTestID("a", "same"), "workers", []byte("tie-a"), storage.Rank{}, storage.Due{State: storage.DueAt, UnixMillis: 10})
	later := mustCreateOrdered(t, index, orderedTestID("acceptance", "later"), "workers", []byte("later"), storage.Rank{}, storage.Due{State: storage.DueAt, UnixMillis: 20})
	mustCreateOrdered(t, index, orderedTestID("acceptance", "not-due"), "workers", []byte("not-due"), storage.Rank{}, storage.Due{State: storage.NotDue})

	page, err := index.ListDue(ctx, "sessions", 10, "", 2)
	if err != nil {
		t.Fatalf("ListDue(first page) unexpected error: %v", err)
	}
	if got, want := orderedScopesAndKeys(page.Records), []string{"acceptance/past", "a/same"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(first page) = %v, want %v", got, want)
	}
	if page.NextCursor == "" {
		t.Fatal("ListDue(first page) returned empty cursor with more rows")
	}
	secondPage, err := index.ListDue(ctx, "sessions", 10, page.NextCursor, 2)
	if err != nil {
		t.Fatalf("ListDue(second page) unexpected error: %v", err)
	}
	if got, want := orderedScopesAndKeys(secondPage.Records), []string{"b/same"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(second page) = %v, want %v", got, want)
	}

	if _, err := index.Update(ctx, later.ID, later.Revision, []byte("later-moved"), storage.Rank{}, storage.Due{State: storage.DueAt, UnixMillis: 1}); err != nil {
		t.Fatalf("Update(due move) unexpected error: %v", err)
	}
	if _, err := index.Update(ctx, tieA.ID, tieA.Revision, []byte("tie-a-not-due"), storage.Rank{}, storage.Due{State: storage.NotDue}); err != nil {
		t.Fatalf("Update(not due) unexpected error: %v", err)
	}
	moved, err := index.ListDue(ctx, "sessions", 10, "", 10)
	if err != nil {
		t.Fatalf("ListDue(after moves) unexpected error: %v", err)
	}
	if got, want := orderedScopesAndKeys(moved.Records), []string{"acceptance/later", "acceptance/past", "b/same"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(after moves) = %v, want %v", got, want)
	}
	if _, err := index.Get(ctx, past.ID); err != nil {
		t.Errorf("Get(past) unexpected error: %v", err)
	}
	if _, err := index.ListDue(ctx, "sessions", 11, page.NextCursor, 2); err == nil {
		t.Fatal("ListDue(cross-bound cursor) returned nil error, want query mismatch")
	} else {
		requireCursorError(t, err, storage.DueCursorKind, storage.OrderedCursorQueryMismatch)
	}
	if _, err := index.ListRanked(ctx, "sessions", "workers", storage.RankedCursor(page.NextCursor), 2); err == nil {
		t.Fatal("ListRanked(due cursor) returned nil error, want wrong kind")
	} else {
		requireCursorError(t, err, storage.RankedCursorKind, storage.OrderedCursorWrongKind)
	}
	if _, err := index.ListDue(ctx, "sessions", 10, storage.DueCursor("v2:d:opaque"), 2); err == nil {
		t.Fatal("ListDue(unknown version) returned nil error")
	} else {
		requireCursorError(t, err, storage.DueCursorKind, storage.OrderedCursorUnknownVersion)
	}
}

func TestOrderedCanceledContextDoesNotMutate(t *testing.T) {
	t.Parallel()

	base := context.Background()
	canceled, cancel := context.WithCancel(base)
	cancel()
	index := orderedTestIndex(t)
	id := orderedTestID("acceptance", "canceled")
	if _, created, err := index.Create(canceled, id, "workers", []byte("value"), storage.Rank{}, storage.Due{State: storage.NotDue}); !errors.Is(err, context.Canceled) || created {
		t.Fatalf("Create(canceled) = created %v, err %v; want false, context.Canceled", created, err)
	}
	if _, err := index.Get(base, id); err == nil {
		t.Fatal("Get() after canceled Create returned nil error, want not found")
	}

	record := mustCreateOrdered(t, index, id, "workers", []byte("before"), storage.Rank{}, storage.Due{State: storage.NotDue})
	if _, err := index.Update(canceled, id, record.Revision, []byte("after"), storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.DueAt, UnixMillis: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Update(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := index.Delete(canceled, id, record.Revision); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete(canceled) error = %v, want context.Canceled", err)
	}
	got, err := index.Get(base, id)
	if err != nil {
		t.Fatalf("Get(after canceled mutations) unexpected error: %v", err)
	}
	if !bytes.Equal(got.Value, []byte("before")) || got.Revision != record.Revision || got.Deleted {
		t.Errorf("canceled mutations changed state to %#v", got)
	}
	if _, err := index.ListOrdered(canceled, "sessions", "acceptance", 0, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("ListOrdered(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := index.ListRanked(canceled, "sessions", "workers", "", 1); !errors.Is(err, context.Canceled) {
		t.Errorf("ListRanked(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := index.ListDue(canceled, "sessions", 0, "", 1); !errors.Is(err, context.Canceled) {
		t.Errorf("ListDue(canceled) error = %v, want context.Canceled", err)
	}
}

func TestOrderedConcurrentDuplicateCreateHasOneWinner(t *testing.T) {
	t.Parallel()

	const writers = 100
	ctx := context.Background()
	index := orderedTestIndex(t)
	id := orderedTestID("acceptance", "contended")
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
	wg.Wait()
	close(results)

	var canonical storage.OrderedRecord
	winners := 0
	allResults := make([]result, 0, writers)
	for result := range results {
		allResults = append(allResults, result)
		if result.err != nil {
			t.Errorf("concurrent Create() unexpected error: %v", result.err)
			continue
		}
		if result.created {
			winners++
			canonical = result.record
		}
	}
	if winners != 1 {
		t.Fatalf("concurrent duplicate winners = %d, want 1", winners)
	}
	if canonical.Order != 1 || canonical.Revision != 1 {
		t.Errorf("winner = %#v, want revision/order 1", canonical)
	}
	for _, result := range allResults {
		if !reflect.DeepEqual(result.record, canonical) {
			t.Errorf("concurrent Create() returned %#v, want canonical %#v", result.record, canonical)
		}
	}
}

func TestOrderedConcurrentDistinctCreatesAreStrictlyMonotonic(t *testing.T) {
	t.Parallel()

	const writers = 100
	ctx := context.Background()
	index := orderedTestIndex(t)
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
			id := orderedTestID("acceptance", storage.StableKey(fmt.Sprintf("key-%03d", i)))
			record, created, err := index.Create(ctx, id, "workers", []byte("value"), storage.Rank{}, storage.Due{State: storage.NotDue})
			results <- result{record: record, created: created, err: err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)

	orders := make([]uint64, 0, writers)
	for result := range results {
		if result.err != nil || !result.created {
			t.Errorf("concurrent distinct Create() = %#v, %v, %v; want record, true, nil", result.record, result.created, result.err)
			continue
		}
		orders = append(orders, result.record.Order)
	}
	if len(orders) != writers {
		t.Fatalf("concurrent distinct creates = %d successful records, want %d", len(orders), writers)
	}
	sort.Slice(orders, func(i int, j int) bool { return orders[i] < orders[j] })
	for i, order := range orders {
		if want := uint64(i + 1); order != want {
			t.Errorf("orders[%d] = %d, want %d", i, order, want)
		}
	}
}

func TestOrderedRevisionExhaustionLeavesStateUntouched(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newOrderedStore()
	id := orderedTestID("acceptance", "exhausted")
	created := mustCreateOrdered(t, store, id, "workers", []byte("before"), storage.Rank{Ranked: true, Value: 4}, storage.Due{State: storage.DueAt, UnixMillis: 8})
	key := orderedIdentityFor(id)
	store.mu.Lock()
	record := store.records[key]
	record.Revision = maxOrderedUint64
	store.records[key] = record
	store.mu.Unlock()

	if _, err := store.Update(ctx, id, record.Revision, []byte("after"), storage.Rank{}, storage.Due{State: storage.NotDue}); err == nil {
		t.Fatal("Update(max revision) returned nil error, want exhaustion")
	} else {
		var target *storage.OrderedRevisionExhaustedError
		if !errors.As(err, &target) || target.Revision != maxOrderedUint64 {
			t.Errorf("Update(max revision) error = %#v, want *storage.OrderedRevisionExhaustedError at max", err)
		}
	}
	if _, err := store.Delete(ctx, id, record.Revision); err == nil {
		t.Fatal("Delete(max revision) returned nil error, want exhaustion")
	} else {
		var target *storage.OrderedRevisionExhaustedError
		if !errors.As(err, &target) || target.Revision != maxOrderedUint64 {
			t.Errorf("Delete(max revision) error = %#v, want *storage.OrderedRevisionExhaustedError at max", err)
		}
	}
	got, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get(after exhausted mutations) unexpected error: %v", err)
	}
	if got.Revision != maxOrderedUint64 || got.Deleted || !bytes.Equal(got.Value, []byte("before")) || got.Rank != created.Rank || got.Due != created.Due {
		t.Errorf("exhausted mutations changed record to %#v", got)
	}
}

func TestOrderedLargeValueAndListingSnapshotsAreCallerOwned(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := orderedTestIndex(t)
	id := orderedTestID("acceptance", "large")
	value := bytes.Repeat([]byte("x"), storage.MaxOrderedValueBytes)
	record := mustCreateOrdered(t, index, id, "workers", value, storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.DueAt, UnixMillis: 1})
	value[0] = 'y'
	record.Value[1] = 'z'

	orderedPage, err := index.ListOrdered(ctx, "sessions", "acceptance", 0, 1)
	if err != nil || len(orderedPage.Records) != 1 {
		t.Fatalf("ListOrdered() = %#v, %v; want one record, nil", orderedPage, err)
	}
	orderedPage.Records[0].Value[2] = 'q'
	rankedPage, err := index.ListRanked(ctx, "sessions", "workers", "", 1)
	if err != nil || len(rankedPage.Records) != 1 {
		t.Fatalf("ListRanked() = %#v, %v; want one record, nil", rankedPage, err)
	}
	rankedPage.Records[0].Value[3] = 'r'
	duePage, err := index.ListDue(ctx, "sessions", 1, "", 1)
	if err != nil || len(duePage.Records) != 1 {
		t.Fatalf("ListDue() = %#v, %v; want one record, nil", duePage, err)
	}
	duePage.Records[0].Value[4] = 's'

	got, err := index.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get() unexpected error: %v", err)
	}
	if len(got.Value) != storage.MaxOrderedValueBytes || got.Value[0] != 'x' || got.Value[1] != 'x' || got.Value[2] != 'x' || got.Value[3] != 'x' || got.Value[4] != 'x' {
		t.Errorf("stored large value was changed through caller-owned slices: length=%d prefix=%q", len(got.Value), got.Value[:5])
	}
}

func FuzzOrderedCursorRejectsMalformedTokens(f *testing.F) {
	for _, seed := range []string{
		"",
		"not-a-cursor",
		"v1:r:",
		"v1:d:payload.signature",
		"v13:r:opaque",
		strings.Repeat("opaque-token-", 128),
	} {
		f.Add(seed)
	}

	store := newOrderedStore()
	ctx := context.Background()
	f.Fuzz(func(t *testing.T, token string) {
		opaqueToken := "looprig-fuzz-opaque-token:" + token
		assertFuzzCursorOutcome(t, opaqueToken, func() error {
			_, err := store.ListRanked(ctx, "sessions", "workers", storage.RankedCursor(opaqueToken), 1)
			return err
		})
		assertFuzzCursorOutcome(t, opaqueToken, func() error {
			_, err := store.ListDue(ctx, "sessions", 0, storage.DueCursor(opaqueToken), 1)
			return err
		})
	})
}

func orderedTestIndex(t *testing.T) storage.OrderedIndex {
	t.Helper()

	index := New().OrderedIndex
	if index == nil {
		t.Fatal("New() left OrderedIndex nil")
	}
	return index
}

func orderedTestID(orderingScope string, stableKey storage.StableKey) storage.OrderedID {
	return storage.OrderedID{Namespace: "sessions", OrderingScope: orderingScope, StableKey: stableKey}
}

func mustCreateOrdered(t *testing.T, index storage.OrderedIndex, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) storage.OrderedRecord {
	t.Helper()

	record, created, err := index.Create(context.Background(), id, rankingScope, value, rank, due)
	if err != nil || !created {
		t.Fatalf("Create(%+v) = %#v, %v, %v; want record, true, nil", id, record, created, err)
	}
	return record
}

func orderedStableKeys(records []storage.OrderedRecord) []string {
	keys := make([]string, len(records))
	for i, record := range records {
		keys[i] = string(record.ID.StableKey)
	}
	return keys
}

func orderedScopesAndKeys(records []storage.OrderedRecord) []string {
	keys := make([]string, len(records))
	for i, record := range records {
		keys[i] = record.ID.OrderingScope + "/" + string(record.ID.StableKey)
	}
	return keys
}

func requireCursorError(t *testing.T, err error, kind storage.OrderedCursorKind, rule storage.OrderedCursorRule) {
	t.Helper()

	var target *storage.InvalidOrderedCursorError
	if !errors.As(err, &target) {
		t.Fatalf("cursor error = %T %v, want *storage.InvalidOrderedCursorError", err, err)
	}
	if target.Kind != kind || target.Rule != rule {
		t.Errorf("cursor error = %#v, want kind %q rule %q", target, kind, rule)
	}
}

func assertRankedCursorRejectedWithoutLeak(t *testing.T, index storage.OrderedIndex, cursor storage.RankedCursor, wantRule storage.OrderedCursorRule) {
	t.Helper()

	_, err := index.ListRanked(context.Background(), "sessions", "workers", cursor, 1)
	if err == nil {
		t.Fatalf("ListRanked(%q) returned nil error, want invalid cursor", cursor)
	}
	requireCursorError(t, err, storage.RankedCursorKind, wantRule)
	if strings.Contains(err.Error(), string(cursor)) {
		t.Errorf("invalid cursor error = %q, must not expose token %q", err, cursor)
	}
}

func assertFuzzCursorOutcome(t *testing.T, token string, list func() error) {
	t.Helper()

	err := list()
	if err == nil {
		t.Fatalf("malformed cursor %q returned nil error", token)
	}
	var target *storage.InvalidOrderedCursorError
	if !errors.As(err, &target) {
		t.Fatalf("cursor %q returned %T %v, want *storage.InvalidOrderedCursorError", token, err, err)
	}
	if token != "" && strings.Contains(err.Error(), token) {
		t.Errorf("cursor error = %q, must not expose opaque token %q", err, token)
	}
}
