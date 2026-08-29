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
	"time"

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
	// A rank move rewrites the ranked slice around one record; the records it
	// steps over must come back byte for byte unchanged.
	for _, want := range []storage.OrderedRecord{high, tieA, tieB} {
		requireOrderedRecordUnchanged(t, ctx, index, want)
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
	// past neither moved nor became not-due, so the two updates above must have
	// left it exactly as created.
	requireOrderedRecordUnchanged(t, ctx, index, past)
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

func TestOrderedDeadlineWhileWaitingForLockDoesNotMutate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(storage.OrderedIndex, context.Context, storage.OrderedID, storage.OrderedID, storage.OrderedRecord) error
	}{
		{
			name: "get",
			call: func(index storage.OrderedIndex, ctx context.Context, id storage.OrderedID, _ storage.OrderedID, _ storage.OrderedRecord) error {
				_, err := index.Get(ctx, id)
				return err
			},
		},
		{
			name: "create",
			call: func(index storage.OrderedIndex, ctx context.Context, _ storage.OrderedID, newID storage.OrderedID, _ storage.OrderedRecord) error {
				_, _, err := index.Create(ctx, newID, "workers", []byte("new"), storage.Rank{}, storage.Due{State: storage.NotDue})
				return err
			},
		},
		{
			name: "update",
			call: func(index storage.OrderedIndex, ctx context.Context, id storage.OrderedID, _ storage.OrderedID, record storage.OrderedRecord) error {
				_, err := index.Update(ctx, id, record.Revision, []byte("changed"), storage.Rank{Ranked: true, Value: 9}, storage.Due{State: storage.DueAt, UnixMillis: 9})
				return err
			},
		},
		{
			name: "delete",
			call: func(index storage.OrderedIndex, ctx context.Context, id storage.OrderedID, _ storage.OrderedID, record storage.OrderedRecord) error {
				_, err := index.Delete(ctx, id, record.Revision)
				return err
			},
		},
		{
			name: "list ordered",
			call: func(index storage.OrderedIndex, ctx context.Context, _ storage.OrderedID, _ storage.OrderedID, _ storage.OrderedRecord) error {
				_, err := index.ListOrdered(ctx, "sessions", "acceptance", 0, 1)
				return err
			},
		},
		{
			name: "list ranked",
			call: func(index storage.OrderedIndex, ctx context.Context, _ storage.OrderedID, _ storage.OrderedID, _ storage.OrderedRecord) error {
				_, err := index.ListRanked(ctx, "sessions", "workers", "", 1)
				return err
			},
		},
		{
			name: "list due",
			call: func(index storage.OrderedIndex, ctx context.Context, _ storage.OrderedID, _ storage.OrderedID, _ storage.OrderedRecord) error {
				_, err := index.ListDue(ctx, "sessions", 10, "", 1)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := newOrderedStore()
			id := orderedTestID("acceptance", "held")
			newID := orderedTestID("acceptance", "new")
			before := mustCreateOrdered(t, store, id, "workers", []byte("before"), storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.DueAt, UnixMillis: 1})
			if err := store.mu.lock(context.Background()); err != nil {
				t.Fatalf("hold orderedStore lock: %v", err)
			}

			deadline, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			ctx := &errObservedContext{Context: deadline, errObserved: make(chan struct{})}
			result := make(chan error, 1)
			go func() {
				result <- tt.call(store, ctx, id, newID, before)
			}()
			select {
			case <-ctx.errObserved:
			case <-time.After(time.Second):
				store.mu.unlock()
				t.Fatal("method did not inspect context before waiting for the lock")
			}

			var err error
			select {
			case err = <-result:
			case <-time.After(time.Second):
				store.mu.unlock()
				<-result
				t.Fatal("method remained blocked after its context deadline elapsed")
			}
			store.mu.unlock()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("blocked method error = %T %v, want context.DeadlineExceeded", err, err)
			}

			got, getErr := store.Get(context.Background(), id)
			if getErr != nil {
				t.Fatalf("Get(after deadline) unexpected error: %v", getErr)
			}
			if !reflect.DeepEqual(got, before) {
				t.Errorf("deadline mutation changed record to %#v, want %#v", got, before)
			}
			if _, getErr := store.Get(context.Background(), newID); getErr == nil {
				t.Error("deadline Create persisted a new record")
			} else {
				var target *storage.OrderedRecordNotFoundError
				if !errors.As(getErr, &target) {
					t.Errorf("Get(new ID) error = %T %v, want *storage.OrderedRecordNotFoundError", getErr, getErr)
				}
			}
		})
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

func TestOrderedCursorRejectsOversizedToken(t *testing.T) {
	t.Parallel()

	// Each token starts as a real cursor this store just issued and is then
	// padded past the bounded diagnostic length. They prove the raw-token
	// limit is applied before a decoder can allocate or parse attacker-supplied
	// padding.
	const paddingBytes = 96 << 10
	paddingMarker := "opaque-cursor-padding-"
	padding := strings.Repeat(paddingMarker, paddingBytes/len(paddingMarker)+1)
	index := orderedTestIndex(t)
	for _, key := range []storage.StableKey{"first", "second"} {
		mustCreateOrdered(t, index, orderedTestID("acceptance", key), "workers", []byte(key), storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.DueAt, UnixMillis: 1})
	}
	rankedPage, err := index.ListRanked(context.Background(), "sessions", "workers", "", 1)
	if err != nil || rankedPage.NextCursor == "" {
		t.Fatalf("ListRanked(first page) = %v, cursor %q; want a continuation cursor", err, rankedPage.NextCursor)
	}
	duePage, err := index.ListDue(context.Background(), "sessions", 1, "", 1)
	if err != nil || duePage.NextCursor == "" {
		t.Fatalf("ListDue(first page) = %v, cursor %q; want a continuation cursor", err, duePage.NextCursor)
	}

	tests := []struct {
		name   string
		kind   storage.OrderedCursorKind
		cursor string
		list   func(string) error
	}{
		{
			name:   "ranked",
			kind:   storage.RankedCursorKind,
			cursor: string(rankedPage.NextCursor) + padding,
			list: func(cursor string) error {
				_, err := index.ListRanked(context.Background(), "sessions", "workers", storage.RankedCursor(cursor), 1)
				return err
			},
		},
		{
			name:   "due",
			kind:   storage.DueCursorKind,
			cursor: string(duePage.NextCursor) + padding,
			list: func(cursor string) error {
				_, err := index.ListDue(context.Background(), "sessions", 1, storage.DueCursor(cursor), 1)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if len(tt.cursor) <= 1<<16-1 {
				t.Fatalf("test cursor length = %d, want more than bounded diagnostic capacity", len(tt.cursor))
			}
			err := tt.list(tt.cursor)
			if err == nil {
				t.Fatal("oversized cursor returned nil error")
			}
			requireCursorError(t, err, tt.kind, storage.OrderedCursorMalformed)
			var target *storage.InvalidOrderedCursorError
			if !errors.As(err, &target) {
				t.Fatalf("oversized cursor error = %T %v, want *storage.InvalidOrderedCursorError", err, err)
			}
			if target.CursorLength != ^uint16(0) {
				t.Errorf("oversized cursor diagnostic length = %d, want %d", target.CursorLength, ^uint16(0))
			}
			if strings.Contains(err.Error(), paddingMarker) || len(err.Error()) > 128 {
				t.Errorf("oversized cursor error diagnostics were not bounded: %q", err)
			}
		})
	}
}

type errObservedContext struct {
	context.Context
	errObserved chan struct{}
	once        sync.Once
}

func (c *errObservedContext) Err() error {
	c.once.Do(func() { close(c.errObserved) })
	return c.Context.Err()
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
	if err := store.mu.lock(context.Background()); err != nil {
		t.Fatalf("hold orderedStore lock: %v", err)
	}
	record := store.records[key]
	record.Revision = maxOrderedUint64
	store.records[key] = record
	store.mu.unlock()

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

// FuzzOrderedCursorParserRejectsUntrustedTokens drives the untrusted-cursor
// parser with arbitrary bytes. Each generated input is tried bare AND behind
// each of the two real cursor headers, because a token that does not start with
// a header is rejected at the very first check: prefixing is what makes the
// base64 payload decode, the field-count split, the version field, the kind
// field, and both position parsers reachable at all.
func FuzzOrderedCursorParserRejectsUntrustedTokens(f *testing.F) {
	// A genuine payload from each encoder seeds the corpus, so the fuzzer starts
	// from inputs that reach the deepest parser and mutates outward from there.
	rankedSeed := strings.TrimPrefix(string(encodeRankedCursor(rankedCursorPosition{
		namespace:     "sessions",
		rankingScope:  "workers",
		rank:          10,
		stableKey:     "first",
		orderingScope: "acceptance",
	})), rankedCursorHeader)
	dueSeed := strings.TrimPrefix(string(encodeDueCursor(dueCursorPosition{
		namespace:     "sessions",
		dueBound:      0,
		dueAt:         -1,
		stableKey:     "first",
		orderingScope: "acceptance",
	})), dueCursorHeader)
	for _, seed := range []string{
		"",
		"not-a-cursor",
		"cGF5bG9hZA",
		"opaque",
		rankedSeed,
		dueSeed,
		strings.Repeat("opaque-token-", 128),
	} {
		f.Add(seed)
	}

	store := newOrderedStore()
	ctx := context.Background()
	f.Fuzz(func(t *testing.T, token string) {
		for _, candidate := range []string{token, rankedCursorHeader + token, dueCursorHeader + token} {
			assertFuzzCursorOutcome(t, candidate, func() (int, error) {
				page, err := store.ListRanked(ctx, "sessions", "workers", storage.RankedCursor(candidate), 1)
				return len(page.Records), err
			})
			assertFuzzCursorOutcome(t, candidate, func() (int, error) {
				page, err := store.ListDue(ctx, "sessions", 0, storage.DueCursor(candidate), 1)
				return len(page.Records), err
			})
		}
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

// minDistinctiveCursorToken is the shortest token for which "the error text
// contains the token" is evidence of a leak rather than a coincidence. A
// one-byte token like "a" occurs inside "malformed" by accident, so a shorter
// token cannot distinguish echoing from ordinary English prose.
const minDistinctiveCursorToken = 8

// assertFuzzCursorOutcome pins the only two outcomes the untrusted-cursor
// parser may produce for arbitrary bytes. Acceptance is one of them: the fuzzer
// can and does construct well-formed, query-matching tokens, and decoding one
// is correct behavior, not a finding. The other is a typed
// *storage.InvalidOrderedCursorError. Any other error type is a parser defect,
// as is any panic (which fails the fuzz target outright). A rejection must
// never echo the token back, and must fail closed with no records.
func assertFuzzCursorOutcome(t *testing.T, token string, list func() (int, error)) {
	t.Helper()

	records, err := list()
	if err == nil {
		return
	}
	var target *storage.InvalidOrderedCursorError
	if !errors.As(err, &target) {
		t.Fatalf("cursor %q returned %T %v, want nil or *storage.InvalidOrderedCursorError", token, err, err)
	}
	if len(token) >= minDistinctiveCursorToken && strings.Contains(err.Error(), token) {
		t.Errorf("cursor error = %q, must not expose opaque token %q", err, token)
	}
	if records != 0 {
		t.Errorf("rejected cursor %q returned %d records, want a fail-closed empty page", token, records)
	}
}

// TestOrderedCursorsAreInstanceIndependent pins the opaque cursor encoding to
// position rather than per-process authority. A cursor is versioned, kind
// tagged, and query bound, and every binding is re-checked against the live
// request; it grants nothing a caller could not ask for directly. Two
// independently constructed stores holding the same records must therefore
// issue byte-identical continuation tokens and accept each other's, which is
// the only behavior a multi-replica provider can implement.
func TestOrderedCursorsAreInstanceIndependent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	build := func() storage.OrderedIndex {
		index := orderedTestIndex(t)
		for _, key := range []storage.StableKey{"alpha", "beta", "gamma"} {
			mustCreateOrdered(t, index, orderedTestID("acceptance", key), "workers", []byte(key), storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.DueAt, UnixMillis: 1})
		}
		return index
	}
	issuer, replica := build(), build()

	issuedRanked, err := issuer.ListRanked(ctx, "sessions", "workers", "", 1)
	if err != nil || issuedRanked.NextCursor == "" {
		t.Fatalf("ListRanked(issuer first page) = %v, cursor %q; want a continuation cursor", err, issuedRanked.NextCursor)
	}
	replicaRanked, err := replica.ListRanked(ctx, "sessions", "workers", "", 1)
	if err != nil {
		t.Fatalf("ListRanked(replica first page) unexpected error: %v", err)
	}
	if replicaRanked.NextCursor != issuedRanked.NextCursor {
		t.Errorf("replica ranked cursor = %q, want issuer cursor %q", replicaRanked.NextCursor, issuedRanked.NextCursor)
	}
	issuerRest, err := issuer.ListRanked(ctx, "sessions", "workers", issuedRanked.NextCursor, 10)
	if err != nil {
		t.Fatalf("ListRanked(issuer continuation) unexpected error: %v", err)
	}
	replicaRest, err := replica.ListRanked(ctx, "sessions", "workers", issuedRanked.NextCursor, 10)
	if err != nil {
		t.Fatalf("ListRanked(replica continuation of issuer cursor) unexpected error: %v", err)
	}
	if got, want := orderedStableKeys(replicaRest.Records), orderedStableKeys(issuerRest.Records); !reflect.DeepEqual(got, want) {
		t.Errorf("replica ranked continuation = %v, want %v", got, want)
	}

	issuedDue, err := issuer.ListDue(ctx, "sessions", 1, "", 1)
	if err != nil || issuedDue.NextCursor == "" {
		t.Fatalf("ListDue(issuer first page) = %v, cursor %q; want a continuation cursor", err, issuedDue.NextCursor)
	}
	replicaDue, err := replica.ListDue(ctx, "sessions", 1, "", 1)
	if err != nil {
		t.Fatalf("ListDue(replica first page) unexpected error: %v", err)
	}
	if replicaDue.NextCursor != issuedDue.NextCursor {
		t.Errorf("replica due cursor = %q, want issuer cursor %q", replicaDue.NextCursor, issuedDue.NextCursor)
	}
	issuerDueRest, err := issuer.ListDue(ctx, "sessions", 1, issuedDue.NextCursor, 10)
	if err != nil {
		t.Fatalf("ListDue(issuer continuation) unexpected error: %v", err)
	}
	replicaDueRest, err := replica.ListDue(ctx, "sessions", 1, issuedDue.NextCursor, 10)
	if err != nil {
		t.Fatalf("ListDue(replica continuation of issuer cursor) unexpected error: %v", err)
	}
	if got, want := orderedStableKeys(replicaDueRest.Records), orderedStableKeys(issuerDueRest.Records); !reflect.DeepEqual(got, want) {
		t.Errorf("replica due continuation = %v, want %v", got, want)
	}
}

// TestOrderedMutationsRefuseToPublishInvalidRecords pins ValidateOrderedRecord
// to the provider's publish boundary. Every mutation builds the next
// externally observable snapshot from stored state, so it validates that
// snapshot before it replaces the authoritative record; a record that would
// violate the contract's observable representation is refused and the store is
// left untouched.
func TestOrderedMutationsRefuseToPublishInvalidRecords(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newOrderedStore()
	id := orderedTestID("acceptance", "corrupt")
	mustCreateOrdered(t, store, id, "workers", []byte("value"), storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.DueAt, UnixMillis: 1})

	key := orderedIdentityFor(id)
	corrupt := store.records[key]
	// Order zero is unreachable through the public API: it is the sentinel the
	// contract reserves for "no order assigned".
	corrupt.Order = 0
	store.records[key] = corrupt

	tests := []struct {
		name   string
		mutate func() (storage.OrderedRecord, error)
	}{
		{
			name: "update",
			mutate: func() (storage.OrderedRecord, error) {
				return store.Update(ctx, id, corrupt.Revision, []byte("next"), storage.Rank{Ranked: true, Value: 2}, storage.Due{State: storage.NotDue})
			},
		},
		{
			name: "delete",
			mutate: func() (storage.OrderedRecord, error) {
				return store.Delete(ctx, id, corrupt.Revision)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record, err := tt.mutate()
			var target *storage.InvalidOrderedRecordError
			if !errors.As(err, &target) {
				t.Fatalf("%s(corrupt record) = %#v, err %T %v; want *storage.InvalidOrderedRecordError", tt.name, record, err, err)
			}
			if target.ID != id {
				t.Errorf("%s(corrupt record) error ID = %v, want %v", tt.name, target.ID, id)
			}
			if stored := store.records[key]; !reflect.DeepEqual(stored, corrupt) {
				t.Errorf("%s(corrupt record) changed stored state: got %#v, want %#v", tt.name, stored, corrupt)
			}
		})
	}
}

// TestOrderedCursorRejectsNoncanonicalIntegerFields pins one token to one
// position. strconv.ParseInt accepts "+10" and "010" as 10, so without a
// canonical-spelling check several distinct byte strings would decode to the
// same cursor position. The tokens are opaque and provider-issued, so any
// spelling this encoder would not have produced is malformed by definition.
func TestOrderedCursorRejectsNoncanonicalIntegerFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := orderedTestIndex(t)

	for _, test := range []struct {
		name   string
		cursor storage.RankedCursor
	}{
		{name: "explicit plus sign", cursor: storage.RankedCursor(encodeOrderedCursorToken(rankedCursorHeader, rankedCursorFields("+10")))},
		{name: "leading zero", cursor: storage.RankedCursor(encodeOrderedCursorToken(rankedCursorHeader, rankedCursorFields("010")))},
		{name: "negative zero", cursor: storage.RankedCursor(encodeOrderedCursorToken(rankedCursorHeader, rankedCursorFields("-0")))},
		{name: "leading space", cursor: storage.RankedCursor(encodeOrderedCursorToken(rankedCursorHeader, rankedCursorFields(" 10")))},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := index.ListRanked(ctx, "sessions", "workers", test.cursor, 2)
			if err == nil {
				t.Fatalf("ListRanked(%q) returned nil error, want malformed", test.cursor)
			}
			requireCursorError(t, err, storage.RankedCursorKind, storage.OrderedCursorMalformed)
		})
	}

	canonical := storage.RankedCursor(encodeOrderedCursorToken(rankedCursorHeader, rankedCursorFields("10")))
	if _, err := index.ListRanked(ctx, "sessions", "workers", canonical, 2); err != nil {
		t.Errorf("ListRanked(canonically spelled cursor) = %v, want nil", err)
	}
}

// rankedCursorFields builds a ranked payload whose rank field carries an
// arbitrary spelling, so a test can vary just that one field.
func rankedCursorFields(rank string) []string {
	return []string{
		orderedCursorVersionField,
		rankedCursorTokenKind,
		encodeOrderedCursorField("sessions"),
		encodeOrderedCursorField("workers"),
		rank,
		encodeOrderedCursorField("first"),
		encodeOrderedCursorField("acceptance"),
	}
}

// requireOrderedRecordUnchanged asserts that Get returns want unchanged, field
// for field and byte for byte. It is the assertion for a record a test expects
// some other record's mutation to have left alone.
func requireOrderedRecordUnchanged(t *testing.T, ctx context.Context, index storage.OrderedIndex, want storage.OrderedRecord) {
	t.Helper()

	got, err := index.Get(ctx, want.ID)
	if err != nil {
		t.Fatalf("Get(%s) unexpected error: %v", want.ID.StableKey, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Get(%s) = %#v, want the unchanged record %#v", want.ID.StableKey, got, want)
	}
}
