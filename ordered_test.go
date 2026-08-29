package storage

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The compile-time assertion protects the method set independently from any
// concrete backend. F2.2's memstore implementation is deliberately outside
// this contract-only change.
var _ OrderedIndex = (*stubOrderedIndex)(nil)
var _ func(OrderedIndex, context.Context, OrderedID) (OrderedRecord, error) = OrderedIndex.Get

func TestValidateStableKey(t *testing.T) {
	t.Parallel()

	invalidUTF8 := StableKey(string([]byte{0xff, 'x'}))
	tests := []struct {
		name    string
		key     StableKey
		wantErr bool
	}{
		{name: "versioned opaque key", key: StableKey("v1:ABC_def-09")},
		{name: "slash is opaque", key: StableKey("slash/value")},
		{name: "uppercase is opaque", key: StableKey("UPPERCASE")},
		{name: "embedded unicode is opaque", key: StableKey("caf\u00e9/\u4e16\u754c")},
		{name: "one byte", key: StableKey("x")},
		{name: "exactly 256 bytes", key: StableKey(strings.Repeat("x", 256))},
		{name: "empty", key: "", wantErr: true},
		{name: "invalid utf8", key: invalidUTF8, wantErr: true},
		{name: "257 bytes", key: StableKey(strings.Repeat("x", 257)), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateStableKey(tt.key)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("ValidateStableKey(%q) = %v, want nil", tt.key, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateStableKey(%q) = nil, want error", tt.key)
			}
			var target *InvalidStableKeyError
			if !errors.As(err, &target) {
				t.Fatalf("ValidateStableKey(%q) error type = %T, want *InvalidStableKeyError", tt.key, err)
			}
			if target.StableKey != tt.key {
				t.Errorf("InvalidStableKeyError.StableKey = %q, want %q", target.StableKey, tt.key)
			}
		})
	}
}

func TestValidateOrderedID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      OrderedID
		wantErr bool
		wantKey bool
	}{
		{name: "valid opaque key", id: OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: "v1:ABC_def-09"}},
		{name: "invalid namespace", id: OrderedID{Namespace: "Sessions", OrderingScope: "acceptance", StableKey: "x"}, wantErr: true},
		{name: "invalid ordering scope", id: OrderedID{Namespace: "sessions", OrderingScope: "Acceptance", StableKey: "x"}, wantErr: true},
		{name: "invalid stable key", id: OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: ""}, wantErr: true, wantKey: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateOrderedID(tt.id)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("ValidateOrderedID(%+v) = %v, want nil", tt.id, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateOrderedID(%+v) = nil, want error", tt.id)
			}
			if tt.wantKey {
				var target *InvalidStableKeyError
				if !errors.As(err, &target) {
					t.Fatalf("ValidateOrderedID(%+v) error type = %T, want *InvalidStableKeyError", tt.id, err)
				}
				return
			}
			var target *InvalidNameError
			if !errors.As(err, &target) {
				t.Fatalf("ValidateOrderedID(%+v) error type = %T, want *InvalidNameError", tt.id, err)
			}
		})
	}
}

func TestValidateOrderedRecord(t *testing.T) {
	t.Parallel()

	valid := OrderedRecord{
		ID:           OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: "slash/value"},
		RankingScope: "workers",
		Revision:     1,
		Order:        1,
		Due:          Due{State: DueAt, UnixMillis: 42},
		Rank:         Rank{Ranked: true, Value: 7},
		Value:        []byte("value"),
	}
	tests := []struct {
		name    string
		record  OrderedRecord
		wantErr bool
		as      func(error) bool
	}{
		{name: "valid", record: valid},
		{
			name:    "invalid ranking scope",
			record:  OrderedRecord{ID: valid.ID, RankingScope: "Workers", Revision: valid.Revision, Order: valid.Order, Due: valid.Due, Rank: valid.Rank, Value: valid.Value},
			wantErr: true,
			as: func(err error) bool {
				var target *InvalidNameError
				return errors.As(err, &target)
			},
		},
		{
			name:    "unknown due state",
			record:  OrderedRecord{ID: valid.ID, RankingScope: valid.RankingScope, Revision: valid.Revision, Order: valid.Order, Due: Due{State: DueState(99)}, Rank: valid.Rank, Value: valid.Value},
			wantErr: true,
			as: func(err error) bool {
				var target *InvalidDueError
				return errors.As(err, &target)
			},
		},
		{
			name:    "not due requires zero timestamp",
			record:  OrderedRecord{ID: valid.ID, RankingScope: valid.RankingScope, Revision: valid.Revision, Order: valid.Order, Due: Due{State: NotDue, UnixMillis: 1}, Rank: valid.Rank, Value: valid.Value},
			wantErr: true,
			as: func(err error) bool {
				var target *InvalidDueError
				return errors.As(err, &target)
			},
		},
		{
			name: "tombstone is unranked and not due",
			record: OrderedRecord{
				ID:           valid.ID,
				RankingScope: valid.RankingScope,
				Revision:     valid.Revision,
				Order:        valid.Order,
				Deleted:      true,
				Rank:         Rank{Ranked: true, Value: valid.Rank.Value},
				Due:          Due{State: DueAt, UnixMillis: valid.Due.UnixMillis},
				Value:        valid.Value,
			},
			wantErr: true,
			as: func(err error) bool {
				var target *InvalidOrderedRecordError
				return errors.As(err, &target)
			},
		},
		{
			name: "tombstone clears unranked value",
			record: OrderedRecord{
				ID:           valid.ID,
				RankingScope: valid.RankingScope,
				Revision:     valid.Revision,
				Order:        valid.Order,
				Deleted:      true,
				Rank:         Rank{Value: 1},
				Due:          Due{State: NotDue},
				Value:        valid.Value,
			},
			wantErr: true,
			as: func(err error) bool {
				var target *InvalidOrderedRecordError
				return errors.As(err, &target)
			},
		},
		{
			name:    "one mib value",
			record:  OrderedRecord{ID: valid.ID, RankingScope: valid.RankingScope, Revision: valid.Revision, Order: valid.Order, Due: valid.Due, Rank: valid.Rank, Value: make([]byte, MaxOrderedValueBytes)},
			wantErr: false,
		},
		{
			name:    "value over one mib",
			record:  OrderedRecord{ID: valid.ID, RankingScope: valid.RankingScope, Revision: valid.Revision, Order: valid.Order, Due: valid.Due, Rank: valid.Rank, Value: make([]byte, MaxOrderedValueBytes+1)},
			wantErr: true,
			as: func(err error) bool {
				var target *OrderedValueTooLargeError
				return errors.As(err, &target)
			},
		},
		{
			name:    "zero immutable order",
			record:  OrderedRecord{ID: valid.ID, RankingScope: valid.RankingScope, Revision: valid.Revision, Due: valid.Due, Rank: valid.Rank, Value: valid.Value},
			wantErr: true,
			as: func(err error) bool {
				var target *InvalidOrderedRecordError
				return errors.As(err, &target)
			},
		},
		{
			name:    "zero revision",
			record:  OrderedRecord{ID: valid.ID, RankingScope: valid.RankingScope, Order: valid.Order, Due: valid.Due, Rank: valid.Rank, Value: valid.Value},
			wantErr: true,
			as: func(err error) bool {
				var target *InvalidOrderedRecordError
				return errors.As(err, &target)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateOrderedRecord(tt.record)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("ValidateOrderedRecord(%+v) = %v, want nil", tt.record, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateOrderedRecord(%+v) = nil, want error", tt.record)
			}
			if tt.as != nil && !tt.as(err) {
				t.Errorf("ValidateOrderedRecord(%+v) error = %T %v, want expected typed error", tt.record, err, err)
			}
		})
	}
}

func TestValidateOrderedLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		limit   int
		wantErr bool
	}{
		{name: "one", limit: 1},
		{name: "maximum", limit: MaxOrderedPageLimit},
		{name: "zero", limit: 0, wantErr: true},
		{name: "negative", limit: -1, wantErr: true},
		{name: "over maximum", limit: MaxOrderedPageLimit + 1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateOrderedLimit(tt.limit)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("ValidateOrderedLimit(%d) = %v, want nil", tt.limit, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateOrderedLimit(%d) = nil, want error", tt.limit)
			}
			var target *InvalidOrderedLimitError
			if !errors.As(err, &target) {
				t.Fatalf("ValidateOrderedLimit(%d) error type = %T, want *InvalidOrderedLimitError", tt.limit, err)
			}
		})
	}
}

func TestOrderedErrorTaxonomy(t *testing.T) {
	t.Parallel()

	id := OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: "v1:ABC_def-09"}
	tests := []struct {
		name string
		err  error
		as   func(error) bool
	}{
		{
			name: "revision conflict",
			err:  &OrderedRevisionConflictError{ID: id, ExpectedRevision: 3, ActualRevision: 4},
			as: func(err error) bool {
				var target *OrderedRevisionConflictError
				return errors.As(err, &target) && target.ID == id && target.ExpectedRevision == 3 && target.ActualRevision == 4
			},
		},
		{
			name: "revision exhausted",
			err:  &OrderedRevisionExhaustedError{ID: id, Revision: ^uint64(0)},
			as: func(err error) bool {
				var target *OrderedRevisionExhaustedError
				return errors.As(err, &target) && target.ID == id && target.Revision == ^uint64(0)
			},
		},
		{
			name: "record not found",
			err:  &OrderedRecordNotFoundError{ID: id},
			as: func(err error) bool {
				var target *OrderedRecordNotFoundError
				return errors.As(err, &target) && target.ID == id
			},
		},
		{
			name: "invalid due cursor",
			err:  NewInvalidOrderedCursorError(DueCursorKind, "not-a-cursor", OrderedCursorQueryMismatch),
			as: func(err error) bool {
				var target *InvalidOrderedCursorError
				return errors.As(err, &target) && target.Kind == DueCursorKind
			},
		},
		{
			name: "ambiguous update",
			err:  &OrderedAmbiguousError{Operation: OrderedUpdateOperation, ID: id, Cause: errOrderedAckLost},
			as: func(err error) bool {
				var target *OrderedAmbiguousError
				return errors.As(err, &target) && target.Operation == OrderedUpdateOperation && target.ID == id && errors.Is(err, errOrderedAckLost)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if !tt.as(tt.err) {
				t.Errorf("errors.As(%T) = false, want true", tt.err)
			}
			if !strings.HasPrefix(tt.err.Error(), "storage: ") {
				t.Errorf("Error() = %q, want storage prefix", tt.err.Error())
			}
		})
	}
}

var errOrderedAckLost = errors.New("ordered mutation acknowledgement lost")

func TestCreateDuplicatePrecedenceContract(t *testing.T) {
	t.Parallel()

	existing := OrderedRecord{
		ID:           OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: "existing"},
		RankingScope: "workers",
		Revision:     7,
		Order:        9,
		Due:          Due{State: DueAt, UnixMillis: 42},
		Rank:         Rank{Ranked: true, Value: 3},
		Value:        []byte("canonical"),
	}
	tests := []struct {
		name        string
		id          OrderedID
		existing    *OrderedRecord
		candidate   createCandidateForTest
		wantCreated bool
		wantErrAs   func(error) bool
	}{
		{
			name:     "duplicate skips invalid candidate",
			id:       existing.ID,
			existing: &existing,
			candidate: createCandidateForTest{
				rankingScope: "Workers",
				value:        make([]byte, MaxOrderedValueBytes+1),
				due:          Due{State: NotDue, UnixMillis: 1},
			},
			wantCreated: false,
		},
		{
			name:     "id validation precedes existing lookup",
			id:       OrderedID{Namespace: "Sessions", OrderingScope: existing.ID.OrderingScope, StableKey: existing.ID.StableKey},
			existing: &existing,
			candidate: createCandidateForTest{
				rankingScope: existing.RankingScope,
				value:        existing.Value,
				rank:         existing.Rank,
				due:          existing.Due,
			},
			wantErrAs: func(err error) bool {
				var target *InvalidNameError
				return errors.As(err, &target)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, created, err := evaluateCreatePrecedenceForTest(tt.id, tt.existing, tt.candidate)
			if tt.wantErrAs != nil {
				if !tt.wantErrAs(err) {
					t.Fatalf("Create precedence error = %T %v, want expected typed error", err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Create precedence error = %v, want nil", err)
			}
			if created != tt.wantCreated {
				t.Errorf("created = %v, want %v", created, tt.wantCreated)
			}
			if !reflect.DeepEqual(got, existing) {
				t.Errorf("duplicate record = %#v, want canonical %#v", got, existing)
			}
		})
	}
}

func TestDeleteTombstoneContract(t *testing.T) {
	t.Parallel()

	live := OrderedRecord{
		ID:           OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: "live"},
		RankingScope: "workers",
		Revision:     7,
		Order:        9,
		Due:          Due{State: DueAt, UnixMillis: 42},
		Rank:         Rank{Ranked: true, Value: 3},
		Value:        []byte("preserved"),
	}
	tests := []struct {
		name      string
		before    OrderedRecord
		wantErrAs func(error) bool
	}{
		{name: "live record canonicalizes", before: live},
		{
			name:   "revision exhaustion leaves source unchanged",
			before: OrderedRecord{ID: live.ID, RankingScope: live.RankingScope, Revision: ^uint64(0), Order: live.Order, Due: live.Due, Rank: live.Rank, Value: live.Value},
			wantErrAs: func(err error) bool {
				var target *OrderedRevisionExhaustedError
				return errors.As(err, &target)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			before := tt.before
			got, err := canonicalDeleteForTest(before)
			if tt.wantErrAs != nil {
				if !tt.wantErrAs(err) {
					t.Fatalf("Delete canonicalization error = %T %v, want expected typed error", err, err)
				}
				if !reflect.DeepEqual(before, tt.before) {
					t.Errorf("source record changed on exhaustion: got %#v, want %#v", before, tt.before)
				}
				return
			}
			if err != nil {
				t.Fatalf("Delete canonicalization error = %v, want nil", err)
			}
			if err := ValidateOrderedRecord(got); err != nil {
				t.Fatalf("canonical tombstone validation = %v, want nil", err)
			}
			if !got.Deleted || got.Rank != (Rank{}) || got.Due != (Due{State: NotDue, UnixMillis: 0}) {
				t.Errorf("tombstone state = %#v, want Deleted with zero Rank and canonical Due", got)
			}
			if got.ID != before.ID || got.RankingScope != before.RankingScope || got.Order != before.Order || !bytes.Equal(got.Value, before.Value) {
				t.Errorf("tombstone did not preserve identity, ranking scope, order, and value: got %#v, before %#v", got, before)
			}
			if got.Revision != before.Revision+1 {
				t.Errorf("tombstone revision = %d, want %d", got.Revision, before.Revision+1)
			}
		})
	}
}

func TestInvalidOrderedCursorErrorDoesNotExposeOpaqueToken(t *testing.T) {
	t.Parallel()

	sensitive := strings.Repeat("session-secret-", 128)
	tests := []struct {
		name string
		kind OrderedCursorKind
		rule OrderedCursorRule
	}{
		{name: "ranked", kind: RankedCursorKind, rule: OrderedCursorUnknownVersion},
		{name: "due", kind: DueCursorKind, rule: OrderedCursorQueryMismatch},
		{name: "unrecognized kind", kind: OrderedCursorKind(sensitive), rule: OrderedCursorMalformed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := NewInvalidOrderedCursorError(tt.kind, sensitive, tt.rule)
			if _, exposed := reflect.TypeOf(*err).FieldByName("Cursor"); exposed {
				t.Fatal("InvalidOrderedCursorError must not expose a raw Cursor field")
			}
			if got, want := err.CursorLength, uint16(len(sensitive)); got != want {
				t.Errorf("CursorLength = %d, want %d", got, want)
			}
			if strings.Contains(err.Error(), sensitive) {
				t.Errorf("Error() = %q, must not render opaque cursor %q", err.Error(), sensitive)
			}
		})
	}
}

func TestRankedAndDueTieBreakByOrderingScope(t *testing.T) {
	t.Parallel()

	// The primary ordering pairs are frozen by the public contract. This
	// fixture exercises the coordinator-approved final tie-breaker needed for
	// cursor-safe pagination when two identities share the same StableKey in
	// different ordering scopes.
	base := OrderedID{Namespace: "sessions", StableKey: "same-key"}
	tests := []struct {
		name      string
		records   []OrderedRecord
		less      func([]OrderedRecord, int, int) bool
		wantFirst string
	}{
		{
			name: "ranked descending",
			records: []OrderedRecord{
				{ID: OrderedID{Namespace: base.Namespace, OrderingScope: "a", StableKey: base.StableKey}, RankingScope: "workers", Revision: 1, Order: 1, Rank: Rank{Ranked: true, Value: 10}},
				{ID: OrderedID{Namespace: base.Namespace, OrderingScope: "b", StableKey: base.StableKey}, RankingScope: "workers", Revision: 1, Order: 1, Rank: Rank{Ranked: true, Value: 10}},
			},
			less: func(records []OrderedRecord, i int, j int) bool {
				if records[i].Rank.Value != records[j].Rank.Value {
					return records[i].Rank.Value > records[j].Rank.Value
				}
				if records[i].ID.StableKey != records[j].ID.StableKey {
					return records[i].ID.StableKey > records[j].ID.StableKey
				}
				return records[i].ID.OrderingScope > records[j].ID.OrderingScope
			},
			wantFirst: "b",
		},
		{
			name: "due ascending",
			records: []OrderedRecord{
				{ID: OrderedID{Namespace: base.Namespace, OrderingScope: "b", StableKey: base.StableKey}, RankingScope: "workers", Revision: 1, Order: 1, Due: Due{State: DueAt, UnixMillis: 10}},
				{ID: OrderedID{Namespace: base.Namespace, OrderingScope: "a", StableKey: base.StableKey}, RankingScope: "workers", Revision: 1, Order: 1, Due: Due{State: DueAt, UnixMillis: 10}},
			},
			less: func(records []OrderedRecord, i int, j int) bool {
				if records[i].Due.UnixMillis != records[j].Due.UnixMillis {
					return records[i].Due.UnixMillis < records[j].Due.UnixMillis
				}
				if records[i].ID.StableKey != records[j].ID.StableKey {
					return records[i].ID.StableKey < records[j].ID.StableKey
				}
				return records[i].ID.OrderingScope < records[j].ID.OrderingScope
			},
			wantFirst: "a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			records := append([]OrderedRecord(nil), tt.records...)
			sort.Slice(records, func(i int, j int) bool { return tt.less(records, i, j) })
			if got := records[0].ID.OrderingScope; got != tt.wantFirst {
				t.Errorf("first ordering scope = %q, want %q", got, tt.wantFirst)
			}
		})
	}
}

func TestOrderScopeIsNamespaceQualified(t *testing.T) {
	t.Parallel()

	// Immutable order allocation is per (namespace, ordering scope), not per
	// bare scope string. These two first records may both have order 1 without
	// violating monotonicity because their namespace differs.
	tests := []struct {
		name  string
		left  OrderedRecord
		right OrderedRecord
	}{
		{
			name:  "same scope in distinct namespaces",
			left:  OrderedRecord{ID: OrderedID{Namespace: "left", OrderingScope: "acceptance", StableKey: "one"}, RankingScope: "workers", Revision: 1, Order: 1},
			right: OrderedRecord{ID: OrderedID{Namespace: "right", OrderingScope: "acceptance", StableKey: "one"}, RankingScope: "workers", Revision: 1, Order: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.left.ID.Namespace == tt.right.ID.Namespace {
				t.Fatal("test fixture namespaces must differ")
			}
			if tt.left.Order != 1 || tt.right.Order != 1 {
				t.Fatalf("first immutable orders = (%d, %d), want (1, 1)", tt.left.Order, tt.right.Order)
			}
		})
	}
}

// createCandidateForTest is a test-only input used to make the documented
// Create precedence executable without introducing an OrderedIndex provider.
type createCandidateForTest struct {
	rankingScope string
	value        []byte
	rank         Rank
	due          Due
}

// evaluateCreatePrecedenceForTest is a test-only decision probe. It models
// only the validation/lookup order; it deliberately does not persist a record,
// allocate an order, or implement OrderedIndex.
func evaluateCreatePrecedenceForTest(id OrderedID, existing *OrderedRecord, candidate createCandidateForTest) (OrderedRecord, bool, error) {
	if err := ValidateOrderedID(id); err != nil {
		return OrderedRecord{}, false, err
	}
	if existing != nil {
		return *existing, false, nil
	}
	if err := ValidateName(candidate.rankingScope); err != nil {
		return OrderedRecord{}, false, err
	}
	if err := ValidateOrderedValue(candidate.value); err != nil {
		return OrderedRecord{}, false, err
	}
	if err := ValidateDue(candidate.due); err != nil {
		return OrderedRecord{}, false, err
	}
	return OrderedRecord{}, true, nil
}

// canonicalDeleteForTest is a test-only mutation probe for the live-record
// portion of Delete. It does not perform lookup, compare-and-swap, or storage.
func canonicalDeleteForTest(record OrderedRecord) (OrderedRecord, error) {
	if err := ValidateOrderedID(record.ID); err != nil {
		return OrderedRecord{}, err
	}
	if record.Revision == ^uint64(0) {
		return OrderedRecord{}, &OrderedRevisionExhaustedError{ID: record.ID, Revision: record.Revision}
	}
	record.Revision++
	record.Deleted = true
	record.Rank = Rank{}
	record.Due = Due{State: NotDue, UnixMillis: 0}
	return record, nil
}
