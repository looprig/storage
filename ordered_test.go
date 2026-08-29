package storage

import (
	"context"
	"errors"
	"reflect"
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
		name     string
		key      StableKey
		wantErr  bool
		wantRule string
	}{
		{name: "versioned opaque key", key: StableKey("v1:ABC_def-09")},
		{name: "slash is opaque", key: StableKey("slash/value")},
		{name: "uppercase is opaque", key: StableKey("UPPERCASE")},
		{name: "embedded unicode is opaque", key: StableKey("caf\u00e9/\u4e16\u754c")},
		{name: "one byte", key: StableKey("x")},
		{name: "exactly 256 bytes", key: StableKey(strings.Repeat("x", 256))},
		{name: "empty", key: "", wantErr: true, wantRule: "empty"},
		{name: "invalid utf8", key: invalidUTF8, wantErr: true, wantRule: "invalid UTF-8"},
		{name: "257 bytes", key: StableKey(strings.Repeat("x", 257)), wantErr: true, wantRule: "too long"},
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
			if target.Rule != tt.wantRule {
				t.Errorf("InvalidStableKeyError.Rule = %q, want %q", target.Rule, tt.wantRule)
			}
		})
	}
}

func TestInvalidStableKeyErrorDoesNotExposeRawInput(t *testing.T) {
	t.Parallel()

	sensitive := StableKey(strings.Repeat("stable-key-secret-", 4096))
	err := ValidateStableKey(sensitive)
	var target *InvalidStableKeyError
	if !errors.As(err, &target) {
		t.Fatalf("ValidateStableKey() error = %T %v, want *InvalidStableKeyError", err, err)
	}
	if target.Rule != "too long" {
		t.Fatalf("InvalidStableKeyError.Rule = %q, want too long", target.Rule)
	}
	if strings.Contains(target.Error(), string(sensitive)) {
		t.Fatal("InvalidStableKeyError.Error() must not render the raw StableKey")
	}

	errorType := reflect.TypeOf(*target)
	if _, exposed := errorType.FieldByName("StableKey"); exposed {
		t.Fatal("InvalidStableKeyError must not retain a raw StableKey field")
	}
	lengthField, found := errorType.FieldByName("StableKeyLength")
	if !found {
		t.Fatal("InvalidStableKeyError must retain a capped StableKeyLength diagnostic")
	}
	if got, want := reflect.ValueOf(*target).FieldByIndex(lengthField.Index).Uint(), uint64(1<<16-1); got != want {
		t.Errorf("InvalidStableKeyError.StableKeyLength = %d, want capped %d", got, want)
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
		name               string
		err                error
		as                 func(error) bool
		wantErrorSubstring string
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
			name:               "revision conflict actual unknown",
			err:                &OrderedRevisionConflictError{ID: id, ExpectedRevision: 3},
			wantErrorSubstring: "actual unknown",
			as: func(err error) bool {
				var target *OrderedRevisionConflictError
				return errors.As(err, &target) && target.ID == id && target.ExpectedRevision == 3 && target.ActualRevision == 0
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
			if tt.wantErrorSubstring != "" && !strings.Contains(tt.err.Error(), tt.wantErrorSubstring) {
				t.Errorf("Error() = %q, want it to contain %q", tt.err.Error(), tt.wantErrorSubstring)
			}
		})
	}
}

var errOrderedAckLost = errors.New("ordered mutation acknowledgement lost")

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

// TestOrderedCursorRuleSetMatchesContract pins the rule enumeration to the four
// rules the frozen OrderedIndex contract defines. Any additional constant is a
// classification providers may never produce, so the first value past
// OrderedCursorQueryMismatch must render as an unrecognized rule.
func TestOrderedCursorRuleSetMatchesContract(t *testing.T) {
	t.Parallel()

	want := map[OrderedCursorRule]string{
		OrderedCursorMalformed:      "malformed",
		OrderedCursorUnknownVersion: "unknown version",
		OrderedCursorWrongKind:      "wrong kind",
		OrderedCursorQueryMismatch:  "query mismatch",
	}
	for rule, wantString := range want {
		if got := rule.String(); got != wantString {
			t.Errorf("OrderedCursorRule(%d).String() = %q, want %q", rule, got, wantString)
		}
	}
	if got := (OrderedCursorQueryMismatch + 1).String(); got != "invalid" {
		t.Errorf("OrderedCursorRule(%d).String() = %q, want %q; the contract defines exactly four rules", OrderedCursorQueryMismatch+1, got, "invalid")
	}
}
