package storetest

import (
	"strings"
	"testing"

	"github.com/looprig/storage"
)

// TestOrderedIndexRecordDifferenceNamesDivergingFields covers the failure
// message provider authors read most often. requireOrderedIndexRecordEqual is
// the suite's most-used assertion, and nearly every call compares a record with
// its own expected form, so the two records share an ID: a message built from
// IDs alone names nothing. The difference string must therefore name the
// diverging field, and must locate a Value divergence by byte, because the
// record summary prints only the value's length.
func TestOrderedIndexRecordDifferenceNamesDivergingFields(t *testing.T) {
	t.Parallel()

	base := storage.OrderedRecord{
		ID:           storage.OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: "key"},
		RankingScope: "workers",
		Revision:     3,
		Order:        7,
		Due:          storage.Due{State: storage.DueAt, UnixMillis: 11},
		Rank:         storage.Rank{Ranked: true, Value: 5},
		Value:        []byte("value"),
	}
	mutate := func(change func(*storage.OrderedRecord)) storage.OrderedRecord {
		record := copyOrderedIndexRecord(base)
		change(&record)
		return record
	}

	for _, test := range []struct {
		name  string
		got   storage.OrderedRecord
		want  []string
		equal bool
	}{
		{name: "identical", got: copyOrderedIndexRecord(base), equal: true},
		{name: "namespace", got: mutate(func(r *storage.OrderedRecord) { r.ID.Namespace = "other" }), want: []string{"ID", "other"}},
		{name: "ranking scope", got: mutate(func(r *storage.OrderedRecord) { r.RankingScope = "other" }), want: []string{"RankingScope", "other"}},
		{name: "revision", got: mutate(func(r *storage.OrderedRecord) { r.Revision = 4 }), want: []string{"Revision", "4", "3"}},
		{name: "order", got: mutate(func(r *storage.OrderedRecord) { r.Order = 8 }), want: []string{"Order", "8", "7"}},
		{name: "due", got: mutate(func(r *storage.OrderedRecord) { r.Due.UnixMillis = 12 }), want: []string{"Due", "12"}},
		{name: "rank", got: mutate(func(r *storage.OrderedRecord) { r.Rank.Value = 6 }), want: []string{"Rank", "6"}},
		{name: "deleted", got: mutate(func(r *storage.OrderedRecord) { r.Deleted = true }), want: []string{"Deleted", "true"}},
		{name: "value byte", got: mutate(func(r *storage.OrderedRecord) { r.Value[2] = 'X' }), want: []string{"Value", "byte 2", "0x58", "0x6c"}},
		{name: "value length", got: mutate(func(r *storage.OrderedRecord) { r.Value = []byte("val") }), want: []string{"Value", "length", "3", "5"}},
		{name: "value nil versus empty", got: mutate(func(r *storage.OrderedRecord) { r.Value = nil }), want: []string{"Value", "length", "0"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			difference := orderedIndexRecordDifference(test.got, base)
			if test.equal {
				if difference != "" {
					t.Errorf("orderedIndexRecordDifference(identical) = %q, want empty", difference)
				}
				return
			}
			if difference == "" {
				t.Fatal("orderedIndexRecordDifference(differing records) = \"\", want a named divergence")
			}
			for _, fragment := range test.want {
				if !strings.Contains(difference, fragment) {
					t.Errorf("orderedIndexRecordDifference = %q, want it to mention %q", difference, fragment)
				}
			}
		})
	}
}

// TestOrderedIndexRecordDifferenceSeparatesNilAndEmptyValue covers the one
// divergence reflect.DeepEqual reports and bytes.Equal does not, so the
// assertion never falls through to its "walk is out of date" fallback.
func TestOrderedIndexRecordDifferenceSeparatesNilAndEmptyValue(t *testing.T) {
	t.Parallel()

	nilValue := storage.OrderedRecord{Value: nil}
	emptyValue := storage.OrderedRecord{Value: []byte{}}
	difference := orderedIndexRecordDifference(nilValue, emptyValue)
	if !strings.Contains(difference, "Value") || !strings.Contains(difference, "nil") {
		t.Errorf("orderedIndexRecordDifference(nil, empty) = %q, want it to name the nil Value", difference)
	}
}
