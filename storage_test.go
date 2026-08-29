package storage

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

// Compile-time assertion for the one primitive *Composite genuinely satisfies
// through embedding: Leaser (its Acquire method is unique among the four).
//
// NOTE: *Composite CANNOT satisfy Ledger, KV, or Blobs by embedding, and the
// combined interface{ Ledger; Leaser; KV; Blobs } is not even a legal type — so
// the "satisfies all four at once" assertion the spec calls for cannot compile:
//   - Ledger, KV, and Blobs each declare Delete(ctx, string) error, so the
//     promoted *Composite.Delete selector is ambiguous (go vet: "ambiguous
//     selector *Composite.Delete") and is not in the type's method set.
//   - KV and Blobs both declare Get and Put with DIFFERENT signatures, so
//     embedding both in one interface is a "duplicate method" compile error.
//
// The Composite struct is still usable via its named fields (c.Ledger.Delete,
// c.KV.Delete, …); only single-value satisfaction of all four is impossible.
// See the task report for the design options to resolve this.
var _ Leaser = (*Composite)(nil)

// The stub types below satisfy each primitive interface so NewComposite can be
// exercised without a real backend. Their method bodies are never invoked
// (NewComposite only checks for nil), so they panic to make an accidental call
// loud rather than silently wrong.

type stubLedger struct{}

func (*stubLedger) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	panic("stubLedger.Append not called")
}
func (*stubLedger) Read(ctx context.Context, name string, from uint64) (Cursor, error) {
	panic("stubLedger.Read not called")
}
func (*stubLedger) Tip(ctx context.Context, name string) (uint64, error) {
	panic("stubLedger.Tip not called")
}
func (*stubLedger) Delete(ctx context.Context, name string) error {
	panic("stubLedger.Delete not called")
}

type stubLeaser struct{}

func (*stubLeaser) Acquire(ctx context.Context, name string) (Lease, error) {
	panic("stubLeaser.Acquire not called")
}

type stubKV struct{}

func (*stubKV) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	panic("stubKV.Get not called")
}
func (*stubKV) Put(ctx context.Context, key string, expectedRev uint64, val []byte) (uint64, error) {
	panic("stubKV.Put not called")
}
func (*stubKV) Keys(ctx context.Context, prefix string) ([]string, error) {
	panic("stubKV.Keys not called")
}
func (*stubKV) Delete(ctx context.Context, key string) error {
	panic("stubKV.Delete not called")
}

type stubBlobs struct{}

func (*stubBlobs) Put(ctx context.Context, key string, r io.Reader) error {
	panic("stubBlobs.Put not called")
}
func (*stubBlobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	panic("stubBlobs.Get not called")
}
func (*stubBlobs) Delete(ctx context.Context, key string) error {
	panic("stubBlobs.Delete not called")
}
func (*stubBlobs) List(ctx context.Context, prefix string) ([]string, error) {
	panic("stubBlobs.List not called")
}

// stubOrderedIndex satisfies OrderedIndex without backing storage so the
// Composite constructors can be exercised without coupling this contract test
// to a particular backend.
type stubOrderedIndex struct{}

func (*stubOrderedIndex) Get(ctx context.Context, id OrderedID) (OrderedRecord, error) {
	panic("stubOrderedIndex.Get not called")
}

func (*stubOrderedIndex) Create(ctx context.Context, id OrderedID, rankingScope string, value []byte, rank Rank, due Due) (OrderedRecord, bool, error) {
	panic("stubOrderedIndex.Create not called")
}

func (*stubOrderedIndex) Update(ctx context.Context, id OrderedID, expectedRevision uint64, value []byte, rank Rank, due Due) (OrderedRecord, error) {
	panic("stubOrderedIndex.Update not called")
}

func (*stubOrderedIndex) Delete(ctx context.Context, id OrderedID, expectedRevision uint64) (OrderedRecord, error) {
	panic("stubOrderedIndex.Delete not called")
}

func (*stubOrderedIndex) ListOrdered(ctx context.Context, namespace string, orderingScope string, afterOrder uint64, limit int) (OrderedPage, error) {
	panic("stubOrderedIndex.ListOrdered not called")
}

func (*stubOrderedIndex) ListRanked(ctx context.Context, namespace string, rankingScope string, after RankedCursor, limit int) (RankedPage, error) {
	panic("stubOrderedIndex.ListRanked not called")
}

func (*stubOrderedIndex) ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after DueCursor, limit int) (DuePage, error) {
	panic("stubOrderedIndex.ListDue not called")
}

func TestNewComposite(t *testing.T) {
	t.Parallel()

	// Distinct, comparable providers so the happy path can assert the returned
	// Composite embeds exactly the values it was handed.
	l := &stubLedger{}
	le := &stubLeaser{}
	kv := &stubKV{}
	bl := &stubBlobs{}

	tests := []struct {
		name string
		l    Ledger
		le   Leaser
		kv   KV
		bl   Blobs
		// wantMissing is nil on the happy path; otherwise the exact Missing
		// slice (field order: Ledger, Leaser, KV, Blobs) that
		// *IncompleteCompositeError must carry.
		wantMissing []string
	}{
		{name: "all present", l: l, le: le, kv: kv, bl: bl},
		{name: "nil ledger", le: le, kv: kv, bl: bl, wantMissing: []string{"Ledger"}},
		{name: "nil leaser", l: l, kv: kv, bl: bl, wantMissing: []string{"Leaser"}},
		{name: "nil kv", l: l, le: le, bl: bl, wantMissing: []string{"KV"}},
		{name: "nil blobs", l: l, le: le, kv: kv, wantMissing: []string{"Blobs"}},
		{name: "ledger and kv nil", le: le, bl: bl, wantMissing: []string{"Ledger", "KV"}},
		{name: "all nil", wantMissing: []string{"Ledger", "Leaser", "KV", "Blobs"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, err := NewComposite(tt.l, tt.le, tt.kv, tt.bl)

			if tt.wantMissing == nil {
				// Happy path: non-nil Composite, nil error, and the embedded
				// fields are exactly the providers passed in.
				if err != nil {
					t.Fatalf("NewComposite() unexpected error: %v", err)
				}
				if c == nil {
					t.Fatal("NewComposite() returned nil Composite on success")
				}
				if c.Ledger != tt.l {
					t.Errorf("Composite.Ledger = %v, want %v", c.Ledger, tt.l)
				}
				if c.Leaser != tt.le {
					t.Errorf("Composite.Leaser = %v, want %v", c.Leaser, tt.le)
				}
				if c.KV != tt.kv {
					t.Errorf("Composite.KV = %v, want %v", c.KV, tt.kv)
				}
				if c.Blobs != tt.bl {
					t.Errorf("Composite.Blobs = %v, want %v", c.Blobs, tt.bl)
				}
				if c.OrderedIndex != nil {
					t.Errorf("Composite.OrderedIndex = %v, want nil from legacy NewComposite", c.OrderedIndex)
				}
				return
			}

			// Error path: nil Composite and an *IncompleteCompositeError whose
			// Missing set is exactly the expected names, in field order.
			if c != nil {
				t.Errorf("NewComposite() returned non-nil Composite on error: %#v", c)
			}
			var ice *IncompleteCompositeError
			if !errors.As(err, &ice) {
				t.Fatalf("NewComposite() error = %v, want *IncompleteCompositeError", err)
			}
			if !reflect.DeepEqual(ice.Missing, tt.wantMissing) {
				t.Errorf("Missing = %v, want exactly %v", ice.Missing, tt.wantMissing)
			}

			// The message must be prefixed like the rest of the taxonomy and
			// name every missing primitive.
			msg := ice.Error()
			if !strings.HasPrefix(msg, "storage: ") {
				t.Errorf("Error() = %q, want prefix %q", msg, "storage: ")
			}
			for _, want := range tt.wantMissing {
				if !strings.Contains(msg, want) {
					t.Errorf("Error() = %q, want it to contain %q", msg, want)
				}
			}
		})
	}
}

func TestNewCompositeWithOrderedIndex(t *testing.T) {
	t.Parallel()

	l := &stubLedger{}
	le := &stubLeaser{}
	kv := &stubKV{}
	bl := &stubBlobs{}
	oi := &stubOrderedIndex{}

	tests := []struct {
		name        string
		l           Ledger
		le          Leaser
		kv          KV
		bl          Blobs
		oi          OrderedIndex
		wantMissing []string
	}{
		{name: "all present", l: l, le: le, kv: kv, bl: bl, oi: oi},
		{name: "nil ordered index", l: l, le: le, kv: kv, bl: bl, wantMissing: []string{"OrderedIndex"}},
		{name: "ledger and ordered index nil", le: le, kv: kv, bl: bl, wantMissing: []string{"Ledger", "OrderedIndex"}},
		{name: "all nil", wantMissing: []string{"Ledger", "Leaser", "KV", "Blobs", "OrderedIndex"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, err := NewCompositeWithOrderedIndex(tt.l, tt.le, tt.kv, tt.bl, tt.oi)

			if tt.wantMissing == nil {
				if err != nil {
					t.Fatalf("NewCompositeWithOrderedIndex() unexpected error: %v", err)
				}
				if c == nil {
					t.Fatal("NewCompositeWithOrderedIndex() returned nil Composite on success")
				}
				if c.Ledger != tt.l || c.Leaser != tt.le || c.KV != tt.kv || c.Blobs != tt.bl || c.OrderedIndex != tt.oi {
					t.Errorf("NewCompositeWithOrderedIndex() returned wrong providers: %#v", c)
				}
				return
			}

			if c != nil {
				t.Errorf("NewCompositeWithOrderedIndex() returned non-nil Composite on error: %#v", c)
			}
			var ice *IncompleteCompositeError
			if !errors.As(err, &ice) {
				t.Fatalf("NewCompositeWithOrderedIndex() error = %v, want *IncompleteCompositeError", err)
			}
			if !reflect.DeepEqual(ice.Missing, tt.wantMissing) {
				t.Errorf("Missing = %v, want exactly %v", ice.Missing, tt.wantMissing)
			}
			if !strings.Contains(ice.Error(), "OrderedIndex") && strings.Contains(strings.Join(tt.wantMissing, " "), "OrderedIndex") {
				t.Errorf("Error() = %q, want it to name OrderedIndex", ice.Error())
			}
		})
	}
}
