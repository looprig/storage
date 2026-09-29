package storetest

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/looprig/storage"
)

// dottedCaseName is the subtest group every suite registers for the
// dotted-extension rule: a valid name, the name with a dotted extension
// ("a.<ext>"), and a name extending that with "/…" ("a.<ext>/b") are three
// distinct names and must coexist, in any creation order.
//
// The '/'-extension rows alone pass against a backend that maps a name to a
// "<path>.<ext>" file, because "a" → "a.ext" and "a/b" → "a/b.ext" never
// contend. The contending pair is "a" beside "a.<ext>/b" (and "a.<ext>" beside
// "a.<ext>/b"). A generic backend's suffix is unknowable, so the rows range
// over a spread of plausible ones; a name that merely looks like an encoded
// leaf must round-trip as itself.
const dottedCaseName = "a name, its dotted extension and that extension's '/' extension coexist"

// dottedExtensions is the spread of dotted suffixes the dotted-extension rows
// exercise: every suffix a known backend has used for a leaf, common file
// extensions, a one-letter one, and a multi-dot one.
var dottedExtensions = []string{".log", ".lock", ".olog", ".kv", ".blob", ".json", ".dat", ".tmp", ".x", ".a.b"}

const dottedBase = "sessions/a"

// dottedNames returns, for ext, the base name, its dotted extension and that
// extension's '/' extension, in that (sorted) order: '.' sorts before '/'.
func dottedNames(ext string) []string {
	return []string{dottedBase, dottedBase + ext, dottedBase + ext + "/b"}
}

// reversed returns a reversed copy of names.
func reversed(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[len(names)-1-i] = n
	}
	return out
}

// dottedCase is one extension in one creation order.
type dottedCase struct {
	name      string
	ext       string
	names     []string // creation order
	deleteKey string
}

// dottedCases crosses every extension with both creation orders; the deleted
// name rotates across the three so every position is deleted by some row.
func dottedCases() []dottedCase {
	out := make([]dottedCase, 0, 2*len(dottedExtensions))
	for i, ext := range dottedExtensions {
		names := dottedNames(ext)
		out = append(out,
			dottedCase{name: ext + " shortest first", ext: ext, names: names, deleteKey: names[i%3]},
			dottedCase{name: ext + " longest first", ext: ext, names: reversed(names), deleteKey: names[(i+1)%3]},
		)
	}
	return out
}

// dottedNestedCases adapts dottedCases to the KV/Blobs key-extension table.
// The sibling "sessions/ab" rides along as in nestedCases.
func dottedNestedCases() []nestedCase {
	out := make([]nestedCase, 0, 2*len(dottedExtensions))
	for _, dc := range dottedCases() {
		base, dotted, child := dottedBase, dottedBase+dc.ext, dottedBase+dc.ext+"/b"
		all := []string{base, dotted, child, nestedSibling}
		out = append(out, nestedCase{
			name:      dc.name,
			keys:      append(append([]string(nil), dc.names...), nestedSibling),
			deleteKey: dc.deleteKey,
			listings: []nestedListing{
				{prefix: "", want: all},
				{prefix: "sessions/", want: all},
				{prefix: base, want: all},
				{prefix: base + ".", want: []string{dotted, child}},
				{prefix: dotted, want: []string{dotted, child}},
				{prefix: dotted + "/", want: []string{child}},
				{prefix: child, want: []string{child}},
			},
		})
	}
	return out
}

// runLedgerDotted registers the Ledger dotted-extension group: each name is
// its own ledger with its own tip and records, and deleting one leaves the
// others intact.
func runLedgerDotted(t *testing.T, ctx context.Context, newBackend func(t *testing.T) storage.Ledger) {
	t.Run(dottedCaseName, func(t *testing.T) {
		for _, tc := range dottedCases() {
			t.Run(tc.name, func(t *testing.T) {
				l := newBackend(t)
				for _, n := range tc.names {
					if err := l.Append(ctx, n, 0, nestedValue(n, "r1")); err != nil {
						t.Fatalf("first Append(%q) with %v already written = %v, want nil", n, without(tc.names, n), err)
					}
				}
				// A second record on each name, so a backend that aliases two
				// names is caught by the tip as well as the payload.
				for _, n := range tc.names {
					if err := l.Append(ctx, n, 1, nestedValue(n, "r2")); err != nil {
						t.Fatalf("second Append(%q, expected 1) = %v, want nil", n, err)
					}
				}
				for _, n := range tc.names {
					assertLedgerRecords(t, ctx, l, n, "r1", "r2")
				}

				if err := l.Delete(ctx, tc.deleteKey); err != nil {
					t.Fatalf("Delete(%q) = %v, want nil", tc.deleteKey, err)
				}
				assertLedgerRecords(t, ctx, l, tc.deleteKey)
				for _, n := range without(tc.names, tc.deleteKey) {
					assertLedgerRecords(t, ctx, l, n, "r1", "r2")
				}

				if err := l.Append(ctx, tc.deleteKey, 0, nestedValue(tc.deleteKey, "r3")); err != nil {
					t.Fatalf("re-create Append(%q, expected 0) after Delete = %v, want nil", tc.deleteKey, err)
				}
				assertLedgerRecords(t, ctx, l, tc.deleteKey, "r3")
				for _, n := range without(tc.names, tc.deleteKey) {
					assertLedgerRecords(t, ctx, l, n, "r1", "r2")
				}
			})
		}
	})
}

// assertLedgerRecords checks name's tip and records are exactly the given
// generations, at 1-based sequences.
func assertLedgerRecords(t *testing.T, ctx context.Context, l storage.Ledger, name string, generations ...string) {
	t.Helper()
	tip, err := l.Tip(ctx, name)
	if err != nil {
		t.Errorf("Tip(%q) = %v, want nil", name, err)
	} else if tip != uint64(len(generations)) {
		t.Errorf("Tip(%q) = %d, want %d", name, tip, len(generations))
	}
	recs := readAll(t, l, name, 1)
	if len(recs) != len(generations) {
		t.Errorf("Read(%q) yielded %d records, want %d", name, len(recs), len(generations))
		return
	}
	for i, gen := range generations {
		if recs[i].Seq != uint64(i+1) {
			t.Errorf("Read(%q)[%d].Seq = %d, want %d", name, i, recs[i].Seq, i+1)
		}
		if want := nestedValue(name, gen); !bytes.Equal(recs[i].Payload, want) {
			t.Errorf("Read(%q)[%d].Payload = %q, want %q", name, i, recs[i].Payload, want)
		}
	}
}

// runLeaserDotted registers the Leaser dotted-extension group: each name is
// its own lease slot, held concurrently, refused to a second acquirer by its
// own holder's epoch, and released without disturbing the others.
func runLeaserDotted(t *testing.T, ctx context.Context, newBackend func(t *testing.T) storage.Leaser) {
	t.Run(dottedCaseName, func(t *testing.T) {
		for _, tc := range dottedCases() {
			t.Run(tc.name, func(t *testing.T) {
				le := newBackend(t)
				leases := make(map[string]storage.Lease, len(tc.names))
				t.Cleanup(func() {
					for _, lease := range leases {
						_ = lease.Release(context.Background())
					}
				})
				for _, n := range tc.names {
					lease, err := le.Acquire(ctx, n)
					if err != nil {
						t.Fatalf("Acquire(%q) with %v held = %v, want success (independent names)", n, without(tc.names, n), err)
					}
					leases[n] = lease
				}
				assertLeasesHeld(t, ctx, le, tc.names, leases)

				released := leases[tc.deleteKey]
				if err := released.Release(ctx); err != nil {
					t.Fatalf("Release(%q) = %v, want nil", tc.deleteKey, err)
				}
				delete(leases, tc.deleteKey)
				survivors := without(tc.names, tc.deleteKey)
				assertLeasesHeld(t, ctx, le, survivors, leases)

				again, err := le.Acquire(ctx, tc.deleteKey)
				if err != nil {
					t.Fatalf("re-Acquire(%q) after its Release with %v held = %v, want success", tc.deleteKey, survivors, err)
				}
				leases[tc.deleteKey] = again
				if again.Epoch() <= released.Epoch() {
					t.Errorf("re-Acquire(%q) Epoch() = %d, want strictly greater than %d", tc.deleteKey, again.Epoch(), released.Epoch())
				}
				assertLeasesHeld(t, ctx, le, tc.names, leases)
			})
		}
	})
}

// assertLeasesHeld checks every name's lease is live and that a second
// Acquire of it is refused with that name and that holder's epoch.
func assertLeasesHeld(t *testing.T, ctx context.Context, le storage.Leaser, names []string, leases map[string]storage.Lease) {
	t.Helper()
	for _, n := range names {
		lease := leases[n]
		if isClosed(lease.Lost()) {
			t.Errorf("Lost(%q) closed while held", n)
		}
		second, err := le.Acquire(ctx, n)
		var held *storage.LeaseHeldError
		if !errors.As(err, &held) {
			if second != nil {
				_ = second.Release(context.Background())
			}
			t.Errorf("second Acquire(%q) = %v, want *LeaseHeldError", n, err)
			continue
		}
		if held.Name != n {
			t.Errorf("second Acquire(%q) LeaseHeldError.Name = %q, want %q", n, held.Name, n)
		}
		if held.HolderEpoch != lease.Epoch() {
			t.Errorf("second Acquire(%q) LeaseHeldError.HolderEpoch = %d, want %d", n, held.HolderEpoch, lease.Epoch())
		}
	}
}

// testOrderedIndexDottedNamespacesCoexist is the OrderedIndex dotted-extension
// group: the same (OrderingScope, StableKey) created in namespaces "a",
// "a.<ext>" and "a.<ext>/b" is three records, each read and listed alone.
func testOrderedIndexDottedNamespacesCoexist(t *testing.T, newBackend OrderedIndexFactory) {
	for _, tc := range dottedCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := orderedIndexContext(t)
			index := freshOrderedIndex(t, newBackend)
			for _, ns := range tc.names {
				id := storage.OrderedID{Namespace: ns, OrderingScope: "acceptance", StableKey: "shared"}
				mustCreateOrderedIndexRecord(t, ctx, index, id, "workers", nestedValue(ns, "v1"), storage.Rank{}, storage.Due{State: storage.NotDue})
			}
			for _, ns := range tc.names {
				id := storage.OrderedID{Namespace: ns, OrderingScope: "acceptance", StableKey: "shared"}
				got := mustGetOrderedIndexRecord(t, ctx, index, id)
				if want := nestedValue(ns, "v1"); !bytes.Equal(got.Value, want) {
					t.Errorf("Get(%s).Value = %q, want %q", orderedIndexIDLabel(id), got.Value, want)
				}
				page, err := index.ListOrdered(ctx, ns, "acceptance", 0, 10)
				if err != nil {
					t.Errorf("ListOrdered(%q) = %v, want nil", ns, err)
					continue
				}
				if len(page.Records) != 1 || page.Records[0].ID != id || !bytes.Equal(page.Records[0].Value, nestedValue(ns, "v1")) {
					labels := make([]string, 0, len(page.Records))
					for _, r := range page.Records {
						labels = append(labels, orderedIndexRecordSummary(r))
					}
					t.Errorf("ListOrdered(%q) = %v, want exactly %s", ns, labels, orderedIndexIDLabel(id))
				}
			}
		})
	}
}
