package storetest

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/looprig/storage"
)

// TestLedger runs the Ledger conformance suite. newBackend must return a fresh,
// empty Ledger; register any cleanup via t.Cleanup inside newBackend.
func TestLedger(t *testing.T, newBackend func(t *testing.T) storekit.Ledger) {
	ctx := context.Background()

	t.Run("append read round-trip with 1-based seq", func(t *testing.T) {
		l := newBackend(t)
		const name = "sessions/round-trip"
		payload := []byte("first-record")

		if err := l.Append(ctx, name, 0, payload); err != nil {
			t.Fatalf("Append: %v", err)
		}
		tip, err := l.Tip(ctx, name)
		if err != nil {
			t.Fatalf("Tip: %v", err)
		}
		if tip != 1 {
			t.Errorf("Tip after one append = %d, want 1", tip)
		}

		recs := readAll(t, l, name, 1)
		if len(recs) != 1 {
			t.Fatalf("read %d records, want 1", len(recs))
		}
		if recs[0].Seq != 1 {
			t.Errorf("Seq = %d, want 1 (1-based)", recs[0].Seq)
		}
		if !bytes.Equal(recs[0].Payload, payload) {
			t.Errorf("Payload = %q, want %q", recs[0].Payload, payload)
		}
	})

	t.Run("contiguity across several appends", func(t *testing.T) {
		l := newBackend(t)
		const name = "sessions/contiguous"
		payloads := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc"), []byte("dddd")}
		for i, p := range payloads {
			if err := l.Append(ctx, name, uint64(i), p); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		}

		tip, err := l.Tip(ctx, name)
		if err != nil {
			t.Fatalf("Tip: %v", err)
		}
		if tip != uint64(len(payloads)) {
			t.Errorf("Tip = %d, want %d", tip, len(payloads))
		}

		recs := readAll(t, l, name, 1)
		if len(recs) != len(payloads) {
			t.Fatalf("read %d records, want %d", len(recs), len(payloads))
		}
		for i, rec := range recs {
			if rec.Seq != uint64(i+1) {
				t.Errorf("record[%d].Seq = %d, want %d (contiguous)", i, rec.Seq, i+1)
			}
			if !bytes.Equal(rec.Payload, payloads[i]) {
				t.Errorf("record[%d].Payload = %q, want %q", i, rec.Payload, payloads[i])
			}
		}
	})

	t.Run("CAS conflict on wrong expected leaves state unchanged", func(t *testing.T) {
		cases := []struct {
			name     string
			pre      int
			expected uint64
		}{
			{name: "expected zero on non-empty", pre: 3, expected: 0},
			{name: "wrong non-zero expected below tip", pre: 3, expected: 2},
			{name: "expected above tip", pre: 3, expected: 5},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				l := newBackend(t)
				const name = "sessions/cas"
				for i := 0; i < tc.pre; i++ {
					if err := l.Append(ctx, name, uint64(i), []byte{byte('a' + i)}); err != nil {
						t.Fatalf("seed Append %d: %v", i, err)
					}
				}

				err := l.Append(ctx, name, tc.expected, []byte("new"))
				var ce *storekit.ConflictError
				if !errors.As(err, &ce) {
					t.Fatalf("Append(expected=%d) = %v, want *ConflictError", tc.expected, err)
				}
				if ce.Name != name {
					t.Errorf("ConflictError.Name = %q, want %q", ce.Name, name)
				}
				if ce.Expected != tc.expected {
					t.Errorf("ConflictError.Expected = %d, want %d", ce.Expected, tc.expected)
				}

				tip, terr := l.Tip(ctx, name)
				if terr != nil {
					t.Fatalf("Tip: %v", terr)
				}
				if tip != uint64(tc.pre) {
					t.Errorf("Tip after rejected CAS = %d, want %d (state unchanged)", tip, tc.pre)
				}
			})
		}
	})

	t.Run("stale writer conflict after tip advances", func(t *testing.T) {
		l := newBackend(t)
		const name = "sessions/stale"
		if err := l.Append(ctx, name, 0, []byte("r1")); err != nil {
			t.Fatalf("Append r1: %v", err)
		}
		// A concurrent writer advances the tip to 2.
		if err := l.Append(ctx, name, 1, []byte("r2")); err != nil {
			t.Fatalf("Append r2: %v", err)
		}
		// A stale writer still holding the old expected==1 must be fenced off.
		err := l.Append(ctx, name, 1, []byte("stale"))
		var ce *storekit.ConflictError
		if !errors.As(err, &ce) {
			t.Fatalf("stale Append(expected=1) = %v, want *ConflictError", err)
		}
		if ce.Expected != 1 {
			t.Errorf("ConflictError.Expected = %d, want 1", ce.Expected)
		}
		tip, err := l.Tip(ctx, name)
		if err != nil {
			t.Fatalf("Tip: %v", err)
		}
		if tip != 2 {
			t.Errorf("Tip = %d, want 2 (stale append rejected)", tip)
		}
	})

	t.Run("absent ledger is empty", func(t *testing.T) {
		l := newBackend(t)
		const name = "sessions/absent"

		tip, err := l.Tip(ctx, name)
		if err != nil {
			t.Fatalf("Tip: %v", err)
		}
		if tip != 0 {
			t.Errorf("Tip of absent ledger = %d, want 0", tip)
		}
		if recs := readAll(t, l, name, 1); len(recs) != 0 {
			t.Errorf("Read of absent ledger drained %d records, want 0", len(recs))
		}
		if err := l.Delete(ctx, name); err != nil {
			t.Errorf("Delete of absent ledger = %v, want nil (no-op)", err)
		}
	})

	t.Run("read from zero yields all records", func(t *testing.T) {
		l := newBackend(t)
		const name = "sessions/from-zero"
		payloads := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
		for i, p := range payloads {
			if err := l.Append(ctx, name, uint64(i), p); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		}
		// from below the first sequence (0) reads every record (seq >= 0 == all),
		// starting at the 1-based first record.
		recs := readAll(t, l, name, 0)
		if len(recs) != len(payloads) {
			t.Fatalf("Read(from=0) drained %d records, want %d", len(recs), len(payloads))
		}
		for i, rec := range recs {
			if rec.Seq != uint64(i+1) {
				t.Errorf("record[%d].Seq = %d, want %d", i, rec.Seq, i+1)
			}
			if !bytes.Equal(rec.Payload, payloads[i]) {
				t.Errorf("record[%d].Payload = %q, want %q", i, rec.Payload, payloads[i])
			}
		}
	})

	t.Run("interior-offset Read yields the correct suffix", func(t *testing.T) {
		// A backend that wrongly CLAMPS from to the first record (returning
		// {1..tip} for any in-range from) would pass every other read case; only
		// an interior from catches it. Ledger replay/resume relies on Read(from=k)
		// for 1 < k <= tip returning exactly the records with Seq >= k.
		payloads := [][]byte{[]byte("r1"), []byte("r2"), []byte("r3"), []byte("r4")}
		cases := []struct {
			name string
			from uint64
		}{
			{name: "from interior k=2", from: 2},
			{name: "from interior k=3", from: 3},
			{name: "from tip yields last record only", from: 4},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				l := newBackend(t)
				const name = "sessions/interior"
				for i, p := range payloads {
					if err := l.Append(ctx, name, uint64(i), p); err != nil {
						t.Fatalf("Append %d: %v", i, err)
					}
				}

				recs := readAll(t, l, name, tc.from)
				want := payloads[tc.from-1:] // the suffix with Seq >= from
				if len(recs) != len(want) {
					t.Fatalf("Read(from=%d) drained %d records, want %d (exactly Seq %d..%d)", tc.from, len(recs), len(want), tc.from, len(payloads))
				}
				for i, rec := range recs {
					wantSeq := tc.from + uint64(i)
					if rec.Seq != wantSeq {
						t.Errorf("record[%d].Seq = %d, want %d (no clamping to the first record)", i, rec.Seq, wantSeq)
					}
					if !bytes.Equal(rec.Payload, want[i]) {
						t.Errorf("record[%d].Payload = %q, want %q", i, rec.Payload, want[i])
					}
				}
			})
		}
	})

	t.Run("read beyond tip is drained", func(t *testing.T) {
		l := newBackend(t)
		const name = "sessions/beyond"
		for i := 0; i < 2; i++ { // tip becomes 2
			if err := l.Append(ctx, name, uint64(i), []byte("p")); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		}
		for _, from := range []uint64{3, 4, 100} { // tip+1, tip+2, far beyond
			if recs := readAll(t, l, name, from); len(recs) != 0 {
				t.Errorf("Read(from=%d) drained %d records, want 0", from, len(recs))
			}
		}
	})

	t.Run("zero-length payload is legal", func(t *testing.T) {
		l := newBackend(t)
		const name = "sessions/zero"
		if err := l.Append(ctx, name, 0, []byte{}); err != nil {
			t.Fatalf("Append(empty): %v", err)
		}
		if err := l.Append(ctx, name, 1, []byte("x")); err != nil {
			t.Fatalf("Append(x): %v", err)
		}

		recs := readAll(t, l, name, 1)
		if len(recs) != 2 {
			t.Fatalf("read %d records, want 2", len(recs))
		}
		if recs[0].Seq != 1 || len(recs[0].Payload) != 0 {
			t.Errorf("record[0] = {Seq:%d, len(Payload):%d}, want {1, 0}", recs[0].Seq, len(recs[0].Payload))
		}
		if recs[1].Seq != 2 || !bytes.Equal(recs[1].Payload, []byte("x")) {
			t.Errorf("record[1] = {Seq:%d, Payload:%q}, want {2, \"x\"}", recs[1].Seq, recs[1].Payload)
		}
	})

	t.Run("payload floor 1 MiB round-trips byte-equal", func(t *testing.T) {
		l := newBackend(t)
		const name = "sessions/floor"
		payload := patternedBytes(payloadFloor)
		if err := l.Append(ctx, name, 0, payload); err != nil {
			t.Fatalf("Append(1 MiB): %v", err)
		}
		recs := readAll(t, l, name, 1)
		if len(recs) != 1 {
			t.Fatalf("read %d records, want 1", len(recs))
		}
		if !bytes.Equal(recs[0].Payload, payload) {
			t.Errorf("1 MiB payload did not round-trip byte-equal")
		}
	})

	t.Run("idempotent Delete", func(t *testing.T) {
		l := newBackend(t)
		const name = "sessions/del"
		for i := 0; i < 3; i++ {
			if err := l.Append(ctx, name, uint64(i), []byte("p")); err != nil {
				t.Fatalf("seed Append %d: %v", i, err)
			}
		}
		for i := 0; i < 2; i++ {
			if err := l.Delete(ctx, name); err != nil {
				t.Fatalf("Delete call %d = %v, want nil (idempotent)", i, err)
			}
		}

		tip, err := l.Tip(ctx, name)
		if err != nil {
			t.Fatalf("Tip: %v", err)
		}
		if tip != 0 {
			t.Errorf("Tip after delete = %d, want 0", tip)
		}
		if recs := readAll(t, l, name, 1); len(recs) != 0 {
			t.Errorf("Read after delete drained %d records, want 0", len(recs))
		}

		// A deleted ledger is truly absent: a fresh Append at expected 0 restarts
		// sequences at 1.
		if err := l.Append(ctx, name, 0, []byte("fresh")); err != nil {
			t.Fatalf("Append after delete = %v, want nil (absent==empty)", err)
		}
		if tip, err = l.Tip(ctx, name); err != nil || tip != 1 {
			t.Errorf("Tip after re-create = %d, %v; want 1, nil", tip, err)
		}
	})

	t.Run("invalid name", func(t *testing.T) {
		methods := []struct {
			method string
			call   func(l storekit.Ledger, name string) error
		}{
			{"Append", func(l storekit.Ledger, name string) error { return l.Append(ctx, name, 0, []byte("x")) }},
			{"Read", func(l storekit.Ledger, name string) error { _, err := l.Read(ctx, name, 1); return err }},
			{"Tip", func(l storekit.Ledger, name string) error { _, err := l.Tip(ctx, name); return err }},
			{"Delete", func(l storekit.Ledger, name string) error { return l.Delete(ctx, name) }},
		}
		for _, m := range methods {
			for _, bad := range invalidNames {
				t.Run(m.method+"/"+bad.label, func(t *testing.T) {
					l := newBackend(t)
					err := m.call(l, bad.value)
					var ine *storekit.InvalidNameError
					if !errors.As(err, &ine) {
						t.Fatalf("%s(%q) = %v, want *InvalidNameError", m.method, bad.value, err)
					}
					if ine.Name != bad.value {
						t.Errorf("InvalidNameError.Name = %q, want %q", ine.Name, bad.value)
					}
				})
			}
		}
	})

	t.Run("concurrent appenders linearize gap-free", func(t *testing.T) {
		l := newBackend(t)
		const name = "sessions/linearize"
		const writers = 8

		// Each writer loops AppendDefinite at the freshly-observed tip, retrying on
		// conflict until its unique payload lands. Goroutine hygiene: a spawned
		// goroutine never calls t.Fatalf/FailNow — it reports over errCh and every
		// assertion runs on the test goroutine after the join.
		errCh := make(chan error, writers)
		var wg sync.WaitGroup
		for i := 0; i < writers; i++ {
			wg.Add(1)
			payload := []byte("writer-" + strconv.Itoa(i))
			go func(payload []byte) {
				defer wg.Done()
				for {
					tip, err := l.Tip(ctx, name)
					if err != nil {
						errCh <- err
						return
					}
					err = storekit.AppendDefinite(ctx, l, name, tip, payload)
					if err == nil {
						errCh <- nil
						return
					}
					var ce *storekit.ConflictError
					if errors.As(err, &ce) {
						continue // lost the race; retry at the fresh tip
					}
					errCh <- err
					return
				}
			}(payload)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				t.Fatalf("writer reported error: %v", err)
			}
		}

		recs := readAll(t, l, name, 1)
		if len(recs) != writers {
			t.Fatalf("committed %d records, want %d (gap-free)", len(recs), writers)
		}
		seen := make(map[string]bool, writers)
		for i, rec := range recs {
			if rec.Seq != uint64(i+1) {
				t.Errorf("record[%d].Seq = %d, want %d (contiguous 1..N)", i, rec.Seq, i+1)
			}
			seen[string(rec.Payload)] = true
		}
		if len(seen) != writers {
			t.Errorf("distinct committed payloads = %d, want %d (all present, none duplicated)", len(seen), writers)
		}
		for i := 0; i < writers; i++ {
			if !seen["writer-"+strconv.Itoa(i)] {
				t.Errorf("writer-%d payload missing from the committed ledger", i)
			}
		}
	})
}
