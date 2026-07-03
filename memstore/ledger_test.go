package memstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/ciram-co/storekit"
)

// drain reads a cursor to exhaustion, returning every record it yields. It fails
// the test on any error other than io.EOF, so callers can assert on the drained
// slice directly.
func drain(t *testing.T, c storekit.Cursor) []storekit.Record {
	t.Helper()
	var out []storekit.Record
	for {
		rec, err := c.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Next() unexpected error: %v", err)
		}
		out = append(out, rec)
	}
}

// appendAll appends payloads in order at their implied 1-based sequence,
// failing the test on any error so the arrange step of a case is trustworthy.
func appendAll(t *testing.T, s *ledgerStore, name string, payloads [][]byte) {
	t.Helper()
	ctx := context.Background()
	for i, p := range payloads {
		if err := s.Append(ctx, name, uint64(i), p); err != nil {
			t.Fatalf("Append(%q, expected=%d) unexpected error: %v", name, i, err)
		}
	}
}

func TestLedgerAppendRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		payloads [][]byte
		from     uint64
		want     []storekit.Record
	}{
		{
			name:     "single record read from 1",
			payloads: [][]byte{[]byte("a")},
			from:     1,
			want:     []storekit.Record{{Seq: 1, Payload: []byte("a")}},
		},
		{
			name:     "contiguous 1-based sequences",
			payloads: [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")},
			from:     1,
			want: []storekit.Record{
				{Seq: 1, Payload: []byte("a")},
				{Seq: 2, Payload: []byte("bb")},
				{Seq: 3, Payload: []byte("ccc")},
			},
		},
		{
			name:     "read from tip yields last record only",
			payloads: [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")},
			from:     3,
			want:     []storekit.Record{{Seq: 3, Payload: []byte("ccc")}},
		},
		{
			name:     "read from tip+1 is drained",
			payloads: [][]byte{[]byte("a")},
			from:     2,
			want:     nil,
		},
		{
			name:     "read far beyond tip is drained",
			payloads: [][]byte{[]byte("a"), []byte("b")},
			from:     100,
			want:     nil,
		},
		{
			name:     "read from zero reads all",
			payloads: [][]byte{[]byte("a"), []byte("b")},
			from:     0,
			want: []storekit.Record{
				{Seq: 1, Payload: []byte("a")},
				{Seq: 2, Payload: []byte("b")},
			},
		},
		{
			name:     "absent ledger is drained",
			payloads: nil,
			from:     1,
			want:     nil,
		},
		{
			name:     "zero-length payload is legal",
			payloads: [][]byte{{}, []byte("x")},
			from:     1,
			want: []storekit.Record{
				{Seq: 1, Payload: []byte{}},
				{Seq: 2, Payload: []byte("x")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := newLedgerStore()
			const name = "sessions/round-trip"
			appendAll(t, s, name, tt.payloads)

			cur, err := s.Read(context.Background(), name, tt.from)
			if err != nil {
				t.Fatalf("Read() unexpected error: %v", err)
			}
			defer func() {
				if cerr := cur.Close(); cerr != nil {
					t.Errorf("Close() = %v, want nil", cerr)
				}
			}()

			got := drain(t, cur)
			if len(got) != len(tt.want) {
				t.Fatalf("drained %d records, want %d (%v)", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i].Seq != tt.want[i].Seq {
					t.Errorf("record[%d].Seq = %d, want %d", i, got[i].Seq, tt.want[i].Seq)
				}
				if !bytes.Equal(got[i].Payload, tt.want[i].Payload) {
					t.Errorf("record[%d].Payload = %q, want %q", i, got[i].Payload, tt.want[i].Payload)
				}
			}
		})
	}
}

func TestLedgerAppendConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		pre          int
		expected     uint64
		wantConflict bool
	}{
		{name: "expected zero on empty commits", pre: 0, expected: 0, wantConflict: false},
		{name: "expected matches tip commits", pre: 3, expected: 3, wantConflict: false},
		{name: "expected zero on non-empty conflicts", pre: 3, expected: 0, wantConflict: true},
		{name: "expected below tip conflicts", pre: 3, expected: 2, wantConflict: true},
		{name: "expected above tip conflicts", pre: 3, expected: 4, wantConflict: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newLedgerStore()
			const name = "sessions/cas"

			pre := make([][]byte, tt.pre)
			for i := range pre {
				pre[i] = []byte{byte('a' + i)}
			}
			appendAll(t, s, name, pre)

			err := s.Append(ctx, name, tt.expected, []byte("new"))

			if !tt.wantConflict {
				if err != nil {
					t.Fatalf("Append() = %v, want nil (commit)", err)
				}
				tip, terr := s.Tip(ctx, name)
				if terr != nil {
					t.Fatalf("Tip() unexpected error: %v", terr)
				}
				if tip != uint64(tt.pre+1) {
					t.Errorf("Tip after commit = %d, want %d", tip, tt.pre+1)
				}
				return
			}

			var ce *storekit.ConflictError
			if !errors.As(err, &ce) {
				t.Fatalf("Append() error = %v, want *storekit.ConflictError", err)
			}
			if ce.Name != name {
				t.Errorf("ConflictError.Name = %q, want %q", ce.Name, name)
			}
			if ce.Expected != tt.expected {
				t.Errorf("ConflictError.Expected = %d, want %d", ce.Expected, tt.expected)
			}

			// A memory backend must NEVER return AmbiguousError.
			var ae *storekit.AmbiguousError
			if errors.As(err, &ae) {
				t.Errorf("Append() returned *AmbiguousError %v, memory backends must never", ae)
			}

			// State must be untouched by a rejected CAS.
			tip, terr := s.Tip(ctx, name)
			if terr != nil {
				t.Fatalf("Tip() unexpected error: %v", terr)
			}
			if tip != uint64(tt.pre) {
				t.Errorf("Tip after conflict = %d, want %d (state must be unchanged)", tip, tt.pre)
			}
		})
	}
}

func TestLedgerTip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appends int
		want    uint64
	}{
		{name: "absent ledger tip is zero", appends: 0, want: 0},
		{name: "one append tip is one", appends: 1, want: 1},
		{name: "several appends tip counts", appends: 5, want: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newLedgerStore()
			const name = "sessions/tip"

			payloads := make([][]byte, tt.appends)
			for i := range payloads {
				payloads[i] = []byte("p")
			}
			appendAll(t, s, name, payloads)

			tip, err := s.Tip(ctx, name)
			if err != nil {
				t.Fatalf("Tip() unexpected error: %v", err)
			}
			if tip != tt.want {
				t.Errorf("Tip() = %d, want %d", tip, tt.want)
			}
		})
	}
}

func TestLedgerDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appends int
		deletes int
	}{
		{name: "delete absent is idempotent nil", appends: 0, deletes: 1},
		{name: "delete existing empties ledger", appends: 3, deletes: 1},
		{name: "double delete is idempotent", appends: 3, deletes: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newLedgerStore()
			const name = "sessions/del"

			payloads := make([][]byte, tt.appends)
			for i := range payloads {
				payloads[i] = []byte("p")
			}
			appendAll(t, s, name, payloads)

			for i := 0; i < tt.deletes; i++ {
				if err := s.Delete(ctx, name); err != nil {
					t.Fatalf("Delete() call %d = %v, want nil (idempotent)", i, err)
				}
			}

			// Absent == empty: Tip 0 and Read drained after deletion.
			tip, err := s.Tip(ctx, name)
			if err != nil {
				t.Fatalf("Tip() unexpected error: %v", err)
			}
			if tip != 0 {
				t.Errorf("Tip after delete = %d, want 0", tip)
			}
			cur, err := s.Read(ctx, name, 1)
			if err != nil {
				t.Fatalf("Read() unexpected error: %v", err)
			}
			if got := drain(t, cur); len(got) != 0 {
				t.Errorf("Read after delete drained %d records, want 0", len(got))
			}

			// A deleted ledger is truly absent: a fresh Append at expected 0
			// re-creates it with sequences restarting at 1.
			if err := s.Append(ctx, name, 0, []byte("fresh")); err != nil {
				t.Fatalf("Append after delete = %v, want nil (absent==empty)", err)
			}
			tip, err = s.Tip(ctx, name)
			if err != nil {
				t.Fatalf("Tip() unexpected error: %v", err)
			}
			if tip != 1 {
				t.Errorf("Tip after re-create = %d, want 1", tip)
			}
		})
	}
}

func TestLedgerInvalidName(t *testing.T) {
	t.Parallel()

	badNames := []struct {
		label string
		value string
	}{
		{label: "empty", value: ""},
		{label: "leading slash", value: "/leading"},
		{label: "trailing slash", value: "trailing/"},
		{label: "doubled slash", value: "a//b"},
		{label: "uppercase", value: "Upper"},
		{label: "space", value: "has space"},
		{label: "dot-dot segment", value: ".."},
	}

	methods := []struct {
		method string
		call   func(s *ledgerStore, name string) error
	}{
		{
			method: "Append",
			call: func(s *ledgerStore, name string) error {
				return s.Append(context.Background(), name, 0, []byte("x"))
			},
		},
		{
			method: "Read",
			call: func(s *ledgerStore, name string) error {
				_, err := s.Read(context.Background(), name, 1)
				return err
			},
		},
		{
			method: "Tip",
			call: func(s *ledgerStore, name string) error {
				_, err := s.Tip(context.Background(), name)
				return err
			},
		},
		{
			method: "Delete",
			call: func(s *ledgerStore, name string) error {
				return s.Delete(context.Background(), name)
			},
		},
	}

	for _, m := range methods {
		for _, bad := range badNames {
			t.Run(m.method+"/"+bad.label, func(t *testing.T) {
				t.Parallel()
				s := newLedgerStore()
				err := m.call(s, bad.value)
				var ine *storekit.InvalidNameError
				if !errors.As(err, &ine) {
					t.Fatalf("%s(%q) error = %v, want *storekit.InvalidNameError", m.method, bad.value, err)
				}
				if ine.Name != bad.value {
					t.Errorf("InvalidNameError.Name = %q, want %q", ine.Name, bad.value)
				}
			})
		}
	}
}

func TestLedgerCursorBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		before int // records appended before Read
		after  int // records appended after Read, must NOT be observed
	}{
		{name: "append after read not observed", before: 2, after: 1},
		{name: "empty at read stays empty", before: 0, after: 2},
		{name: "no later appends", before: 3, after: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newLedgerStore()
			const name = "sessions/bounded"

			before := make([][]byte, tt.before)
			for i := range before {
				before[i] = []byte{byte('a' + i)}
			}
			appendAll(t, s, name, before)

			// Snapshot the tip by opening the cursor now.
			cur, err := s.Read(ctx, name, 1)
			if err != nil {
				t.Fatalf("Read() unexpected error: %v", err)
			}

			// Appends that land AFTER Read must not be tailed by this cursor.
			for i := 0; i < tt.after; i++ {
				if err := s.Append(ctx, name, uint64(tt.before+i), []byte("late")); err != nil {
					t.Fatalf("late Append unexpected error: %v", err)
				}
			}

			got := drain(t, cur)
			if len(got) != tt.before {
				t.Fatalf("bounded cursor drained %d records, want %d", len(got), tt.before)
			}
			for i := range got {
				if got[i].Seq != uint64(i+1) {
					t.Errorf("record[%d].Seq = %d, want %d", i, got[i].Seq, i+1)
				}
			}

			// A cursor opened AFTER the late appends does observe them, proving
			// the earlier cursor's blindness was boundedness, not data loss.
			cur2, err := s.Read(ctx, name, 1)
			if err != nil {
				t.Fatalf("Read() unexpected error: %v", err)
			}
			if got2 := drain(t, cur2); len(got2) != tt.before+tt.after {
				t.Errorf("fresh cursor drained %d records, want %d", len(got2), tt.before+tt.after)
			}
		})
	}
}

func TestLedgerCopyIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(p []byte)
	}{
		{name: "overwrite first byte", mutate: func(p []byte) { p[0] = 'Z' }},
		{name: "zero whole slice", mutate: func(p []byte) {
			for i := range p {
				p[i] = 0
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newLedgerStore()
			const name = "sessions/copyin"

			orig := []byte("hello")
			p := make([]byte, len(orig))
			copy(p, orig)

			if err := s.Append(ctx, name, 0, p); err != nil {
				t.Fatalf("Append() unexpected error: %v", err)
			}

			// Mutating the caller's slice after Append must not touch stored data.
			tt.mutate(p)

			cur, err := s.Read(ctx, name, 1)
			if err != nil {
				t.Fatalf("Read() unexpected error: %v", err)
			}
			rec, err := cur.Next(ctx)
			if err != nil {
				t.Fatalf("Next() unexpected error: %v", err)
			}
			if !bytes.Equal(rec.Payload, orig) {
				t.Errorf("stored payload = %q, want %q (Append must copy-in)", rec.Payload, orig)
			}
		})
	}
}

func TestLedgerCopyOut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(p []byte)
	}{
		{name: "overwrite first byte", mutate: func(p []byte) { p[0] = 'Z' }},
		{name: "zero whole slice", mutate: func(p []byte) {
			for i := range p {
				p[i] = 0
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newLedgerStore()
			const name = "sessions/copyout"

			orig := []byte("hello")
			p := make([]byte, len(orig))
			copy(p, orig)
			if err := s.Append(ctx, name, 0, p); err != nil {
				t.Fatalf("Append() unexpected error: %v", err)
			}

			cur, err := s.Read(ctx, name, 1)
			if err != nil {
				t.Fatalf("Read() unexpected error: %v", err)
			}
			rec, err := cur.Next(ctx)
			if err != nil {
				t.Fatalf("Next() unexpected error: %v", err)
			}

			// Mutating the slice Next returned must not touch stored data.
			tt.mutate(rec.Payload)

			cur2, err := s.Read(ctx, name, 1)
			if err != nil {
				t.Fatalf("Read() unexpected error: %v", err)
			}
			rec2, err := cur2.Next(ctx)
			if err != nil {
				t.Fatalf("Next() unexpected error: %v", err)
			}
			if !bytes.Equal(rec2.Payload, orig) {
				t.Errorf("stored payload = %q, want %q (Next must copy-out)", rec2.Payload, orig)
			}
		})
	}
}

func TestLedgerConcurrent(t *testing.T) {
	t.Parallel()

	// Each name is worked by its own goroutine doing Append/Read/Tip/Delete, so
	// -race exercises the RWMutex guarding the shared map.
	names := []string{"sessions/a", "sessions/b", "sessions/c", "sessions/d"}
	s := newLedgerStore()
	ctx := context.Background()

	const perName = 50
	done := make(chan struct{})
	for _, name := range names {
		go func(name string) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < perName; i++ {
				if err := s.Append(ctx, name, uint64(i), []byte("x")); err != nil {
					t.Errorf("Append(%q, %d) unexpected error: %v", name, i, err)
					return
				}
				if _, err := s.Tip(ctx, name); err != nil {
					t.Errorf("Tip(%q) unexpected error: %v", name, err)
					return
				}
				cur, err := s.Read(ctx, name, 1)
				if err != nil {
					t.Errorf("Read(%q) unexpected error: %v", name, err)
					return
				}
				// Drain inline: t.Fatalf/FailNow are invalid from a non-test
				// goroutine, so report with t.Errorf and return instead.
				for {
					_, nerr := cur.Next(ctx)
					if errors.Is(nerr, io.EOF) {
						break
					}
					if nerr != nil {
						t.Errorf("Next(%q) unexpected error: %v", name, nerr)
						return
					}
				}
			}
		}(name)
	}
	for range names {
		<-done
	}

	for _, name := range names {
		tip, err := s.Tip(ctx, name)
		if err != nil {
			t.Fatalf("Tip(%q) unexpected error: %v", name, err)
		}
		if tip != perName {
			t.Errorf("Tip(%q) = %d, want %d", name, tip, perName)
		}
	}
}

func TestLedgerAppendContention(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newLedgerStore()
	const name = "sessions/contention"
	const writers = 16

	// Every writer races to claim seq 1 (expected == 0) on the SAME fresh
	// ledger. CAS must let exactly one win; the rest see a *ConflictError.
	type result struct {
		payload []byte
		err     error
	}
	results := make(chan result, writers)
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		payload := []byte{byte('a' + i)} // distinct per writer
		go func(payload []byte) {
			<-start // release all writers together for maximal contention
			err := s.Append(ctx, name, 0, payload)
			results <- result{payload: payload, err: err}
		}(payload)
	}
	close(start)

	var winners int
	var winnerPayload []byte
	for i := 0; i < writers; i++ {
		r := <-results
		if r.err == nil {
			winners++
			winnerPayload = r.payload
			continue
		}
		var ce *storekit.ConflictError
		if !errors.As(r.err, &ce) {
			t.Errorf("loser Append error = %v, want *storekit.ConflictError", r.err)
			continue
		}
		if ce.Expected != 0 {
			t.Errorf("ConflictError.Expected = %d, want 0", ce.Expected)
		}
	}
	if winners != 1 {
		t.Fatalf("winners = %d, want exactly 1", winners)
	}

	tip, err := s.Tip(ctx, name)
	if err != nil {
		t.Fatalf("Tip() unexpected error: %v", err)
	}
	if tip != 1 {
		t.Errorf("Tip = %d, want 1", tip)
	}

	cur, err := s.Read(ctx, name, 1)
	if err != nil {
		t.Fatalf("Read() unexpected error: %v", err)
	}
	got := drain(t, cur)
	if len(got) != 1 {
		t.Fatalf("drained %d records, want 1", len(got))
	}
	if !bytes.Equal(got[0].Payload, winnerPayload) {
		t.Errorf("stored payload = %q, want the winner's %q", got[0].Payload, winnerPayload)
	}
}
