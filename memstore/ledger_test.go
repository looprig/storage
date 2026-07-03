package memstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/looprig/storekit"
)

// This file holds only the memstore-SPECIFIC ledger tests that the shared
// storetest conformance suite (run from conformance_test.go) deliberately does
// NOT cover: payload copy-in/copy-out ownership, cursor boundedness after Read,
// and the single-winner append-contention race. The shared behaviors
// (append/read round-trip, contiguity, CAS conflicts, absent-ledger emptiness,
// reads beyond tip, zero-length payloads, invalid names, the 1 MiB payload
// floor, idempotent Delete, stale-writer conflict, and concurrent-appender
// linearization) live in package storetest.

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
