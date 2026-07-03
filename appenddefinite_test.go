package storekit

import (
	"context"
	"errors"
	"io"
	"testing"
)

// fakeLedger is an in-file, scripted Ledger for exercising AppendDefinite in
// isolation. Append returns the pre-programmed results in order (one per call);
// Read returns a scripted cursor or a scripted error. Tip/Delete are unused and
// panic so an accidental call is loud rather than silently wrong. Call counts
// are recorded so tests can assert exactly how many times each method ran.
type fakeLedger struct {
	appendResults []error // one result per Append call, consumed in order

	appendCalls int

	readCursor Cursor // returned by Read when readErr is nil (may be nil to model a contract-violating (nil, nil) return)
	readErr    error  // if non-nil, Read returns this (and a nil cursor)
	readCalls  int
	readFrom   uint64 // the `from` of the last Read call
}

func (f *fakeLedger) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	i := f.appendCalls
	f.appendCalls++
	if i >= len(f.appendResults) {
		panic("fakeLedger.Append called more times than scripted")
	}
	return f.appendResults[i]
}

func (f *fakeLedger) Read(ctx context.Context, name string, from uint64) (Cursor, error) {
	f.readCalls++
	f.readFrom = from
	if f.readErr != nil {
		return nil, f.readErr
	}
	return f.readCursor, nil
}

func (f *fakeLedger) Tip(ctx context.Context, name string) (uint64, error) {
	panic("fakeLedger.Tip not called")
}

func (f *fakeLedger) Delete(ctx context.Context, name string) error {
	panic("fakeLedger.Delete not called")
}

// fakeCursor models the three verify-relevant cursor shapes:
//   - a record then EOF   (hasRec == true)
//   - immediately drained  (hasRec == false, nextErr == io.EOF)
//   - a Next error         (hasRec == false, nextErr == some non-EOF error)
type fakeCursor struct {
	rec     Record
	hasRec  bool
	nextErr error

	nextCalls int
	closed    bool
}

func (c *fakeCursor) Next(ctx context.Context) (Record, error) {
	c.nextCalls++
	if c.hasRec && c.nextCalls == 1 {
		return c.rec, nil
	}
	if c.nextErr != nil {
		return Record{}, c.nextErr
	}
	return Record{}, io.EOF
}

func (c *fakeCursor) Close() error {
	c.closed = true
	return nil
}

// otherError is an arbitrary typed error used for the fail-closed default path
// (a definite failure that is neither Conflict nor Ambiguous) and the non-EOF
// Next path.
type otherError struct{ msg string }

func (e *otherError) Error() string { return e.msg }

func TestAppendDefinite(t *testing.T) {
	t.Parallel()

	const (
		name     = "sessions/abc"
		expected = uint64(5)
	)
	// verifySeq is the record AppendDefinite must read to resolve ambiguity.
	const verifySeq = expected + 1

	ours := []byte("our-payload")
	foreign := []byte("foreign-payload")

	// recCursor builds a cursor that yields one record carrying payload, then EOF.
	recCursor := func(payload []byte) *fakeCursor {
		return &fakeCursor{rec: Record{Seq: verifySeq, Payload: payload}, hasRec: true}
	}
	drainedCursor := func() *fakeCursor { return &fakeCursor{nextErr: io.EOF} }
	nextErrCursor := func(err error) *fakeCursor { return &fakeCursor{nextErr: err} }

	tests := []struct {
		name string
		// build wires a scripted ledger for the case and returns a checker that
		// asserts the outcome. Closures let the checker reference the very
		// sentinel errors the ledger was scripted with (for errors.Is identity).
		build func() (*fakeLedger, func(t *testing.T, l *fakeLedger, err error))
	}{
		{
			name: "clean success",
			build: func() (*fakeLedger, func(t *testing.T, l *fakeLedger, err error)) {
				l := &fakeLedger{appendResults: []error{nil}}
				return l, func(t *testing.T, l *fakeLedger, err error) {
					if err != nil {
						t.Fatalf("err = %v, want nil", err)
					}
					if l.appendCalls != 1 {
						t.Errorf("appendCalls = %d, want 1", l.appendCalls)
					}
					if l.readCalls != 0 {
						t.Errorf("readCalls = %d, want 0", l.readCalls)
					}
				}
			},
		},
		{
			name: "definite conflict, foreign at tip",
			build: func() (*fakeLedger, func(t *testing.T, l *fakeLedger, err error)) {
				conflict := &ConflictError{Name: name, Expected: expected}
				cur := recCursor(foreign)
				l := &fakeLedger{appendResults: []error{conflict}, readCursor: cur}
				return l, func(t *testing.T, l *fakeLedger, err error) {
					var ce *ConflictError
					if !errors.As(err, &ce) {
						t.Fatalf("err = %v, want *ConflictError", err)
					}
					if !errors.Is(err, conflict) {
						t.Errorf("err = %v, want the scripted conflict returned", err)
					}
					if l.appendCalls != 1 {
						t.Errorf("appendCalls = %d, want 1", l.appendCalls)
					}
					if l.readCalls != 1 {
						t.Errorf("readCalls = %d, want 1", l.readCalls)
					}
					if l.readFrom != verifySeq {
						t.Errorf("readFrom = %d, want %d", l.readFrom, verifySeq)
					}
					if !cur.closed {
						t.Error("verify cursor was not closed")
					}
				}
			},
		},
		{
			name: "definite conflict, ours at tip",
			build: func() (*fakeLedger, func(t *testing.T, l *fakeLedger, err error)) {
				conflict := &ConflictError{Name: name, Expected: expected}
				cur := recCursor(ours)
				l := &fakeLedger{appendResults: []error{conflict}, readCursor: cur}
				return l, func(t *testing.T, l *fakeLedger, err error) {
					if err != nil {
						t.Fatalf("err = %v, want nil (our record landed)", err)
					}
					if l.appendCalls != 1 {
						t.Errorf("appendCalls = %d, want 1", l.appendCalls)
					}
					if l.readCalls != 1 {
						t.Errorf("readCalls = %d, want 1", l.readCalls)
					}
					if !cur.closed {
						t.Error("verify cursor was not closed")
					}
				}
			},
		},
		{
			name: "ambiguous then retry lands",
			build: func() (*fakeLedger, func(t *testing.T, l *fakeLedger, err error)) {
				amb := &AmbiguousError{Name: name, Expected: expected}
				l := &fakeLedger{appendResults: []error{amb, nil}}
				return l, func(t *testing.T, l *fakeLedger, err error) {
					if err != nil {
						t.Fatalf("err = %v, want nil", err)
					}
					if l.appendCalls != 2 {
						t.Errorf("appendCalls = %d, want 2", l.appendCalls)
					}
					if l.readCalls != 0 {
						t.Errorf("readCalls = %d, want 0", l.readCalls)
					}
					// The retry reuses the same name/expected/payload by
					// construction (AppendDefinite never reassigns them), so
					// there is nothing meaningful to assert about identity here.
				}
			},
		},
		{
			name: "ambiguous then conflict, ours",
			build: func() (*fakeLedger, func(t *testing.T, l *fakeLedger, err error)) {
				amb := &AmbiguousError{Name: name, Expected: expected}
				conflict := &ConflictError{Name: name, Expected: expected}
				cur := recCursor(ours)
				l := &fakeLedger{appendResults: []error{amb, conflict}, readCursor: cur}
				return l, func(t *testing.T, l *fakeLedger, err error) {
					if err != nil {
						t.Fatalf("err = %v, want nil (our record landed on retry)", err)
					}
					if l.appendCalls != 2 {
						t.Errorf("appendCalls = %d, want 2", l.appendCalls)
					}
					if l.readCalls != 1 {
						t.Errorf("readCalls = %d, want 1", l.readCalls)
					}
				}
			},
		},
		{
			name: "ambiguous then conflict, foreign",
			build: func() (*fakeLedger, func(t *testing.T, l *fakeLedger, err error)) {
				amb := &AmbiguousError{Name: name, Expected: expected}
				conflict := &ConflictError{Name: name, Expected: expected}
				cur := recCursor(foreign)
				l := &fakeLedger{appendResults: []error{amb, conflict}, readCursor: cur}
				return l, func(t *testing.T, l *fakeLedger, err error) {
					var ce *ConflictError
					if !errors.As(err, &ce) {
						t.Fatalf("err = %v, want *ConflictError", err)
					}
					if !errors.Is(err, conflict) {
						t.Errorf("err = %v, want the retry's conflict returned", err)
					}
					if l.appendCalls != 2 {
						t.Errorf("appendCalls = %d, want 2", l.appendCalls)
					}
					if l.readCalls != 1 {
						t.Errorf("readCalls = %d, want 1", l.readCalls)
					}
				}
			},
		},
		{
			name: "ambiguous twice",
			build: func() (*fakeLedger, func(t *testing.T, l *fakeLedger, err error)) {
				amb1 := &AmbiguousError{Name: name, Expected: expected, Cause: errors.New("first lost ack")}
				amb2 := &AmbiguousError{Name: name, Expected: expected, Cause: errors.New("second lost ack")}
				l := &fakeLedger{appendResults: []error{amb1, amb2}}
				return l, func(t *testing.T, l *fakeLedger, err error) {
					var ae *AmbiguousError
					if !errors.As(err, &ae) {
						t.Fatalf("err = %v, want *AmbiguousError", err)
					}
					if ae.Name != name || ae.Expected != expected {
						t.Errorf("surfaced AmbiguousError = %+v, want Name=%q Expected=%d", ae, name, expected)
					}
					// Cause must be the FIRST ambiguous error, not the second.
					if ae.Cause != error(amb1) {
						t.Errorf("Cause = %v, want the first ambiguous error", ae.Cause)
					}
					if !errors.Is(err, amb1) {
						t.Error("errors.Is(err, firstAmbiguous) = false, want true")
					}
					if errors.Is(err, amb2) {
						t.Error("errors.Is(err, secondAmbiguous) = true, want false (second must be discarded)")
					}
					if l.appendCalls != 2 {
						t.Errorf("appendCalls = %d, want 2", l.appendCalls)
					}
					if l.readCalls != 0 {
						t.Errorf("readCalls = %d, want 0", l.readCalls)
					}
				}
			},
		},
		{
			name: "conflict but nothing at expected+1",
			build: func() (*fakeLedger, func(t *testing.T, l *fakeLedger, err error)) {
				conflict := &ConflictError{Name: name, Expected: expected}
				cur := drainedCursor()
				l := &fakeLedger{appendResults: []error{conflict}, readCursor: cur}
				return l, func(t *testing.T, l *fakeLedger, err error) {
					var ce *ConflictError
					if !errors.As(err, &ce) {
						t.Fatalf("err = %v, want *ConflictError", err)
					}
					if !errors.Is(err, conflict) {
						t.Errorf("err = %v, want the scripted conflict returned (nothing of ours at tip)", err)
					}
					if l.appendCalls != 1 {
						t.Errorf("appendCalls = %d, want 1", l.appendCalls)
					}
					if l.readCalls != 1 {
						t.Errorf("readCalls = %d, want 1", l.readCalls)
					}
					if !cur.closed {
						t.Error("verify cursor was not closed")
					}
				}
			},
		},
		{
			name: "read error during verify (Read fails)",
			build: func() (*fakeLedger, func(t *testing.T, l *fakeLedger, err error)) {
				conflict := &ConflictError{Name: name, Expected: expected}
				readErr := &otherError{msg: "read transport failure"}
				l := &fakeLedger{appendResults: []error{conflict}, readErr: readErr}
				return l, func(t *testing.T, l *fakeLedger, err error) {
					var ve *AppendVerifyError
					if !errors.As(err, &ve) {
						t.Fatalf("err = %v, want *AppendVerifyError", err)
					}
					if ve.Seq != verifySeq {
						t.Errorf("AppendVerifyError.Seq = %d, want %d", ve.Seq, verifySeq)
					}
					if !errors.Is(err, readErr) {
						t.Error("errors.Is(err, readErr) = false, want the read error preserved via Unwrap")
					}
					if l.appendCalls != 1 {
						t.Errorf("appendCalls = %d, want 1", l.appendCalls)
					}
					if l.readCalls != 1 {
						t.Errorf("readCalls = %d, want 1", l.readCalls)
					}
				}
			},
		},
		{
			name: "read error during verify (Next non-EOF)",
			build: func() (*fakeLedger, func(t *testing.T, l *fakeLedger, err error)) {
				conflict := &ConflictError{Name: name, Expected: expected}
				nextErr := &otherError{msg: "cursor Next failure"}
				cur := nextErrCursor(nextErr)
				l := &fakeLedger{appendResults: []error{conflict}, readCursor: cur}
				return l, func(t *testing.T, l *fakeLedger, err error) {
					var ve *AppendVerifyError
					if !errors.As(err, &ve) {
						t.Fatalf("err = %v, want *AppendVerifyError", err)
					}
					if ve.Seq != verifySeq {
						t.Errorf("AppendVerifyError.Seq = %d, want %d", ve.Seq, verifySeq)
					}
					if !errors.Is(err, nextErr) {
						t.Error("errors.Is(err, nextErr) = false, want the Next error preserved via Unwrap")
					}
					if !cur.closed {
						t.Error("verify cursor was not closed")
					}
				}
			},
		},
		{
			name: "conflict, Read returns nil cursor with nil error",
			build: func() (*fakeLedger, func(t *testing.T, l *fakeLedger, err error)) {
				conflict := &ConflictError{Name: name, Expected: expected}
				// readCursor and readErr both left nil: Read returns (nil, nil),
				// a contract violation that must fail closed, not panic.
				l := &fakeLedger{appendResults: []error{conflict}}
				return l, func(t *testing.T, l *fakeLedger, err error) {
					var ve *AppendVerifyError
					if !errors.As(err, &ve) {
						t.Fatalf("err = %v, want *AppendVerifyError", err)
					}
					if ve.Seq != verifySeq {
						t.Errorf("AppendVerifyError.Seq = %d, want %d", ve.Seq, verifySeq)
					}
					if !errors.Is(err, errNilCursor) {
						t.Error("errors.Is(err, errNilCursor) = false, want the nil-cursor sentinel via Unwrap")
					}
					if l.appendCalls != 1 {
						t.Errorf("appendCalls = %d, want 1", l.appendCalls)
					}
					if l.readCalls != 1 {
						t.Errorf("readCalls = %d, want 1", l.readCalls)
					}
				}
			},
		},
		{
			name: "definite non-conflict, non-ambiguous failure",
			build: func() (*fakeLedger, func(t *testing.T, l *fakeLedger, err error)) {
				fail := &otherError{msg: "definite storage failure"}
				l := &fakeLedger{appendResults: []error{fail}}
				return l, func(t *testing.T, l *fakeLedger, err error) {
					var oe *otherError
					if !errors.As(err, &oe) {
						t.Fatalf("err = %v, want *otherError", err)
					}
					if !errors.Is(err, fail) {
						t.Error("errors.Is(err, fail) = false, want the raw error returned (fail closed)")
					}
					if l.appendCalls != 1 {
						t.Errorf("appendCalls = %d, want 1", l.appendCalls)
					}
					if l.readCalls != 0 {
						t.Errorf("readCalls = %d, want 0 (no verify on a definite non-conflict failure)", l.readCalls)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l, check := tt.build()
			err := AppendDefinite(context.Background(), l, name, expected, ours)
			check(t, l, err)
		})
	}
}
