package storage

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestErrorsMessageAndAs(t *testing.T) {
	t.Parallel()

	// sentinel is an arbitrary leaf cause used to exercise wrapping/Unwrap.
	sentinel := errors.New("underlying transport failure")

	tests := []struct {
		name string
		// err is the concrete storage error under test.
		err error
		// wantSubstrs are substrings Error() must contain — the subject
		// (Name/Key) plus any relevant number.
		wantSubstrs []string
		// as attempts errors.As into the matching concrete type after the
		// error has been wrapped; it reports whether recovery succeeded and
		// whether the recovered fields survived the round trip.
		as func(err error) bool
	}{
		{
			name:        "ConflictError",
			err:         &ConflictError{Name: "sessions/abc", Expected: 7},
			wantSubstrs: []string{"sessions/abc", "7"},
			as: func(err error) bool {
				var target *ConflictError
				return errors.As(err, &target) && target.Name == "sessions/abc" && target.Expected == 7
			},
		},
		{
			name:        "AmbiguousError with cause",
			err:         &AmbiguousError{Name: "sessions/abc", Expected: 42, Cause: sentinel},
			wantSubstrs: []string{"sessions/abc", "42"},
			as: func(err error) bool {
				var target *AmbiguousError
				return errors.As(err, &target) && target.Name == "sessions/abc" && target.Expected == 42
			},
		},
		{
			name:        "RecordNotFoundError",
			err:         &RecordNotFoundError{Name: "sessions/abc", Seq: 3},
			wantSubstrs: []string{"sessions/abc", "3"},
			as: func(err error) bool {
				var target *RecordNotFoundError
				return errors.As(err, &target) && target.Name == "sessions/abc" && target.Seq == 3
			},
		},
		{
			name:        "KeyNotFoundError",
			err:         &KeyNotFoundError{Key: "kv/token"},
			wantSubstrs: []string{"kv/token"},
			as: func(err error) bool {
				var target *KeyNotFoundError
				return errors.As(err, &target) && target.Key == "kv/token"
			},
		},
		{
			name:        "BlobNotFoundError",
			err:         &BlobNotFoundError{Key: "blobs/xyz"},
			wantSubstrs: []string{"blobs/xyz"},
			as: func(err error) bool {
				var target *BlobNotFoundError
				return errors.As(err, &target) && target.Key == "blobs/xyz"
			},
		},
		{
			name:        "BlobConflictError",
			err:         &BlobConflictError{Key: "blobs/xyz"},
			wantSubstrs: []string{"blobs/xyz"},
			as: func(err error) bool {
				var target *BlobConflictError
				return errors.As(err, &target) && target.Key == "blobs/xyz"
			},
		},
		{
			name:        "LeaseHeldError",
			err:         &LeaseHeldError{Name: "leases/w1", HolderEpoch: 9},
			wantSubstrs: []string{"leases/w1", "9"},
			as: func(err error) bool {
				var target *LeaseHeldError
				return errors.As(err, &target) && target.Name == "leases/w1" && target.HolderEpoch == 9
			},
		},
		{
			name:        "LeaseLostError",
			err:         &LeaseLostError{Name: "leases/w1", Epoch: 5},
			wantSubstrs: []string{"leases/w1", "5"},
			as: func(err error) bool {
				var target *LeaseLostError
				return errors.As(err, &target) && target.Name == "leases/w1" && target.Epoch == 5
			},
		},
	}

	const prefix = "storage: "

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			msg := tt.err.Error()
			if msg == "" {
				t.Fatalf("%s: Error() returned empty string", tt.name)
			}
			if !strings.HasPrefix(msg, prefix) {
				t.Errorf("%s: Error() = %q, want prefix %q", tt.name, msg, prefix)
			}
			for _, sub := range tt.wantSubstrs {
				if !strings.Contains(msg, sub) {
					t.Errorf("%s: Error() = %q, want it to contain %q", tt.name, msg, sub)
				}
			}

			// The canonical usage: a backend wraps the cause with
			// fmt.Errorf("...: %w", &storage.XxxError{...}); callers must
			// recover the concrete type across that wrap with errors.As.
			wrapped := fmt.Errorf("backend op failed: %w", tt.err)
			if !tt.as(wrapped) {
				t.Errorf("%s: errors.As failed to recover concrete type across wrap (or fields lost): %v", tt.name, wrapped)
			}
		})
	}
}

func TestAmbiguousErrorUnwrap(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("append ack lost")

	tests := []struct {
		name      string
		cause     error
		wantIs    bool // errors.Is(wrapped, sentinel) should hold
		wantNilUn bool // Unwrap() should return nil
	}{
		{name: "carries cause", cause: sentinel, wantIs: true},
		{name: "nil cause", cause: nil, wantNilUn: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			amb := &AmbiguousError{Name: "sessions/abc", Expected: 1, Cause: tt.cause}

			// Unwrap must not panic and must return exactly Cause.
			if got := amb.Unwrap(); got != tt.cause {
				t.Errorf("Unwrap() = %v, want %v", got, tt.cause)
			}
			if tt.wantNilUn && amb.Unwrap() != nil {
				t.Errorf("Unwrap() = %v, want nil", amb.Unwrap())
			}

			// errors.Is must reach the sentinel cause through an outer wrap.
			wrapped := fmt.Errorf("outer: %w", amb)
			if got := errors.Is(wrapped, sentinel); got != tt.wantIs {
				t.Errorf("errors.Is(wrapped, sentinel) = %v, want %v", got, tt.wantIs)
			}
		})
	}
}
