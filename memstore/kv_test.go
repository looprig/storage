package memstore

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ciram-co/storekit"
)

func TestKVCreateAndGet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newKVStore()
	const key = "sessions/one"
	val := []byte("hello")

	// Create-only Put (expectedRev 0) on an absent key succeeds at rev 1.
	rev, err := s.Put(ctx, key, 0, val)
	if err != nil {
		t.Fatalf("Put(create) unexpected error: %v", err)
	}
	if rev != 1 {
		t.Errorf("Put(create) rev = %d, want 1", rev)
	}

	gotVal, gotRev, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get() unexpected error: %v", err)
	}
	if !bytes.Equal(gotVal, val) {
		t.Errorf("Get() val = %q, want %q", gotVal, val)
	}
	if gotRev != 1 {
		t.Errorf("Get() rev = %d, want 1", gotRev)
	}
}

func TestKVPutBumpsRev(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newKVStore()
	const key = "sessions/rev"

	rev, err := s.Put(ctx, key, 0, []byte("v1"))
	if err != nil {
		t.Fatalf("Put(create) unexpected error: %v", err)
	}
	// Each Put at the correct expectedRev bumps the per-key revision by one.
	for want := uint64(2); want <= 4; want++ {
		rev, err = s.Put(ctx, key, rev, []byte("v"))
		if err != nil {
			t.Fatalf("Put(update) unexpected error: %v", err)
		}
		if rev != want {
			t.Fatalf("Put(update) rev = %d, want %d", rev, want)
		}
	}
}

func TestKVPutConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		preRevs     int    // number of successful Puts before the conflicting one
		expectedRev uint64 // wrong expectedRev handed to the conflicting Put
	}{
		{name: "create-only on existing key", preRevs: 1, expectedRev: 0},
		{name: "stale expected below current", preRevs: 3, expectedRev: 2},
		{name: "expected above current", preRevs: 1, expectedRev: 5},
		{name: "update on absent key", preRevs: 0, expectedRev: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newKVStore()
			const key = "sessions/cas"

			var curRev uint64
			for i := 0; i < tt.preRevs; i++ {
				var err error
				curRev, err = s.Put(ctx, key, curRev, []byte("seed"))
				if err != nil {
					t.Fatalf("seed Put %d unexpected error: %v", i, err)
				}
			}

			_, err := s.Put(ctx, key, tt.expectedRev, []byte("nope"))
			var ce *storekit.ConflictError
			if !errors.As(err, &ce) {
				t.Fatalf("Put() error = %v, want *storekit.ConflictError", err)
			}
			if ce.Name != key {
				t.Errorf("ConflictError.Name = %q, want %q", ce.Name, key)
			}
			if ce.Expected != tt.expectedRev {
				t.Errorf("ConflictError.Expected = %d, want %d", ce.Expected, tt.expectedRev)
			}

			// State must be unchanged: current rev is still curRev.
			if tt.preRevs == 0 {
				if _, _, gerr := s.Get(ctx, key); !errors.As(gerr, new(*storekit.KeyNotFoundError)) {
					t.Errorf("Get after rejected Put = %v, want key still absent", gerr)
				}
				return
			}
			_, gotRev, gerr := s.Get(ctx, key)
			if gerr != nil {
				t.Fatalf("Get after rejected Put unexpected error: %v", gerr)
			}
			if gotRev != curRev {
				t.Errorf("rev after rejected Put = %d, want %d (state unchanged)", gotRev, curRev)
			}
		})
	}
}

func TestKVGetAbsent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newKVStore()
	const key = "sessions/missing"

	_, _, err := s.Get(ctx, key)
	var nf *storekit.KeyNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("Get(absent) error = %v, want *storekit.KeyNotFoundError", err)
	}
	if nf.Key != key {
		t.Errorf("KeyNotFoundError.Key = %q, want %q", nf.Key, key)
	}
}

func TestKVKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		seed   []string
		prefix string
		want   []string
	}{
		{
			name:   "empty prefix returns all sorted",
			seed:   []string{"sessions/c", "sessions/a", "workspaces/z", "sessions/b"},
			prefix: "",
			want:   []string{"sessions/a", "sessions/b", "sessions/c", "workspaces/z"},
		},
		{
			name:   "prefix filters and sorts",
			seed:   []string{"sessions/c", "sessions/a", "workspaces/z", "sessions/b"},
			prefix: "sessions/",
			want:   []string{"sessions/a", "sessions/b", "sessions/c"},
		},
		{
			name:   "prefix matching nothing yields empty",
			seed:   []string{"sessions/a", "sessions/b"},
			prefix: "workspaces/",
			want:   nil,
		},
		{
			name:   "partial-segment prefix (not a valid name) still filters",
			seed:   []string{"sessions/a", "sessions/b", "session-x"},
			prefix: "sessions/",
			want:   []string{"sessions/a", "sessions/b"},
		},
		{
			name:   "empty store yields empty",
			seed:   nil,
			prefix: "",
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newKVStore()
			for _, k := range tt.seed {
				if _, err := s.Put(ctx, k, 0, []byte("v")); err != nil {
					t.Fatalf("seed Put(%q) unexpected error: %v", k, err)
				}
			}

			got, err := s.Keys(ctx, tt.prefix)
			if err != nil {
				t.Fatalf("Keys() unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Keys(%q) = %v, want %v", tt.prefix, got, tt.want)
			}
		})
	}
}

func TestKVDeleteIdempotent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		seed    bool
		deletes int
	}{
		{name: "delete absent is nil", seed: false, deletes: 1},
		{name: "delete existing removes", seed: true, deletes: 1},
		{name: "double delete is nil", seed: true, deletes: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := newKVStore()
			const key = "sessions/del"

			if tt.seed {
				if _, err := s.Put(ctx, key, 0, []byte("v")); err != nil {
					t.Fatalf("seed Put unexpected error: %v", err)
				}
			}
			for i := 0; i < tt.deletes; i++ {
				if err := s.Delete(ctx, key); err != nil {
					t.Fatalf("Delete() call %d = %v, want nil (idempotent)", i, err)
				}
			}

			// Absent after delete: Get reports not-found.
			if _, _, err := s.Get(ctx, key); !errors.As(err, new(*storekit.KeyNotFoundError)) {
				t.Errorf("Get after delete = %v, want *KeyNotFoundError", err)
			}
			// A deleted key is truly absent: a fresh create-only Put re-creates
			// it at rev 1 (per-key revisions do not persist across delete).
			rev, err := s.Put(ctx, key, 0, []byte("fresh"))
			if err != nil {
				t.Fatalf("Put(create) after delete unexpected error: %v", err)
			}
			if rev != 1 {
				t.Errorf("rev after re-create = %d, want 1", rev)
			}
		})
	}
}

func TestKVCopyIn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newKVStore()
	const key = "sessions/copyin"

	orig := []byte("hello")
	caller := make([]byte, len(orig))
	copy(caller, orig)

	if _, err := s.Put(ctx, key, 0, caller); err != nil {
		t.Fatalf("Put() unexpected error: %v", err)
	}
	// Mutating the caller's slice after Put must not reach stored data.
	caller[0] = 'Z'

	got, _, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get() unexpected error: %v", err)
	}
	if !bytes.Equal(got, orig) {
		t.Errorf("stored val = %q, want %q (Put must copy-in)", got, orig)
	}
}

func TestKVCopyOut(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newKVStore()
	const key = "sessions/copyout"

	orig := []byte("hello")
	if _, err := s.Put(ctx, key, 0, orig); err != nil {
		t.Fatalf("Put() unexpected error: %v", err)
	}

	got, _, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get() unexpected error: %v", err)
	}
	// Mutating the slice Get returned must not reach stored data.
	got[0] = 'Z'

	again, _, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get() unexpected error: %v", err)
	}
	if !bytes.Equal(again, orig) {
		t.Errorf("stored val = %q, want %q (Get must copy-out)", again, orig)
	}
}

func TestKVInvalidName(t *testing.T) {
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
		call   func(s *kvStore, key string) error
	}{
		{
			method: "Get",
			call: func(s *kvStore, key string) error {
				_, _, err := s.Get(context.Background(), key)
				return err
			},
		},
		{
			method: "Put",
			call: func(s *kvStore, key string) error {
				_, err := s.Put(context.Background(), key, 0, []byte("x"))
				return err
			},
		},
		{
			method: "Delete",
			call: func(s *kvStore, key string) error {
				return s.Delete(context.Background(), key)
			},
		},
	}

	for _, m := range methods {
		for _, bad := range badNames {
			t.Run(m.method+"/"+bad.label, func(t *testing.T) {
				t.Parallel()
				s := newKVStore()
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
