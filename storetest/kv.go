package storetest

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/looprig/storage"
)

// TestKV runs the KV conformance suite. newBackend must return a fresh, empty
// KV; register any cleanup via t.Cleanup inside newBackend.
func TestKV(t *testing.T, newBackend func(t *testing.T) storage.KV) {
	ctx := conformanceContext(t)

	t.Run("get absent returns KeyNotFoundError", func(t *testing.T) {
		kv := newBackend(t)
		const key = "sessions/missing"
		_, _, err := kv.Get(ctx, key)
		var nf *storage.KeyNotFoundError
		if !errors.As(err, &nf) {
			t.Fatalf("Get(absent) = %v, want *KeyNotFoundError", err)
		}
		if nf.Key != key {
			t.Errorf("KeyNotFoundError.Key = %q, want %q", nf.Key, key)
		}
	})

	t.Run("create with expectedRev 0 returns rev 1 and Get reads it back", func(t *testing.T) {
		kv := newBackend(t)
		const key = "sessions/one"
		val := []byte("hello")

		rev, err := kv.Put(ctx, key, 0, val)
		if err != nil {
			t.Fatalf("Put(create): %v", err)
		}
		if rev != 1 {
			t.Errorf("create rev = %d, want 1", rev)
		}

		gotVal, gotRev, err := kv.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !bytes.Equal(gotVal, val) {
			t.Errorf("Get val = %q, want %q", gotVal, val)
		}
		if gotRev != 1 {
			t.Errorf("Get rev = %d, want 1", gotRev)
		}
	})

	t.Run("correct-rev Put bumps rev", func(t *testing.T) {
		kv := newBackend(t)
		const key = "sessions/rev"
		last, err := kv.Put(ctx, key, 0, []byte("v1"))
		if err != nil {
			t.Fatalf("Put(create): %v", err)
		}
		for i := 0; i < 3; i++ {
			val := []byte("v" + strconv.Itoa(i+2))
			rev, err := kv.Put(ctx, key, last, val)
			if err != nil {
				t.Fatalf("Put(update) %d: %v", i, err)
			}
			if rev <= last {
				t.Errorf("update rev = %d, want strictly greater than %d", rev, last)
			}
			gotVal, gotRev, gerr := kv.Get(ctx, key)
			if gerr != nil {
				t.Fatalf("Get: %v", gerr)
			}
			if gotRev != rev {
				t.Errorf("Get rev = %d, want %d", gotRev, rev)
			}
			if !bytes.Equal(gotVal, val) {
				t.Errorf("Get val = %q, want %q", gotVal, val)
			}
			last = rev
		}
	})

	t.Run("wrong-rev Put conflicts leaving state unchanged", func(t *testing.T) {
		cases := []struct {
			name        string
			preRevs     int
			expectedRev uint64
		}{
			{name: "create-only on existing key", preRevs: 1, expectedRev: 0},
			{name: "stale rev below current", preRevs: 3, expectedRev: 1},
			{name: "rev above current", preRevs: 1, expectedRev: 5},
			{name: "update on absent key", preRevs: 0, expectedRev: 1},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				kv := newBackend(t)
				const key = "sessions/cas"
				var curRev uint64
				for i := 0; i < tc.preRevs; i++ {
					var err error
					curRev, err = kv.Put(ctx, key, curRev, []byte("seed"))
					if err != nil {
						t.Fatalf("seed Put %d: %v", i, err)
					}
				}

				_, err := kv.Put(ctx, key, tc.expectedRev, []byte("nope"))
				var ce *storage.ConflictError
				if !errors.As(err, &ce) {
					t.Fatalf("Put(expectedRev=%d) = %v, want *ConflictError", tc.expectedRev, err)
				}
				if ce.Name != key {
					t.Errorf("ConflictError.Name = %q, want %q", ce.Name, key)
				}
				if ce.Expected != tc.expectedRev {
					t.Errorf("ConflictError.Expected = %d, want %d", ce.Expected, tc.expectedRev)
				}

				if tc.preRevs == 0 {
					if _, _, gerr := kv.Get(ctx, key); !errors.As(gerr, new(*storage.KeyNotFoundError)) {
						t.Errorf("Get after rejected Put = %v, want key still absent", gerr)
					}
					return
				}
				gotVal, gotRev, gerr := kv.Get(ctx, key)
				if gerr != nil {
					t.Fatalf("Get: %v", gerr)
				}
				if gotRev != curRev {
					t.Errorf("rev after rejected Put = %d, want %d (state unchanged)", gotRev, curRev)
				}
				if !bytes.Equal(gotVal, []byte("seed")) {
					t.Errorf("val after rejected Put = %q, want %q (state unchanged)", gotVal, "seed")
				}
			})
		}
	})

	t.Run("Keys sorted deduped prefix-filtered", func(t *testing.T) {
		cases := []struct {
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
				name:   "partial-segment prefix filters as substring",
				seed:   []string{"sessions/a", "sessions/b", "session-x"},
				prefix: "sessions/",
				want:   []string{"sessions/a", "sessions/b"},
			},
			{
				name:   "prefix matching nothing is empty",
				seed:   []string{"sessions/a"},
				prefix: "workspaces/",
				want:   nil,
			},
			{
				name:   "empty store is empty",
				seed:   nil,
				prefix: "",
				want:   nil,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				kv := newBackend(t)
				for _, k := range tc.seed {
					if _, err := kv.Put(ctx, k, 0, []byte("v")); err != nil {
						t.Fatalf("seed Put(%q): %v", k, err)
					}
				}
				got, err := kv.Keys(ctx, tc.prefix)
				if err != nil {
					t.Fatalf("Keys: %v", err)
				}
				if !equalStringSlices(got, tc.want) {
					t.Errorf("Keys(%q) = %v, want %v", tc.prefix, got, tc.want)
				}
			})
		}
	})

	t.Run("Delete idempotent", func(t *testing.T) {
		cases := []struct {
			name    string
			seed    bool
			deletes int
		}{
			{name: "delete absent is nil", seed: false, deletes: 1},
			{name: "delete existing removes", seed: true, deletes: 1},
			{name: "double delete is nil", seed: true, deletes: 2},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				kv := newBackend(t)
				const key = "sessions/del"
				if tc.seed {
					if _, err := kv.Put(ctx, key, 0, []byte("v")); err != nil {
						t.Fatalf("seed Put: %v", err)
					}
				}
				for i := 0; i < tc.deletes; i++ {
					if err := kv.Delete(ctx, key); err != nil {
						t.Fatalf("Delete call %d = %v, want nil (idempotent)", i, err)
					}
				}

				if _, _, err := kv.Get(ctx, key); !errors.As(err, new(*storage.KeyNotFoundError)) {
					t.Errorf("Get after delete = %v, want *KeyNotFoundError", err)
				}
				// A deleted key is truly absent: a fresh create-only Put succeeds.
				if _, err := kv.Put(ctx, key, 0, []byte("fresh")); err != nil {
					t.Errorf("create-only Put after delete = %v, want nil (key free)", err)
				}
			})
		}
	})

	t.Run("invalid key", func(t *testing.T) {
		methods := []struct {
			method string
			call   func(kv storage.KV, key string) error
		}{
			{"Get", func(kv storage.KV, key string) error { _, _, err := kv.Get(ctx, key); return err }},
			{"Put", func(kv storage.KV, key string) error { _, err := kv.Put(ctx, key, 0, []byte("x")); return err }},
			{"Delete", func(kv storage.KV, key string) error { return kv.Delete(ctx, key) }},
		}
		for _, m := range methods {
			for _, bad := range invalidNames {
				t.Run(m.method+"/"+bad.label, func(t *testing.T) {
					kv := newBackend(t)
					err := m.call(kv, bad.value)
					var ine *storage.InvalidNameError
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

	t.Run("value floor 1 MiB accepted", func(t *testing.T) {
		kv := newBackend(t)
		const key = "sessions/floor"
		val := patternedBytes(payloadFloor)
		rev, err := kv.Put(ctx, key, 0, val)
		if err != nil {
			t.Fatalf("Put(1 MiB): %v", err)
		}
		if rev != 1 {
			t.Errorf("create rev = %d, want 1", rev)
		}
		got, gotRev, err := kv.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if gotRev != 1 {
			t.Errorf("Get rev = %d, want 1", gotRev)
		}
		if !bytes.Equal(got, val) {
			t.Errorf("1 MiB value did not round-trip byte-equal")
		}
	})
}
