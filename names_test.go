package storekit

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantErr  bool
		wantRule string
	}{
		// valid
		{name: "single char", input: "a"},
		{name: "uuid-shaped path", input: "sessions/0b0e1f2a-3c4d-5e6f-7a8b-9c0d1e2f3a4b"},
		{name: "all legal segment bytes", input: "a/b_c.d-e"},
		{name: "exactly 512 bytes", input: strings.Repeat("a", 512)},

		// invalid
		{name: "empty", input: "", wantErr: true, wantRule: "empty"},
		{name: "uppercase", input: "A/b", wantErr: true, wantRule: "segment must start with [a-z0-9]"},
		{name: "doubled slash", input: "a//b", wantErr: true, wantRule: "empty segment"},
		{name: "dot segment", input: "a/./b", wantErr: true, wantRule: "segment must start with [a-z0-9]"},
		{name: "dotdot segment", input: "a/../b", wantErr: true, wantRule: "segment must start with [a-z0-9]"},
		{name: "leading slash", input: "/a", wantErr: true, wantRule: "empty segment"},
		{name: "trailing slash", input: "a/", wantErr: true, wantRule: "empty segment"},
		{name: "segment starts with dot", input: ".a", wantErr: true, wantRule: "segment must start with [a-z0-9]"},
		{name: "segment starts with hyphen", input: "-a", wantErr: true, wantRule: "segment must start with [a-z0-9]"},
		{name: "segment starts with underscore", input: "_a", wantErr: true, wantRule: "segment must start with [a-z0-9]"},
		{name: "513 bytes over cap", input: strings.Repeat("a", 513), wantErr: true, wantRule: "too long"},
		{name: "space", input: "a b", wantErr: true, wantRule: "illegal byte in segment"},
		{name: "nul byte", input: "a\x00b", wantErr: true, wantRule: "illegal byte in segment"},
		{name: "uppercase mid-segment", input: "aB", wantErr: true, wantRule: "illegal byte in segment"},
		{name: "non-ascii mid-segment", input: "a\xffb", wantErr: true, wantRule: "illegal byte in segment"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateName(tt.input)

			if !tt.wantErr {
				if err != nil {
					t.Fatalf("ValidateName(%q) = %v, want nil", tt.input, err)
				}
				return
			}

			if err == nil {
				t.Fatalf("ValidateName(%q) = nil, want error", tt.input)
			}

			var ine *InvalidNameError
			if !errors.As(err, &ine) {
				t.Fatalf("ValidateName(%q) error type = %T, want *InvalidNameError", tt.input, err)
			}
			if ine.Name != tt.input {
				t.Errorf("ValidateName(%q) InvalidNameError.Name = %q, want %q", tt.input, ine.Name, tt.input)
			}
			if ine.Rule != tt.wantRule {
				t.Errorf("ValidateName(%q) InvalidNameError.Rule = %q, want %q", tt.input, ine.Rule, tt.wantRule)
			}
		})
	}
}

// FuzzValidateName exercises the grammar parser on arbitrary bytes: it must
// never panic, and its verdict must be deterministic — in particular a valid
// (nil) result must re-validate to nil.
func FuzzValidateName(f *testing.F) {
	seeds := []string{
		"a",
		"sessions/0b0e1f2a-3c4d-5e6f-7a8b-9c0d1e2f3a4b",
		"a/b_c.d-e",
		strings.Repeat("a", 512),
		"",
		"A/b",
		"a//b",
		"a/./b",
		"a/../b",
		"/a",
		"a/",
		".a",
		"-a",
		"_a",
		strings.Repeat("a", 513),
		"a b",
		"a\x00b",
		"aB",
		"a\xffb",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, name string) {
		// A panic here fails the fuzz target automatically.
		err := ValidateName(name)

		// Deterministic: the same input yields the same nil/non-nil verdict
		// every time, so a nil (valid) result always re-validates to nil.
		again := ValidateName(name)
		if (err == nil) != (again == nil) {
			t.Fatalf("ValidateName(%q) nondeterministic: first=%v second=%v", name, err, again)
		}
	})
}
