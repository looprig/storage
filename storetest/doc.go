// Package storetest provides backend-conformance suites for the five storage
// primitives — Ledger, Leaser, KV, Blobs, and OrderedIndex. A backend's own
// _test.go calls the relevant Test* function with a factory that returns a
// fresh, empty primitive; the suite drives every contract behavior shared by
// all backends and classifies failures with errors.As against the
// storage-canonical typed errors — never by string.
//
// The suites live in regular (non-_test.go) files and take an extra newBackend
// parameter, so `go test` never auto-runs them and `go vet`'s test-signature
// check (which inspects only _test.go files) never flags them — the same pattern
// the standard library uses for testing/fstest.TestFS. Importing "testing" from a
// regular file is idiomatic for this conformance-suite shape.
//
// Backend-specific behavior — cursor internals, cross-process lease reclaim,
// and ctx-honoring — is deliberately out of scope and stays in each backend's
// own tests. The shared OrderedIndex suite does cover its public
// copy-in/copy-out ownership guarantees. The separate Leaser lifecycle suite
// uses a provider-supplied, test-only harness to cover deterministic renewal
// and expiry without expanding the production Leaser interface. For the same
// reason, TestBlobReaderLifecycle is separate from the base Blobs suite: only
// providers claiming that optional capability run its concurrent and post-Close
// checks. A provider that can block inside external I/O must additionally test
// its documented bound with a backend-specific deterministic probe. The
// OrderedIndex suite takes malformed and unknown-version cursor
// tokens from a required OrderedCursorProbe parameter: cursor bytes are
// opaque, so a literal token here would pin one provider's grammar as the
// contract, and making the probe a parameter turns "this provider owes the
// suite two tokens" into a compile error rather than a runtime failure.
package storetest
