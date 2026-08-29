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
// copy-in/copy-out ownership guarantees.
package storetest
