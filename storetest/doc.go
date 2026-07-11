// Package storetest provides backend-conformance suites for the four storage
// primitives — Ledger, Leaser, KV, and Blobs. A backend's own _test.go calls
// TestLedger/TestLeaser/TestKV/TestBlobs with a factory that returns a fresh,
// empty primitive; the suite drives every contract behavior shared by all
// backends and classifies failures with errors.As against the storage-canonical
// typed errors — never by string.
//
// The suites live in regular (non-_test.go) files and take an extra newBackend
// parameter, so `go test` never auto-runs them and `go vet`'s test-signature
// check (which inspects only _test.go files) never flags them — the same pattern
// the standard library uses for testing/fstest.TestFS. Importing "testing" from a
// regular file is idiomatic for this conformance-suite shape.
//
// Backend-specific behavior — copy-in/copy-out ownership, cursor internals,
// cross-process lease reclaim, ctx-honoring — is deliberately out of scope and
// stays in each backend's own tests.
package storetest
