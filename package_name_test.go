package storage_test

import "github.com/looprig/storage"

// Compile-time proof that the default identifier for the root module is storage.
// An explicit import alias would hide a stale package declaration, so this import is
// intentionally unaliased.
var _ storage.Ledger
