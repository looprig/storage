# Blob Reader Shutdown Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a compatible, testable shutdown contract for readers returned by `Blobs.Get` and make `memstore` conform.

**Architecture:** The interface remains `Get(context.Context, string) (io.ReadCloser, error)`. The contract requires a provider-specific non-nil terminal read error after Close, concurrent `Read`/`Close` safety, stable idempotent close, bounded provider-controlled waits, and non-nil successful readers. `memstore` returns a fresh mutex-owned reader over a copied byte slice, with offset and closed state guarded by the same lock and uses `fs.ErrClosed` internally.

**Tech Stack:** Go 1.26.6 standard library, `testing`, race detector, existing `storetest` conformance suite.

---

### Task 1: Pin the provider contract in conformance tests

**Files:**
- Modify: `storetest/blobs.go`
- Modify: `storage.go`

1. Add conformance assertions that a successful `Get` returns a non-nil reader.
2. Add a close lifecycle subtest that reads a prefix, calls `Close` twice with a stable result, and requires every later `Read` to return zero bytes and a non-nil terminal error.
3. Add a coordinated read-loop/close subtest that proves no successful read begins after `Close` returns and exercises `Read` against `Close` under `-race`.
4. Run `GOWORK=off go test ./storetest ./memstore -run Blobs`; expect RED because `io.NopCloser` permits reads after close.
5. Amend the `Blobs.Get` documentation to state the compatible contract and non-nil guarantee.

### Task 2: Implement the close-aware memstore reader

**Files:**
- Modify: `memstore/blobs.go`
- Modify: `memstore/blobs_test.go`

1. Add focused memstore tests for close-before-read, repeated close, copied ownership, and concurrent read/close.
2. Run the focused tests and confirm RED.
3. Implement an unexported reader holding `mu sync.Mutex`, owned `data []byte`, `offset int`, and `closed bool`; `Read` returns `fs.ErrClosed` after closure, otherwise copies and advances under the lock; `Close` idempotently publishes closure under the lock.
4. Return this reader from `blobStore.Get` over the existing copied slice.
5. Run focused and conformance tests; expect GREEN under `-race`.

### Task 3: Document compatibility and mutation-test

**Files:**
- Modify: `README.md`

1. Add a planned-v0.5.1 compatibility note describing the strengthened reader lifecycle contract.
2. In a disposable archive, remove the closed-state check and confirm conformance fails.
3. In a disposable archive, remove reader locking and confirm race/conformance failure.
4. Restore/confirm the real repository remains unchanged by mutations.

### Task 4: Verify and commit

1. Run `GOWORK=off go mod tidy` and confirm no module drift.
2. Run `GOWORK=off go test -race ./...`.
3. Run `GOWORK=off make check`.
4. Run `GOOS=linux GOARCH=386 GOWORK=off go test ./...`.
5. Review `git diff --check`, `git diff`, and repository status.
6. Commit only storage-local files with `feat: define blob reader shutdown contract`; do not push or tag.
