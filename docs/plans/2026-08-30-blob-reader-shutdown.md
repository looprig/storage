# Optional Blob Reader Lifecycle Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an optional, testable bounded-shutdown capability for readers returned by `Blobs.Get` without strengthening the base `Blobs` contract.

**Architecture:** `BlobReaderLifecycle` embeds `Blobs` and publishes a positive close bound. Only providers claiming it guarantee concrete non-nil readers, concurrent Read/Close safety, stable idempotent Close classification, bounded provider-controlled waits, and non-EOF terminal reads after Close. `memstore` claims the capability through its mutex-owned reader; legacy providers remain valid `Blobs` implementations.

**Tech Stack:** Go 1.26.6 standard library, `testing`, reflection, race detector, and the existing `storetest` package.

---

### Task 1: Freeze the optional contract

1. Restore the v0.5.0 `Blobs` documentation.
2. Add exported `BlobReaderLifecycle`, embedding `Blobs` and returning a positive `time.Duration` bound.
3. Document stable Close classification and the provider-specific terminal error.
4. Keep the addition compatible and plan it for v0.6.0.

### Task 2: Split conformance

1. Leave `TestBlobs` restricted to base behavior.
2. Add explicit `TestBlobReaderLifecycle` for capability providers.
3. Detect both nil interfaces and dynamic typed-nil readers.
4. Test positive bound, immediate Close, stable repeated Close, post-Close non-EOF errors, and concurrent Read/Close termination within the bound.
5. State that a provider with blocking external I/O owes a deterministic provider-specific bounded-wait test; the shared loop cannot prove causal unblock.

### Task 3: Make memstore opt in

1. Preserve the mutex-owned copied-byte reader and `fs.ErrClosed` behavior.
2. Add a conservative positive close bound and compile-time capability assertion.
3. Run both base and lifecycle conformance under the race detector.

### Task 4: Mutate, verify, and commit

1. Mutate typed-nil Get, EOF-only post-Close behavior, missing capability, and reader locking in disposable archives; require relevant suites to fail.
2. Run `GOWORK=off go mod tidy`, the full race suite, and `GOWORK=off make check`.
3. Cross-compile every test binary with `GOOS=linux GOARCH=386 GOWORK=off go test -exec=/usr/bin/true ./...`.
4. Review diff/status and commit a reviewable correction without pushing or tagging.
