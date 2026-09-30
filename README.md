# storage

Neutral, stdlib-only storage contracts for durable session/workspace persistence.

`storage` defines five small primitives:

- **`Ledger`** — an append-only, CAS-sequenced record log.
- **`Leaser`** — renewable single-writer ownership fenced by a monotonic epoch.
- **`KV`** — revision-CAS key/value metadata.
- **`Blobs`** — content-addressed immutable bytes.
- **`OrderedIndex`** — durable records with immutable acceptance order and current
  ranked and due views.

It also provides typed errors, name validation (`ValidateName`), the `AppendDefinite`
ambiguity resolver, an in-memory reference backend (`memstore`), and reusable backend
conformance suites (`storetest`). Consumers depend only on these interfaces; concrete
backends (`fsstore`, `natsstore`, `pgstore`, `s3store`, `rclonestore`) live in their
own modules.

This module has **zero third-party dependencies** and will keep it that way.

## Status

Released and in use by every Looprig storage backend and by `sessionstore`,
`harness`, `flow/store` and the modules above them. The current contract includes
the optional `BlobReaderLifecycle` capability (with an opt-in adapter for local
providers) and the nested-name rule described below.

## Install

```sh
go get github.com/looprig/storage@latest
```

## Packages

| Package | Purpose |
|---|---|
| `github.com/looprig/storage` | The five primitive interfaces, `Composite`, typed errors, validators, `AppendDefinite`, and the opt-in `WithBoundedBlobReaders` adapter. |
| `github.com/looprig/storage/memstore` | In-memory reference backend; `memstore.New()` returns a complete five-primitive `*storage.Composite`. It is the conformance oracle other backends are checked against. |
| `github.com/looprig/storage/storetest` | Backend conformance suites: `TestLedger`, `TestLeaser`, `TestLeaserLifecycle`, `TestKV`, `TestBlobs`, `TestBlobReaderLifecycle`, `TestOrderedIndex`, `TestOrderedIndexRevisionConflicts`. |
| `github.com/looprig/storage/examples/...` | Runnable examples for each primitive, composition and conformance. |

## Composition

The two constructors deliberately have different compatibility contracts.
`NewComposite` assembles the legacy four primitives (`Ledger`, `Leaser`, `KV`, and
`Blobs`) and leaves `OrderedIndex` nil; it never synthesizes an index for legacy
callers. `NewCompositeWithOrderedIndex` requires and wires all five primitives, so a
consumer that needs ordered records can fail at composition time. A missing primitive
is reported as `*IncompleteCompositeError`.

```go
memory := memstore.New()
store, err := storage.NewCompositeWithOrderedIndex(
	memory.Ledger, memory.Leaser, memory.KV, memory.Blobs, memory.OrderedIndex)
if err != nil {
	return err
}
```

See `examples/composite` for the full, tested version.

## Names

Names and keys are canonical by construction (see `ValidateName`), so no two valid
names alias one backend location. A name and a name that extends it with `/…` (for
example `sessions/a` and `sessions/a/meta`) are **distinct names that must coexist**
in `KV` and `Blobs`, in either creation order, each with its own value and revision;
deleting one leaves the other intact, and `KV.Keys` / `Blobs.List` prefix filtering is
plain string-prefix matching. `storetest.TestKV` and `storetest.TestBlobs` exercise
this, so a backend must pass those cases before it can claim conformance to this
contract.

Likewise a name, its dotted extension, and that extension's `/…` extension (for
example `sessions/a`, `sessions/a.log` and `sessions/a.log/b`) are three distinct names
for **every** primitive: `Ledger` and `Leaser` names, `KV` and `Blobs` keys, and
`OrderedIndex` namespaces. A backend that encodes a name as a file with a suffix must
pick a suffix no valid name can spell. Every conformance suite exercises this over a
spread of extensions (`.log`, `.lock`, `.olog`, `.kv`, `.blob`, `.json`, `.dat`, `.tmp`,
`.x`, `.a.b`) in both creation orders.

Every backend must accept ledger payloads and KV values up to 1 MiB; larger payloads
are the engine's responsibility to offload to `Blobs`.

## Optional Blob reader lifecycle

The base `Blobs` contract does not require bounded reader shutdown.
`BlobReaderLifecycle` is a compatible optional capability: a provider opts in by
publishing a positive `BlobReaderCloseBound()` and guaranteeing concrete non-nil
readers, concurrent-safe `Read` and `Close`, stable idempotent `Close`
classification, non-EOF terminal reads after `Close`, and bounded provider-controlled
waits. Consumers that require bounded shutdown (for example `sessionstore`) test for
this capability; `storetest.TestBlobReaderLifecycle` verifies it. `memstore`
implements it.

### Opting a local provider in: `WithBoundedBlobReaders`

A filesystem-shaped provider such as `fsstore` deliberately does **not** implement
`BlobReaderLifecycle`: portable file I/O has no read deadlines, so it cannot promise
that a blocked `Read` returns. `sessionstore.Open` therefore refuses it
(`invalid backend: missing BlobReaderLifecycle`). A **local, single-host** deployment
that accepts the trade-off below can opt in explicitly:

```go
fs, err := fsstore.Open(fsstore.Options{Root: dir})
if err != nil {
	return err // handle fsstore.ErrLegacyLayout with "move or delete <Root>", never a retry
}
defer fs.Close()

backend, err := fs.Backend().WithBoundedBlobReaders() // a copy; fs.Backend() is not mutated
if err != nil {
	return err
}
sessions, err := sessionstore.Open(ctx, backend)
```

`storage.WithBoundedBlobReaders(b Blobs)` adapts a single provider;
`(*Composite).WithBoundedBlobReaders()` returns a copy of a composite with only its
`Blobs` adapted. The contract:

- **What is bounded.** `Get` hands back the read half of an in-memory pipe and copies
  the provider's reader into it on a goroutine the adapter owns. The returned reader's
  `Close` closes the pipe: it returns within `BoundedBlobReaderCloseBound` (1s, a
  generous declared ceiling for a pipe close) and releases any `Read` blocked on the
  stream, which returns `*storage.BlobReaderClosedError` (wrapping `io.ErrClosedPipe`,
  never `io.EOF`). `Close` never waits on the provider.
- **What it costs.** The provider's `Read` is **abandoned, not cancelled**. If it is
  blocked when the caller closes, the pump goroutine and the provider reader — for a
  filesystem store, an open file descriptor — stay alive until that `Read` returns on
  its own; only then is the provider reader closed. On a local disk that is
  microseconds; on a wedged network or FUSE mount it may be never. Callers must still
  `Close` every reader they obtain.
- **Integrity.** A complete stream ends in a genuine `io.EOF` after the provider reader
  closed cleanly. A provider `Read` error mid-stream, or a provider `Close` error after
  the last byte, reaches the caller as that error — never as a clean EOF. `Get` errors
  (including a typed `*BlobNotFoundError`) are returned by `Get` itself, eagerly; a
  provider returning a nil reader is refused with `*NilBlobReaderError`.
- **Scope.** It is for local, single-host use. A distributed or network backend should
  implement `BlobReaderLifecycle` natively by genuinely cancelling its I/O (as `s3store`
  does). A provider that already implements it is returned unchanged, with its own
  bound. `Put`, `Delete` and `List` delegate unchanged, and `PathReporter` is forwarded
  exactly when the wrapped provider implements it. A nil provider is refused
  (`*NilBlobsError`, or `*IncompleteCompositeError` naming `Blobs` for a composite).

## Writing a backend

A backend's own tests call the relevant `storetest` suites with a factory that
returns a fresh, empty primitive:

```go
func TestConformance(t *testing.T) {
	storetest.TestLedger(t, func(t *testing.T) storage.Ledger { return memstore.New().Ledger })
	storetest.TestKV(t, func(t *testing.T) storage.KV { return memstore.New().KV })
	storetest.TestBlobs(t, func(t *testing.T) storage.Blobs { return memstore.New().Blobs })
}
```

## Where it sits

Tier 0 in the Looprig dependency graph: no Looprig dependencies.

## Development

Go 1.26.8 baseline. Verify standalone, without a workspace:

```sh
GOWORK=off go test ./...
make check   # fmt-check, vet, staticcheck, gosec, govulncheck, race tests, build
```

`make test` runs `GOWORK=off go test -race ./...`. `make check` (what CI runs) invokes
staticcheck, gosec and govulncheck at pinned versions with `go run`, so they add
nothing to `go.mod`. See `CONTRIBUTING.md`.

## License

Apache License 2.0; see `LICENSE`.
