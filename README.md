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
the optional `BlobReaderLifecycle` capability and the nested-name rule described
below.

## Install

```sh
go get github.com/looprig/storage@latest
```

## Packages

| Package | Purpose |
|---|---|
| `github.com/looprig/storage` | The five primitive interfaces, `Composite`, typed errors, validators, `AppendDefinite`. |
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
