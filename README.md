# storage

Neutral, stdlib-only storage contracts for durable session/workspace persistence.

`storage` defines five small primitives:

- **`Ledger`** — an append-only, CAS-sequenced record log.
- **`Leaser`** — renewable single-writer ownership fenced by a monotonic epoch.
- **`KV`** — revision-CAS key/value metadata.
- **`Blobs`** — content-addressed immutable bytes.
- **`OrderedIndex`** — durable records with immutable acceptance order and current
  ranked and due views.

It also provides typed errors, the `AppendDefinite` ambiguity resolver, an in-memory
reference backend (`memstore`), and reusable backend conformance suites (`storetest`).
Consumers depend only on these interfaces; concrete backends (`fsstore`, `natsstore`,
…) live in their own modules.

## Composition

The two constructors deliberately have different compatibility contracts.
`NewComposite` assembles the legacy four primitives (`Ledger`, `Leaser`, `KV`, and
`Blobs`) and leaves `OrderedIndex` nil; it never synthesizes an index for legacy
callers. `NewCompositeWithOrderedIndex` requires and wires all five primitives, so a
consumer that needs ordered records can fail at composition time. `memstore.New()`
returns a complete five-primitive composite for examples and tests.

This module has **zero third-party dependencies** and will keep it that way.
