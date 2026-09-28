# Go code reference

Dingo's code-level documentation is maintained with the code it describes.
Package comments are normally in `doc.go`; comments on exported types,
functions, methods, and fields remain beside those declarations. Go tooling
combines both into a reference for the exact revision checked out.

## Read documentation with `go doc`

Run these commands from the repository root:

```sh
# Package overview and exported API
go doc ./ledger

# Package overview plus documentation for every exported declaration
go doc -all ./ledger

# One exported type or symbol
go doc ./ledger LedgerState

# A package by its full import path
go doc github.com/blinklabs-io/dingo/database
```

To discover packages with package comments:

```sh
go list -buildvcs=false \
  -f '{{if .Doc}}{{.ImportPath}}: {{.Doc}}{{end}}' ./...
```

Prefer `go doc` before inferring an API from call sites. It combines package
documentation with the current exported surface and avoids documentation from
a different branch or release.

## Documented package map

| Area | Package documentation | Responsibility |
| --- | --- | --- |
| Client API | [`api/utxorpc/doc.go`](../api/utxorpc/doc.go) | UTxO RPC v1alpha and v1beta server behavior |
| Chain state | [`chain/doc.go`](../chain/doc.go) | Primary and candidate chains, forks, and rollback orchestration |
| Chain choice | [`chainselection/doc.go`](../chainselection/doc.go) | Multi-peer Praos and Genesis chain selection |
| Sync state | [`chainsync/doc.go`](../chainsync/doc.go) | Per-peer chain-sync state and stall recovery signals |
| Connections | [`connmanager/doc.go`](../connmanager/doc.go) | Inbound and outbound connection lifecycle |
| Storage | [`database/doc.go`](../database/doc.go) | Blob and metadata stores, CBOR offsets, and query guidance |
| Events | [`event/doc.go`](../event/doc.go) | EventBus publishing, ordering, delivery, and subscription contracts |
| Ledger | [`ledger/doc.go`](../ledger/doc.go) | Consensus-critical state, validation, rollback, epochs, and nonces |
| Leios | [`ledger/leios/doc.go`](../ledger/leios/doc.go) | CIP-0164 committees, votes, quorum, and certificates |
| Mempool | [`mempool/doc.go`](../mempool/doc.go) | Transaction admission, validation, ordering, and watermarks |
| Ouroboros | [`ouroboros/doc.go`](../ouroboros/doc.go) | NtN/NtC mini-protocol handlers and message routing |
| Peer governance | [`peergov/doc.go`](../peergov/doc.go) | Peer sources, tiers, churn, and ingress eligibility |

This table points to package-level entry points, not every package. Use
`go list ./...`, repository search, and `go doc` to discover subpackages and
symbol documentation relevant to a task.

## Published reference

[pkg.go.dev](https://pkg.go.dev/github.com/blinklabs-io/dingo) renders the Go
reference for published module versions. It is useful when no checkout is
available. For development work, use the local `go doc` output so the reference
matches the branch being changed.

## Maintaining package documentation

- Keep one package comment for each package and start it with `Package <name>`.
- Put package-level concepts and contracts in `doc.go`; keep symbol-specific
  behavior on the declaration it governs.
- Link to the implementation or a versioned repository document for details
  that would drift if repeated.
- Run `go doc` for the changed package and confirm the rendered text still
  describes its exported surface.
