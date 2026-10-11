# Dingo development

Use the Go version declared in [`go.mod`](../go.mod) and GNU Make.

## Build and test

From the repository root:

```sh
make          # format and build
make build    # build the command binaries
make test     # run the test suite with race detection
```

For a focused test, name the package and test directly:

```sh
go test -race -run TestName ./ledger
```

The Makefile also provides `make lint`, `make docs-parity`,
`make config-parity`, and `make sql-check`. After changing SQL queries, run
`make sql` to regenerate the checked-in code before `make sql-check`.

## Conformance profiles

Dingo reports compatibility in separate layers; a green ledger result is not
complete node conformance.

| Profile | Command | Scope |
| --- | --- | --- |
| Ledger rules | `go test ./internal/test/conformance/` | Pinned Cardano Blueprint ledger vectors from `ouroboros-mock`, Dingo era validation entry points, and real metadata backends |
| Deterministic consensus | `go test ./ouroboros/ -run TestConsensusConformance` | Shared `ouroboros-mock` consensus scenarios: final chain choice, rollback points, and the ChainSync Dingo serves downstream |
| Reference node | `./internal/test/devnet/run-tests.sh --conformance` | Dingo beside `cardano-node` on the live DevNet; not run by either deterministic profile |

The release and Linux CI gates run the ledger and deterministic consensus
profiles as part of `./...`, and their verbose output reports the exact corpus
and scenario counts. The [conformance tests](../internal/test/conformance/README.md)
document what each profile proves and excludes.

The tag-triggered release workflow also generates a Conway CBOR decoding
conformance report from the upstream corpus and uploads it as a dated JSON
release asset. A release proceeds to asset upload only when Dingo accepts and
rejects the corpus samples as expected and matches the reference encodings.

## Local development workflows

- [Run Dingo on the local DevNet](devnet.md).
- [Run benchmarks and collect profiles](benchmarks.md).
- Read the [architecture](../ARCHITECTURE.md) and [database design](../DATABASE.md)
  when a change affects component ownership or persisted state.
- Use the [documentation index](README.md) to find package, integration, and
  operator documentation.
