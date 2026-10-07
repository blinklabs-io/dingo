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

## Pull-request CI selection

Pull requests that change only root-level Markdown files or Markdown files
under `docs/` skip lint, vulnerability scanning, Go tests, binary builds, and
Docker builds. All other paths run the full staged pipeline, including Go
package documentation (`doc.go`), configuration, fixtures, dependencies, and
CI scripts. Mixed documentation and code changes also run the full pipeline.

The `changes` job compares the PR merge base with its head using Git, including
both sides of renames. An empty diff or a failed comparison runs full CI.
Manual CI runs, main-branch publishing, and release tags always run full CI.
The workflow still starts for documentation-only PRs so existing required job
checks can report skipped instead of remaining pending. Commit-message checks
continue to run.

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

## Local development workflows

- [Run Dingo on the local DevNet](devnet.md).
- [Run benchmarks and collect profiles](benchmarks.md).
- Read the [architecture](../ARCHITECTURE.md) and [database design](../DATABASE.md)
  when a change affects component ownership or persisted state.
- Use the [documentation index](README.md) to find package, integration, and
  operator documentation.
