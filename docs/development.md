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
under `docs/` run documentation parity checks, then skip lint, vulnerability
scanning, the full Go test suite, binary builds, and Docker builds. All other
paths run the full staged pipeline, including Go package documentation
(`doc.go`), configuration, fixtures, dependencies, and CI scripts. Mixed
documentation and code changes also run the full pipeline.

The `changes` job runs the classifier from the workflow commit in a separate
checkout and compares the PR checkout's merge base with its head using Git,
including both sides of renames. An empty diff or a failed comparison runs
full CI.
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

The Linux tag workflow generates four conformance reports and uploads them as
dated JSON release assets: Conway CBOR decoding, ledger rules, UPLC
conformance and UPLC turbo performance. The CBOR dataset and ledger fixtures
are pinned by commit; the UPLC and Turbo archives used by NDC are pinned by
SHA-256. A report that records failures is still uploaded; the release step
requires all four reports to exist. Report generation never stops a release:
when a generator crashes or its inputs cannot be prepared, an `error` report
naming the scenario and the cause is uploaded in its place. Generator and
corpus preparation deadlines leave time to write all four reports and compress
them within the 60-minute report step. The Linux release job has a 90-minute
limit for setup and asset publishing. The turbo report measures every program
in NDC's curated corpus by default; `TURBO_MAX_PROGRAMS` can select an evenly
spaced subset for diagnostics. It uses 50 samples per program and the mean of
the best 30. A program that cannot
be decoded or evaluated is omitted from measurements, and the generation log
records the error; NDC counts a missing benchmark as a failure.

## Local development workflows

- [Run Dingo on the local DevNet](devnet.md).
- [Run benchmarks and collect profiles](benchmarks.md).
- Read the [architecture](../ARCHITECTURE.md) and [database design](../DATABASE.md)
  when a change affects component ownership or persisted state.
- Use the [documentation index](README.md) to find package, integration, and
  operator documentation.
