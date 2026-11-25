# Dingo development

Use the Go version declared in [`go.mod`](../go.mod) and GNU Make.

## Nix

On x86_64 Linux, the flake provides pinned Go 1.26, GNU Make, a C compiler
for race tests, gopls, golines, golangci-lint and a Nix formatter:

```sh
nix develop
make build
./dingo devnet
```

`GOTOOLCHAIN=local` keeps Go from downloading a different compiler. Commit
`flake.lock` when updating inputs, and keep the selected Go version compatible
with both `go.mod` and `sdk/go.mod`. With direnv installed, `direnv allow`
activates the same shell through `.envrc`.

To build just the node with all storage providers, or start a temporary local
devnet directly:

```sh
nix build
./result/bin/dingo --help
nix run . -- devnet
```

`nix fmt` formats Nix files. Go formatting and validation remain the Makefile's
responsibility. The Nix package skips Go tests; run the checks below in
`nix develop`, including conformance and race tests.

When dependencies change, update `vendorHash` in `perSystem/packages.nix`:
temporarily set it to `lib.fakeHash`, run `nix build`, then replace it with the
`got: sha256-...` hash from Nix's expected fixed-output mismatch and rebuild.
The package builds only `cmd/dingo`; `make build` builds all command binaries.

Network shortcuts use the same package and select the network explicitly:

```sh
nix run .#preview
nix run .#preprod
nix run .#mainnet
nix run .#preview -- --help
nix build .#preview # result/bin/dingo-preview
```

They default to `.dingo-preview`, `.dingo-preprod` and `.dingo-mainnet` under
the working directory. `CARDANO_DATABASE_PATH` or `--data-dir` overrides that
default. Other arguments pass through to Dingo; Ctrl+C reaches the node.
The wrappers use Dingo's default ports, so concurrent networks need distinct
listener and socket settings.

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

The `cancel-obsolete-merge-groups` workflow scans active merge-group runs when
queue CI is requested or completed, and when a queue branch is deleted. It
cancels a run only after the GitHub API confirms that its queue ref is absent.
Existing queue refs keep their runs. The cleanup uses the default-branch
workflow without checking out merge-group code; test jobs retain read-only
permissions.
Normal cancellation gives runner cleanup 30 seconds to finish before forcing
any obsolete runs that remain active to stop.

## Plugin SDK module

`sdk/` is a separate Go module, `github.com/blinklabs-io/dingo/sdk`. The root
module's `./...` does not include it; `make test` and
`make mod-tidy` run each nested module on their own. Its import rules are
checked by `make import-boundaries`.

Release it by pushing an `sdk/v0.MINOR.PATCH` tag. The `sdk-release` workflow
validates the tag, runs the SDK tests and refreshes the Go module proxy; it
builds no node binaries or images. The SDK stays at major version 0 until a
1.0 is declared, and the tag check rejects any other major version.

## Conformance profiles

Dingo reports compatibility in separate layers; a green ledger result is not
complete node conformance.

| Profile | Command | Scope |
| --- | --- | --- |
| Ledger rules | `go test ./internal/test/conformance/` | Pinned Cardano Blueprint ledger vectors from `ouroboros-mock`, Dingo era validation entry points, and the LedgerDB set |
| Deterministic consensus | `go test ./ouroboros/ -run TestConsensusConformance` | Shared `ouroboros-mock` consensus scenarios: final chain choice, rollback points, and the ChainSync Dingo serves downstream |
| Reference node | `./internal/test/devnet/run-tests.sh --conformance` | Dingo beside `cardano-node` on the live DevNet; not run by either deterministic profile |

The release and Linux CI gates run the ledger and deterministic consensus
profiles as part of `./...`, and their verbose output reports the exact corpus
and scenario counts. The [conformance
tests](../internal/test/conformance/README.md) document what each profile proves
and excludes.

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
- Read the [architecture](../ARCHITECTURE.md) and [database
  design](../DATABASE.md) when a change affects component ownership or persisted
  state.
- Use the [documentation index](README.md) to find package, integration, and
  operator documentation.
