# Ledger Rules Conformance Tests

This package runs the [Amaru ledger rules conformance vectors](https://github.com/pragma-org/amaru)
against Dingo's ledger implementation. The shared harness and embedded test
data live in `github.com/blinklabs-io/ouroboros-mock/conformance`; this package
provides `DingoStateManager`, an adapter that drives Dingo's database and
ledger packages with an in-memory SQLite backend so every vector runs against
a clean state.

## What the vectors cover

The vectors exercise **Conway era** ledger rules:

- UTxO validation — inputs, outputs, fees, collateral
- Certificate processing — stake, pool, DRep, committee
- Governance — proposals, voting, enactment
- Script execution — native scripts, Plutus V1/V2/V3

## Running the tests

Run the full suite:

```bash
go test ./internal/test/conformance/
```

Run with verbose vector-level output (useful when investigating a failure):

```bash
go test -v ./internal/test/conformance/ -run TestRulesConformanceVectors
```

Run the variant that reports per-vector pass/fail statistics:

```bash
go test -v ./internal/test/conformance/ -run TestRulesConformanceVectorsWithResults
```

Run a single vector by substring match (delegated by the harness):

```bash
go test -v ./internal/test/conformance/ -run TestRulesConformanceVectors -vector <name>
```

## When to run them

**Conformance tests are mandatory after every ledger-affecting change**, not
just once at the end of a branch. Specifically, run them after any edit under
`ledger/`, `database/plugin/metadata/`, `database/models/`, or any dependency
bump of `gouroboros`, `plutigo`, or `ouroboros-mock`. A regression here almost
always indicates a correctness bug that CI on unit tests will miss.

Cross-repo change cascades that must re-run this suite:

| Changed repo | Must run conformance tests in |
|---|---|
| `plutigo`    | plutigo → gouroboros → **dingo** |
| `gouroboros` | gouroboros → **dingo** |
| `dingo`      | **dingo** |

## How it works

1. The test extracts embedded vectors from `ouroboros-mock/conformance` into
   a temp directory (`ExtractEmbeddedTestdata`).
2. A fresh `DingoStateManager` spins up an in-memory SQLite database and
   applies Dingo's GORM migrations.
3. The harness (`conformance.NewHarness`) walks every vector, feeding
   transactions through the state manager and comparing expected vs. actual
   ledger state after each step.
4. `RunAllVectors` fails the Go test on any vector mismatch;
   `RunAllVectorsWithResults` returns structured pass/fail counts instead
   so progress can be tracked.

## Files

| File | Purpose |
|---|---|
| `conformance_test.go` | Go test entry points (`TestRulesConformanceVectors`, `…WithResults`) |
| `state_manager.go`    | `DingoStateManager` — implements `conformance.StateManager` against Dingo's DB/ledger |
| `state_provider.go`   | State-query adapters used by the harness |

## Updating vectors

The vectors themselves are **embedded in `ouroboros-mock`**, not in this repo.
To update the corpus, bump the `ouroboros-mock` dependency in `go.mod` and
re-run the suite. Do not add or mutate vectors locally.

## Debugging a failing vector

1. Re-run the failing vector in isolation with `-v` so the harness prints
   per-step diagnostics.
2. Check whether the failure is Dingo-side (ledger logic) or state-manager-side
   (`state_manager.go` mapping between `common.*` types and Dingo's GORM
   models). State-manager bugs usually surface as the same vector failing
   identically across multiple eras; ledger bugs are usually era-specific.
3. If the upstream vector itself looks wrong, file an issue against
   `blinklabs-io/ouroboros-mock` rather than patching around it here.
