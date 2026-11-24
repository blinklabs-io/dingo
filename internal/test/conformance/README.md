# Ledger-Rule Conformance Tests

This package runs the pinned [Cardano Blueprint ledger conformance
corpus](https://github.com/cardano-scaling/cardano-blueprint/tree/main/src/ledger/conformance-test-vectors)
against Dingo's ledger implementation. The shared harness and embedded test data
live in `github.com/blinklabs-io/ouroboros-mock/conformance`; this package
provides `DingoStateManager`, an adapter that drives the ledger's database,
its LedgerDB set and the `ledger/governance` package -- the same production
code the node uses, not a hand-rolled second implementation. The ledger's
database keeps no metadata store, so every vector's UTxO, certificate,
governance, protocol-parameter and network-state writes and reads go through
the LedgerDB set (see [How it works](#how-it-works)). It needs no setup.

`ratifyProposals` decides most action types' ratification with its own
vote-shape heuristic ("did each voter group say yes"), not by calling
`ledger/governance`'s real `ShouldRatify` -- `NoConfidence` and
`UpdateCommittee` are the only pair whose ratification calls the real
`ShouldRatify` (via `committeeActionRatified` in `state_manager.go`), because
those two vectors need production's actual stake-weighted thresholds to
distinguish. Building the `governance.ProposalTally` those two action types
pass to `ShouldRatify` still happens in the harness rather than
`ledger/governance`'s own tally. The harness adds active proposal deposits to
each return account's delegated stake locally. Production applies the same
CIP-1694 rule through `ActiveProposalDepositDRepPower`.

## What the vectors cover

The pinned archive contains the Conway ImpSpec ledger corpus plus one synthetic
rollback fixture. Its paths group inherited rule specifications under Shelley,
Allegra, Mary, Alonzo, Babbage, and Conway labels, but Dingo executes every
archive vector through the Conway validation entry point:

- UTxO validation — inputs, outputs, fees, collateral
- Certificate processing — stake, pool, DRep, committee
- Governance — proposals, voting, enactment
- Script execution — native scripts, Plutus V1/V2/V3

The corpus is ledger-rule coverage only. It does not prove consensus,
networking, node-to-node protocol, or end-to-end compatibility with
`cardano-node`.

## Node conformance matrix

| Profile | Source | Default Dingo test | What it proves | What it excludes |
| --- | --- | --- | --- | --- |
| Ledger rules | Pinned Cardano Blueprint archive in `ouroboros-mock` | `go test ./internal/test/conformance/` | Dingo's ledger entry points and backend behavior against the shared ledger corpus | Consensus, networking, and reference-node behavior |
| Deterministic consensus | Shared `ouroboros-mock/consensus` captured scenarios | `go test ./ouroboros/ -run TestConsensusConformance` | Final chain choice, within-k and beyond-k behavior, rollback/intersection points, tie-breaking, and downstream ChainSync observations | Live sockets, full block bodies, and cardano-node process behavior |
| Reference node | DevNet `--conformance` profile | `./internal/test/devnet/run-tests.sh --conformance` | Dingo beside `cardano-node` in the configured live topology | Not run by the ledger or deterministic consensus profiles |

The deterministic corpus currently contains seven scenarios: one origin
roll-forward smoke test, one non-origin intersection, one within-k fork, the
same fork with the winning peer fed first, one longer fork using `local_tip`
whose losing fork is denser after the fork point, one equal-length slot battle,
and one beyond-k no-switch case. The corpus has no generated multi-peer
schedules (see the Tweag runner below). The tests log the exact scenario and
ledger coverage counts; a passing ledger profile must not be summarized as
complete node conformance. The release and Linux CI gates run the ledger and
deterministic consensus profiles as part of `./...`; the reference-node profile
is a separate DevNet check and is not represented as passing when it was not
run.

The consensus replay feeds each peer's captured headers through Dingo's
ChainSync client handlers into the real chain selector, then asserts the final
tip, the selector's reported rollback point on fork switches, and the ChainSync
messages Dingo serves downstream. For the last, the selected peer's headers
are added to a Dingo chain as header-only blocks (the replay has no block
bodies, and a node-to-node `RollForward` carries only the header) and a
node-to-node client syncs that chain through Dingo's ChainSync server until it
receives `AwaitReply`. The client intersects where the selected peer's trace
starts: at origin, or at the point of a leading `RollBackward`, in which case a
stand-in block with that point's slot and hash anchors the chain below the
first header and is never served.

### Tweag Node-vs-Environment runner

[tweag/cardano-conformance-testing-of-consensus](https://github.com/tweag/cardano-conformance-testing-of-consensus)
designs three tools around `ouroboros-consensus`'s Node-vs-Environment tests:
`testgen` generates a test file holding a point schedule and a property,
`runner` serves that schedule from simulated upstream peers and judges the node
under test through a downstream observer peer, and `shrinkview` prints shrunk
counterexamples. Each `testgen`/`runner` pair is one property-test case, to be
run in a loop.

Dingo does not consume it yet:

- No release exists. The executables live on unreleased `conformance-testing`
  branches of Tweag forks of `cardano-node` (based on 10.5.1) and
  `ouroboros-consensus`, and there is no tagged test-file format or corpus to
  pin in `go.mod` or `ouroboros-mock`.
- The node under test must take a generated topology, connect to the runner's
  peers over real sockets, and accept headers without VRF validation, because
  the generators place blocks in arbitrary slots. Dingo has no option to skip
  VRF validation.
- Runs depend on the runner's wall-clock ticking and a Haskell toolchain, so
  they belong beside the reference-node DevNet profile rather than in the
  deterministic `go test` profile.

What it would add is generated density, peer-scheduling and adversarial
schedules with shrinking. Until a tagged runner release exists, the captured
scenarios in `ouroboros-mock` are the deterministic consensus coverage, and new
fork-choice cases are added there.

## Running the tests

Run the full suite:

```bash
go test ./internal/test/conformance/
```

Run with verbose vector-level output (useful when investigating a failure).
This also prints the per-vector pass/fail statistics, which a separate
`…WithResults` test used to produce from its own second replay of the corpus:

```bash
go test -v ./internal/test/conformance/ -run TestRulesConformanceVectors
```

Run a single vector by substring match (delegated by the harness):

```bash
go test -v ./internal/test/conformance/ -run TestRulesConformanceVectors -vector <name>
```

## When to run them

**Conformance tests are mandatory after every ledger-affecting change**, not
just once at the end of a branch. Specifically, run them after any edit under
`ledger/`, `database/ledgerdb/`, `internal/utxoset/`, `database/models/`, or
any dependency
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
2. A fresh `DingoStateManager` opens the ledger's database (a local Badger
   block store resolved through the same `plugin.Resolve` path the node uses,
   and no metadata store) and its LedgerDB set, and reconciles the set with
   the database the way the node does before a Mithril import
   ([`database.go`](database.go)).
3. `LoadInitialState` seeds the vector's initial state the way a ledger-state
   import does: the outputs through the set's `LoadUtxoSet`, then accounts,
   pools, DReps, the committee, the constitution, governance proposals and
   protocol parameters through `ledger.ImportLedgerDB`, the LedgerDB the
   Mithril import writes through. Committee hot keys and resignations are
   imported as certificates (`ImportCertificates`). The import records its
   point as the ledger's tip and finishes; the protocol parameters the harness
   validates with are then read back from the set. The node-diversity
   ledger-rule fixtures then add the treasury and reserves, the dormant DRep
   epoch count and the enacted proposal roots through the same LedgerDB
   (`LoadLedgerRulesInitialState`).
4. The harness (`conformance.NewHarness`) walks every vector. Each transaction
   goes through the import LedgerDB's `SetTransactionWithOpts`, the writer
   block application uses for certificates, withdrawals and the transaction
   index, and through `ledger/governance` for proposals, votes and DRep
   activity; its spent and produced outputs reach the set when the
   transaction commits. A write the set refuses fails the event. Epoch
   boundaries enact through `governance.EnactProposal` against the same
   LedgerDB and write changed protocol parameters to the set.
5. Every read the state provider answers comes from that LedgerDB or the set
   itself (outputs and per-credential UTxO stake), never from an in-memory
   mirror; see the provider's type doc comment for the one exception,
   reward-account balances. The live outputs the harness compares after each
   vector are enumerated from the set.
6. The corpus replays once per process through `RunAllVectorsWithResults`,
   which returns structured per-vector results. `assertCorpus` turns those
   results back into named per-vector subtests and fails on any mismatch, so
   the single replay serves as both the pass/fail gate and the progress
   statistics. See [Corpus replay budget](#corpus-replay-budget).
7. Between vectors, `Reset()` closes the backend, removes its directory and
   opens an empty one, so each vector starts from an empty set rather than
   from clean bookkeeping alone.

## Dingo validation entry point coverage

A green corpus run is **not** evidence that Dingo's own transaction validation
ran. The shared harness validates each vector with
`common.VerifyTransaction` over `conformance.ConformanceValidationRules`, a
list of upstream `gouroboros` rule functions. Nothing in that path reaches
`ledger/eras`' `EraDesc.ValidateTxFunc` -- `ValidateTxByron` through
`ValidateTxDijkstra` -- which is what the node runs against live transactions
and which differs from the upstream list (Conway and Dijkstra substitute Dingo
implementations for the committee-certificate, unknown-voter, Plutus, fee and
PlutusV1/V2 feature rules; the pre-Alonzo eras replace the upstream fee and
max-size rules outright).

The pinned `ouroboros-mock v0.20.5-0.20261002194245-6bfd701ae86c` corpus
contains 2,574 Blueprint vectors and one synthetic rollback fixture. A complete
run therefore reports 2,575/2,575, 100%, with the breakdown by era and
rule family shown in the verbose test output.

`state_provider_test.go` closes that gap:

- `TestConformanceVectorsExerciseDingoEraEntryPoints` replays the corpus a
  second time, routing every vector transaction through the production entry
  point for the era its protocol parameters select, and asserts **per vector**
  that the entry point resolved the inputs that transaction declares through
  live ledger state. The assertion is independent of the verdict, so it is not
  satisfied by a validator that returns the vector fixture's own result.
  Dingo's rule set is a superset of the corpus rule set (it keeps the fee and
  max-size rules the corpus excludes because the vectors carry
  Haskell-computed values), so agreement on verdicts is not asserted here.
- `TestDingoEraRegistryExposesValidationEntryPoints` fails on a `nil`
  `ValidateTxFunc` or a protocol-major-version gap between adjacent eras.
- `TestDingoEraEntryPointsRejectInputlessTransaction` covers **every** era in
  the registry, not just Conway. The corpus is Conway-only and validation
  rules are duplicated per era, so Conway coverage says nothing about
  `ValidateTxShelley` or `ValidateTxDijkstra`.
- `TestEntryPointExecutionFaultDetectsBypassedValidator` and
  `TestDingoEraEntryPointsReportsMissingValidator` prove the detectors detect,
  by substituting a no-op validator, a fixture-only verdict, a
  reject-without-reading validator, and a nil registry entry, and requiring
  each to be reported.

## Corpus replay budget

One full replay of the vector corpus is the single most expensive thing in this
package, and the corpus exercises ledger rules, so replaying it more than once
buys no additional rule coverage. The corpus replays **exactly once per
`go test` process**, sharded across up to eight state managers, and every
consumer reads that one memoized result set: the pass/fail gate and the
progress statistics. The vector extraction is shared the same way.

The one deliberate exception is the entry-point replay described in
[Dingo validation entry point coverage](#dingo-validation-entry-point-coverage).
The shared harness offers no hook for a caller-supplied validator, so that
coverage is not obtainable from the harness pass at any replay count. It is
also memoized once per process. Its cost is the state replay rather than the
validation: with the entry-point call removed the pass takes the same wall
clock.

## Files

| File | Purpose |
|---|---|
| `conformance_test.go` | Corpus replay entry point and per-vector assertions |
| `corpus_shard_test.go`, `corpus_shard_check_test.go` | Shard the corpus across state managers and check the shards partition it |
| `process_cleanup_test.go` | `TestMain`, which removes the shared vector extraction |
| `entry_points_test.go` | Dingo era validation entry-point coverage |
| `dijkstra_state_manager_test.go` | Dijkstra state-manager regression tests |
| `database.go` | `openLedgerBackend` -- opens the ledger's database, its LedgerDB set and the import LedgerDB, and reconciles the set |
| `state_manager.go` | `DingoStateManager` -- implements `conformance.StateManager` against the LedgerDB set and `ledger/governance`, reusing production import and transaction writers |
| `ledger_scenario.go` | Seeds the extra governance state of the node-diversity ledger-rule fixtures |
| `state_manager_test.go` | State-manager tests: restart survival, transaction rollback, and an epoch-transition/stake-snapshot test driving `ledger/governance.ProcessEpoch` and `ledger/snapshot.Manager` end to end |
| `state_provider.go` | State-query adapters used by the harness -- every read queries the LedgerDB or the set live (see its type doc comment for the one narrow, documented exception) |
| `state_provider_test.go` | State-provider tests and the shared vector extraction |

## Peras vectors

`peras_vectors_test.go` discovers Peras (CIP-0140) conformance vectors in a
`peras/` directory of the extracted `ouroboros-mock` corpus, beside `eras/`
and `synthetic/`. The corpus does not ship that directory yet, and
`TestPerasConformanceVectors` skips cleanly while it is absent or holds no
vectors. Expected layout:

```
peras/
├── <group>/              # optional, any depth
│   └── <vector>          # one vector per file
└── pparams-by-hash/      # skipped, as in eras/
```

Files are collected by `conformance.CollectVectorFiles`, the same rules the
rest of the corpus uses: `pparams-by-hash/` and `scripts/` are skipped, as are
`README` and `*.md` files, and the result is in lexical path order. Peras
vectors arrive through an `ouroboros-mock` bump like every other vector. The
loader performs no Peras validation yet; each discovered vector is only
checked to be non-empty.

## Updating vectors

The vectors themselves are **embedded in `ouroboros-mock`**, not in this repo.
The current import is `ouroboros-mock v0.20.5-0.20261002194245-6bfd701ae86c`,
whose `conformance/CORPUS.md` records Blueprint revision
`0f0c17e1ca24b062c868d216ae50708fc19c83ab`, archive SHA-256
`574ff7a17857dfc1f0cf477f7eb9eba1c2a0f901453396a779de4b2392ef6863`, and
the vector/protocol-parameter inventory. To update the corpus, bump the
`ouroboros-mock` dependency in `go.mod`, update the expected count and this
provenance entry, and re-run the suite. Do not add or mutate vectors locally.

## Debugging a failing vector

1. Re-run the failing vector in isolation with `-v` so the harness prints
   per-step diagnostics.
2. Check whether the failure is Dingo-side (ledger logic) or state-manager-side
   (`state_manager.go` mapping between `common.*` types and the LedgerDB
   models). State-manager bugs usually surface as the same vector failing
   identically across multiple eras; ledger bugs are usually era-specific.
3. If the upstream vector itself looks wrong, file an issue against
   `blinklabs-io/ouroboros-mock` rather than patching around it here.
