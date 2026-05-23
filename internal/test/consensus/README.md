# consensus

Consensus-conformance capture harness. Records cardano-node's served
chainsync / blockfetch traces into JSON test vectors. cardano-node is
the oracle.

## Layout

Shared base at the package root; one self-contained directory per
capture scenario:

```
internal/test/consensus/
  format/                    Go package: TestVector type + JSON codecs
  recorder.go                Recorder: callback-driven capture buffer
  conversation.go            capture-conversation.json loader + steps
                             (find_intersect / request_next / drain_to_tip)
  sidecar.go                 Sidecar runtime (connection + driver loop)
  emit.go                    WriteVector helper
  compose.go                 Multi-peer vector composition (used by
                             cmd/compose-consensus-vector)
  diff.go                    Structural-tolerance golden diff
  dispatch.go                Replay entry point: LoadVector + RunVector
                             (routes consensus/ledger by category)
  consensus_runner.go        Consensus-category replay driver
                             (drives chainselection.ChainSelector)
  ledger_runner.go           Ledger-category replay driver (stub —
                             awaits the conversion tool + an exported
                             final-state comparison helper)
  sidecar_test.go            Offline round-trip + recorder tests
  consensus_test.go          Walks testdata/captured/ + testdata/converted/
                             and replays each vector through RunVector
  runner_test.go             Dispatch + per-driver unit tests on
                             synthetic vectors
  live_capture_test.go       Build-tag-gated end-to-end smoke test
  golden_test.go             Build-tag-gated golden-corpus assertions
  cmd/
    capture-sidecar/         Binary that does one capture run
    compose-consensus-vector/ Binary that merges N single-peer captures
                              into one multi-peer vector + golden diff
  Dockerfile.configurator    Shared base image (genesis toolchain)
  Dockerfile.capture_sidecar Shared base image (Go build of cmd/sidecar)
  Dockerfile.compose_consensus_vector
                             Shared base image (Go build of cmd/composer)
  capture-scenario.sh        Dispatcher: forwards to scenarios/<n>/run.sh
  scenarios/
    intersect_origin_one_rollforward/   Single-peer smoke test
    fork_and_select_v1/                 Two-peer fork-and-select scenario
  testdata/
    fixtures/                Hand-crafted vectors for format/ tests
    captured/                Committed goldens from live captures
```

Adding a scenario means dropping in a new `scenarios/<name>/`
directory. The shared base does not change.

## Running a scenario

```bash
./capture-scenario.sh intersect_origin_one_rollforward -out /tmp/vector.json
```

The dispatcher resolves `scenarios/<name>/run.sh` and execs it. Each
scenario owns its own orchestration shape (number of cardano peers,
configurator behavior, number of sidecar invocations, whether a
composer + golden diff runs at the end) so the dispatcher itself
stays trivial.

See each scenario's `README.md` for what it captures and how to run it
directly. Existing scenarios:

| Scenario | Peers | What it tests |
|---|---|---|
| `intersect_origin_one_rollforward` | 1 | Smoke-test: handshake → find_intersect[origin] → roll_backward → roll_forward |
| `fork_and_select_v1` | 2 | Praos chain selection + rollback to non-genesis intersect across two divergent chains with a shared prefix |

Multi-peer scenarios use the `cmd/compose-consensus-vector` binary to
merge per-peer captures into the multi-peer vector and diff against
the committed golden (structural-tolerance match per
`internal/test/consensus/diff.go`).

## Vector format

JSON, schema-versioned, with a top-level `category` discriminant
(`consensus` or `ledger`). Binary fields (header bytes, msg bytes,
state blobs, hashes) are hex-encoded into JSON strings so vectors stay
diffable.

`consensus` vectors carry per-peer captured `chainsync` / `blockfetch`
traces as inputs and an `expected_output` with two sides: a wire-level
chainsync trace and a structured chain tip.

`ledger` vectors carry opaque CBOR config + initial/final
NewEpochState blobs and an event stream.

See `format/vector.go` for the Go shape and `testdata/fixtures/` for
hand-crafted examples.

## Tests

```bash
# Fast: offline format + recorder + composer + diff + runner tests.
# Also walks testdata/captured/ and testdata/converted/ via the
# replay runner — each committed vector becomes a subtest.
go test ./internal/test/consensus/...

# Build-tag-gated: golden-corpus structural assertions (load each
# committed vector under testdata/captured/ and verify shape).
go test -tags consensuscapture -run "TestCapturedGoldensDecode|TestForkAndSelectV1SharedPrefix" \
    ./internal/test/consensus/...

# Slow: live end-to-end capture (docker required).
go test -tags consensuscapture -run TestCaptureScenarioLiveStack \
    ./internal/test/consensus/...
```

The replay runner (`TestConsensusConformanceVectors` /
`TestLedgerConformanceVectorsNewFormat`) runs in plain `go test` —
no build tag — so every PR exercises the committed corpus against
dingo's chain-selection logic.

## Replay runner

`dispatch.go` is the entry point: `LoadVector(path)` decodes a JSON
vector, `RunVector(t, v)` dispatches on `v.Category` to either the
consensus driver (`consensus_runner.go`) or the ledger driver
(`ledger_runner.go`).

Today's consensus driver is scoped to **chain selection**: for each
peer in the vector it derives the last `roll_forward`'s tip from the
served trace and feeds it to `chainselection.ChainSelector` via
`UpdatePeerTip`, then asserts the selector's chosen peer's tip
matches `expected_output.final_tip`. That's the meaningful question
the `fork_and_select_v1` scenario exists to answer ("did Praos pick
the longer chain?"); extending the driver to drive
`chain.Manager` + full chainsync handler dispatch is a later step
once captures carry block bodies (currently only headers).

The ledger driver is a stub: it returns a clear "not yet wired"
error rather than silently passing. It will fill in once the
Amaru-corpus conversion tool populates `testdata/converted/` and an
exported final-state comparison helper lands in ouroboros-mock.

When a replay fails, the runner returns a structured error
(`tip slot mismatch: got X, want Y`, etc.). The build-tag-gated
golden tests in `golden_test.go` also validate the committed corpus
shape independently of the replay runner so a corrupt vector
surfaces both ways.

## Recording layer

The capture sidecar records via gouroboros's decoded protocol
callbacks (`RollForwardRawFunc`, `RollBackwardFunc`) so it does not
touch gouroboros internals. Raw header / block bytes flow through to
the vector's `header_cbor` / `block_cbor` fields untouched; envelope
fields (slot, hash, era, tip, points) are populated from the callback
arguments as structured JSON.
