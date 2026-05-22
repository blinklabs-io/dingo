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
  sidecar_test.go            Offline round-trip + recorder tests
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
# Fast: offline format + recorder + composer + diff tests.
go test ./internal/test/consensus/...

# Build-tag-gated: golden-corpus assertions (load each committed
# vector under testdata/captured/ and verify structural invariants).
go test -tags consensuscapture -run "TestCapturedGoldensDecode|TestForkAndSelectV1SharedPrefix" \
    ./internal/test/consensus/...

# Slow: live end-to-end capture (docker required).
go test -tags consensuscapture -run TestCaptureScenarioLiveStack \
    ./internal/test/consensus/...
```

The build-tag-gated tests bring up a scenario's docker-compose stack
(or load a committed golden) and assert shape invariants. `make test`
does not include them.

## Recording layer

The capture sidecar records via gouroboros's decoded protocol
callbacks (`RollForwardRawFunc`, `RollBackwardFunc`) so it does not
touch gouroboros internals. Raw header / block bytes flow through to
the vector's `header_cbor` / `block_cbor` fields untouched; envelope
fields (slot, hash, era, tip, points) are populated from the callback
arguments as structured JSON.
