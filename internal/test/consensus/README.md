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
  sidecar.go                 Sidecar runtime (connection + driver loop)
  emit.go                    WriteVector helper
  sidecar_test.go            Offline round-trip + recorder tests
  live_capture_test.go       Build-tag-gated end-to-end test
  cmd/capture-sidecar/       Binary that does one capture run
  Dockerfile.configurator    Shared base image (genesis toolchain)
  Dockerfile.capture_sidecar Shared base image (Go build of cmd/)
  capture-scenario.sh        Dispatcher: forwards to scenarios/<n>/run.sh
  scenarios/
    intersect_origin_one_rollforward/
      configurator.sh            Per-scenario genesis/config setup
      docker-compose.yml         Per-scenario service stack
      testnet.yaml               Per-scenario testnet shape
      topology/single.json       Per-scenario topology
      capture-conversation.json  Per-scenario sidecar-driving script
      run.sh                     Per-scenario orchestration
      README.md                  What this scenario tests
  testdata/
    fixtures/                Hand-crafted vectors for format/ tests
```

Adding a scenario means dropping in a new `scenarios/<name>/`
directory. The shared base does not change.

## Running a scenario

```bash
./capture-scenario.sh intersect_origin_one_rollforward -out /tmp/vector.json
```

The dispatcher resolves `scenarios/<name>/run.sh` and execs it. Each
scenario owns its own orchestration shape (number of cardano peers,
configurator behavior, number of sidecar invocations) so the
dispatcher itself stays trivial.

See each scenario's `README.md` for what it captures and how to run it
directly.

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
# Fast: offline format + recorder tests.
go test ./internal/test/consensus/...

# Slow: live end-to-end capture (docker required).
go test -tags consensuscapture -run TestCaptureScenarioLiveStack \
    ./internal/test/consensus/...
```

The build-tag-gated test brings up a scenario's docker-compose stack,
runs `capture-scenario.sh`, decodes the produced vector, and asserts
shape invariants. `make test` does not include it.

## Recording layer

The capture sidecar records via gouroboros's decoded protocol
callbacks (`RollForwardRawFunc`, `RollBackwardFunc`) so it does not
touch gouroboros internals. Raw header / block bytes flow through to
the vector's `header_cbor` / `block_cbor` fields untouched; envelope
fields (slot, hash, era, tip, points) are populated from the callback
arguments as structured JSON.
