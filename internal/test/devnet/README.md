# DevNet

A self-contained private Cardano network for end-to-end testing of Dingo as a
block producer. The whole thing runs locally under Docker Compose: a
configurator container generates fresh genesis files and pool keys on each
start, then nodes come up and forge blocks against each other while a
`txpump` sidecar continuously submits payment transactions into the mempool.

Two networks are available, selected by a Docker Compose profile:

- dingo (default, profile `dingo`) — three Dingo producers plus one Dingo
  relay, all running the tree under test. This is what `run-tests.sh`,
  `start.sh`, and `stop.sh` bring up with no flags. It validates the generic
  consensus/liveness suite dingo-vs-dingo and hosts dingo-only feature tests
  (e.g. CIP-50 pledge leverage) that have no `cardano-node` reference to
  compare against.
- conformance (opt-in, profile `conformance`, pass `--conformance`) — one
  Dingo producer alongside `cardano-node` (producer + relay), for
  compatibility/reference validation against the upstream implementation.

The Go test harness lives alongside this directory: `internal/test/devnet/`
(helpers and config loader at the top level, runnable scenarios under
`internal/test/devnet/scenarios/`). The layout mirrors
`internal/test/antithesis/`. Every Go file in the tree has a `linux` build
constraint because the harness requires a native Linux Docker engine, Bash,
Linux container networking, and Unix ownership semantics. Code that talks to
a running network additionally requires the `devnet` build tag, and
conformance-only tests also require `devnet_conformance` (see Test scenarios
below).

The pure logic the harness is built on — the network-spec loader and its
validation, the observed-chain state machine, and the scenario plan — carries
only the `linux` constraint, so its unit tests run in the ordinary Linux
`go test ./...` with no Docker involved. That is what keeps an invalid
accelerated spec or a broken rollback rule from only surfacing as a mysterious
DevNet stall.

## Topology — dingo mode (default)

| Service        | Role                            | Container IP  | NtN host port | NtC (LocalStateQuery) host port |
|----------------|----------------------------------|---------------|---------------|----------------------------------|
| `configurator-dingo` | One-shot: generates keys + genesis for 3 pools | — | — | — |
| `dingo-1`      | Dingo, forging with pool 1 keys | `172.20.0.13` | `3010`        | `3020`                            |
| `dingo-2`      | Dingo, forging with pool 2 keys | `172.20.0.14` | `3013`        | `3021`                            |
| `dingo-3`      | Dingo, forging with pool 3 keys | `172.20.0.15` | `3014`        | `3022`                            |
| `dingo-relay`  | Dingo relay (no forging)        | `172.20.0.16` | `3015`        | `3023`                            |
| `txpump-dingo` | Submits payment txs into `dingo-1`'s mempool | `172.20.0.21` | —      | —                                 |

The three producers and the relay are wired into a ring topology by
`configurator.sh` (`DINGO_POOL_IDS="1 2 3"`), with the relay peering with all
three producers so block diffusion is exercised in addition to direct
producer-to-producer sync. Network spec: `testnet-dingo.yaml` (3 pools,
`poolPledge: 0` — required for the CIP-50 scenario below).

## Topology — conformance mode (`--conformance`)

| Service            | Role                                 | Container IP  | NtN host port | NtC (LocalStateQuery) host port |
|--------------------|---------------------------------------|---------------|---------------|----------------------------------|
| `configurator`     | One-shot: generates keys + genesis  | —             | —             | —                                 |
| `dingo-producer`   | Dingo, forging with pool 1 keys     | `172.20.0.10` | `3010`        | `3030`                            |
| `cardano-producer` | `cardano-node`, forging with pool 2 | `172.20.0.11` | `3011`        | `3031`                            |
| `cardano-relay`    | `cardano-node` relay (no forging)   | `172.20.0.12` | `3012`        | —                                 |
| `txpump`           | Submits payment txs into Dingo      | `172.20.0.20` | —             | —                                 |

This is unchanged from the original two-pool network: pool 1 and pool 2 are
wired into a ring topology, and the relay peers with both producers. Network
spec: `testnet.yaml`.

`dingo-producer` and `cardano-producer` — the two nodes that actually forge
blocks — both have an NtC host port mapped, so the host test harness can run
LocalStateQuery against each and compare ledger state (see
`TestLedgerStateConsensus` under Test scenarios below). `cardano-relay` has no
NtC host port mapped; it isn't part of that comparison.

In both modes, `txpump` is a load generator: it talks Ouroboros NtC over the
Dingo node's container-network TCP endpoint and submits one payment
transaction per round with a 5–15s cooldown, funded from the genesis UTxO keys
generated by the configurator. Limiting each round to one transaction prevents
txpump from immediately building an unconfirmed dependency chain that can be
invalidated by an early fork. txpump starts only after every node in the active
profile is healthy. Outputs created by a submitted transaction remain
unavailable to txpump for 600 slots, longer than the accelerated scenario, so
that profile never recycles an output it has not observed on-chain. It exists
to keep the mempool exercised while the consensus tests run, so block bodies
are non-empty and tx-submission / mempool paths are continuously hit. The image
is built from
`internal/test/antithesis/` (`Dockerfile.txpump`, `cmd/txpump/`).

## Network parameters

Dingo mode reads `testnet-dingo.yaml`, conformance mode reads `testnet.yaml`.
Notable values shared by both:

- `networkMagic: 42`
- `epochLength: 500` slots, `slotLength: 1s` (~8 min epochs)
- `activeSlotsCoeff: 0.4`, `securityParam (k): 40`
- All hard forks at epoch 0 — the network starts in Conway with protocol
  version 10.0.
- `systemStart` is set to `now + 30s` by the configurator after key
  generation, so nodes have time to come up before slot 0.

Dingo mode differs in pool count and pledge: `poolCount: 3`, `poolPledge: 0`,
`delegatedSupply: 2100000000000` (divisible by 3, required by the
generator). The zero pledge is deliberate — it's what makes the CIP-50
pledge-leverage scenario meaningful (see Test scenarios below).

### Accelerated network parameters

`--accelerated` swaps the network spec for a compressed-timing variant —
`testnet-dingo-accelerated.yaml` (dingo mode) or `testnet-accelerated.yaml`
(conformance mode) — so a whole scenario fits the reference-runner budget:

- `epochLength: 120` slots, `slotLength: 0.5s` → **60s epochs**
- `activeSlotsCoeff: 0.4`, `securityParam (k): 10` → a block every ~1.25s
- Byron `protocolConsts.k` tracks `securityParam`
- Everything else (network magic, hard forks at epoch 0, supply) matches
  the canonical specs.

Shortening an epoch is only safe if the stability windows still fit inside
it, and both are derived from `k` and `f` rather than configured directly:

| Window | Formula | Accelerated | Canonical |
|--------|---------|-------------|-----------|
| nonce stability | `4k/f` | 100 slots | 400 slots |
| blockfetch stability | `3k/f` | 75 slots | 300 slots |
| epoch length | — | 120 slots | 500 slots |

`DevNetConfig.Validate()` enforces `4k/f < epochLength` and
`3k/f < epochLength`, and `TestCheckedInSpecsAreValid` in
`internal/test/devnet/config_test.go` runs it against every checked-in
spec. That test carries only the `linux` constraint, so an edit that shrinks an
epoch without shrinking `k` fails in the ordinary Linux `go test ./...` run
instead of turning into a DevNet stall nobody can explain.

The canonical specs are deliberately *not* accelerated: they are what soak
and canary runs use, and `TestCanonicalSpecsKeepCanonicalTiming` fails if
someone quietly speeds them up.

## Prerequisites

- Docker with the Compose plugin (`docker compose version` must work).
- Go 1.26+ on the host (matching `go.mod`) to run the integration tests.
- Outbound network access on first run to pull
  `ghcr.io/blinklabs-io/cardano-node:11.0.1` (conformance mode only) and to
  clone `cardano-foundation/testnet-generation-tool` inside the configurator
  image.
- Local build of Dingo via the repo root `Dockerfile` — Compose builds it
  automatically on `up` / `run-tests.sh`.
- Local build of `txpump` from `../antithesis/` — also built automatically
  by Compose.

## Building Dingo

You do not need to `make build` Dingo locally — Compose builds the Dingo
node images (`dingo-1`/`dingo-2`/`dingo-3`/`dingo-relay` in dingo mode,
`dingo-producer` in conformance mode) directly from the repo by referencing
the top-level `Dockerfile` (`build.context: ../../..` in
`docker-compose.yml`). The build runs `make build` inside the Go builder
image pinned by that `Dockerfile` against your working tree, so any
uncommitted local changes are included.

Force a rebuild after editing Dingo source:

```bash
# from this directory (internal/test/devnet), dingo mode
docker compose -f docker-compose.yml build dingo-1 dingo-2 dingo-3 dingo-relay

# conformance mode
docker compose -f docker-compose.yml build dingo-producer
```

`run-tests.sh` always rebuilds the active profile's images before starting
(`docker compose build`, scoped to `COMPOSE_PROFILES`). `start.sh` does not
— it only runs `up -d`, so add an explicit `build` (or pass `--build` to
`up`) when you want fresh code in the running containers.

The configurator image (`Dockerfile.configurator`) is built from this
directory and pinned to upstream `testnet-generation-tool@v0.1.0`; it rarely
needs rebuilding once cached.

## Where to run from

`start.sh`, `stop.sh`, and `run-tests.sh` all resolve their own location via
`SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"`, so they work
from any current working directory:

```bash
# from this directory
./start.sh

# from the repo root
./internal/test/devnet/start.sh

# from anywhere
/home/me/dingo/internal/test/devnet/run-tests.sh -run TestBasicBlockForging
```

Plain `docker compose` invocations are not location-independent — pass
`-f <path>/docker-compose.yml` if you are not in this directory, since the
compose file's `build.context` paths (`.` and `../../..`) are resolved
relative to the compose file itself, not your shell's CWD.

The block replay validation path is opt-in, matching production defaults.
Set both passthroughs when a change needs that path in either DevNet profile:

```bash
DEVNET_BLOCK_PIPELINE_ENABLED=true \
DEVNET_BLOCK_PIPELINE_VALIDATE_ENABLED=true \
  ./run-tests.sh --accelerated
```

The scripts must, however, stay at this path inside the repo: the compose
file references `../../..` to pick up the Dingo source tree for its build
context, and the `topology/*.json` and `testnet*.yaml` files are mounted by
relative path. Don't copy the directory elsewhere expecting it to work
standalone.

## Manual usage

```bash
./start.sh               # dingo mode (default): 3 producers + relay
./start.sh --conformance # conformance mode: dingo + cardano-node
./start.sh --accelerated # bring the network up on the accelerated spec
./stop.sh                 # tear down the default dingo network
./stop.sh --conformance   # tear down the conformance network
./stop.sh --accelerated   # accepted and ignored; teardown is spec-independent
```

Both scripts set `COMPOSE_PROFILES` for you (`dingo` or `conformance`), so
`docker compose up -d` / `down -v` only touch the services in the selected
profile. Tail logs while it runs:

```bash
docker compose -f docker-compose.yml logs -f
docker compose -f docker-compose.yml logs -f dingo-1

# conformance profile
COMPOSE_PROFILES=conformance docker compose -f docker-compose.yml logs -f
COMPOSE_PROFILES=conformance docker compose -f docker-compose.yml logs -f cardano-producer
```

Open a shell inside a node:

```bash
docker exec -it dingo-1 sh

# conformance mode
docker exec -it cardano-producer bash
```

Each Dingo node's socket is on its own named `*-ipc` volume at
`/ipc/dingo.socket` (`dingo-1-ipc`, `dingo-2-ipc`, `dingo-3-ipc`,
`dingo-relay-ipc` in dingo mode; `dingo-producer-ipc` in conformance mode);
the `cardano-node` sockets live on each node's `*-ipc` volume at
`/ipc/node.socket`. These are container-internal named volumes, not host
bind mounts — see LocalStateQuery access below for how the host reaches a
node's NtC endpoint.

## Running the integration tests

`run-tests.sh` is the entry point for a complete native-Linux DevNet run:

```bash
./run-tests.sh                              # dingo mode (default): bring up, run devnet tests, tear down
./run-tests.sh --conformance                 # conformance mode: dingo + cardano-node
./run-tests.sh --accelerated                # fast event-driven scenario timeline (see below)
./run-tests.sh --accelerated --conformance  # the same timeline against the reference topology
./run-tests.sh -run TestBasicBlockForging   # forward -run (and other flags) to `go test`
./run-tests.sh --keep-up                    # leave the network running on success (for poking around)
DEVNET_MEMPOOL_PROVIDER=dag ./run-tests.sh  # exercise the DAG mempool on every Dingo node
```

What it does:

1. Builds the images for the selected profile (`dingo` by default,
   `conformance` with `--conformance`).
2. `docker compose up -d` and waits up to 120s for all nodes in the profile
   to report `healthy` (four nodes in dingo mode: `dingo-1`, `dingo-2`,
   `dingo-3`, `dingo-relay`; three in conformance mode).
3. Verifies the profile's `txpump` service (`txpump-dingo` or `txpump`) is
   running.
4. Dingo mode only: copies the genesis stake keys out of the `utxo-keys`
   Docker volume to a host temp directory and exports
   `DEVNET_STAKE_KEYS_DIR`, so the CIP-50 scenario can load them. This is
   best-effort — if the volume or keys aren't found, the copy is skipped and
   the CIP-50 scenario just skips (see Test scenarios below).
5. From the repo root, runs `go test -tags "<mode tags>" -count=1 -v
   ./internal/test/devnet/...` — `devnet` alone in dingo mode, `devnet
   devnet_conformance` in conformance mode.
6. Tears the network down. On failure it dumps the last 100 lines of compose
   logs first, then preserves the full failure evidence (see Failure
   artifacts below). It also checks that `txpump` logged at least one
   accepted submission; zero accepted submissions fails the run even if the
   Go tests passed.

It also exports `DEVNET_TESTNET_YAML` pointing at the spec it actually
brought the network up with, so `devnet.LoadDevNetConfig()` in the tests
reads the same parameters the configurator generated genesis from rather
than falling back to `testnet.yaml`.

## Accelerated scenario timeline

`--accelerated` is the fast path used for scheduled and release
integration evidence. It brings the network up on the accelerated spec and
runs one test — `TestAcceleratedScenarioTimeline` — instead of the full
suite.

What makes it fast is not just the shorter slots. The canonical suite
queries each node's tip by opening a fresh Node-to-Node connection every
couple of seconds, because a short-lived ChainSync client only ever
reports the tip it captured when it intersected. The accelerated scenario
instead opens **one persistent ChainSync session per node** and follows
the chain, so `RollForward` and `RollBackward` arrive as the nodes produce
them. Assertions are then conditions over those observed events bounded by
a context deadline — there is no polling interval and no `time.Sleep`
anywhere in the synchronisation path.

The second difference is that every assertion shares **one timeline**.
Each phase's deadline is measured from the scenario's start rather than
from the end of the previous phase, so a phase that finishes early hands
its slack forward instead of each test paying for its own relative slot
window:

| Phase | What it verifies |
|-------|-------------------|
| `readiness` | Every node accepted a ChainSync session and sent a header |
| `propagation` | A block forged after the baseline reaches every node, including the relay, with matching hashes; and one carrying transactions does too |
| `agreement` | Every node agrees on the block hash at the deepest slot they have all observed |
| `epoch-transition` | The chain crosses the next epoch boundary and the nodes agree on a header built above it (exercising the new epoch nonce) |
| `peer-interruption` | A producer is stopped, the network visibly advances without it, and it rejoins and reconverges |
| `relay-restart` | The same for the relay |

Both disruption phases hold the node down until the rest of the network
has advanced a derived number of blocks — `k/2`, so the outage stays
inside what the security parameter can reconcile — rather than for a fixed
wall-clock time. That keeps the disruption equally meaningful on a fast
and a slow runner.

The whole schedule is derived from the network spec by
`devnet.NewScenarioPlan`. For the accelerated spec it works out as
`readiness<=45s propagation<=1m10s agreement<=1m22.5s
epoch-transition<=2m50s peer-interruption<=3m33.75s relay-restart<=4m17.5s`
— a **4m17.5s** worst case against a **5-minute hard timeout**. Those are
ceilings, not expected times; the run ends as soon as the last condition
is observed. Both bounds are asserted
without Docker by `TestAcceleratedPlanFitsReferenceBudgetAndCanonicalDoesNot`,
which also asserts that the canonical spec does *not* fit — so the fast
scenario can never be quietly pointed at the soak configuration.

Reference runner: a 4-vCPU / 16 GB Linux runner (GitHub Actions
`ubuntu-latest` class) with images already built. Image build time is
excluded from the budget; `run-tests.sh` rebuilds before starting, and the
scenario's clock only starts once the containers are healthy.

The scenario runs in **both topologies**: it derives the producer it
interrupts and the relay it restarts from `LoadEndpoints()`, so
`--accelerated` gives the all-Dingo network and `--accelerated
--conformance` gives Dingo beside `cardano-node`. In both cases the node
it interrupts is the last producer, which is never the node `txpump`
submits to, so mempool traffic keeps flowing through the outage.

To drive it by hand against a network you brought up yourself:

```bash
./start.sh --accelerated
```

`start.sh --accelerated` prints the exact `go test` command to run next,
including the `DEVNET_COMPOSE_PROJECT` and `DEVNET_*_ADDR` values for
whichever project/ports this run actually landed on — `devnet_ports` may
have derived non-default host ports, so copy that printed command rather
than retyping one with the default `localhost:3010`-style addresses, which
would connect to the wrong ports. Then:

```bash
./stop.sh --accelerated
```

`TestAcceleratedScenarioTimeline` skips unless `DEVNET_ACCELERATED=1` is
set, so it never runs against the canonical-timing network, where its
budget could not be met.

## Failure artifacts

When a run fails, `run-tests.sh` keeps `DEVNET_ARTIFACT_DIR` (a temp
directory it creates, or one you set) and prints its path instead of
deleting it. On success the directory is removed.

Teardown is best-effort and never replaces the test exit status. In dingo
mode, the temporary stake-key copy is written with the host uid/gid so the
runner can remove its own temporary tree after either result.

| Path | Contents |
|------|----------|
| `network/container-status.txt` | `docker compose ps --all` for the profile |
| `network/compose.log` | Full compose logs for every service |
| `network/generated-configs/` | The genesis and node configuration the configurator generated |
| `network/testnet*.yaml` | The network spec the run used |
| `<scenario>/observed-chains.json` | Every node's observed chain: tip, retained headers, roll-forward/roll-backward counts, deepest rollback, connect/disconnect churn |
| `<scenario>/container-status.txt` | Container status as the scenario saw it |
| `<scenario>/<service>.log` | Per-service logs captured by the scenario, capped at the last `CapturedLogTailLines` lines |

`<scenario>` is `accelerated-timeline` for the accelerated timeline,
which captures explicitly through `NodeControl.CaptureFailureArtifacts`,
and the Go test name for every canonical scenario, which
`NewTestHarness` wires up on its own. A subtest renders as
`parent/child`, so the separator is percent-escaped to keep the evidence
in one directory per test, as is anything a filesystem would reject or
rewrite -- Windows refuses `: * ? " < > |` and strips a trailing dot or
space, which would otherwise put two scenarios in one directory. A name
built from Go identifiers is used as written. Case is left alone, so
directories stay readable; on a filesystem that folds case, two scenario
names differing only in case would share one.

Observation and writing happen at different times. Chain observers run
for the whole of a scenario, so `observed-chains.json` is a continuous
record of what every node's chain did while that scenario was failing.
The files themselves are written from the scenario's `t.Cleanup`, which
runs once that test finishes and before the network is torn down, so
container status and the service logs describe the network as it stood
at the end of the failing scenario rather than at the instant of the
failure.

The `network/` entries, by contrast, are collected once after the whole
run, by which point a chain that stalled and then recovered looks
healthy again. That is the difference that separates a stall nobody
forged through from one nobody propagated.

The per-scenario service logs are capped, because a DevNet node logs at
debug level and emits tens of megabytes a minute; `network/compose.log`
is the complete, uncapped record for the whole run, so the per-scenario
copy only carries the window around the failure.

Capture stays off unless `DEVNET_ARTIFACT_DIR` is set and the topology
names containers, so a harness built over endpoints that are never
dialled does not reach for Docker.

The harness reads endpoint addresses from mode-specific environment
variables that `run-tests.sh` sets based on the host port mappings (dingo
mode: `DEVNET_DINGO1_ADDR`, `DEVNET_DINGO2_ADDR`, `DEVNET_DINGO3_ADDR`,
`DEVNET_DINGO_RELAY_ADDR`; conformance mode: `DEVNET_DINGO_ADDR`,
`DEVNET_CARDANO_ADDR`, `DEVNET_RELAY_ADDR`). Tests also re-parse the mode's
`testnet*.yaml` at runtime via `devnet.LoadDevNetConfig()`, so changes to
network parameters flow through without code edits.

## LocalStateQuery access

The host Go test harness reaches a Dingo node's LocalStateQuery interface
over node-to-client (NtC) Ouroboros, dialed as plain TCP — not a UNIX
socket. Each Dingo node sets `DINGO_PRIVATE_BIND_ADDR=0.0.0.0` (private/NtC
port 3002 inside the container), and `docker-compose.yml` maps that port to
a host port per node: `dingo-1` → `3020`, `dingo-2` → `3021`, `dingo-3` →
`3022`, `dingo-relay` → `3023` (override with `DEVNET_DINGO1_NTC_ADDR` etc.,
or the `DEVNET_DINGO{1,2,3}_NTC_PORT` / `DEVNET_DINGO_RELAY_NTC_PORT` host
port vars). `internal/test/devnet/endpoints_dingo.go`'s `DingoNtcAddrs()`
returns these as a `map[string]string` keyed by node name, and
`internal/test/devnet/lsq.go`'s `RewardAccountsByNtc(addr, magic, creds)`
dials that address with `ouroboros.New(...).DialTimeout("tcp", addr, ...)`
and queries delegations + reward balances via
`GetFilteredDelegationsAndRewardAccounts`.

This TCP-based approach replaced an earlier unix-socket host bind-mount
design: the Dingo image runs as uid 1000 and cannot bind a socket inside a
host-owned bind mount, so the in-container socket (used by `txpump` and the
healthcheck) stays on a named Docker volume, and the host harness talks NtC
over TCP instead. There is no host socket bind mount and no
`DEVNET_IPC_DIR` environment variable.

### LocalStateQuery in conformance mode

Conformance mode exposes NtC for both producers, so ledger state (not just
chain tip/growth) can be compared against the reference implementation
(`TestLedgerStateConsensus`, blinklabs-io/dingo#1900):

- `dingo-producer` (in `--conformance` mode) exposes NtC the same way dingo
  mode's nodes do (`DINGO_PRIVATE_BIND_ADDR=0.0.0.0` on private port 3002),
  mapped to host port `3030` unless overridden with `DEVNET_DINGO_NTC_ADDR`
  or `DEVNET_DINGO_NTC_PORT`.
- `cardano-producer` (in `--conformance` mode) has no built-in TCP NtC
  support — cardano-node only serves LocalStateQuery over its unix socket.
  The `ghcr.io/blinklabs-io/cardano-node` image's `run-node` entrypoint
  bridges this: setting `SOCAT_PORT` spawns a background `socat
  TCP-LISTEN:<port>,fork UNIX-CLIENT:<socket-path>` inside the container.
  `docker-compose.yml` sets `SOCAT_PORT: "3002"` on `cardano-producer` (the
  same private-port number Dingo uses, purely by convention — it has no
  special meaning to cardano-node), mapped to host port `3031` unless
  overridden with `DEVNET_CARDANO_NTC_ADDR` or `DEVNET_CARDANO_NTC_PORT`.

`internal/test/devnet/endpoints_conformance.go`'s `DingoProducerNtcAddr()`
and `CardanoProducerNtcAddr()` return these host addresses, and
`internal/test/devnet/ledger_state.go`'s `LedgerStateAtTip(addr, magic)`
queries one node's current protocol parameters, stake distribution, and
whole UTxO set (normalized into a comparable form) via a single acquired
LocalStateQuery session; `DiffLedgerStates(a, b)` reports every divergence
between two such snapshots.

`GetStakeDistribution` and `GetUTxOWhole` (the two queries `LedgerStateAtTip`
needs beyond the ones already used elsewhere in this harness) did not have
server-side support in Dingo before this scenario — they were part of the
`// TODO (#394)` block in `ledger/queries.go`'s query dispatcher. They are
implemented in `ledger/queries_stakedistribution.go` and
`ledger/queries_utxowhole.go`, closing out #394 for those two query types
specifically (the rest of that TODO block is unrelated to this scenario and
remains open).

Dingo's LocalStateQuery server (`ouroboros/localstatequery.go`) does not yet
implement point-specific ledger views: every `Acquire` — even
`Acquire(point)` for a specific historical block — is answered against the
node's live tip (tracked upstream as blinklabs-io/dingo#382). Until that
lands, `LedgerStateAtTip` only supports "acquire the current volatile tip",
and comparing two nodes at the same point requires confirming via NtN
chain-tip polling that both report an identical tip immediately before and
after the LocalStateQuery round trip — see `TestLedgerStateConsensus` for
the retry loop this requires. This is why the scenario samples ledger state
periodically at settled common tips rather than replaying every block.

### Running the CIP-50 pledge-leverage scenario

`TestCIP50PledgeLeverageRewardEffect` (dingo mode only) is long-running: it
waits into epoch 4 so the delayed reward update (applied from the stake
snapshot three epochs back, see `ledger/chainsync.go`) has had a chance to
credit rewards, which takes roughly 35 minutes per pass at
`epochLength=500`, `slotLength=1s`. It is gated behind `DEVNET_CIP50_TEST=1`
and never runs as part of the default suite, and it requires two independent
passes against two separately launched networks to be meaningful:

```bash
# 1. baseline: bring up the network with leverage off (the default), then run
./start.sh
DEVNET_CIP50_TEST=1 DEVNET_STAKE_KEYS_DIR=/path/to/stake/keys \
  go test -tags devnet -run TestCIP50PledgeLeverageRewardEffect -timeout 50m \
  ./internal/test/devnet/scenarios/
./stop.sh

# 2. leveraged: bring up the network with pledge leverage enabled, then run
DEVNET_DINGO_PLEDGE_LEVERAGE_ENABLED=true ./start.sh
DEVNET_CIP50_TEST=1 DEVNET_DINGO_PLEDGE_LEVERAGE_ENABLED=true \
  DEVNET_STAKE_KEYS_DIR=/path/to/stake/keys \
  go test -tags devnet -run TestCIP50PledgeLeverageRewardEffect -timeout 50m \
  ./internal/test/devnet/scenarios/
./stop.sh
```

The baseline pass expects total member rewards greater than zero (rewards
flow without a cap); the leveraged pass expects every delegated credential's
reward to be exactly zero (the leverage cap with zero pledge zeroes
reward-eligible stake). `run-tests.sh` does not run this scenario by default;
setting `DEVNET_CIP50_TEST=1` can trigger one pass when the stake-key copy
succeeds.
A meaningful baseline-versus-leveraged comparison still requires the two
separately launched passes shown above. `DEVNET_STAKE_KEYS_DIR` must point at
the genesis stake key directory exposed by the configurator (copy it out of
the `utxo-keys` Docker volume, the same way `run-tests.sh` does for its own
runs).

## Test scenarios

All scenarios live in `internal/test/devnet/scenarios/` and use the harness
in `internal/test/devnet/`.

Generic scenarios (`//go:build linux && devnet`) run in both dingo and
conformance mode:

| Test | What it verifies |
|------|-------------------|
| `TestBasicBlockForging` | Dingo forges at least one block and all nodes converge within `securityParam` |
| `TestDingoChainAdvances` | Dingo's tip slot advances over a short observation window |
| `TestChainGrowthRate` | Block production rate is within the expected range derived from `activeSlotsCoeff` and `slotLength` |
| `TestRelayPropagation` | Blocks reach the non-forging relay |
| `TestSustainedConsensus` | All nodes stay in agreement across multiple sampling intervals |
| `TestEpochBoundaryConsensus` | All nodes remain in consensus across at least one epoch boundary (exercises candidate-nonce freeze, lab nonce roll, and new-epoch VRF verification) |
| `TestAcceleratedScenarioTimeline` | The accelerated scenario timeline: readiness, block and transaction propagation, chain agreement, an epoch transition, a peer interruption with recovery, and a relay restart — all on one shared clock, driven by streamed ChainSync events. Skipped unless `DEVNET_ACCELERATED=1`; see Accelerated scenario timeline above. |

Reference-conformance scenario
(`//go:build linux && devnet && devnet_conformance`) runs only with
`--conformance`:

| Test | What it verifies |
|------|-------------------|
| `TestCardanoProducerChainAdvances` | `cardano-producer`'s tip advances (sanity check on the reference node) |
| `TestLedgerStateConsensus` | dingo-producer's and cardano-producer's ledger state (protocol parameters, stake distribution, whole UTxO set) match at several settled common-tip samples over the run; fails with a diagnostic naming every divergence found |

Dingo-only feature scenario
(`//go:build linux && devnet && !devnet_conformance`) runs only in the default
dingo mode — no `cardano-node` reference exists for this feature:

| Test | What it verifies |
|------|-------------------|
| `TestCIP50PledgeLeverageRewardEffect` | With `poolPledge: 0`, compares a leverage-off baseline (member rewards greater than zero by epoch 4) against a leveraged pass (member rewards exactly zero for every delegated stake credential). Skipped unless `DEVNET_CIP50_TEST=1` is set; requires two separately launched networks to run both passes (see above). |

## Port and address overrides

`start.sh`/`run-tests.sh` derive a worktree-specific block for all 13
variables below by default (see `devnet_ports` in [Cleanup](#cleanup)), so
the defaults in this table only apply to a bare `docker compose up` or a
single-worktree run with `DEVNET_*_PORT` set explicitly. Set any one of them
to opt out of auto-derivation entirely and take full manual control (via
environment or a local `.env` file next to `docker-compose.yml`; the
checked-in `.env` sets `COMPOSE_PROFILES=dingo` as the default).

Dingo mode:

| Variable                      | Default | Used by                                     |
|-------------------------------|---------|----------------------------------------------|
| `DEVNET_DINGO1_PORT`          | `3010`  | docker-compose host port for `dingo-1` NtN   |
| `DEVNET_DINGO2_PORT`          | `3013`  | docker-compose host port for `dingo-2` NtN   |
| `DEVNET_DINGO3_PORT`          | `3014`  | docker-compose host port for `dingo-3` NtN   |
| `DEVNET_DINGO_RELAY_PORT`     | `3015`  | docker-compose host port for `dingo-relay` NtN |
| `DEVNET_DINGO1_NTC_PORT`      | `3020`  | docker-compose host port for `dingo-1` NtC   |
| `DEVNET_DINGO2_NTC_PORT`      | `3021`  | docker-compose host port for `dingo-2` NtC   |
| `DEVNET_DINGO3_NTC_PORT`      | `3022`  | docker-compose host port for `dingo-3` NtC   |
| `DEVNET_DINGO_RELAY_NTC_PORT` | `3023`  | docker-compose host port for `dingo-relay` NtC |
| `DEVNET_DINGO{1,2,3}_ADDR`, `DEVNET_DINGO_RELAY_ADDR` | `localhost:<port above>` | `run-tests.sh`-derived NtN addresses for the Go harness |
| `DEVNET_DINGO{1,2,3}_NTC_ADDR`, `DEVNET_DINGO_RELAY_NTC_ADDR` | `localhost:<port above>` | NtC addresses for `DingoNtcAddrs()` |
| `TEST_TIMEOUT`                | `20m`   | `go test -timeout` in `run-tests.sh` |

Conformance mode:

| Variable                 | Default | Used by                                     |
|--------------------------|---------|-----------------------------------------------|
| `DEVNET_DINGO_PORT`      | `3010`  | docker-compose host port for Dingo NtN      |
| `DEVNET_CARDANO_PORT`    | `3011`  | docker-compose host port for cardano NtN   |
| `DEVNET_RELAY_PORT`      | `3012`  | docker-compose host port for relay NtN     |
| `DEVNET_DINGO_NTC_PORT`  | `3030`  | docker-compose host port for `dingo-producer` NtC |
| `DEVNET_CARDANO_NTC_PORT`| `3031`  | docker-compose host port for `cardano-producer` NtC (bridged by socat) |
| `DEVNET_DINGO_NTC_ADDR`  | `localhost:<port above>` | `DingoProducerNtcAddr()` override |
| `DEVNET_CARDANO_NTC_ADDR`| `localhost:<port above>` | `CardanoProducerNtcAddr()` override |
| `DINGO_PORT`          | falls back to `DEVNET_DINGO_PORT`   | `run-tests.sh` only |
| `CARDANO_PORT`        | falls back to `DEVNET_CARDANO_PORT` | `run-tests.sh` only |
| `RELAY_PORT`          | falls back to `DEVNET_RELAY_PORT`   | `run-tests.sh` only |

`run-tests.sh` derives the `DEVNET_*_ADDR` variables from these so the test
harness and the compose port mappings always agree.

## Files

| File                         | Purpose |
|------------------------------|---------|
| `docker-compose.yml`         | Service, volume, and network definitions for both the `dingo` and `conformance` profiles |
| `Dockerfile.configurator`    | Builds the genesis/key generator image (cardano-foundation/testnet-generation-tool v0.1.0) |
| `configurator.sh`            | Runs inside the configurator: drives `genesis-cli.py`, builds ring topology (mode-aware pool count via `DINGO_POOL_IDS`), sets `systemStart`, relaxes key permissions for non-root node containers and chowns each Dingo pool's keys to `DINGO_UID`/`DINGO_GID` (passed in by compose, defaulting to the `1000:1000` pinned in the repo root `Dockerfile`) |
| `testnet-dingo.yaml`         | Canonical network spec for dingo mode: 3 pools, `poolPledge: 0` |
| `testnet.yaml`               | Canonical network spec for conformance mode: 2 pools |
| `testnet-dingo-accelerated.yaml` | Accelerated dingo-mode spec: 60s epochs, `k: 10`, 0.5s slots |
| `testnet-accelerated.yaml`   | Accelerated conformance-mode spec: same timing, 2 pools |
| `topology/dingo-1.json`, `dingo-2.json`, `dingo-3.json`, `dingo-relay.json` | Static peer lists for dingo mode |
| `topology/dingo-producer.json`, `cardano-producer.json`, `relay.json` | Static peer lists for conformance mode |
| `.env`                       | Sets the default `COMPOSE_PROFILES=dingo` |
| `compose-project.sh`         | Derives a stable, worktree-specific Compose project name, a collision-checked bridge subnet and host port block, and a rendered topology directory; wraps `docker compose up` with a retry on subnet collision |
| `start.sh` / `stop.sh`       | Convenience wrappers around `docker compose up -d` / `down -v`; accept `--conformance` |
| `run-tests.sh`               | Full native-Linux bring-up → test → tear-down runner; accepts `--conformance`, `--keep-up`, and forwards other flags to `go test` |
| `../antithesis/Dockerfile.txpump`, `../antithesis/cmd/txpump/` | Source for the `txpump` load generator image |
| `harness.go`                 | Go test harness: Ouroboros NtN client, tip queries, consensus checks, the `WaitForChainStart` genesis gate, and per-scenario failure capture (build tag `devnet`) |
| `config.go`                  | `testnet*.yaml` loader, derived timings, and spec validation (**no build tag** — its tests run in the ordinary `go test ./...`) |
| `chainstate.go`              | Observed-chain state machine: applies RollForward/RollBackward, tracks tip and retained headers, and exposes cross-node agreement helpers and bounded-context conditions (**no build tag**) |
| `timeline.go`                | `ScenarioPlan`: derives the accelerated scenario's phases, deadlines, outage length, and hard timeout from the network spec (**no build tag**) |
| `observer.go`                | Persistent per-node ChainSync sessions feeding `chainstate.go`, with automatic reconnect across container restarts (build tag `devnet`) |
| `nodectl.go`                 | Stops/starts compose services for the disruption phases and supplies the Docker side of failure capture (build tag `devnet`) |
| `artifacts.go`               | What a failed scenario preserves and where: capture planning and artifact writing (**no build tag** — its tests run in the ordinary `go test ./...`) |
| `endpoints.go`               | The `NodeEndpoint` description shared by the harness, observers, and failure capture (**no build tag**) |
| `endpoints_dingo.go`         | Dingo-mode node endpoints and NtC addresses (`//go:build devnet && !devnet_conformance`) |
| `endpoints_conformance.go`   | Conformance-mode node endpoints, plus `DingoProducerNtcAddr()` / `CardanoProducerNtcAddr()` (`//go:build devnet && devnet_conformance`) |
| `lsq.go`                     | `RewardAccountsByNtc` / `RewardAccountsByNtcForCreds`: LocalStateQuery over NtC TCP (build tag `devnet`) |
| `ledger_state.go`            | `LedgerStateAtTip` / `DiffLedgerStates`: normalized protocol-params/stake-distribution/whole-UTxO snapshot and diff for cross-node ledger-state comparison (build tag `devnet`) |
| `credentials.go`             | Loads genesis stake credentials for the CIP-50 scenario (build tag `devnet`) |
| `harness_test.go`, `credentials_test.go` | Tests for the harness/credential helpers themselves (build tag `devnet`) |
| `scenarios/`                 | Devnet test scenarios (one or more `Test*` per file, gated per the Test scenarios table above) |

## Cleanup

`start.sh`, `stop.sh`, and `run-tests.sh` derive `COMPOSE_PROJECT_NAME` from
the worktree's absolute path. Compose therefore gives each worktree distinct
containers, networks, volumes, and project state. Set `COMPOSE_PROJECT_NAME`
explicitly to run more than one DevNet in the same worktree. `NodeControl`
receives that project as `DEVNET_COMPOSE_PROJECT`, resolves service containers
inside it, and still uses direct `docker stop`/`docker start` for disruption
phases.

A distinct network *name* isn't enough on its own: the compose network's
subnet is a separate axis, and Docker refuses to create two networks with an
overlapping subnet even under different project names ("Pool overlaps with
other one on this address space"). The same three scripts also derive
`DEVNET_NET_BASE`, a `172.24-172.31.x` /24, and use it for the compose
network's subnet and each service's static IP — the topology still needs
static IPs because the peer lists in `topology/*.json` are static,
checked-in files. `devnet_render_topology` rewrites a worktree-local copy of
those files with the run's `DEVNET_NET_BASE` into `DEVNET_TOPOLOGY_DIR`,
which `docker-compose.yml` mounts instead of the checked-in `./topology`
(its fallback default, so a bare `docker compose up` without going through
these scripts still works — for a single run at a time).

A worktree-path hash only picks a *starting* subnet: two worktrees can hash
to the same one, and this range isn't reserved for DevNet, so an unrelated
Docker network could already sit on part of it. `devnet_net_base` actually
checks every subnet `docker network ls`/`inspect` currently reports and
walks forward from the hash to a genuinely free `/24` (falling back to the
hash alone if Docker isn't reachable). A residual race remains between that
check and the network actually being created — `devnet_compose_up` (used by
`start.sh`/`run-tests.sh` in place of a bare `docker compose up -d`) covers
it: on a "Pool overlaps" failure it recomputes `DEVNET_NET_BASE` (which now
sees the winner's network and skips it), re-renders topology, and retries,
up to 3 attempts. Set `DEVNET_NET_BASE` explicitly for the same reason
you'd set `COMPOSE_PROJECT_NAME`.

Published host ports are a fourth axis Compose does not scope by project at
all: two worktrees' default ports (3010/3013-3015, 3020-3023, and
conformance's 3010-3012, 3030-3031) collide outright with "port is already
allocated".
`devnet_ports` derives a worktree-specific block of 13 ports (one hash-based
starting point, actually checked against what's listening on
`127.0.0.1` and shifted forward a whole block at a time until every port in
it is free) and exports it as the `DEVNET_*_PORT` / `DEVNET_*_NTC_PORT`
variables `docker-compose.yml` already reads. It's skipped entirely if the
caller has already set any of those variables, so a manual port override
stays in full manual control.

`stop.sh` and the `run-tests.sh` trap run `docker compose down -v` only for
that project, removing its config and data volumes without touching another
worktree's run, and remove that project's rendered topology directory.
Genesis is regenerated on every start, so this is the desired default —
there's no state worth preserving between runs. If a previous run left
orphaned containers or volumes around, use the same project name:

```bash
COMPOSE_PROJECT_NAME=<project> docker compose -f docker-compose.yml down -v --remove-orphans
docker volume ls | grep devnet                       # inspect leftovers
docker network ls | grep dingo-devnet                # inspect leftover networks
```

## Troubleshooting

- Nodes don't reach `healthy` within 120s. `run-tests.sh` dumps `docker
  compose ps` and the last 100 log lines on timeout. Most often this is a
  build issue with the Dingo image or genesis-time skew (the configurator
  schedules `systemStart` 30s in the future; if your machine's clock is far
  off, regenerate by tearing down and starting again).
- `port is already allocated`. Set the `DEVNET_*_PORT` overrides above.
- Configurator fails on first run. It clones
  `cardano-foundation/testnet-generation-tool` at build time; check
  outbound network access and re-run `docker compose build configurator`
  (or `configurator-dingo` in dingo mode).
- Dingo image is stale. `run-tests.sh` rebuilds the active profile's Dingo
  images on every invocation; for `start.sh` runs, force a rebuild with
  `docker compose -f docker-compose.yml build` (scoped by `COMPOSE_PROFILES`).
- A single scenario fails on its own but the whole suite passes. Every
  scenario now calls `WaitForChainStart` before taking a baseline, so this
  should no longer happen. The cause was that the configurator schedules
  `systemStart` 30s after it exits while the compose health checks pass as
  soon as a node opens its socket: a node answers tip queries reporting
  slot 0 / block 0 for that whole window. Timeouts derived from slot
  counts only measure chain time, so charging one against the pre-genesis
  wait expired it before the chain produced anything — and a test only
  passed because an earlier one in the package had already absorbed the
  wait. If you add a scenario, gate it on `WaitForChainStart` rather than
  relying on ordering.
- `TestAcceleratedScenarioTimeline` skips. It requires
  `DEVNET_ACCELERATED=1`, which `run-tests.sh --accelerated` sets. Running
  it against the canonical-timing network is deliberately prevented: its
  phase budgets are derived from the spec and cannot be met at
  `epochLength: 500`, `slotLength: 1s`.
- The accelerated scenario fails at `NewNodeControl`. It needs to reach
  Docker Compose to stop and start nodes. Run it through `run-tests.sh`,
  which exports `DEVNET_COMPOSE_FILE` and `DEVNET_COMPOSE_PROJECT`, or set
  both variables yourself. It
  fails rather than skipping the disruption phases on purpose — a pass
  that quietly omitted them would not be release evidence.
- `TestCIP50PledgeLeverageRewardEffect` skips. It requires
  `DEVNET_CIP50_TEST=1` and a non-empty `DEVNET_STAKE_KEYS_DIR` to run at
  all, and needs two separately launched networks (leverage off, then
  `DEVNET_DINGO_PLEDGE_LEVERAGE_ENABLED=true`) to be meaningful; see Running
  the CIP-50 pledge-leverage scenario above.
