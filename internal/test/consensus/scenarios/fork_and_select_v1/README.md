# fork_and_select_v1

Multi-peer capture scenario. Two cardano-node peers serve **divergent
chains sharing a common prefix**; a non-forging observation node runs
Praos chain selection against both and picks the longer one (peer B).
The produced vector exercises the chain-selection + rollback-to-
non-genesis-intersect path that real consensus failures hit.

## How it stages chains

The configurator drives cardano-node through three forge phases
(per `configurator.sh`):

| Phase | Forging pool | Starting DB | Kill slot | Snapshot to |
|---|---|---|---|---|
| A | pool 1 | fresh | `PREFIX_KILL_SLOT` (50) | `shared-prefix-db/` |
| B | pool 1 | copy of shared prefix | `+ PEER_A_EXTENSION_SLOTS` (+50 ⇒ 100) | `peer-a-data/` |
| C | pool 2 | copy of shared prefix | `+ PEER_B_EXTENSION_SLOTS` (+150 ⇒ 200) | `peer-b-data/` |

Both pools are registered block producers in the genesis from slot 0
onward. Whether a pool wins a given slot depends on its VRF; whether
its win produces a block depends on the pool's node being running with
its keys at that wall-clock time. Phase A had only pool 1's node up,
so pool 2's wins were missed; phase C runs only pool 2's node on a
chain whose prefix happens to be pool 1's blocks (which validate
because pool 1's keys are in the genesis) and pool 2 forges from
there.

No key rotation, no block splicing, no hand-synthesized blocks. Just
running cardano-node with different key mounts at different times
against the same genesis.

`PEER_B_EXTENSION_SLOTS` is ~3× `PEER_A_EXTENSION_SLOTS` so peer B
reliably wins Praos longest-chain selection.

## Stack contents

| Service | Role |
|---|---|
| `configurator` | Genesis generation + three-phase forge. Exits 0 when done. |
| `cardano-peer-a` | Forging cardano-node serving peer A's chain. Pool 1 keys. |
| `cardano-peer-b` | Forging cardano-node serving peer B's longer chain. Pool 2 keys. |
| `cardano-observation` | Non-forging cardano-node with both peers as static localRoots. |
| `capture-sidecar` (capture profile) | Runs the drain_to_tip conversation; invoked 3× by `run.sh`. |
| `compose` (capture profile) | Merges the three single-peer captures into the multi-peer vector. |

Subnet: `172.24.0.0/24`.

## How to run

```bash
# Via the dispatcher.
../../capture-scenario.sh fork_and_select_v1

# Regenerate the committed golden (skips the regression check).
../../capture-scenario.sh fork_and_select_v1 --skip-golden

# Direct invocation.
./run.sh -out /tmp/fork_and_select_v1.json
```

By default the produced vector overwrites the committed golden at
`internal/test/consensus/testdata/captured/fork_and_select_v1.json`.

`--keep-up` leaves the docker-compose stack running on success — useful
when poking the cardano-* services by hand.

## Why this scenario

The vector exercises three things at W3 replay time:

1. **Praos longest-chain selection** with two upstream peers serving
   divergent chains.
2. **Rollback to a non-genesis intersect point.** The observation node,
   having adopted some of peer A's tail, must roll back to the shared
   prefix tip and re-apply peer B's tail. The "find common ancestor at
   slot K > 0" path is the one real consensus failures hit; the
   forge-in-isolation alternative (no shared prefix) would short-
   circuit to slot 0 and not exercise it.
3. **Stabilized chain agreement.** dingo at replay time must reach the
   same `final_tip` (peer B's tip) given the same per-peer inputs.

## Determinism caveats

`tip.slot >= KILL_SLOT` polling makes each phase reproducible up to
cardano-node startup jitter (one run may land at slot 51, another at
slot 53). Block content drifts when the kill slot lands on a different
winning-slot boundary. The composer's golden-diff (`run.sh` step 6)
checks structural equivalence — same per-peer message types in order,
same `final_tip.slot` — not byte equality on `header_cbor` / hashes.
If the structural diff trips during a regeneration, that's a sign the
testnet shape has drifted; rerun and inspect.
