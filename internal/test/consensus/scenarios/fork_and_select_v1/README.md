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
| A | pool 1 | fresh | `PREFIX_KILL_SLOT` (default 10) | `shared-prefix-db/` |
| B | pool 1 | copy of shared prefix | `+ PEER_A_EXTENSION_SLOTS` (default +15 ⇒ ~25) | `peer-a-data/` |
| C | pool 2 | copy of shared prefix | `+ PEER_B_EXTENSION_SLOTS` (default +80 ⇒ ~90) | `peer-b-data/` |

Slot counts are deliberately small so each phase's wall-clock-vs-chain
gap stays inside cardano-node's Genesis State Machine "CaughtUp"
tolerance (~3k/f = ~45 slots at k=6, f=0.4). The forging cardano-node
refuses to produce blocks when the gap is wider, and rewriting
`systemStart` between phases to close the gap would invalidate the
Byron genesis hash. See `configurator.sh` for the tunable defaults
and the comment above them.

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

`PEER_B_EXTENSION_SLOTS` is sized so peer B's expected **block count**
substantially exceeds peer A's. Praos longest-chain selection is by
block count, not slot number, so picking similar slot counts for the
two extensions can let leadership-lottery variance flip which peer
wins on a given run. With the defaults (15 vs 80) peer B's expected
block lead is ~14 blocks — robustly outside reasonable variance.

## Stack contents

| Service | Role |
|---|---|
| `configurator` | Genesis generation + three-phase forge. Exits 0 when done. |
| `cardano-peer-a` | Non-forging cardano-node serving peer A's pre-forged chain. |
| `cardano-peer-b` | Non-forging cardano-node serving peer B's pre-forged chain (the longer one). |
| `cardano-observation` | Non-forging cardano-node with both peers as static localRoots. |
| `capture-sidecar` (capture profile) | Runs the drain_to_tip conversation; invoked 3× by `run.sh`. |
| `composer` (capture profile) | Merges the three single-peer captures into the multi-peer vector. |

The runtime peer-a / peer-b services **do not mount pool keys** — if
they did, cardano-node would keep extending the chain past the
configurator's baked-in tip the moment wall-clock advanced past it,
defeating the "two divergent chains with fixed tips" model. The
configurator does all the forging up front; the runtime nodes just
serve.

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

The vector exercises three things at replay time:

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

`tip.slot >= KILL_SLOT` polling is reproducible up to cardano-node
startup jitter plus the 2-second poll cadence in `run_forge_phase`.
At PREFIX_KILL_SLOT=10 the per-phase tip-slot variance is intrinsically
±2-3 slots, and that variance **compounds** across phases (phase B
and phase C both target `phase-A-tip + N` rather than an absolute
slot, so phase A's overshoot drags both downstream targets forward).
The composer's golden-diff in `internal/test/consensus/diff.go` sets
`FinalTipSlotTolerance = 20` to absorb compounded variance without
masking a real chain-selection flip.

The diff checks structural equivalence — peer count, presence of
roll_forwards per peer, `final_tip.slot` within tolerance — not byte
equality on `header_cbor` / hashes. If the diff trips during a
regeneration, that's a sign the testnet shape has drifted (or the
selector picked the wrong peer); rerun and inspect.
