# Ouroboros Genesis from-origin sync (operator guide)

This guide covers configuring Dingo for **from-origin** (from-genesis) sync using
Ouroboros Genesis density selection with a biased fast-sync source — for example
a local shallow peer or the Genesis Sync Accelerator (GSA) — corroborated by
independent ledger peers.

For the internal design, see `ARCHITECTURE.md` → **Chain Selection → Ouroboros
Genesis trust model**.

## When this applies

Genesis mode is used only when **all** of these hold:

- `genesisBootstrap.enabled: true` (the default), and
- the node is starting **from origin** (no `intersectTip`, no explicit
  intersect points), i.e. an empty database syncing from slot 0.

A node bootstrapped from a Mithril snapshot, or resuming an existing database,
does not enter Genesis mode — it syncs with normal Praos chain selection.

## Trust model at a glance

| Mode | What it trusts |
|------|----------------|
| **Praos** (normal) | The longest valid chain from any peer. |
| **Mithril bootstrap** | A signed snapshot at a trust boundary, then Praos above it. |
| **Genesis** (from origin) | Header **density** within a `3k/f`-slot window, **corroborated by independent peers**. |

The security goal for from-origin sync is that a fast source which serves blocks
quickly but is not itself trustworthy (a shallow local peer, GSA) **cannot steer
the node onto an untrue chain**. Dingo enforces this with a **corroboration
gate**: the densest fast source is only followed while independent peers confirm
its recent blocks. A divergent or uncorroborated source is **denied selection**
and the node **stalls** rather than following it.

Assumption: at least `corroborationPeers` **honest** peers are reachable (e.g.
seeded from a ledger peer snapshot). Under that assumption a bad or divergent
fast source can at worst stall the node.

## Configuration

`dingo.yaml`:

```yaml
genesisBootstrap:
  enabled: true
  # 0 auto-derives the density window from Shelley genesis params (3k/f).
  windowSlots: 0
  # Independent peers that must report the same recent blocks before a fast
  # source may drive Genesis selection. 0 disables corroboration (density-only).
  # Set this to the number of independent snapshot/ledger peers you expect to
  # corroborate the fast source.
  corroborationPeers: 2
```

Equivalent CLI flags / environment variables:

- `--genesis-bootstrap-enabled` / `DINGO_GENESIS_BOOTSTRAP_ENABLED`
- `--genesis-bootstrap-window-slots` / `DINGO_GENESIS_BOOTSTRAP_WINDOW_SLOTS`
- `--genesis-bootstrap-corroboration-peers` / `DINGO_GENESIS_BOOTSTRAP_CORROBORATION_PEERS`

## Topology: fast source + corroborating snapshot

Put the fast source in `localRoots` (mark it `trustable` so peer governance
keeps it as a preferred ingress source) and point `peerSnapshotFile` at a
cardano-node ledger peer snapshot. When Genesis selection is active and the
snapshot has relays, Dingo seeds the snapshot relays as independent ledger peers
before outbound startup — these are the corroborators.

`topology.json`:

```json
{
  "localRoots": [
    {
      "accessPoints": [
        { "address": "gsa.internal.example", "port": 3001 }
      ],
      "advertise": false,
      "trustable": true,
      "valency": 1
    }
  ],
  "publicRoots": [
    { "accessPoints": [], "advertise": false }
  ],
  "peerSnapshotFile": "peer-snapshot.json",
  "useLedgerAfterSlot": 185500763
}
```

The `peerSnapshotFile` path is resolved relative to the topology file. If the
snapshot yields no usable peers, startup falls back to topology `bootstrapPeers`
(so keep a bootstrap-peer list configured as a safety net).

## What you should observe

- **Log at startup**: `Genesis chain selection enabled` with `genesis_window_slots`,
  `security_param`, and `min_corroborating_peers`.
- **Uncorroborated fast source**: a throttled warning
  `genesis fast source lacks corroboration; denying chain selection` and a
  `chainselection.genesis_corroboration_failed` event carrying the source
  connection, its observed density, the corroborator count, and the required
  count. While this persists the node makes no forward progress on that source
  (it stalls) — this is expected and safe. Investigate whether the snapshot
  peers are reachable and on the same chain as the fast source.
- **Exit to Praos**: once the local tip catches up to within the Genesis window
  of the best corroborated peer, the node logs `exiting Genesis selection mode`
  and emits `chainselection.genesis_mode_exited` (local slot, best known slot,
  window). Normal Praos selection takes over from there.

`GenesisStatus()` on the chain selector exposes the live mode, window, selected
fast source, and per-peer density/corroboration for metrics or debugging.

## Tuning `corroborationPeers`

- **0** — corroboration off. The densest source wins on density alone (prior
  behavior). Use only when every configured peer is already trusted.
- **1** — a single independent peer must confirm the fast source. Minimal
  protection; a lone honest snapshot peer is enough to keep going.
- **2+** — require a quorum of independent peers. More robust against a small
  set of colluding/divergent sources, but the node stalls until that many
  reachable peers agree, so size it to how many independent snapshot peers you
  actually connect.

## Limitations (deferred)

This is a corroboration gate, not the reference implementation's full Ouroboros
Genesis. Specifically **not** implemented:

- **ChainSync Jumping** and devoted BlockFetch dynamics (a
  performance/robustness optimization for downloading across many peers).
- **Density-at-intersection** resolution of a fork whose intersection is *inside*
  the window (the gate compares recent shared frontier points, not block counts
  after an exact intersection).
- **Peer-governance demotion** wired to the corroboration-failure event; today
  the source is denied selection (stall) but kept connected so it can serve
  blocks once corroboration arrives.

These do not affect the from-origin **security** property above; they are
performance and refinement work.
