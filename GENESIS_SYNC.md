# Ouroboros Genesis from-origin sync (operator guide)

This guide covers Dingo's from-origin sync using Genesis density selection,
Limit on Eagerness, density disconnection, patience and authenticated headers.
The default uses the network's published topology without adding operator roots
or requiring a corroboration quorum. A configured peer snapshot can supply
additional relays. Deployments with a
biased fast-sync source can opt into additional peer corroboration.

For the internal design, see `ARCHITECTURE.md` → **Chain Selection → Ouroboros
Genesis trust model**.

## When this applies

Genesis mode is used only when **all** of these hold:

- `genesisBootstrap.enabled: true` (the default), and
- no `intersectTip` or explicit intersect points are configured, and
- the network configuration does not select Praos consensus mode.

This decision does not inspect database contents. A resumed database with the
same configuration can enter Genesis mode at its persisted tip and rebuild
its peer candidate histories. Explicit intersection options select normal
Praos chain selection.

## Trust model at a glance

| Mode | What it trusts |
|------|----------------|
| **Praos** (normal) | The longest valid chain from any peer. |
| **Mithril bootstrap** | A signed snapshot at a trust boundary, then Praos above it. |
| **Genesis** (from origin) | Authenticated chains, eagerness bounded by candidate intersections, and density within a `3k/f`-slot window. |

Genesis assumes at least one honest, caught-up, available upstream peer. Peer
discovery must make that peer reachable; endpoint or operator labels cannot
prove this assumption. Limit on Eagerness constrains progress past competing
candidates' intersection, while density disconnection and patience remove
unsuitable peers. The fully fetched local and peer fork paths are compared from
their exact common intersection. See the [reference Genesis
design](https://ouroboros-consensus.cardano.intersectmbo.org/docs/references/miscellaneous/genesis_design/).

A positive `corroborationPeers` adds a separate policy: peers in distinct
independence groups must confirm a source's recent chain before it can drive
selection. Every overlapping block must agree; one shared old ancestor does not
confirm a divergent suffix. Configured `controlGroups` assert common operator
control and form an exclusive witness set. Otherwise trusted ledger/snapshot
pool groups are used where available, with address groups as a fallback for
unidentified topology or ledger peers. Address groups do not prove different
operators. Missing witnesses stall the configured policy rather than silently
disabling it. Its security assumption requires reachable, honest, independently
controlled witnesses within the Genesis window; raising the count cannot
establish their independence. The default zero retains the ordinary Genesis
mechanisms without requiring a second independence group.

## Configuration

`dingo.yaml`:

```yaml
genesisBootstrap:
  enabled: true
  # 0 auto-derives the density window from Shelley genesis params (3k/f).
  windowSlots: 0
  # Optional peer corroboration quorum. Default 0 retains all Genesis checks
  # without requiring operator labels. A positive value requires that many
  # reachable witnesses in distinct groups and stalls if they are absent.
  corroborationPeers: 0
  # Genesis Limit on Patience: disconnect a ChainSync peer that delivers its
  # advertised progress more slowly than limitOnPatienceRate headers per
  # second beyond a limitOnPatienceCapacity token allowance. 0 selects the
  # defaults (1000 tokens, 5 per second: a 200-second allowance).
  limitOnPatienceEnabled: true
  limitOnPatienceCapacity: 0
  limitOnPatienceRate: 0
  # ChainSync Jumping: one identified outbound hot peer streams headers; the
  # other outbound hot peers answer a MsgFindIntersect for its chain every k/2
  # blocks instead of streaming it, and an identified peer's answer vouches
  # for it. Inbound peers take no CSJ role. On by default.
  chainSyncJumping: true
```

Equivalent CLI flags / environment variables:

- `--genesis-bootstrap-enabled` / `DINGO_GENESIS_BOOTSTRAP_ENABLED`
- `--genesis-bootstrap-window-slots` / `DINGO_GENESIS_BOOTSTRAP_WINDOW_SLOTS`
- `--genesis-bootstrap-corroboration-peers` /
  `DINGO_GENESIS_BOOTSTRAP_CORROBORATION_PEERS`
- `--genesis-bootstrap-limit-on-patience-enabled` /
  `DINGO_GENESIS_BOOTSTRAP_LIMIT_ON_PATIENCE_ENABLED`
- `--genesis-bootstrap-limit-on-patience-capacity` /
  `DINGO_GENESIS_BOOTSTRAP_LIMIT_ON_PATIENCE_CAPACITY`
- `--genesis-bootstrap-limit-on-patience-rate` /
  `DINGO_GENESIS_BOOTSTRAP_LIMIT_ON_PATIENCE_RATE`
- `--genesis-csj` / `DINGO_GENESIS_CSJ`

## Optional topology: fast source + corroborating snapshot

Put the fast source in `localRoots` (mark it `trustable` so peer governance
keeps it as a preferred ingress source) and point `peerSnapshotFile` at a
cardano-node ledger peer snapshot. When Genesis selection is active and the
snapshot has relays, Dingo seeds the snapshot relays as ledger peers before
outbound startup — these are the corroborators.

**Validate corroborator independence.** Optional `controlGroups` declare common
operator control and add roots; without them, trusted ledger/snapshot pool
identities supply grouping where available and unidentified topology/ledger
peers fall back to address groups. Remote IPs, DNS aliases, valency groups
and multiple stake entries sharing one endpoint do not prove independence.
Configured groups form the exclusive witness set, with aliases counted once and
conflicts rejected. Choose independently controlled witnesses and a satisfiable
positive quorum before enabling this extra policy.

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

The `peerSnapshotFile` path is resolved relative to the topology file. Snapshot
metadata and relay endpoints are validated before outbound startup; malformed
snapshots fail startup rather than falling back. If a validated snapshot
produces no peers accepted by peer governance, startup falls back to topology
`bootstrapPeers` (so keep a bootstrap-peer list configured as a safety net).
When snapshot peers are accepted, configured bootstrap peers remain dormant
for the bootstrap recovery cooldown. If snapshot connections cannot sustain
the minimum hot-peer set afterward, normal bootstrap recovery re-enables them
when automatic recovery is enabled and no usable warm gossip or ledger peers
are available to promote.

Local-root groups accept `hotValency` or the legacy `valency` spelling;
`valency` takes precedence. An omitted `warmValency` uses the hot target.
An explicit warm target must be at least the hot target and cannot exceed
the access-point count for a nonempty group. Unknown shared topology fields
remain accepted.

Dingo accepts cardano-node's version 23 snapshot format. Its network magic must
match the configured node, its block point must carry a 32-byte hexadecimal
hash, and it must select exactly one of `bigLedgerPools` or `allLedgerPools`.
Snapshot relays must have a host/IP address and an explicit TCP port from 1 to
65535; portless SRV relays are not currently supported and fail startup.

## What you should observe

- **Log at startup**: `Genesis chain selection enabled` with
  `genesis_window_slots`, `security_param`, and `min_corroborating_peers`.
- **Uncorroborated fast source**: a throttled warning
  `genesis fast source lacks corroboration; denying chain selection` and a
  `chainselection.genesis_corroboration_failed` event carrying the source
  connection, its observed density, the corroborator count, and the required
  count. While this persists the source's **blocks are withheld from the
  ledger** (its tips are still observed so corroboration can form, but its
  `ledger.ChainsyncEvent`/blockfetch is gated off), so the node makes no forward
  progress on that source — this is expected and safe. A best-peer → none
  transition also logs `chain selection stalled: no selectable peer` and emits
  `chainselection.selected_none`. Investigate whether the snapshot peers are
  reachable and on the same chain as the fast source.
- **Slow peer disconnected**: a peer that keeps advertising a better tip but
  delivers headers too slowly logs
  `chainsync client exhausted the Genesis Limit on Patience, disconnecting`,
  emits `chainsync.client_patience_exhausted`, increments
  `dingo_chainsync_patience_exhausted_total`, and is closed with recycle reason
  `genesis_patience_exhausted`. A peer that goes silent is instead reported as
  `chainsync.client_stalled` by the two-minute stall watchdog. A caught-up peer
  waiting for new blocks is not charged.
- **Exit to Praos**: once the local tip catches up to within the Genesis window
  of the best corroborated peer's *advertised* tip (the network tip — not the
  headers delivered so far), the node logs `exiting Genesis selection mode` and
  emits `chainselection.genesis_mode_exited` (local slot, best known slot,
  window). Normal Praos selection takes over from there.

`GenesisStatus()` on the chain selector exposes the live mode, window, selected
fast source, and per-peer density/corroboration for metrics or debugging.

Authenticated delivered headers can maintain the Limit on Eagerness frontier
without promoting a peer's ledger-admission tip. If local verification omitted
an intermediate delivery, Dingo attempts a bounded reconstruction from the exact
admission point, or a lower applied anchor needed to recover a witness gap,
through applied canonical blocks and fully verified peer ancestry. Missing,
withdrawn or unverified links keep the original limit. This recovery preserves
the independent-peer checks and requires no configuration. A parked callback can
reverify its last authenticated delivery when a ledger publication expires that
evidence, including while its next raw header waits at the observer forecast
gate. This never advances the cursor or authorizes the pending raw header. The
retry ends with the callback. An authenticated header whose verdict expires
before candidate staging is reverified on that callback. Local publication races
do not count as peer rejections; invalid claims and failed renewed
authentication remain excluded. A clean restart caused by the local forecast
horizon also avoids peer-instability backoff. Actual connection and protocol
failures retain normal retry penalties. A fully authenticated path can also
supply a missing exact corroboration point when independent peers' delivered
histories no longer overlap. Only the strict proof range counts, with current
generation and original-session/withdrawal fences; ordinary linked delivery
cannot refresh that certificate. Identity, exact-point and suffix checks remain
unchanged, and no density or admission credit is added by reconstruction.

## Tuning `corroborationPeers`

- **0 (default)** — no additional corroboration quorum. Genesis eagerness,
  density, patience, ancestry and header verification remain active.
- **1** — require a witness in another independence group before selection.
- **2+** — require that many witnesses in distinct groups. Missing or
  disagreeing witnesses stall selection. Raising the count does not establish
  operator independence; choose the peer sources accordingly.

## Limitations (deferred)

Dingo implements intersection-anchored density for authoritative local fork
choice, but not every optimization in the reference implementation.
Specifically **not** implemented:

- **Devoted BlockFetch** (a performance/robustness optimization for
  downloading across many peers), and ChainSync Jumping's objector bisection
  and its limit of one objector at a time: every objector streams.
- **Independent testimony beyond every witness frontier.** The corroboration
  gate confirms a witness's chain within their overlap, but cannot testify about
  a not-yet-witnessed suffix. A fast source that forks only after every honest
  witness's frontier remains corroborated until a witness advances past the
  fork. Intersection-anchored density still resolves a conflict with the local
  candidate; independent confirmation of the suffix cannot happen before a
  witness observes it.
- **Peer-governance demotion** wired to the corroboration-failure event; today
  the source is denied selection (stall) but kept connected so it can serve
  blocks once corroboration arrives.

The first and third are performance/refinement work; the second is an inherent
limit on what the configured corroborators have observed, documented here so
operators do not mistake current overlap for testimony about a future suffix.
