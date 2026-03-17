# Dingo Debugging Session — 2026-03-16

## Objective
Get dingo (blinklabs-io/dingo) running as a preview network block producer on a k3s cluster, staying at tip, forging blocks, and serving them to peers.

## Infrastructure Setup

### Build Pipeline
- Installed nerdctl + BuildKit on k3s-texas (amd64 worker node)
- Created `~/bin/dingo-build` script: rsync source to node, nerdctl build, load into k3s containerd, update helmfile values
- Image built as `dingo:dev` with `imagePullPolicy: Never` — no registry needed
- Build cycle: edit → build (~20s) → delete pod → test

### Deployment
- Single peer topology: `cardano-node-preview-az1.cardano.svc.cluster.local:3001`
- Peer sharing disabled to prevent gossip peer discovery disrupting chain selection
- Badger cache bumped to 4GB block / 512MB index via env vars:
  - `DINGO_DATABASE_BLOB_BADGER_BLOCK_CACHE_SIZE=4294967296`
  - `DINGO_DATABASE_BLOB_BADGER_INDEX_CACHE_SIZE=536870912`

## Bugs Found and Fixed

### 1. Chain Selection Oscillation
**File:** `chainselection/selector.go` — `EvaluateAndSwitch()`

**Problem:** Multiple peers at the same block height triggered constant switching via VRF/connection-ID tiebreakers. Each switch reset the ledger pipeline.

**Fix:** Incumbent advantage — don't switch unless the new peer is strictly ahead by block number.

### 2. Connection Switch Destroys Pipeline
**File:** `ledger/chainsync.go` — `detectConnectionSwitch()`

**Problem:** Every chain selection switch called `ClearHeaders()` + `blockfetchRequestRangeCleanup()`, destroying all queued headers and aborting in-flight blockfetch. The pipeline reset to zero on every switch.

**Fix:** Removed `ClearHeaders()` — headers are valid regardless of source. Kept `blockfetchRequestRangeCleanup()` to unstick blocked batches, but no longer clear the header queue.

### 3. Blockfetch Connection ID Filter Drops Valid Blocks
**File:** `ledger/chainsync.go` — `handleEventBlockfetchBlock()`

**Problem:** `if e.ConnectionId != ls.activeBlockfetchConnId { return nil }` silently dropped in-flight blocks from the previous connection after a switch. Valid blocks that would advance the chain were discarded.

**Fix:** Removed the connection ID filter. `AddBlockWithPoint` validates blocks by hash — wrong blocks are rejected by the chain, not by connection ID.

### 4. Inbound Peers Disrupt Chain Selection
**File:** `ouroboros/ouroboros.go` — `HandleInboundConnEvent()`
**File:** `ouroboros/chainsync.go` — `chainsyncClientRollForward()`

**Problem:** Full-duplex inbound connections (k3s ServiceLB proxies, ephemeral peers) started chainsync client protocols, which published `PeerTipUpdateEvent` to chain selection. These ephemeral peers would win chain selection, then disconnect, causing the ledger to stall.

**Fix:**
- Disabled chainsync client startup on inbound connections (server protocols only)
- Added `IsInboundConnection()` to ConnectionManager
- Filtered `PeerTipUpdateEvent` publishing to outbound connections only

### 5. KES Signing Uses Absolute Period Instead of Relative
**File:** `ledger/forging/keys.go` — `UpdateKESPeriod()` and `KESSign()`

**Problem:** The forger passed absolute KES period (e.g., 825) but the KES key only supports 0–63 evolutions. Error: `key is exhausted at period 63 (max 63)`. The opcert start period (795) was never subtracted.

**Fix:** Both `UpdateKESPeriod` and `KESSign` now subtract `opCert.KESPeriod` internally to convert absolute to relative period. Callers unchanged.

### 6. Leader Schedule Not Computed After Mithril Restore
**File:** `ledger/leader/election.go` — `ShouldProduceBlock()`

**Problem:** After mithril bootstrap, the epoch nonce wasn't available. `computeSchedule` returned nil with no retry. The node never checked VRF leadership — `node_is_leader` was always 0.

**Fix:** Inline VRF fallback in `ShouldProduceBlock()` — when no cached schedule exists, compute VRF for the single current slot using `consensus.IsSlotLeader` from gouroboros.

## New Metrics Added

### Block Propagation (ledger layer)
- `dingo_block_propagation_delay_seconds` — Histogram, fires on every block applied to chain at tip. Captures actual propagation delay from slot time to application. Buckets: 100ms to 30s.

### Blockfetch (ouroboros layer)
- `cardano_node_metrics_blockfetchclient_blockdelay_seconds` — Histogram for blockfetch-specific delay
- `dingo_blockfetch_blocks_received_total` — Counter, works during catchup (not gated by IsAtTip)

### Badger Cache
- `database_blob_block_cache_hits_total` — Ristretto block cache hits
- `database_blob_block_cache_misses_total` — Ristretto block cache misses
- Added `PromRegistrySetter` interface to plugin system to wire prometheus registry to blob plugin

### CDF Update
- Removed 32-block throttle interval on CDF gauge updates — now updates on every block

## Dashboard
- Created `dingo-debug.json` Grafana dashboard
- Variables: `network` (single select) + `node` (multi-select by alias)
- Sections: Node Status, Chain Progress, Block Propagation, Forge, Peers & Connections, Cache & Storage, Resources
- Color-coded thresholds throughout

## Tests Written
- `TestIncumbentAdvantageNoSwitchAtEqualBlockNumber`
- `TestIncumbentAdvantageSwitchesWhenBehind`
- `TestIncumbentAdvantageInitialSelectionStillWorks`
- `TestNoOscillationWithMultiplePeersAtSameTip`
- `TestKESSignWithOpCertOffset`
- `TestKESSignWithoutOpCert`
- `TestUpdateKESPeriodWithOpCertOffset`
- `TestUpdateKESPeriodExhaustedWithOffset`
- `TestShouldProduceBlockInlineVRFFallback`

All pass alongside existing test suite.

## Root Cause Analysis

Dingo's architecture gates every pipeline stage on a single "active connection" concept. When chain selection switches that connection — even for trivial tiebreaker reasons — the entire pipeline resets. The Cardano reference node (ouroboros-network) doesn't have this problem because it:
- Downloads headers from all peers in parallel
- Has a blockfetch decision component that picks the best peer per range
- No "active connection" concept — all connections contribute simultaneously

A phased plan to align with the reference architecture was produced covering:
- Phase 0: Upstream the fixes from this session
- Phase 1: Accept headers from all peers
- Phase 2: Blockfetch retry/fallback across peers
- Phase 3: Parallel blockfetch with peer assignment
- Phase 4: Fragment-based chain selection
- Phase 5: Chain selection hysteresis

## Current State

- Node at tip (block 4,112,267+), tip gap 0
- Single outbound peer (cardano-node preview relay)
- 5 inbound full-duplex connections (server protocols only)
- Block propagation: p50 = 401ms, p95 = 4.25s, average = 1.04s
- Badger cache hit ratio: 57%
- Forge enabled, KES valid (34 periods remaining)
- Inline VRF checking every slot
- Waiting for leader slot (~3-5 per epoch, ~11 hours remaining in epoch 1238)

## Files Modified (uncommitted in ~/git/dingo)

- `chainselection/selector.go` — incumbent advantage
- `chainselection/selector_test.go` — 4 new tests
- `ledger/chainsync.go` — pipeline fixes (connection switch, blockfetch filter, header filter)
- `ledger/state.go` — block propagation delay histogram
- `ledger/metrics.go` — blockPropagationDelay metric
- `ledger/forging/forger.go` — forge pipeline (no code change, reverted)
- `ledger/forging/keys.go` — KES absolute-to-relative period
- `ledger/forging/keys_test.go` — 4 new tests
- `ledger/leader/election.go` — inline VRF fallback
- `ledger/leader/election_test.go` — 1 new test
- `ouroboros/ouroboros.go` — disable client protocols on inbound
- `ouroboros/chainsync.go` — filter PeerTipUpdate to outbound only
- `ouroboros/blockfetch.go` — histogram + counter metrics
- `connmanager/connection_manager.go` — IsInboundConnection method
- `database/database.go` — PromRegistrySetter wiring
- `database/plugin/log.go` — PromRegistrySetter interface
- `database/plugin/blob/badger/database.go` — SetPromRegistry method
- `database/plugin/blob/badger/metrics.go` — ristretto cache prometheus collector
