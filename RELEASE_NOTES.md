---
title: Release notes
---

# Release notes

## v0.22.0 (March 7, 2026)

Hi folks! Here’s what’s included in v0.22.0.

### ✨ What's New

### 💪 Improvements

### 🔧 Fixes

### 🧾 What You Need to Know

### 🙏 Thank You

<details>
<summary>Raw release notes data (for auditing)</summary>

```json
{
  "✨ What's New": [
    "You can now run Dingo with built-in HTTP APIs so external services can query chain and ledger data without custom integrations. Specifically, the node now includes a minimal Blockfrost-compatible server and a Mesh (Coinbase Rosetta–compatible) server, both wired through a LedgerState adapter and configurable listen-address options.",
    "You can now bootstrap a node from Mithril snapshots more completely, reducing the time and work required to get a usable node from existing network state. This adds integrated Mithril snapshot import into ledger state, new database schema and bulk import APIs (including UTxO-HD and raw block loading), and new `mithril` / `sync --mithril` CLI support.",
    "You can now use a new archive path to fetch blocks via signed URLs, which helps when blocks are stored remotely and need controlled access. This introduces a bark archive server and a proxy blob store that generates/serves presigned S3/GCS block URLs, with supporting blob/database API extensions and node configuration wiring.",
    "You can now enable a new Leios execution mode to start experimenting with early Leios protocol plumbing. This adds a `leios` run mode plus initial leiosfetch/leiosnotify protocol wiring and associated dependency updates.",
    "You can now run and test against a reproducible Cardano DevNet setup, making end-to-end testing easier and more consistent. This adds a Docker-based DevNet, a Go test harness, devnet-tagged integration tests, and a configuration loader that supports dynamic slot and system-start parameters."
  ],
  "💪 Improvements": [
    "Syncing is now faster and more reliable, especially during catch-up and fork scenarios, so the node spends less time stalled or doing unnecessary work. Internally, chainsync moved toward per-block commits with iterator notifications, added fork/stall detection and resync orchestration, improved intersect handling/timeouts, and introduced multi-client chainsync state with header deduplication and connection-switch hooks.",
    "Ledger validation during initial sync is now more efficient, so you can reach a usable tip sooner without giving up full validation once you’re close to real-time. This is implemented by a persistent `validationEnabled` flag on `LedgerState`, a stability-window cutoff (now using `max(chain tip, wall-clock slot)`), header-only cryptographic verification during chainsync gated by `validationEnabled`, and logic to flush accumulated deltas before validating the first block near the tip.",
    "Block production is now more production-ready and easier to operate, improving correctness and observability when forging is enabled. This adds leader election and an asynchronous per-epoch leader schedule cache, secure VRF/KES/OpCert keystore management with configurable KES parameters and stricter permissions, slot-aligned forging with sync/slot guards and configurable tolerances, plus expanded forging metrics and structured event emission.",
    "Storage and query performance improved, especially for CBOR-heavy data paths and larger metadata workloads. This introduces offset-based CBOR storage/extraction for blocks/transactions/UTxOs, tiered storage modes with API-controlled transaction-metadata persistence, bulk-load optimizations across MySQL/Postgres/SQLite, separate SQLite read/write pools, and additional cache metrics and deletion support.",
    "Governance and stake tracking are now more complete, which improves the quality of ledger-driven governance and pool/stake insights. This adds governance/committee/constitution models with CRUD and rollback, Conway proposal/vote processing persisted via governance models, DRep voting power and activity tracking, stake snapshot calculation/queries, and real per-pool live stake aggregation using UTxOs across all metadata backends.",
    "Networking and peer management behavior is now more predictable under load, improving stability when many peers connect or compete. This adds inbound connection limits (including per-IP limiting), hard caps on peer lists and tracked peers with eviction events, peer plausibility checks in chain selection, full-duplex node-to-node connections for InitiatorAndResponder, and routable-only peer address enforcement for gossip/ledger discovery.",
    "Events and observability are now safer and more actionable so you can integrate listeners without risking deadlocks or missed signals. This expands event types (epoch transitions, block-forged, slot-battle, per-block and per-transaction apply/rollback), makes delivery non-blocking and time-bounded with metrics, adds panic-safe handler invocation, and improves Prometheus metrics (node start time, forging-enabled, forging/stake pool/KES/outcome stats).",
    "Developer documentation and operator guidance are clearer, making it easier to understand how to run and extend the node. This adds and expands ARCHITECTURE.md, AGENTS.md, README sections for Mithril/bootstrap/APIs/storage modes/events/plugins, and additional protocol documentation for Praos stake snapshots.",
    "Transaction validation is now more consistent across eras, improving correctness when validating older or transition-era transactions. This adds shared tx size/ex-unit validation, explicit fee validation for Alonzo/Babbage/Conway, Byron-era transaction validation, era-compatibility checks to validate era-1 using previous-era protocol parameters, and reliance on era UTxO rules for Babbage/Conway validation where appropriate."
  ],
  "📋 What You Need to Know": [
    "If you rely on event type strings, you may need to update your integrations because several event type constants were renamed. The event string values were changed to underscore-based names, so consumers that match on the old string values should update their filters and tests.",
    "If you enable API-focused storage modes, you may see different metadata persistence behavior depending on configuration. Tiered storage modes now control whether full transaction metadata is persisted, and a resumable metadata backfill process was added for API storage mode to populate indexes after the fact.",
    "If you run with forging enabled in production, review your key and forging configuration before upgrading to avoid startup failures. The node now enforces stricter key file permission checks and adds configurable forging tolerances, plus new metrics that make forging status visible and easier to monitor."
  ],
  "🔧 Fixes": [
    "Rollback handling is now safer, reducing the chance of inconsistent on-chain state after forks or operator-triggered rollbacks. This adds database rollback APIs integrated with ledger rollback (including epoch deletion and cache reload), maximum rollback depth enforcement based on security parameter K, and refactors to compute rollback/protocol-parameter state locally and apply atomically with additional safety checks and tests.",
    "Several deadlocks and race conditions were eliminated, improving stability under concurrent load and event-heavy scenarios. This includes fixes for slot-battle detection deadlocks, read-lock protection around ledger queries, moving event publication outside write locks/transactions (chain and mempool), non-blocking subscriber sends to prevent Publish/Close deadlocks, and time-bounded event delivery to detect and prevent goroutine leaks.",
    "Block and header verification is now stricter and more correct, preventing invalid blocks from being accepted during sync. This adds VRF/KES verification before processing chainsync blocks, epoch-cache lookups for header verification (rejecting when no suitable epoch/nonce exists), 64-byte VRF comparison enforcement, increased certificate chain depth support, and verification-failure-triggered connection recycling.",
    "Fee and execution-unit arithmetic is now more robust, preventing overflows and edge-case crashes. This adds overflow and zero-denominator checks, overflow-safe ExUnits summation with error reporting, and later simplifies the fee APIs to saturating arithmetic while updating callers and tests to match.",
    "Input validation and security hardening were improved to reduce misuse and unsafe file/path behaviors. This adds strict network name validation to prevent path traversal, a 10 MB topology config size limit, stronger data-dir and profiling file permission checks, conditional SOPS usage with plaintext fallback when not configured, and safer request/body size limits for HTTP endpoints."
  ]
}

```

</details>
