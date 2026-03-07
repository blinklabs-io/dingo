# Release Notes

## v0.22.0 (March 7, 2026)

**Title:** v0.22.0 release

**Date:** March 7, 2026

**Version:** v0.22.0

Hi folks! Here’s what’s included in v0.22.0.

### ✨ What's New

### 💪 Improvements

### 🔧 Fixes

### 📋 What You Need to Know

### 🙏 Thank You

Thank you for trying Dingo—your feedback helps us keep improving.

---

<!-- Raw structured release-note data (to be formatted into the sections above). -->

```json
{
  "✨ What's New": [
    "Node operators can now bootstrap a Dingo node from a Mithril snapshot and have the ledger state imported automatically. This adds integrated Mithril snapshot download/import support, new `mithril` and `sync --mithril` CLI flows, schema and bulk-import extensions (including raw block loading and UTxO-HD formats), and additional handling for Mithril “gap blocks”.",
    "You can run Dingo with built-in, configurable HTTP APIs for common ecosystem compatibility. This introduces minimal Blockfrost-compatible and Mesh/Rosetta-compatible servers (including shared pagination utilities), plus optional “bark” archive/proxy support to serve blocks via S3/GCS signed URLs and new blob/database APIs for presigned block URL retrieval.",
    "Block production is now supported with production-grade leader election and key management. This adds Praos leader election and scheduling, a keystore that loads/validates VRF/KES/OpCert material and manages KES evolution, and end-to-end forging wiring from config through block building and broadcasting with forging tolerances and metrics.",
    "The node now emits richer on-chain lifecycle events that applications can subscribe to. This adds epoch transition, hard fork, block-forged, slot-battle, ledger apply/undo, and per-transaction apply/rollback event types, along with tests and safer asynchronous delivery behavior.",
    "Governance and Conway-era features are now available in the on-chain metadata pipeline. This adds governance/committee/constitution models with CRUD and rollback support, Conway proposal and vote processing during ledger delta application, and LedgerView governance queries (including `GetActiveDreps`) with tests.",
    "A new “leios” mode is available for early experimentation with Leios protocols. This adds a `leios` run mode and initial leiosfetch/leiosnotify protocol wiring alongside required dependency updates.",
    "Stake snapshots and stake distribution are now first-class features with robust persistence and querying. This adds snapshot calculation/management APIs, rollback-safe deletion, deterministic ordering (slot, `block_index`, `cert_index`), and real per-pool live stake aggregation across SQLite/MySQL/Postgres backends with extensive tests."
  ],
  "💪 Improvements": [
    "Syncing is now faster and safer by reducing unnecessary validation during initial catch-up. This introduces a persistent `validationEnabled` flag on `LedgerState` and updates chainsync/block processing to skip historical block validation until within the stability window (with improved validation cutoffs and safer delta flushing when validation turns on).",
    "Block and transaction storage can now be tuned for performance and cost with tiered storage and caching options. This adds configurable tiered CBOR cache settings, offset-based CBOR storage/extraction for blocks/tx/UTxO across database/blob/cache/ledger, and tiered storage modes that only persist full transaction metadata when API storage is enabled.",
    "Rollback and resync behavior is more robust under forks and stalled peers. This improves fork detection and resync orchestration, adds database rollback APIs (including epoch deletion after a slot), enforces rollback depth limits based on security parameter K, and introduces peer filtering/plateau-based recycling with better timeouts and connection coordination.",
    "Network and peer management now scales more predictably under load. This adds configurable inbound and per-IP connection limits, a hard cap on peer list size, ChainSelector peer tracking limits with LRU eviction and a `PeerEvicted` event, and routable-only peer address enforcement while still auto-connecting discovered peers.",
    "Transaction processing is more resilient and configurable under pressure. This adds mempool eviction/rejection watermarks, TTL-based background cleanup with metrics, per-peer token-bucket rate limiting for TxSubmission, and optional ledger-backed revalidation plus intra-block double-spend detection in the block builder.",
    "Observability has been expanded across sync, forging, and storage. This adds Prometheus gauges for node start time and forging-enabled status, forging and stake-pool metrics (KES, outcomes, block stats, slot battles), structured GORM logging via slog-based integration, and optional registry wiring through node components.",
    "Epoch, slot, and nonce handling better matches Cardano semantics and edge cases. This adds a real-time slot clock, improved `SlotToEpoch`/`TimeToSlot` behavior (including pre-genesis handling), big.Int-based epoch duration helpers, and Praos-style evolving/candidate/epoch nonce tracking (including resumption after imports via ranged nonce queries).",
    "Developer and operator documentation has been significantly expanded. This adds and refines `ARCHITECTURE.md`, `AGENTS.md`, and README coverage for block production, Mithril bootstrap, APIs, storage modes, events, plugins, and current feature status, plus DevNet tooling and integration test harnesses."
  ],
  "📋 What You Need to Know": [
    "If you rely on event type strings, you may need to update consumers due to naming changes. Several event type string constants were renamed to underscore-based names, so downstream filters and subscriptions should be checked and adjusted.",
    "If you enable API storage or new tiered storage modes, you may want to run or schedule metadata backfill. The system adds resumable metadata backfill checkpoints for API storage mode and changes when full transaction metadata is persisted, which can affect query completeness until backfill is done.",
    "If you deploy forging in production, verify key file paths and permissions before enabling it. Block production now depends on keystore-managed VRF/KES/OpCert material with stricter permission checks and configurable forging tolerances, so misconfigured keys or permissions will prevent forging from starting."
  ],
  "🔧 Fixes": [
    "Sync stability issues around header/block fetching and chainsync coordination have been addressed. This replaces fragile waiting/backpressure mechanisms with readiness channels, improves timeouts and cleanup, clears header queues on chain-tip mismatch, adds rollback loop detection, and tightens server-side intersect/idle timeouts with better error wrapping.",
    "Several concurrency and event-delivery deadlocks and goroutine leak risks have been eliminated. This makes event delivery non-blocking and time-bounded, publishes certain events asynchronously, adds panic-safe subscriber handler invocation, ensures Publish/Close cannot deadlock with full channels, and refactors lock usage so event emission occurs after locks/transactions.",
    "Security hardening has been added for configuration, filesystem usage, and key material. This adds strict network name validation to prevent path traversal, size limits for topology configs and HTTP request bodies, hardened file permissions for data and profiling paths, and explicit zeroization of VRF/KES secret key material during keystore shutdown and KES evolution.",
    "Cryptographic and protocol-validation correctness has been tightened across eras. This adds header VRF/KES verification before processing chainsync blocks (including header-only verification gating via `validationEnabled`), enforces VRF length constraints, uses epoch-cache lookups for verification, increases allowed certificate chain depth, and improves fee/min-fee and ExUnits handling (including overflow-safe accumulation and Conway invalid-tx fast-path handling).",
    "Storage key encoding issues in object stores have been resolved. This hex-encodes S3/GCS object keys and prefixes on write and decodes them on list operations to ensure consistent addressing and listing behavior."
  ]
}

```