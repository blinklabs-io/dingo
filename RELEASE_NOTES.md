# Release notes

## Dingo v0.22.0

**Date:** March 7, 2026  
**Version:** v0.22.0

Hi folks! Here’s what’s new in this release.

<!--
NOTE: The block below is auto-injected structured data used to draft the
human-friendly release notes.
-->

{
  "✨ What's New": [
    "The node can now be bootstrapped from Mithril snapshots end-to-end, so you can start from an existing chain snapshot instead of syncing everything from scratch. This adds integrated Mithril snapshot download/import into `LedgerState`, extends the DB schema and bulk import APIs (including raw block loading and UTxO-HD formats), and exposes new `mithril` and `sync --mithril` CLI workflows with tighter snapshot/config/event handling.",
    "You can now run optional HTTP APIs alongside the node to support common ecosystem integrations. This introduces a minimal Blockfrost-compatible server plus a Mesh (Rosetta-compatible) server, with shared pagination helpers and `mesh` listen-address configuration wired through a `LedgerState` adapter.",
    "Block production is now supported in production mode for stake pools, so operators can forge and broadcast blocks using configured keys and schedules. This wires VRF/KES/OpCert key management (including KES evolution), leader election/scheduling, slot-aligned forging guards, and event/metrics emission through the node startup and forger pipeline.",
    "The node now supports Conway-era governance tracking so governance state can be queried and rolled back consistently. This adds governance/committee/constitution models with CRUD + rollback across metadata backends, processes Conway proposals/votes during ledger delta application, and populates ledger-view governance queries (including `GetActiveDreps`) with tests.",
    "Nodes can now run in an initial Leios mode to experiment with Leios protocol integration. This adds a `leios` run mode and initial `leiosfetch`/`leiosnotify` protocol wiring alongside the required dependency updates.",
    "A new “bark” archive server and proxy blob store can now serve blocks via signed URLs, enabling external consumers to fetch block bodies from object storage safely. This adds a bark HTTP server, presigned URL support for S3/GCS in blob/database APIs, and node configuration wiring for signed block URL retrieval."
  ],
  "💪 Improvements": [
    "Syncing is faster and safer because historical validation is reduced until the node is close to the tip, while still validating when it matters. This introduces a persistent `validationEnabled` flag on `LedgerState`, gates historical block validation by a stability window, adds header-only cryptographic verification during chainsync, and uses `max(chain tip, wall-clock slot)` for the validation cutoff while flushing accumulated deltas before the first validated block.",
    "Chain synchronization is more resilient under forks, stalls, and multi-peer operation, reducing stuck states and improving recovery behavior. This adds configurable multi-client chainsync state with header deduplication and fork/stall detection, per-block commit processing with resync hooks, event-driven recycling of stalled clients, plateau-based peer filtering around chain selection, and tighter iterator/connection coordination with expanded timeouts and logging.",
    "Rollback handling is more complete and consistent across the ledger and databases, so state can be restored cleanly to an earlier slot. This adds database rollback APIs integrated with ledger rollback (including deleting epochs after a slot and reloading epoch caches), enforces a maximum rollback depth based on security parameter K, and adds integration tests using immutable rollback testdata.",
    "Storage and caching for CBOR-encoded chain data is more configurable and efficient, reducing overhead while keeping behavior observable. This introduces tiered/offset-based CBOR storage and extraction for blocks/tx/UTxOs across DB/blob/cache/ledger (including genesis), configurable tiered cache settings with defaults and examples, and Prometheus-backed cache size metrics plus delete support.",
    "Stake snapshots and stake distribution queries are more accurate and usable for scheduling and APIs. This adds stake snapshot calculation/CRUD (including rollback-oriented deletes), slot-aware ordering via `added_slot`/`block_index`/`cert_index`, live per-pool stake aggregation from UTxOs across backends, and updates stake-related queries to use the `mark` snapshot where appropriate.",
    "Leader election and epoch/slot tracking is more reliable, especially across epoch boundaries and snapshot/import scenarios. This adds a real VRF-based leader election path, an asynchronous cached per-epoch leader schedule, a slot clock integrated with ledger epoch transition events, and more robust Praos nonce tracking (including ranged block-nonce queries to resume nonce computation correctly after snapshots/imports).",
    "Network and peer management is more robust under load and hostile inputs, improving operator control and safety. This adds strict network-name validation to prevent path traversal, hard caps and eviction for peer lists (including `ErrPeerListFull` and `PeerEvicted`), inbound and per-IP connection limits, routable-only peer address enforcement for discovery, and full-duplex node-to-node connection support to reduce redundant outbound links.",
    "Mempool behavior is more predictable under pressure and long-running operation, reducing deadlocks and unbounded growth. This adds eviction/rejection watermarks, TTL-based background cleanup with metrics, and refactors validation/event publication to occur outside write locks with tests to prevent deadlocks.",
    "Observability is improved so operators can understand forging, syncing, and performance at runtime. This adds Prometheus gauges for node start time and forging-enabled status, extensive forging/stake-pool metrics (KES, forge outcomes, block stats, slot battle counts), and more structured/less noisy logging across node, database backends, and chainsync.",
    "The project’s docs and operational guidance have been expanded to make setup and architecture easier to understand. This adds and refines `ARCHITECTURE.md`, `AGENTS.md`, and README coverage for block production, Mithril bootstrap, APIs, storage modes, events, plugins, and current feature status (including Praos stake snapshot documentation)."
  ],
  "🔧 Fixes": [
    "Event delivery and subscription is less likely to deadlock or leak goroutines, improving stability under load and during shutdown. This makes event delivery non-blocking and time-bounded, adds panic-safe `SubscribeFunc` handler invocation, ensures subscriber channels use non-blocking sends, coordinates ledger shutdown with event goroutines, and fixes a slot-battle double-lock deadlock with regression tests.",
    "Header verification and cryptographic checks are stricter and more correct across epochs and key formats, preventing acceptance of invalid chain data. This enforces 64-byte VRF length checks, switches header verification to an epoch-cache lookup (rejecting blocks when no suitable epoch/nonce exists), uses a fixed 48-byte raw payload for OpCert signature verification, and increases allowed certificate chain depth with coverage tests.",
    "Slot/epoch conversion and time handling is safer around genesis and edge conditions to prevent underflow and divide-by-zero issues. This adds explicit pre-genesis handling with dedicated errors, makes the slot clock wait until genesis, hardens `SlotToEpoch` for empty/early slots and zero-length epochs, and uses big.Int-based epoch duration calculation with checked uint64 slot addition.",
    "Chainsync/blockfetch behavior is more robust when the peer tip or chain tip changes unexpectedly, reducing stalls and invalid requests. This adds header-queue clearing on chain-tip mismatches, maximum header-queue capacity with specific errors, maximum allowed slot ranges for blockfetch requests, validated/logged handling of request ranges, and improved connection-switch/rollback-loop detection and cleanup.",
    "Fee and execution-unit accounting is more resilient to overflow and edge cases so invalid transactions are handled safely. This adds overflow/zero-denominator checks to fee calculation, introduces overflow-safe ExUnits summation used by validation/forging, and adjusts forging to skip transactions that exceed remaining ExUnits budget while keeping tests for overflow/block-limit behavior.",
    "Object storage key handling is safer and more interoperable for non-ASCII or binary identifiers. This hex-encodes S3 and GCS object keys/prefixes when writing and decodes them when listing to prevent path and encoding issues.",
    "Security and operational hardening has been improved around configuration and file handling. This adds topology-config size limits with ensured file-handle closure, hardens profiling/data-dir file permissions, adds HTTP request body size limits, and conditionally allows plaintext commit timestamp storage when SOPS is not configured."
  ]
}

