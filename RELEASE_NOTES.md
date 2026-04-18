# Release Notes


## v0.35.2 (April 18, 2026)

**Title:** Safer era forecasts and refreshed dependencies

**Date:** April 18, 2026

**Version:** v0.35.2

Hi folks! Here’s what we shipped in v0.35.2.

### ✨ What's New

* Noted **no new features:** This patch focuses on improvements and fixes.

### 💪 Improvements

* Improved **protocol library compatibility:** Dingo now uses gouroboros v0.165.1 to stay current with upstream protocol library updates.

### 🔧 Fixes

* Corrected **safer era forecasts:** Hard fork era history now stays within the safe forecast horizon, so slot and time lookups do not promise certainty too far past the current ledger tip.
* Restored **release history continuity:** The changelog now includes the v0.35.1 entry so recent release history stays complete and easier to scan.

### 📋 What You Need to Know

* Simplified **upgrade guidance:** No action is required for most users, and upgrading to v0.35.2 is sufficient.

### 🙏 Thank You

Thank you for trying!

---

## v0.35.1 (April 17, 2026)

**Title:** Steadier peers and faster schedule calculations

**Date:** April 17, 2026

**Version:** v0.35.1

Hi folks! Here’s what we shipped in v0.35.1.

### ✨ What's New

- **No new features:** This patch focuses on improvements and fixes.

### 💪 Improvements

- **Faster epoch nonce computation:** Ledger processing moves faster because epoch nonce calculation now uses stored nonce data instead of decoding every block again.
- **Quicker leader schedule generation:** Schedule calculation finishes dramatically faster, especially on lower-powered hardware, while keeping the same scheduling behavior.
- **Current upstream protocol compatibility:** Cardano protocol support stays current because key upstream dependencies were refreshed for smoother compatibility.
- **Stronger PostgreSQL compatibility:** PostgreSQL-backed setups are more robust because upstream database coverage and compatibility were improved.
- **More predictable Antithesis defaults:** Test environments are easier to reason about because the default `IMAGE_TAG` now resolves to `main` instead of `latest`.

### 🔧 Fixes

- **Steadier peer selection near tip:** Nodes are less likely to bounce between near-tip peers because chain selection now compares the tips it has actually observed.
- **Safer UTxO RPC predicate handling:** Deep or cyclic request predicates no longer recurse indefinitely, so requests do not hang or risk crash-like behavior.
- **More reliable UTxO RPC queries:** SQLite-backed address queries now handle the reserved `transaction` table name correctly, and broader Connect RPC coverage helps harden query, submit, sync, and watch behavior.

### 📋 What You Need to Know

- **No action required for most users:** You're all set—just upgrade to v0.35.1.
- **Antithesis users:** If you rely on the default IMAGE_TAG in the dingo-praos testnet compose file, it now resolves to `main` instead of `latest`.

### 🙏 Thank You

Thank you for trying!

---


## v0.35.0 (April 15, 2026)

**Title:** Smoother sync and faster block lookups

**Date:** April 15, 2026

**Version:** v0.35.0

Hi folks! Here’s what we shipped in v0.35.0.

### ✨ What's New

- **Blockfrost-compatible metadata-label transaction endpoints:** Building Blockfrost-style integrations is easier because you can now query transaction metadata labels using Blockfrost-compatible endpoints.

### 💪 Improvements

- **More complete Mithril bootstrap snapshots:** Startup is smoother because nodes now prepare more of the snapshot data needed for the current epoch window after a Mithril bootstrap.
- **More resilient peer retry behavior:** Sync is more rock-solid because chainsync and blockfetch retry using the active best peer when target connections aren’t available.
- **Faster block lookup by hash:** Hash-based block queries are faster because block-by-hash resolution is streamlined across blob storage backends.
- **Larger event queue capacity:** Bursty workloads are easier to handle because the event queue capacity was increased from 10,000 to 100,000.
- **Stronger connection direction tracking:** Networking is more rock-solid because chainsync tracks outbound-started clients and the connection manager detects inbound/outbound `ConnectionId` collisions.
- **Simpler SQLite metadata batching:** Batched metadata writes are easier to manage because a `BatchAccumulator` streamlines collecting and resetting SQLite metadata model batches.

### 🔧 Fixes

- **No user-facing fixes:** This release focuses on new features and improvements.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.35.0.

### 🙏 Thank You

Thank you for trying!

---


## v0.34.0 (April 14, 2026)

**Title:** Blockfrost-compatible metadata label endpoints

**Date:** April 14, 2026

**Version:** v0.34.0

Hi folks! Here’s what we shipped in v0.34.0.

### ✨ What's New

- **Blockfrost-compatible metadata-label transaction endpoints:** Building Blockfrost-style integrations is easier because you can now retrieve transactions by metadata label using endpoints that match Blockfrost conventions.

### 💪 Improvements

- **Ledger validation guardrails:** Ledger validation and processing is more consistent in edge cases, which helps avoid repeated work and makes failures easier to understand.
- **Safer Unix-domain socket startup:** Local Unix socket startup is safer and more reliable, reducing the chance of startup failures due to leftover files.
- **Clearer submit errors (`utxorpc/submit.go`):** Error messages now provide clearer context when a submission fails, making troubleshooting faster.
- **Shared SQL address filtering and sentinel errors:** SQL plugins now share a consistent way to filter UTxO addresses and report common failure cases, which improves maintainability and debugging.
- **Dependency refresh:** Dependencies were refreshed to keep the project current and compatible with upstream fixes and improvements.
- **More adaptive Antithesis workflow defaults:** CI workflow behavior is now more adaptive, helping reduce unintended pinning to outdated behavior in automation runs.

### 🔧 Fixes

- **No user-facing fixes:** This release focuses on new features and improvements.

### 📋 What You Need to Know

- **Blockfrost integrations:** If you rely on Blockfrost-compatible behavior, you can start using the new metadata-label transaction endpoints immediately without changing existing endpoints.

### 🙏 Thank You

Thank you for trying!

---


## v0.33.0 (April 13, 2026)

**Title:** Blockfrost-compatible address endpoints

**Date:** April 13, 2026

**Version:** v0.33.0

Hi folks! Here’s what we shipped in v0.33.0.

### ✨ What's New

- **Blockfrost-compatible address endpoints:** Plugging Dingo into existing Blockfrost-based tooling is easier because you can now query address UTxOs and transactions via Blockfrost-compatible HTTP endpoints.
- **Epoch-scoped protocol parameters:** Clients can fetch the exact settings they need for historical or upcoming validation because protocol parameters can now be requested for a specific epoch.

### 💪 Improvements

- **Node internals split into focused files:** The codebase is easier to navigate because chainsync recycling, forging, and shutdown logic moved out of `node.go` into dedicated files.
- **Makefile help and lint targets:** Build and contributor workflows are easier to run consistently because the Makefile now includes `help` and `lint` targets with inline target descriptions.
- **Dependency and CI refresh:** Compatibility stays smoother because Go module dependencies (including `golang.org/x`), key CI actions, and the txtop Docker image were updated.
- **Clearer package docs and licensing:** Development is simpler because core packages now include Apache 2.0 headers, richer package docs, and conformance test suite docs under `internal/test/conformance`.
- **More complete `dingo.yaml.example`:** Getting started is easier because the example config now includes a default `databasePath` that matches the expected on-disk layout.

### 🔧 Fixes

- **Safer chainsync recycling on small topologies:** Small networks are less likely to lose their only workable connection because the recycler now avoids recycling the only eligible peer.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.33.0.

### 🙏 Thank You

Thank you for trying!

---


## v0.32.3 (April 10, 2026)

**Title:** Certificate-aware transaction matching

**Date:** April 10, 2026

**Version:** v0.32.3

Hi folks! Here’s what we shipped in v0.32.3.

### ✨ What's New

- **Certificate-aware transaction pattern matching (`has_certificate`):** Filtering certificate-related transactions is easier because you can now match and validate Cardano certificates directly in transaction pattern rules.

### 💪 Improvements

- **More reliable persistent-fork recovery:** Sync is more rock-solid because chain sync now closes the affected connection when it detects a persistent fork.

### 🔧 Fixes

- **No user-facing fixes:** This patch focuses on new features and improvements.

### 📋 What You Need to Know

- **Transaction-pattern configs:** If you maintain custom transaction-pattern rules, you can start using `has_certificate` to filter or detect certificate-related transactions.

### 🙏 Thank You

Thank you for trying!

---


## v0.32.2 (April 9, 2026)

**Title:** Stability and polish updates

**Date:** April 9, 2026

**Version:** v0.32.2

Hi folks! Here’s what we shipped in v0.32.2.

### ✨ What's New

- **No new features:** This patch focuses on improvements and fixes.

### 💪 Improvements

- **More reliable connection cleanup:** Network operations stay more rock-solid because the node now detects inactive connections and purges leftover state tied to closed links.
- **Refreshed telemetry and runtime dependencies:** Observability and compatibility stay smoother because core dependencies (including OpenTelemetry, gRPC, and gonum) were refreshed.

### 🔧 Fixes

- **Clearer v0.32.1 release notes:** Documentation is clearer because v0.32.1 release notes are now populated and placeholder text was removed.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.32.2.

### 🙏 Thank You

Thank you for trying!

---


## v0.32.1 (April 7, 2026)

**Title:** Smoother submission controls and matching

**Date:** April 7, 2026

**Version:** v0.32.1

Hi folks! Here’s what we shipped in v0.32.1.

### ✨ What's New

- **No new features:** This patch focuses on improvements and fixes.

### 💪 Improvements

- **Disable transaction submission throttling:** High-throughput workloads are easier to run because setting the transaction submission rate limit to `0` now fully disables throttling.
- **Refined transaction pattern matching:** Filtering rules are easier to reason about because transaction pattern matching now uses dedicated helpers for `consumes`, `produces`, `has_address`, and asset filters.

### 🔧 Fixes

- **AWS SDK patch updates:** AWS S3 integrations are more rock-solid because AWS SDK for Go v2 S3 modules were bumped to newer patch versions.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.32.1.

### 🙏 Thank You

Thank you for trying!

---


## v0.32.0 (April 7, 2026)

**Title:** Background pruning and scalable UTxO search

**Date:** April 7, 2026

**Version:** v0.32.0

Hi folks! Here’s what we shipped in v0.32.0.

### ✨ What's New

- **Periodic Bark block pruning:** Long-running nodes stay smooth because a nifty background task now periodically prunes no-longer-needed Bark blocks and starts and stops cleanly with node startup and shutdown.
- **Structured `SearchUtxos` pagination and asset filtering:** Finding and listing UTxOs is more consistent and scalable because `SearchUtxos` pagination and asset filtering now use a structured query model handled in the database layer.

### 💪 Improvements

- **Slot-aware transaction validation:** Transaction validation is more rock-solid across slot-clock changes because the ledger and mempool now validate against the current-or-tip slot and track slot-clock fallbacks.
- **Clearer external interfaces docs:** Integration decisions are simpler because docs now describe client-facing **external interfaces**, clarify Bark’s role, and expand port and configuration tables with defaults and role descriptions.
- **Updated CI and Docker Go toolchains:** Maintenance is smoother because workflows now use actions/setup-go v6.4.0 and the Dockerfile build image moved from Go 1.25.8-1 to 1.26.1-1.
- **Dependency refresh:** Compatibility stays rock-solid because we updated fxamacker/cbor to v2.9.1, plutigo to v0.1.1, AWS SDK for Go v2 S3 modules, OpenTelemetry Go to v1.43.0, and google.golang.org/api to v0.274.0.

### 🔧 Fixes

- **No user-facing fixes:** This release focuses on new features and improvements.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.32.0.

### 🙏 Thank You

Thank you for trying!

---


## v0.31.1 (April 4, 2026)

**Title:** Clearer rollback recovery outcomes

**Date:** April 4, 2026

**Version:** v0.31.1

Hi folks! Here’s what we shipped in v0.31.1.

### ✨ What's New

- **Clearer rollback recovery status:** Recovery is simpler to understand because chain recovery now reports clearer outcomes after a local rollback.

### 💪 Improvements

- **More consistent recovery staleness checks:** Recovery decisions are more rock-solid because staleness checks are now based on the primary chain tip.

### 🔧 Fixes

- **Rollback-aware recovery test coverage:** Reliability is more rock-solid because recovery and chainsync tests now validate rollback-aware behavior.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.31.1.

### 🙏 Thank You

Thank you for trying!

---


## v0.31.0 (April 3, 2026)

**Title:** Storage safeguards and smarter peer sync

**Date:** April 3, 2026

**Version:** v0.31.0

Hi folks! Here’s what we shipped in v0.31.0.

### ✨ What's New

- **Immutable storage settings:** Upgrades are more rock-solid because the node now persists storage mode and network metadata and refuses to open a database if they don’t match your current `--storage-mode` configuration.
- **Earlier ledger peer discovery:** Peer discovery starts sooner because reconciliation now invokes ledger-based discovery earlier with debug logs and configurable target and bound settings.
- **Shared transaction filtering (`TxPredicate`):** Transaction submit and watch are more consistent because filtering now uses a shared `TxPredicate` evaluation engine.

### 💪 Improvements

- **Quieter near-tip chain sync logs:** Routine operation is less noisy because chain-sync progress logging now pauses once you’re at least 99.9% synced.
- **Smarter peer lag filtering:** Sync catch-up is smoother because peer selection now skips nodes that are far behind the best known tip using a `securityParam`-based filter.
- **Smoother Prometheus defaults and cache visibility:** Metrics setup is simpler because the node now uses the default Prometheus registry when none is provided and exports Badger cache gauges.
- **More accurate SQLite disk reporting:** Troubleshooting is easier because SQLite disk size reporting and error diagnostics are now more accurate.
- **Clearer CI and release documentation:** Release automation is easier to follow because workflow behavior and related documentation were refined.

### 🔧 Fixes

- **Safer fork extension handling:** Fork recovery is smoother because block fetching now restarts cleanly on forks and skips unnecessary rollbacks when a fork extends from your local tip.
- **Earlier validity-interval rejection:** Mempool processing is more efficient because transactions that can’t be valid yet are rejected up front.
- **Security dependency update:** Security stays solid because `github.com/go-jose/go-jose/v4` was updated to v4.1.4.

### 📋 What You Need to Know

- **No action required for most users:** You're all set—just upgrade to v0.31.0.
- **Operators changing storage settings:** If you change storage mode or network for an existing database, the node will now fail fast to prevent opening it with incompatible settings.

### 🙏 Thank You

Thank you for trying!

---


## v0.30.0 (April 2, 2026)

**Title:** Paginated pools API and steadier automation

**Date:** April 2, 2026

**Version:** v0.30.0

Hi folks! Here’s what we shipped in v0.30.0.

### ✨ What's New

- **Paginated extended stake pool details (HTTP API):** Pool queries are easier to work with because you can now request extended stake pool data in smaller pages via `/api/v0/pools/extended`.

### 💪 Improvements

- **Dependency refresh:** Builds stay more rock-solid because we updated a key upstream dependency.
- **More predictable Antithesis runs:** Automation is easier to reason about because the Antithesis GitHub Actions workflow now runs with the expected default argument.

### 🔧 Fixes

- **No surprise automation runs:** CI noise is lower because affected workflows no longer auto-trigger unexpectedly.

### 📋 What You Need to Know

- **Pools API users:** If you use the pools API, consider switching to `/api/v0/pools/extended` to page through extended pool data.

### 🙏 Thank You

Thank you for trying!

---


## v0.29.1 (April 2, 2026)

**Title:** Safer APIs and steadier sync

**Date:** April 2, 2026

**Version:** v0.29.1

Hi folks! Here’s what we shipped in v0.29.1.

### ✨ What's New

- **Rolled out expanded transaction builders and txpump testing:** End-to-end testing is easier because you can now build more transaction types and run a randomized txpump loop.
- **Handy devnet and testnet setup tooling:** Spinning up local and testnet environments is simpler because you now have additional helper scripts, specs, and Antithesis wiring.

### 💪 Improvements

- **Safer API opt-in defaults:** Service exposure is clearer because APIs now only activate when you explicitly opt in.
- **Steadier rollback iteration:** Catch-up and restart behavior is more predictable because chain iteration is now safer under rollbacks.
- **Relaxed peer-tip validation during catch-up:** Sync is smoother because peer tip checks are less likely to reject useful peers while you’re catching up.
- **More reproducible CI and release automation:** Builds are easier to operate because CI and Antithesis automation were hardened.
- **Clearer release documentation:** What changed is easier to see because release documentation was updated.
- **Refreshed dependency set:** Compatibility stays rock-solid because a key dependency was updated.

### 🔧 Fixes

- **Iterator cleanup in ledger iteration:** Long-running processes are steadier because ledger iteration now avoids potential resource leaks.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.29.1.
- **API users:** If you rely on Dingo APIs, make sure your configuration explicitly opts in so the endpoints are enabled.

### 🙏 Thank You

Thank you for trying!

---


## v0.29.0 (March 31, 2026)

**Title:** Smoother operations and API refinements

**Date:** March 31, 2026

**Version:** v0.29.0

Hi folks! Here’s what we shipped in v0.29.0.

### ✨ What's New

- **Conway-era transaction builders and txpump:** End-to-end testing is easier because you can now generate and run newer-era transactions in automated runs.
- **Devnet and testnet helpers:** Spinning up test environments is simpler because you now have wallet-focused tests, a configurator script, local devnet helpers, a Dingo testnet spec, and Antithesis `docker-compose` wiring.
- **Storage disk-usage metrics:** Capacity planning is easier because the blob store and metadata store now export disk-usage (`DiskSize`) Prometheus gauge metrics.

### 💪 Improvements

- **Faster post-snapshot replay:** Restarts are faster because chain-sync now uses a Mithril trust-boundary slot to skip replay work already covered by a snapshot.
- **Configurable CBOR cache sizing:** Performance tuning is simpler because you can now set the CBOR cache size in configuration.
- **More predictable Badger defaults:** Deployments are more consistent because storage-mode-specific defaults only apply when values are truly unset.
- **More accurate Blockfrost responses:** Block and epoch data is more reliable because latest block, epoch, and protocol-parameter responses are now sourced from ledger state and the database.

### 🔧 Fixes

- **Safer rollbacks behind snapshots:** Rollback behavior is more rock-solid because the Mithril trust-boundary now resets when a rollback crosses it.
- **Rock-solid rollback recovery:** Recovery is smoother because “rollback point not found” now follows the same handling path as `local_tip_plateau`.
- **Rock-solid WatchTx rollbacks:** Transaction watching is more reliable because WatchTx now supports undo and rollback during chain reorganizations.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.29.0.
- **Replay and rollback near snapshots:** Testing runs may look different because chain-sync replay now respects the Mithril trust-boundary and resets it on rollback.

### 🙏 Thank You

Thank you for trying!

---


## v0.28.0 (March 30, 2026)

**Title:** Network-aware metrics and steadier block production

**Date:** March 30, 2026

**Version:** v0.28.0

Hi folks! Here's what we shipped in v0.28.0.

### ✨ What's New

- **Network-labelled Prometheus metrics and build info:** Dashboard setup is easier because every Prometheus metric now carries a `network` label automatically, and a new `dingo_build_info` gauge exposes version, commit, and Go version at a glance.
- **Tip gap and epoch gauges:** Sync monitoring is simpler because three new gauges track tip gap (wall-clock slot minus chain tip), Shelley genesis start time, and epoch length.
- **Transaction metadata label index (Blockfrost):** Metadata queries are faster because a new transaction metadata label table powers indexed lookups with deterministic pagination and rollback-safe cleanup.

### 💪 Improvements

- **Smarter chain selection near tip:** Tip following is steadier because chain selection now prefers the actually-delivered chainsync tip over the advertised remote tip and sticks with the incumbent peer when two peers report the same frontier.
- **Paginated DumpHistory (UTxO RPC):** Large history queries are more reliable because `DumpHistory` now uses a chain iterator with `next_token` pagination and sensible defaults when `max_items` is omitted.
- **Leaner TxSubmission pipeline:** CPU usage under load is lower because the unnecessary rate limiter on the pull-based TxSubmission protocol was removed, eliminating a tight retry loop that starved chainsync and blockfetch.
- **Leios protocols gated behind config:** Compatibility with non-Leios peers is more rock-solid because Leios mini-protocols are now only registered when `EnableLeios` is set, preventing muxer errors from peers that reject unknown protocols.

### 🔧 Fixes

- **Post-Mithril leader election:** Block production after a Mithril bootstrap is more reliable because `EpochNonce()` now falls through to the database when the in-memory nonce is empty, and `CaptureGenesisSnapshot()` falls back to the latest epoch start slot when slot 0 yields no pools.
- **Forged-block rollback exemption:** Fork resolution after slot battles is correct because the rollback loop detector now exempts rollbacks through slots where the node forged a block.
- **Richer UTxO RPC responses:** Script evaluation and data queries are more complete because `evalTx` now includes redeemer payloads and `readData` now returns parsed datums.

### 📋 What You Need to Know

- **Metrics dashboards:** If you run Prometheus dashboards, all metrics now include a `network` label — update your queries or selectors if you filter by metric name alone.
- **Leios users:** If you use Leios mode, make sure `EnableLeios` is set in your configuration; the protocols are no longer registered by default.
- **Default make target:** `make` no longer runs tests by default — use `make test` explicitly.

### 🙏 Thank You

Thank you for trying!

---


## v0.27.7 (March 24, 2026)

**Title:** Steadier sync and leaner storage

**Date:** March 24, 2026

**Version:** v0.27.7

Hi folks! Here’s what we shipped in v0.27.7.

### ✨ What's New

- **Configurable network ID:** Connecting to the right Cardano network is more rock-solid because you can now set the network identifier in configuration.
- **Optional blob-store compression:** Disk usage can be lower because you can now enable compression for blob storage in some environments.
- **Devnet transaction pump:** Testing transaction flow is easier because development networks now include an additional transaction pump service.

### 💪 Improvements

- **More stable chain following:** Sync is more rock-solid because chain following and recovery is now more stable during tip changes and temporary peer issues.
- **Safer rollbacks and replay:** Reorg handling is more predictable because rollback and ledger replay behavior is now safer.
- **Lighter rewind pruning:** Larger cleanups are smoother because rewind and pruning operations now put less pressure on storage backends.
- **Stickier best-peer selection:** Peer churn can be lower because peer selection now better preserves stable connections when multiple peers report equivalent tips.

### 🔧 Fixes

- **More consistent blob deletion:** Cleanup is more reliable because blob deletion now better matches transaction behavior.
- **Graceful missing-blob recovery:** Processing is more resilient because missing blob data is now handled more gracefully in some cases.

### 📋 What You Need to Know

- **Multi-network deployments:** If you run against different Cardano networks, review your config to ensure the correct network is selected.
- **Docker builds:** If you rely on custom Docker build caching, Docker builds may behave differently.
- **Tooling refreshes:** Routine updates to build tooling and libraries may land as part of keeping the project secure and compatible.

### 🙏 Thank You

Thank you for trying!

---


## v0.27.5 (March 19, 2026)

**Title:** Faster UTxO lookups and steadier sync

**Date:** March 19, 2026

**Version:** v0.27.5

Hi folks! Here’s what we shipped in v0.27.5.

### ✨ What's New

- **Stable UTxO ordering and address queries:** Wallet and explorer-style queries are more predictable because you can now query UTxOs with a stable ordering and look them up efficiently by address.
- **Observability-only chain-sync clients:** Metrics are clearer because you can now run observability-only chain-sync clients that are counted in metrics without affecting normal client operation.
- **Automatic chain realignment on startup:** Recoveries after interruptions are easier because ledger startup can now realign chain state automatically without noisy side effects.

### 💪 Improvements

- **More consistent rollback scheduling:** Recovery after chain reorganizations is more reliable because rollback handling now keeps scheduling state more consistent.
- **Sliding-window chain density:** Chain health signals are more representative because chain density calculations now use a sliding window of recent slots and blocks.
- **Safer raw block copy resume:** Resuming raw/direct block copying is safer because resume checks are now stricter and reduce accidental skipping or duplication.
- **Smoother peer governance convergence:** Early runtime is steadier because peer governance now converges faster after startup and follows configuration defaults more consistently.
- **Better default peer targets:** Peer configuration works out of the box more often because Dingo now falls back to Cardano P2P peer target values when Dingo peer targets aren’t set.
- **Richer UTxO RPC chain references:** Downstream indexing is easier because UTxO RPC responses now include a more complete reference to chain position.
- **Clearer stake snapshot errors (SQLite):** Operational debugging is easier because stake snapshot maintenance errors now include clearer context.
- **Dependency refresh:** Builds are more rock-solid because dependencies were refreshed to keep compatibility and security posture current.
- **Clearer tests and documentation:** Maintenance is easier because tests and documentation were clarified for long-term readability.

### 🔧 Fixes

- **Resilient ledger block processing:** The node is less likely to go down on transient ledger errors because block processing now restarts on non-fatal errors instead of exiting.
- **Safer block fetch flushing:** Block downloads recover more cleanly after flush failures because block fetch now cleans up state when flushing pending blocks fails.

### 📋 What You Need to Know

- **Client-count metrics may shift:** Capacity planning may look different because observability-only chain-sync clients are now tracked separately from eligible clients.

### 🙏 Thank You

Thank you for trying!

---


## v0.27.4 (March 18, 2026)

**Title:** Reliability and usability refinements

**Date:** March 18, 2026

**Version:** v0.27.4

Hi folks! Here’s what we shipped in v0.27.4.

### ✨ What's New

- **Rock-solid chain-sync recovery:** Sync stays more rock-solid because chain-sync recovers more reliably from stalled or unstable network connections.
- **Solid chain selection:** Sync wastes less time on poor candidates because chain selection now takes peer suitability into account.

### 💪 Improvements

- **Simpler config loading:** Configuration stays under your control because config loading no longer fills in a default Cardano configuration path.
- **Sleeker block downloads:** Block downloads are more consistent because header processing no longer gets blocked by duplicate headers.
- **More robust intersections:** Synchronization starts from the right place more often because intersection point selection is now more robust.

### 🔧 Fixes

- **No missing boundary blocks:** Historical imports are more complete because older-era boundary blocks are no longer skipped during block loading.

### 📋 What You Need to Know

- **Inbound peers excluded from chain choice:** Chain selection behavior may change in mixed inbound/outbound topologies because inbound peers no longer influence which chain is selected.
- **Documentation-only change:** This release includes a release-notes update with no runtime impact.

### 🙏 Thank You

Thank you for trying!

---

## v0.27.3 (March 17, 2026)

**Title:** Safer rollbacks and steadier leader election

**Date:** March 17, 2026

**Version:** v0.27.3

Hi folks! Here’s what we shipped in v0.27.3.

### ✨ What's New

- **Follow-tip reset and rollback:** Tip tracking is easier to manage because the follow-tip API now supports safe reset and rollback with clearer metadata about what changed.
- **Leader election readiness:** Block producer readiness is easier to track because leader election now surfaces epoch-nonce readiness and carries schedule state more reliably across restarts.

### 💪 Improvements

- **More consistent cache sizing:** Sizing runs are easier to tune because cache defaults are more consistent and the BP/PI sizing script now supports explicit memory limits and optional cache overrides.
- **Predictable KES period semantics:** Operational certificate (KES) periods are more predictable because KES endpoints now standardize on absolute periods while translating internally from the certificate start period.
- **Preserved original block bytes:** Downstream tooling can retain exact block bytes because API block objects now include the original encoded bytes.
- **Standard network identifiers from genesis:** Network identification is simpler because genesis reads now return a standard CAIP-2 network identifier derived from network magic.
- **Smoother peer switching:** Sync stays more rock-solid because chain-sync now preserves its state while only swapping the active connection during a peer switch.
- **More consistent Mithril imports:** Mithril snapshot imports are more consistent across epochs because imports now normalize snapshot types and centralize persistence and epoch-summary handling.
- **Protocol dependency validation:** Modern-era transaction handling is more reliable because protocol dependencies were updated and regression tests now guard transaction size behavior.
- **Safer default containers:** Default containers are safer because the main Docker image now runs as a non-root `dingo` user.

### 🔧 Fixes

- **Resilient background monitoring:** Long-running monitoring is more rock-solid because the stall checker now recovers from panics instead of crashing its background loop.
- **Robust block fetch batching:** Block fetch serving is more robust because batching now handles iterator errors and connection closes correctly.
- **Clearer unexpected event handling:** Event processing is easier to debug because chain-sync and block fetch now log unexpected event payload types instead of failing silently.
- **Reliable shutdown error reporting:** Shutdowns are easier to troubleshoot because node and metrics server shutdown now propagates errors instead of exiting abruptly.
- **Clear tx-submission failures:** Transaction submission fails more clearly because the tx-submission handlers now return an explicit error when no mempool consumer exists.

### 📋 What You Need to Know

- **Docker volume permissions:** If you run Dingo in Docker with mounted volumes, make sure the data directory is writable by the `dingo` user inside the container.
- **API integrations:** If you integrate with follow-tip or KES APIs, give your client code a quick check for the updated reset/rollback and period semantics.

### 🙏 Thank You

Thank you for trying!

---

## v0.27.2 (March 16, 2026)

**Title:** Snapshot events and safer services

**Date:** March 16, 2026

**Version:** v0.27.2

Hi folks! Here’s what we shipped in v0.27.2.

### ✨ What's New

- **Snapshot event publishing and clean shutdown:** Relay operation is more reliable because the relay now publishes events from snapshots and shuts down cleanly without dropping in-flight work.

### 💪 Improvements

- **HTTP timeouts for public APIs:** Network-facing services are more resilient under slow or stalled connections thanks to new write/read/idle timeouts on the Bark, Blockfrost, and UTxO RPC HTTP servers.
- **Streamlined peer and connection management:** Peer and connection management is faster and uses fewer resources on constrained machines thanks to quicker inbound host lookups, tighter Badger cache defaults, and expanded benchmarks and sizing guidance.
- **More consistent key-period handling:** Block production key period handling is more consistent across configurations thanks to improved key-period calculations with added validation and tests.
- **Race detection in CI:** Test runs catch concurrency bugs earlier because the Linux test job now runs with the Go race detector enabled.

### 🔧 Fixes

- **Safer concurrent chain reads:** Reads are more consistent under load because primary chain and protocol-parameter access are now protected with read locks.
- **Validated epoch nonce reuse:** Nonce reuse is safer because cached epoch nonce entries are now validated against the nonce provided for the current run before reuse.
- **Graceful invalid hash handling:** Malformed block metadata no longer crashes encoding because previous-hash length issues now return errors instead of panicking.
- **SQLite VACUUM actually runs:** Database maintenance now completes as intended because SQLite VACUUM is now executed rather than only prepared.

### 📋 What You Need to Know

- **Release notes alignment:** Release documentation was updated to reflect the final set of changes, including a detailed v0.27.1 section in `RELEASE_NOTES.md`.
- **Build provenance updates:** Supply-chain attestations are easier to verify because build provenance now uses `actions/attest` and updated Docker Hub image subjects.

### 🙏 Thank You

Thank you for trying!

---

## v0.27.1 (March 16, 2026)

**Title:** Smoother reconnects and safer chain-sync

**Date:** March 16, 2026

**Version:** v0.27.1

Hi folks! Here’s what we shipped in v0.27.1.

### ✨ What's New

- **Better chain-sync intersection:** Resuming sync is easier and faster because chain-sync uses a denser, wider set of intersect points to improve `ChainSync` intersection behavior.

### 💪 Improvements

- **Inbound connection reuse and `TxSubmission`:** Networking is more rock-solid on reconnect because peer reuse and governance now normalize exact peer addresses, require client-capable connections for reuse, and start `TxSubmission` on duplex inbound connections.
- **Stake snapshot and epoch summary upserts:** Data storage is more consistent across supported databases because write paths now upsert across DB backends and report errors more clearly.
- **More robust delegation parsing:** Delegation reads are more reliable because parsing now handles multiple account encodings with expanded tests.

### 🔧 Fixes

- **Dependency refresh:** Upgrades are less error-prone because Go modules were refreshed (including AWS SDK v2/S3, `golang.org/x/*`, `plutigo` v0.0.27, `go-ethereum` v1.17.1, and `google.golang.org/api` v0.271.0).

### 📋 What You Need to Know

- **Go module sync (some builds):** You’re all set for most setups, but if you vendor dependencies or run reproducible builds you may need to re-sync Go modules (update `go.mod`/`go.sum`) to pick up refreshed versions.

### 🙏 Thank You

Thank you for trying!

---

## v0.27.0 (March 15, 2026)

**Title:** S3-backed CI tests and embedded network configs

**Date:** March 15, 2026

**Version:** v0.27.0

Hi folks! Here’s what we shipped in v0.27.0.

### ✨ What's New

- **S3-backed CI storage tests:** Storage testing is more rock-solid because CI now spins up a MinIO S3-compatible service and runs coverage across all supported storage backends, including S3.
- **Embedded network config bundles:** Getting started is easier because preview, preprod, mainnet, and devnet network configs are now embedded and loaded via `EmbeddedConfigFS`.

### 💪 Improvements

- **Snapshot epoch transitions:** Snapshot handling is more reliable because the snapshot manager now processes every epoch transition event during rapid chain progress.
- **Tip ingestion fast paths:** Sync near the chain tip is sleeker because blockfetch and block insertion reuse queued header data and caller-supplied points to cut redundant work.

### 🔧 Fixes

- **No fixes:** No user-facing fixes shipped in this release.

### 📋 What You Need to Know

- **Compact block metadata format (Badger):** Disk usage can be smaller because you can opt into an optional Badger setting that stores block metadata in a compact binary format.

### 🙏 Thank You

Thank you for trying!

---

## v0.26.0 (March 14, 2026)

**Title:** Trusted Mithril downloads and tuned peers

**Date:** March 14, 2026

**Version:** v0.26.0

Hi folks! Here’s what we shipped in v0.26.0.

### ✨ What's New

- **End-to-end Mithril verification:** Mithril downloads are more rock-solid because Dingo now verifies the full certificate chain and signatures using genesis keys from your Cardano configuration.

### 💪 Improvements

- **Peer governor tuning:** Peer connectivity is easier to tune because you can now adjust peer-governor settings, including hot-peer promotion behavior and its defaults.

### 🔧 Fixes

- **No fixes:** No user-facing fixes shipped in this release.

### 📋 What You Need to Know

- **Release notes:** Release notes are easier to scan because `RELEASE_NOTES.md` now includes an entry for v0.25.1.

### 🙏 Thank You

Thank you for trying!

---

## v0.25.1 (March 13, 2026)

**Title:** Configurable storage and smoother sync

**Date:** March 13, 2026

**Version:** v0.25.1

Hi folks! Here’s what we shipped in v0.25.1.

### ✨ What's New

- **Configurable data directory:** Running in different environments is easier because you can now choose where the node stores its data on disk with `--data-dir`.
- **Built-in Mithril bootstrap:** Containerized bootstrapping is simpler because Dingo can now spin up Mithril sync without an external client.
- **Expanded architecture docs:** Understanding the system is easier because `ARCHITECTURE.md` now includes clearer diagrams and explanations.

### 💪 Improvements

- **Idle connection stability:** Long-running connections are more rock-solid because idle sessions are less likely to be dropped unexpectedly.
- **Resilient sync across reconnections:** Sync is more rock-solid because chainsync and blockfetch handle connection switches more reliably.
- **Blockfetch overhead:** Observability is sleeker because blockfetch avoids extra work when no one is listening for events.
- **Dependencies:** Builds are more solid because dependencies were refreshed for consistency.

### 🔧 Fixes

- **Release notes alignment:** Tracking changes is easier because release documentation now matches shipped content.

### 📋 What You Need to Know

- **Upgrade:** You’re all set—no required configuration changes for this release.
- **Custom storage path:** If you want to store node data somewhere else, pass `--data-dir <path>` at startup.

### 🙏 Thank You

Thank you for trying!

---

## v0.25.0 (March 12, 2026)

**Title:** Trusted replay verification and peer counts

**Date:** March 12, 2026

**Version:** v0.25.0

Hi folks! Here’s what we shipped in v0.25.0.

### ✨ What's New

- **Trusted chain verification:** Startup and recovery runs are more rock-solid because your node can now verify the chain from a trusted, immutable replay path.

### 💪 Improvements

- **Peer metrics accuracy:** Peer limits and metrics are more accurate because local client connections are now tracked separately.

### 🔧 Fixes

- **Mithril key defaults (Docker):** Docker deployments are less error-prone because the correct Mithril genesis verification key is now picked up automatically when you don’t provide one.

### 📋 What You Need to Know

- **Peer counts:** If you rely on peer-count metrics or peer-limit tuning, expect local client connections to no longer be included in those counts.

### 🙏 Thank You

Thank you for trying!

---

## v0.24.1 (March 12, 2026)

**Title:** Stability and polish

**Date:** March 12, 2026

**Version:** v0.24.1

Hi folks! Here’s what we shipped in v0.24.1.

### 💪 Improvements

- **Deeper rollback scanning:** Sync is more rock-solid because deep rollbacks are only enabled when needed, and block fetch can scan a wider window to find the data it needs.

### 🔧 Fixes

- **Shutdown reliability:** Shutdowns are more rock-solid because node and ledger shutdown paths are less likely to hang, and logs now show how long shutdown took.
- **Epoch cache consistency checks:** Diagnosing cache issues is easier because epoch cache validation is stricter and catches inconsistencies earlier.

### 📋 What You Need to Know

- **Release notes:** Release notes are easier to scan because v0.24.0 notes are now included in `RELEASE_NOTES.md`.

### 🙏 Thank You

Thank you for trying!

---

## v0.24.0 (March 11, 2026)

**Title:** Peer sharing controls and import visibility

**Date:** March 11, 2026

**Version:** v0.24.0

Hi folks! Here’s what we shipped in v0.24.0.

### ✨ What's New

- **Peer sharing controls:** Networking is easier to lock down because you can now explicitly enable or disable peer sharing.
- **CLI help text:** Running the tool is easier because the root command now includes clearer Short and Long help text.
- **Import progress reporting:** Large data setup runs feel less like a black box because Mithril and ledger/UTxO imports now emit rate-limited progress updates.
- **Benchmark suite:** Performance testing is more consistent because the benchmark tooling now includes a baseline and detailed ingestion benchmarks.

### 💪 Improvements

- **Docs:** Evaluating Dingo is safer because the README now includes expanded usage, deployment, and DevNet guidance plus a clear “not for production” warning.
- **Dev mode:** Local runs are less surprising because dev mode now forces storage mode to API and logs when it overrides your configured value.
- **Connection manager:** Peer connectivity is more rock-solid because inbound connections are treated as the bidirectional link and expected transition timeouts are handled without tearing down peers.
- **Storage tuning and logging:** Troubleshooting storage is easier because cache defaults were reset and configured cache sizes are now logged on open, with additional Badger and SQLite tuning.
- **Era-aware validation:** Cross-era validation is more accurate because protocol parameter extraction is now era-aware and governance-state decoding uses raw-element parsing.
- **Overlay-aware acceptance:** Transaction validity is more consistent because ledger and mempool validation now uses a temporary UTxO overlay with descendant pruning.
- **CI and publishing:** Releases are more repeatable because CI and publishing workflows were refreshed with Node.js 24.x and newer pinned Docker and GitHub Actions.
- **Dependencies:** Compatibility is better because key dependencies were updated, including OpenTelemetry Go and gouroboros.
- **Go toolchain:** Builds are more up to date because the minimum Go version is now 1.25 and CI was updated to match.
- **Release notes:** Scanning changes is easier because release notes were expanded for recent versions.

### 🔧 Fixes

- **Connection cleanup tests:** Network tests are less flaky because keepalive timeouts are deterministic and connection cleanup is safer.
- **Block caching and reconciliation:** Sync is more reliable because block caching now uses fixed-size typed hash keys and reconciliation compares hashes consistently.

### 📋 What You Need to Know

- **Go version:** If you build from source, you’ll need Go 1.25 or newer.
- **Dev mode:** If you run dev mode, Dingo will force storage mode to the API option.

### 🙏 Thank You

Thank you for trying!

---

## v0.23.1 (March 11, 2026)

**Title:** Clearer release notes and rock-solid Docker publishing

**Date:** March 11, 2026

**Version:** v0.23.1

Hi folks! Here’s what we shipped in v0.23.1.

### ✨ What's New

- **Release notes:** Release notes are easier to scan because `RELEASE_NOTES.md` now includes a complete set of notes for v0.23.0.

### 💪 Improvements

- **Docker publishing (Antithesis image):** Publishing is more consistent because the build pipeline now builds, tags, pushes, and generates an attestation for a Linux amd64-only Antithesis Docker image variant.

### 🔧 Fixes

- **No fixes:** No user-facing fixes shipped in this patch.

### 📋 What You Need to Know

- **Upgrade:** You’re all set—no required configuration changes for this release.

### 🙏 Thank You

Thank you for trying!

---

## v0.23.0 (March 10, 2026)

**Title:** Overlay-aware validation and a smoother dev mode

**Date:** March 10, 2026

**Version:** v0.23.0

Hi folks! Here’s what we shipped in v0.23.0.

### ✨ What's New

- **Transaction validation:** Transaction validation is more accurate because ledger and mempool checks now account for pending, in-flight changes with a temporary UTxO overlay.

### 💪 Improvements

- **Docs:** Setup is easier because `README.md` now includes expanded usage, deployment, and DevNet guidance plus a clear “not for production” warning.
- **Publish workflow:** Publishing is more rock-solid because the release workflow now targets Node.js `24.x` and pins key GitHub Actions versions.
- **Release notes:** Release notes are easier to scan because `RELEASE_NOTES.md` now includes v0.22.1 and tightens up transaction validation wording.

### 🔧 Fixes

- **Connection cleanup:** Connection-related tests are less flaky because keepalive timeouts are deterministic and connection cleanup is safer.

### 📋 What You Need to Know

- **Dev mode:** If you run dev mode, Dingo will automatically switch the storage mode to API, so check logs if you expected a different mode.

### 🙏 Thank You

Thank you for trying!

---

## v0.22.1 (March 8, 2026)

**Title:** Stability updates and polish

**Date:** March 8, 2026

**Version:** v0.22.1

Hi folks! Here’s what we shipped in v0.22.1.

### ✨ What's New

- **Release notes:** We added v0.22.0 release notes to `RELEASE_NOTES.md` so you can scan changes in one place.

### 💪 Improvements

- **Transaction validation:** Transaction validation is more consistent because Conway UTxO validation now runs even when a transaction is marked invalid, while script evaluation is still skipped.
- **Epoch processing:** Epoch processing recovers more gracefully because nonce recomputation falls back to recomputing from epoch start when an anchor block nonce is missing.
- **Queue handling:** Queue handling is more solid under load because the main event queue size increased from 1,000 to 10,000 and the header queue size is now clamped to at least the default.
- **Implausible-tip checks:** Implausible-tip checks are safer across edge cases because the logic now uses peer-based reference blocks with overflow-safe arithmetic.
- **Publish workflow login:** Publishing is more rock-solid because the publish workflow was tweaked to log in using `docker/login-action@v4`.
- **Publish workflow runtime:** Automation stays more up to date because Node.js `24.x` was rolled out for the publish workflow.

### 🔧 Fixes

- **Epoch cache rollbacks:** Epoch cache handling is safer during concurrent rollbacks because `advanceEpochCache` now guards against empty caches and validates the tail before appending a new epoch.
- **Test timing:** Tests are less flaky on slower machines because `TestSchedulerRunFailFunc` timing parameters were relaxed.

### 📋 What You Need to Know

- **API bind address:** Config validation no longer defaults the API bind address to `0.0.0.0`, so set it explicitly if you need it.
- **CI and publishing scripts:** If you maintain custom publishing or CI scripts, give them a quick check for compatibility with Node.js `24.x` and `docker/login-action@v4`.

### 🙏 Thank You

Thank you for trying!

---

## v0.22.0 (March 7, 2026)

**Title:** Mithril bootstrap, built-in APIs, and block production

**Date:** March 7, 2026

**Version:** v0.22.0

Hi folks! Here’s what we shipped in v0.22.0.

### ✨ What's New

- **Mithril bootstrap:** Node operators can now bootstrap a Dingo node from a Mithril snapshot and have the ledger state imported automatically (see “Fast Bootstrapping with Mithril” in `README.md`).
- **Built-in HTTP APIs:** You can run Dingo with built-in, configurable HTTP APIs for common ecosystem compatibility.
- **Block production:** Block production is now supported with Praos leader election and keystore-backed key management.
- **Lifecycle events:** The node now emits richer on-chain lifecycle events that applications can subscribe to.
- **Conway governance metadata:** Governance and Conway-era features are now available in the on-chain metadata pipeline.
- **Leios mode:** A new “leios” mode is available for early experimentation with Leios protocols.
- **Stake snapshots:** Stake snapshots and stake distribution are now available with persistence and querying.

### 💪 Improvements

- **Faster catch-up validation:** Syncing is now faster and safer by reducing unnecessary validation during initial catch-up.
- **Tiered storage and caching:** Block and transaction storage can now be tuned for performance and cost with tiered storage and caching options.
- **Rollback and resync:** Rollback and resync behavior is more robust under forks and stalled peers.
- **Peer management:** Network and peer management now scales more predictably under load.
- **Mempool resilience:** Transaction processing is more resilient and configurable under pressure.
- **Observability:** Observability has been expanded across sync, forging, and storage.
- **Time and nonce handling:** Epoch, slot, and nonce handling better matches Cardano semantics and edge cases.
- **Docs:** Developer and operator documentation has been significantly expanded.

### 🔧 Fixes

- **Chainsync stability:** Chainsync is more reliable around header/block fetching and coordination.
- **Concurrency safety:** Several concurrency and event-delivery deadlocks and goroutine leak risks have been eliminated.
- **Security hardening:** Security hardening has been added for configuration, filesystem usage, and key material.
- **Protocol validation:** Cryptographic and protocol-validation correctness has been tightened across eras.
- **Object-store key encoding:** Storage key encoding issues in object stores have been resolved.

### 📋 What You Need to Know

- **Event consumers:** If you rely on event type strings, update consumers to match the renamed event type strings.
- **API storage backfill:** If you enable API storage or new tiered storage modes, run a metadata backfill so queries return complete results.
- **Forging setup:** If you enable forging, double-check VRF/KES/OpCert key paths and permissions first.

### 🙏 Thank You

Thank you for trying!

---