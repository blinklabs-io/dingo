# Release Notes

## v0.22.1 (March 8, 2026)

**Title:** Stability updates and polish

**Date:** March 8, 2026

**Version:** v0.22.1

Hi folks! Here’s what we shipped in v0.22.1.

### ✨ What's New

- **README updates:** Getting started is easier because we beefed up `README.md` with usage, deployment, and DevNet documentation and a clear warning that the project is not intended for production use.
- **Release notes:** Upgrades are easier to track because `RELEASE_NOTES.md` now includes updated entries for v0.22.0 and v0.22.1.

### 💪 Improvements

- **Main event queue:** Performance under load is more rock-solid because the main event queue size increased from 1,000 to 10,000.
- **Header queue sizing:** Header processing is more predictable because we refined header queue sizing to clamp to at least the default when derived from the security parameter.
- **TestSchedulerRunFailFunc:** CI runs are less flaky because we tweaked `TestSchedulerRunFailFunc` timing parameters to be more tolerant.
- **Implausible-tip checks:** Implausible-tip detection is safer because checks now use peer-based reference blocks with overflow-safe arithmetic.
- **CI workflows:** Builds are more reproducible because we rolled out updated GitHub Actions Docker actions (pinned by SHA) and `actions/setup-node` v6.3.0.
- **AWS SDK:** AWS integrations are more up to date because `aws-sdk-go-v2` was bumped from 1.41.2 to 1.41.3.

### 🔧 Fixes

- **Epoch cache rollbacks:** Concurrent rollbacks are less likely to panic because `advanceEpochCache` now guards against empty epoch caches.
- **Epoch nonce recomputation:** Epoch processing is more resilient because nonce recomputation can fall back to recomputing from epoch start when an anchor block nonce is missing.
- **Conway UTxO validation:** Validation results are more consistent because Conway UTxO validation now runs even when a transaction is marked invalid (with script evaluation still skipped).
- **Epoch cache tail validation:** Epoch cache updates are safer because the epoch cache tail is validated before appending a new epoch.

### 📋 What You Need to Know

- **API bind address:** If you need the API to bind to all interfaces, set it explicitly because config validation no longer defaults the bind address to `0.0.0.0`.

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