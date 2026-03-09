# Release Notes

## v0.22.1 (March 8, 2026)

**Title:** Stability updates and polish

**Date:** March 8, 2026

**Version:** v0.22.1

Hi folks! Here’s what we shipped in v0.22.1.

### ✨ What's New

- **Docs refresh:** The documentation now makes it easier to understand how to use and deploy the project, and it clearly warns against using it in production environments.

### 💪 Improvements

- **Event throughput:** The system can now handle higher event throughput with fewer slowdowns under load.
- **Queue sizing:** Queue sizing is now more consistent so performance doesn’t degrade due to undersized configuration defaults.
- **Tip plausibility checks:** Implausible-tip detection is now more solid and safer around edge-case arithmetic.
- **Test stability:** Automated tests are now less flaky on slower machines and CI runners.

### 🔧 Fixes

- **Epoch processing:** Epoch processing is now more resilient so it can recover rather than stopping when some reference data is missing.
- **Rollback safety:** Concurrent rollbacks are now less likely to crash the process.
- **Epoch cache safety:** Epoch cache updates are now safer under concurrency to reduce the risk of corrupted state.
- **Transaction validation:** Transaction validation is now more consistent so invalid transactions still get the expected ledger checks.

### 📋 What You Need to Know

- **API bind address:** You may need to explicitly set the API bind address in your configuration instead of relying on an implicit default.
- **Release tooling:** Release packaging and CI builds are now using updated, pinned tooling for better supply-chain safety and reproducibility.
- **Release notes:** Release documentation has been updated so you can review what changed in the latest versions.

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