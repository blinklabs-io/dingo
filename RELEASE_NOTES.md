# Release Notes

## v0.22.1 (March 8, 2026)

**Title:** Stability updates and polish

**Date:** March 8, 2026

**Version:** v0.22.1

Hi folks! Here’s what we shipped in v0.22.1.

<!-- BEGIN GENERATED RELEASE NOTES (v0.22.1) -->

```json
{
  "✨ What's New": [
    "Documentation now includes clearer, more complete guidance for getting started and using the project safely. The README was expanded with detailed usage, deployment, and DevNet documentation and adds a prominent non-production warning.",
    "Release documentation is now available for the latest patch version so you can see what changed at a glance. RELEASE_NOTES.md now includes a dedicated section documenting v0.22.1."
  ],
  "💪 Improvements": [
    "The system can now continue operating in more cases where historical data is incomplete, reducing unnecessary failures. When an anchor block nonce is missing, it will recompute the epoch nonce from the epoch start instead of failing.",
    "The system can now handle higher throughput with less risk of backpressure during busy periods. The main event queue size constant was increased from 1,000 to 10,000.",
    "Queue sizing is now more robust so deployments don’t accidentally run with undersized buffers. The header queue size is now enforced to be at least the default even when derived from the security parameter, and the related tests were updated.",
    "Build and release automation is now more consistent and up to date, reducing maintenance risk in CI. GitHub Actions workflows were updated to newer docker actions (metadata-action v6 pinned by SHA, build-push-action v7, setup-buildx-action v4) and actions/setup-node was updated to v6.3.0.",
    "Dependencies are now slightly more current to pick up upstream fixes and improvements. The aws-sdk-go-v2 core dependency was updated from 1.41.2 to 1.41.3.",
    "Automated tests should be less flaky on slower machines and CI runners. The timing parameters in TestSchedulerRunFailFunc were relaxed to be more tolerant of slow execution.",
    "Transaction validation is now more consistent so results are easier to reason about across scenarios. Conway UTxO validation now runs even for transactions marked invalid, and only script evaluation is skipped for those transactions.",
    "Validation is now stricter about configuration mistakes so deployments don’t silently bind to unexpected interfaces. Config validation no longer automatically defaults the API bind address to 0.0.0.0.",
    "The release documentation has been refreshed to reflect the current state of the project. RELEASE_NOTES.md was updated with the v0.22.0 release notes."
  ],
  "🔧 Fixes": [
    "The system is less likely to crash during edge cases involving concurrent state changes. advanceEpochCache now includes an empty-epoch-cache guard to avoid panics during concurrent rollbacks.",
    "Epoch cache updates are now safer under concurrency, reducing the chance of subtle state corruption. A concurrency-safe validation of the epoch cache tail was added before appending a new epoch.",
    "Checks related to implausible tips are now more reliable and less prone to arithmetic edge cases. The implausible-tip logic was updated to use peer-based reference blocks with overflow-safe arithmetic, and the tests were updated accordingly."
  ]
}

```

<!-- END GENERATED RELEASE NOTES (v0.22.1) -->

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