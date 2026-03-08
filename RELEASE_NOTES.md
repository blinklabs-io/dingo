# Release Notes

## v0.22.1 (March 8, 2026)

**Title:** v0.22.1 patch release

**Date:** March 8, 2026

**Version:** v0.22.1

Quick update: Here’s what we rolled out in v0.22.1.

### ✨ What's New

- Notes to be categorized and formatted from the source update below.

```json
{
  "💪 Improvements": [
    "The system can now recover more gracefully when a required reference value is missing, instead of stopping the process. Specifically, epoch nonce recomputation now falls back to recomputing from the epoch start when an anchor block nonce is unavailable.",
    "The platform can now handle heavier workloads without falling behind as quickly. Specifically, the main event queue size constant was increased from 1,000 to 10,000 to reduce backpressure under bursty conditions.",
    "The system now keeps a safer minimum capacity for queued header work to avoid slowdowns during high activity. Specifically, the header queue size is now guaranteed to be at least the default even when it is derived from the security parameter, and the related tests were updated.",
    "Transaction checks now run more consistently so users get the expected validation behavior even when a transaction is flagged as invalid. Specifically, Conway UTxO validation now still executes for invalid-marked transactions, while only script evaluation is skipped for those transactions."
  ],
  "🔧 Fixes": [
    "Tip-validation is now more reliable and less likely to behave incorrectly in edge-case block layouts. Specifically, the implausible-tip checks were updated to use peer-based reference blocks with overflow-safe arithmetic, and the associated tests were adjusted.",
    "Automated tests are now less flaky on slower machines and more consistent across environments. Specifically, the timing parameters in TestSchedulerRunFailFunc were relaxed to better tolerate slower runners.",
    "Rollback-related concurrency is now safer and less likely to crash under rare timing conditions. Specifically, advanceEpochCache now guards against an empty epoch cache to prevent panics during concurrent rollbacks.",
    "Epoch cache updates are now safer under concurrent activity and less likely to corrupt state. Specifically, a concurrency-safe validation of the epoch cache tail was added before appending a new epoch.",
    "Configuration validation now avoids silently picking a network bind setting that may not match what you intended. Specifically, the automatic default of 0.0.0.0 for the API bind address was removed during config validation."
  ]
}

```

### 💪 Improvements

- Notes to be categorized and formatted from the source update above.

### 🔧 Fixes

- Notes to be categorized and formatted from the source update above.

### 📋 What You Need to Know

- Notes to be categorized and formatted from the source update above.

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