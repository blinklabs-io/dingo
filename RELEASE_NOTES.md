# Release Notes

## v0.22.1 (March 8, 2026)

{
  "💪 Improvements": [
    "Transaction validation is now more consistent, even when a transaction is ultimately rejected. Specifically, Conway UTxO validation now runs for transactions marked invalid, and only the script evaluation phase is skipped for those transactions.",
    "Chain synchronization now has a more resilient fallback when expected data is missing. Specifically, the epoch nonce can be recomputed from the epoch start when an anchor block nonce cannot be found, instead of failing.",
    "Event processing can now handle larger bursts of work without backing up as easily. Specifically, the main event queue size constant was increased from 1,000 to 10,000 entries.",
    "Queue sizing is now more predictable in security-parameter-driven configurations. Specifically, the header queue size is clamped so it is at least the default value even when derived from the security parameter, and the related tests were updated.",
    "Consensus and chain-quality checks are now safer and more accurate under edge conditions. Specifically, the implausible-tip checks were updated to use peer-based reference blocks with overflow-safe arithmetic, and the tests were adjusted accordingly.",
    "The API configuration validation now avoids silently choosing a potentially unsafe default. Specifically, config validation no longer automatically defaults the API bind address to 0.0.0.0."
  ],
  "🔧 Fixes": [
    "Epoch cache handling is now safer during concurrent state changes. Specifically, an empty-epoch-cache guard was added to `advanceEpochCache` to prevent panics during concurrent rollbacks.",
    "Epoch cache updates are now more robust under concurrency. Specifically, the epoch cache tail is now validated in a concurrency-safe way before appending a new epoch.",
    "Test runs are now less flaky on slower machines and CI environments. Specifically, the timing parameters in `TestSchedulerRunFailFunc` were relaxed to better tolerate slower runners."
  ]
}


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