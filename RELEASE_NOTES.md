# Release Notes

## v0.22.1 (March 8, 2026)

**Title:** Stability updates and polish

**Date:** March 8, 2026

**Version:** v0.22.1

Quick update: here’s what we rolled out in v0.22.1.

### ✨ What's New

```json
{
  "✨ What's New": [
    "Release notes for version 0.22.0 were added so you can see a clear summary of changes in one place. The v0.22.0 notes were appended to `RELEASE_NOTES.md` based on the existing knowledge base entry."
  ],
  "💪 Improvements": [
    "Transaction validation is now more consistent so that invalid transactions are still checked for important correctness rules. Conway UTxO validation now runs even when a transaction is marked invalid, and only script evaluation is skipped for those transactions.",
    "The system can recover more gracefully when historical anchor information is missing during epoch processing. Epoch nonce recomputation now falls back to recomputing from epoch start when an anchor block nonce is not found, instead of failing.",
    "Queue handling is more resilient under high load to reduce backpressure and dropped work. The main event queue size constant was increased from 1,000 to 10,000, and the header queue size is now clamped to at least the default even when derived from the security parameter.",
    "Arithmetic and reference handling in implausible-tip checks is now safer and more robust across edge cases. The implausible-tip logic was updated to use peer-based reference blocks with overflow-safe arithmetic, and the test suite was updated to match."
  ],
  "📋 What You Need to Know": [
    "Configuration validation will no longer silently substitute a default API bind address, which may require you to set it explicitly. The automatic default of `0.0.0.0` for the API bind address was removed during config validation, so missing values will remain unset rather than being auto-filled."
  ],
  "🔧 Fixes": [
    "Epoch cache operations are safer during concurrent rollback scenarios to avoid crashes. An empty-epoch-cache guard was added in `advanceEpochCache`, and a concurrency-safe validation of the epoch-cache tail was added before appending a new epoch.",
    "Test timing is less flaky on slower machines and CI runners. `TestSchedulerRunFailFunc` timing parameters were relaxed to be more tolerant of slower execution."
  ]
}

```

### 💪 Improvements

### 🔧 Fixes

### 📋 What You Need to Know

### 🙏 Thank You

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