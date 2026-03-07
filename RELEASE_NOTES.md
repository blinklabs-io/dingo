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
    "Node operators can now bootstrap a Dingo node from a Mithril snapshot and have the ledger state imported automatically.",
    "You can run Dingo with built-in, configurable HTTP APIs for common ecosystem compatibility.",
    "Block production is now supported with production-grade leader election and key management.",
    "The node now emits richer on-chain lifecycle events that applications can subscribe to.",
    "Governance and Conway-era features are now available in the on-chain metadata pipeline.",
    "A new “leios” mode is available for early experimentation with Leios protocols.",
    "Stake snapshots and stake distribution are now first-class features with robust persistence and querying."
  ],
  "💪 Improvements": [
    "Syncing is now faster and safer by reducing unnecessary validation during initial catch-up.",
    "Block and transaction storage can now be tuned for performance and cost with tiered storage and caching options.",
    "Rollback and resync behavior is more robust under forks and stalled peers.",
    "Network and peer management now scales more predictably under load.",
    "Transaction processing is more resilient and configurable under pressure.",
    "Observability has been expanded across sync, forging, and storage.",
    "Epoch, slot, and nonce handling better matches Cardano semantics and edge cases.",
    "Developer and operator documentation has been significantly expanded."
  ],
  "📋 What You Need to Know": [
    "If you rely on event type strings, you may need to update consumers due to naming changes.",
    "If you enable API storage or new tiered storage modes, you may want to run or schedule metadata backfill.",
    "If you deploy forging in production, verify key file paths and permissions before enabling it."
  ],
  "🔧 Fixes": [
    "Sync stability issues around header/block fetching and chainsync coordination have been addressed.",
    "Several concurrency and event-delivery deadlocks and goroutine leak risks have been eliminated.",
    "Security hardening has been added for configuration, filesystem usage, and key material.",
    "Cryptographic and protocol-validation correctness has been tightened across eras.",
    "Storage key encoding issues in object stores have been resolved."
  ]
}

```