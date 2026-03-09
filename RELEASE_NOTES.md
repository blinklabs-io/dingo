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
    "Documentation now includes clearer, more complete guidance for getting started and using the project safely, with an expanded README covering usage, deployment, DevNet documentation, and a prominent non-production warning.",
    "Release documentation is now easier to scan because RELEASE_NOTES.md now includes a dedicated section documenting v0.22.1."
  ],
  "💪 Improvements": [
    "Epoch processing recovers more gracefully by recomputing the epoch nonce from epoch start when an anchor block nonce is missing.",
    "The node handles higher throughput by increasing the main event queue size constant from 1,000 to 10,000.",
    "Queue sizing is more robust because the header queue size is clamped to at least the default even when derived from the security parameter.",
    "CI automation is more consistent with updated GitHub Actions docker workflows (metadata-action v6 pinned by SHA, build-push-action v7, setup-buildx-action v4) and actions/setup-node v6.3.0.",
    "Dependencies are more current with aws-sdk-go-v2 updated from 1.41.2 to 1.41.3.",
    "Automated tests are less flaky because TestSchedulerRunFailFunc timing parameters are now more tolerant of slow execution.",
    "Transaction validation is more consistent because Conway UTxO validation now runs even for transactions marked invalid while script evaluation is still skipped.",
    "Config validation is stricter because it no longer defaults the API bind address to 0.0.0.0.",
    "Release notes are easier to scan because RELEASE_NOTES.md now includes the v0.22.0 section."
  ],
  "🔧 Fixes": [
    "Epoch cache handling is safer during concurrent rollbacks because advanceEpochCache now guards against empty caches.",
    "Epoch cache updates are safer under concurrency because the epoch cache tail is validated before appending a new epoch.",
    "Implausible-tip checks are more reliable because the logic now uses peer-based reference blocks with overflow-safe arithmetic."
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