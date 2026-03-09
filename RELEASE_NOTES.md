# Release Notes

## v0.22.1 (March 8, 2026)

**Title:** Stability updates and polish

**Date:** March 8, 2026

**Version:** v0.22.1

Hi folks! Here’s what we shipped in v0.22.1.

{
  "✨ What's New": [
    "The documentation now makes it easier to understand how to use and deploy the project, and it clearly warns against using it in production environments. Specifically, the README was expanded with detailed usage, deployment, and DevNet guidance and adds a prominent non-production warning."
  ],
  "💪 Improvements": [
    "The system can now handle higher event throughput with fewer slowdowns under load. Specifically, the main event queue size constant was increased from 1,000 to 10,000 to better absorb spikes.",
    "Queue sizing is now more consistent so performance doesn’t degrade due to undersized configuration defaults. Specifically, the header queue size is now clamped to at least the default even when derived from the security parameter, and the related test was updated.",
    "Implausible-tip detection is now more accurate and safer around edge-case arithmetic. Specifically, the implausible-tip checks were updated to use peer-based reference blocks with overflow-safe arithmetic and the tests were adjusted accordingly.",
    "Automated tests are now less flaky on slower machines and CI runners. Specifically, timing parameters in `TestSchedulerRunFailFunc` were relaxed to be more tolerant of slow scheduling and execution."
  ],
  "📋 What You Need to Know": [
    "You may need to explicitly set the API bind address in your configuration instead of relying on an implicit default. Specifically, config validation no longer auto-defaults the API bind address to `0.0.0.0`.",
    "Release packaging and CI builds are now using updated, pinned tooling for better supply-chain safety and reproducibility. Specifically, GitHub workflows were updated to newer Docker actions (with `docker/metadata-action` pinned by commit SHA) and `actions/setup-node` was bumped, and `aws-sdk-go-v2` core was updated from 1.41.2 to 1.41.3.",
    "Release documentation has been updated so you can review what changed in the latest versions. Specifically, `RELEASE_NOTES.md` was updated to include entries for v0.22.0 and v0.22.1."
  ],
  "🔧 Fixes": [
    "Epoch processing is now more resilient so it can recover rather than stopping when some reference data is missing. Specifically, epoch nonce recomputation can now restart from the epoch start when an anchor block nonce is not found instead of failing.",
    "Concurrent rollbacks are now less likely to crash the process. Specifically, `advanceEpochCache` now includes an empty-epoch-cache guard to prevent panics during rollback races.",
    "Epoch cache updates are now safer under concurrency to reduce the risk of corrupted state. Specifically, the epoch cache tail is now validated in a concurrency-safe way before appending a new epoch.",
    "Transaction validation is now more consistent so invalid transactions still get the expected ledger checks. Specifically, Conway UTxO validation now runs even for transactions marked invalid, and only script evaluation is skipped for those transactions."
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