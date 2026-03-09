# Release Notes

## v0.22.1 (March 8, 2026)

**Title:** Stability updates and polish

**Date:** March 8, 2026

**Version:** v0.22.1

Hi folks! Here’s what we shipped in v0.22.1.

<!--
The structured source data below is injected automatically.

We keep it briefly so we can format it consistently with the rest of this file.
-->

{
  "✨ What's New": [
    "You can now rely on an updated, more complete guide when getting started and planning how to run the project. The README now includes detailed usage, deployment, and DevNet documentation, and it also adds a prominent warning that the project is not intended for production use.",
    "You now have an in-repo record of what changed across recent versions, making upgrades easier to track. The project added and updated the documented release notes sections for v0.22.0 and v0.22.1 in `RELEASE_NOTES.md`."
  ],
  "💪 Improvements": [
    "The system can handle larger bursts of activity with fewer slowdowns under load. The main event queue size constant was increased from 1,000 to 10,000.",
    "Queue sizing is now more predictable when derived from security parameters, helping avoid under-provisioning. The header queue size is now forced to be at least the default value even when calculated from the security parameter, and the related test was updated.",
    "Some timing-sensitive tests are now less likely to fail on slower machines and CI runners. The timing parameters in `TestSchedulerRunFailFunc` were relaxed to be more tolerant of slower execution environments.",
    "Implausible-tip detection is now more robust, reducing the chance of incorrect results due to arithmetic edge cases. The implausible-tip checks were updated to use peer-based reference blocks with overflow-safe arithmetic, and the tests were adjusted accordingly.",
    "Build and release automation is now more up to date and reproducible, improving CI reliability. GitHub Actions workflows were updated to use newer Docker actions (including `docker/build-push-action` v7, `docker/setup-buildx-action` v4, and `docker/metadata-action` v6 pinned by SHA) and `actions/setup-node` v6.3.0.",
    "The AWS client stack is now on the latest patch level, which can include small fixes and compatibility improvements. The `aws-sdk-go-v2` core dependency was updated from 1.41.2 to 1.41.3."
  ],
  "📋 What You Need to Know": [
    "If you relied on the API automatically binding to all interfaces by default, you may need to explicitly set a bind address in your configuration. Configuration validation no longer auto-defaults the API bind address to `0.0.0.0`, so you must provide the intended address explicitly when required."
  ],
  "🔧 Fixes": [
    "The system is less likely to crash during edge-case rollback scenarios, improving stability. An empty-epoch-cache guard was added in `advanceEpochCache` to avoid panics during concurrent rollbacks.",
    "Epoch processing is now more resilient when expected anchor data is missing, preventing avoidable failures. The node now allows epoch nonce recomputation from epoch start when an anchor block nonce cannot be found instead of failing.",
    "Validation behavior is now more consistent so you get clearer, safer outcomes even when a transaction is flagged as invalid. Conway UTxO validation now runs even for transactions marked invalid, and only script evaluation is skipped for those transactions.",
    "Epoch cache updates are safer under concurrency, reducing the risk of race conditions and corrupted state. The code now performs concurrency-safe validation of the epoch cache tail before appending a new epoch."
  ]
}


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