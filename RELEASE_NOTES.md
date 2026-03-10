# Release Notes

## v0.23.1 (March 10, 2026)

**Title:** Solid maintenance updates and polish

**Date:** March 10, 2026

**Version:** v0.23.1

Hi folks! Here’s what we shipped in v0.23.1.

<!--
SOURCE_NOTES (do not edit by hand):

{
  "✨ What's New": [
    "Documentation now makes it easier to understand how to use and deploy the project safely. The README was expanded with detailed usage, deployment, and DevNet guidance and now includes a prominent warning that the project is not intended for production use.",
    "Transaction validation now better reflects the full set of in-flight changes, which helps prevent invalid transactions from slipping through. The ledger and mempool now perform UTxO overlay-aware validation with descendant pruning, and this behavior is covered by new tests."
  ],
  "💪 Improvements": [
    "Development runs are now more predictable by ensuring they use the intended storage behavior. Dev mode now forces the storage mode to API and emits a log message when it overrides the configured value.",
    "Publishing and CI workflows now use newer tooling to improve consistency and keep dependencies current. The GitHub Actions publish workflow was updated to Node.js 24.x, upgraded Docker actions (login-action v4, metadata-action v6 pinned by SHA, build-push-action v7, setup-buildx-action v4), and refreshed actions/setup-node to v6.3.0 and aws-sdk-go-v2 core to 1.41.3.",
    "Dependencies were refreshed to incorporate upstream improvements and maintain compatibility. The project updated gouroboros to v0.160.1 (with added error handling around vrf.MkInputVrf calls) and upgraded github.com/blinklabs-io/bursa from v0.15.0 to v0.16.0.",
    "Release documentation is now clearer and easier to reference when tracking what changed across versions. RELEASE_NOTES.md was updated to add and refine the v0.22.1 entry, including clearer transaction validation wording and notes about publish workflow and CI tooling updates."
  ],
  "🔧 Fixes": [
    "Connection stability tests are now less flaky and cleanup behavior is safer during failures. The connection manager error test now relies on a deterministic keepalive timeout, and channel closing plus connection cleanup were made more robust."
  ]
}


END SOURCE_NOTES
-->

### ✨ What's New

- **Docs:** Docs are easier to follow because the README was expanded with usage, deployment, and DevNet guidance plus a clear “not for production use” warning.
- **Transaction validation:** Transaction validation is safer because the ledger and mempool now perform UTxO overlay-aware validation with descendant pruning, covered by new tests.

### 💪 Improvements

- **Dev mode:** Dev mode runs are more predictable because it now forces storage mode to API and logs when it overrides your configured value.
- **Publishing workflow:** Publishing and CI are more consistent because the GitHub Actions publish workflow now uses Node.js `24.x` and upgraded Docker and Node setup actions.
- **Dependencies:** Compatibility is more solid because gouroboros was updated to v0.160.1 (including additional `vrf.MkInputVrf` error handling) and bursa was upgraded to v0.16.0.
- **Release notes:** Release documentation is easier to scan because `RELEASE_NOTES.md` now includes a refined v0.22.1 entry with clearer transaction validation wording and CI/publish tooling notes.

### 🔧 Fixes

- **Connmanager tests:** Connection stability tests are less flaky because the connection manager error test now uses a deterministic keepalive timeout and cleanup is more robust.

### 📋 What You Need to Know

- **Dev mode override:** If you run with dev mode enabled, Dingo will override storage mode to API and log the change.
- **Not for production:** If you’re evaluating Dingo, note that the README now calls out that the project is not intended for production use.

### 🙏 Thank You

Thank you for trying!

---

## v0.22.1 (March 8, 2026)

**Title:** Stability updates and polish

**Date:** March 8, 2026

**Version:** v0.22.1

Hi folks! Here’s what we shipped in v0.22.1.

### ✨ What's New

- **Release notes:** We added v0.22.0 release notes to `RELEASE_NOTES.md` so you can scan changes in one place.

### 💪 Improvements

- **Transaction validation:** Transaction validation is more consistent because Conway UTxO validation now runs even when a transaction is marked invalid, while script evaluation is still skipped.
- **Epoch processing:** Epoch processing recovers more gracefully because nonce recomputation falls back to recomputing from epoch start when an anchor block nonce is missing.
- **Queue handling:** Queue handling is more solid under load because the main event queue size increased from 1,000 to 10,000 and the header queue size is now clamped to at least the default.
- **Implausible-tip checks:** Implausible-tip checks are safer across edge cases because the logic now uses peer-based reference blocks with overflow-safe arithmetic.
- **Publish workflow login:** Publishing is more rock-solid because the publish workflow was tweaked to log in using `docker/login-action@v4`.
- **Publish workflow runtime:** Automation stays more up to date because Node.js `24.x` was rolled out for the publish workflow.

### 🔧 Fixes

- **Epoch cache rollbacks:** Epoch cache handling is safer during concurrent rollbacks because `advanceEpochCache` now guards against empty caches and validates the tail before appending a new epoch.
- **Test timing:** Tests are less flaky on slower machines because `TestSchedulerRunFailFunc` timing parameters were relaxed.

### 📋 What You Need to Know

- **API bind address:** Config validation no longer defaults the API bind address to `0.0.0.0`, so set it explicitly if you need it.
- **CI and publishing scripts:** If you maintain custom publishing or CI scripts, give them a quick check for compatibility with Node.js `24.x` and `docker/login-action@v4`.

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