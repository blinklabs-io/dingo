# Release Notes

## v0.23.1 (March 10, 2026)

**Title:** Safer dependent transaction validation

**Date:** March 10, 2026

**Version:** v0.23.1

Hi folks! Here’s what we shipped in v0.23.1.

<!--
{
  "✨ What's New": [
    "You can now validate transactions correctly even when they depend on other unconfirmed transactions, which helps avoid accepting transactions that won’t actually work once everything is applied."
  ],
  "💪 Improvements": [
    "It’s easier to understand how to run and deploy this project safely, so you can get started faster and avoid accidental production usage.",
    "Build and publish automation is more up to date, which improves long-term maintainability and compatibility with current tooling.",
    "Dependency updates reduce the risk of incompatibilities and keep the codebase aligned with upstream improvements.",
    "Release documentation is clearer and easier to follow, so you can quickly understand what changed in the version you’re using."
  ],
  "🔧 Fixes": [
    "Network connection testing is more reliable and cleanup is safer, which reduces flaky failures and unexpected resource leaks during development and CI runs.",
    "You’re less likely to hit confusing runtime errors during VRF-related operations, improving stability in scenarios that depend on VRF inputs.",
    "Development mode now behaves more predictably, preventing misconfiguration that can lead to confusing storage behavior."
  ]
}

-->

### ✨ What's New

- **Dependent transaction validation:** Transaction validation is safer because the node now validates transactions against unconfirmed dependencies before accepting them.

### 💪 Improvements

- **Docs and safety messaging:** Getting started is easy because the README now includes clearer usage, deployment, and DevNet guidance plus a prominent non-production warning.
- **CI and publishing automation:** Automation is more rock-solid because the publish workflow and related CI were refreshed to Node.js `24.x` and updated Docker actions.
- **Dependency updates:** Compatibility is more solid because key networking and infrastructure dependencies were updated.
- **Release notes:** Release notes are easier to scan because v0.22.1 now has a documented entry in `RELEASE_NOTES.md`.

### 🔧 Fixes

- **Connection manager tests:** Connection testing is more solid because keepalive timeouts are deterministic and cleanup is safer.
- **VRF error handling:** Verifiable random function (VRF) operations fail more gracefully because VRF input creation now has explicit error handling.
- **Dev mode storage:** Dev mode is less surprising because enabling it now forces API storage mode and logs the override.

### 📋 What You Need to Know

- **Custom CI scripts:** If you maintain custom CI or publishing scripts, double-check compatibility with Node.js `24.x` and updated Docker actions.
- **Dev mode:** If you rely on non-API storage while in dev mode, note that it will now be overridden to API mode.

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