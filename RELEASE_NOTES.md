# Release Notes

## v0.23.1 (March 10, 2026)

**Title:** Updates and fixes

**Date:** March 10, 2026

**Version:** v0.23.1

Hi folks! Here’s what we shipped in v0.23.1.

<!--
{
  "✨ What's New": [
    "You can now validate transactions correctly even when they depend on other unconfirmed transactions, which helps avoid accepting transactions that won’t actually work once everything is applied. Specifically, the ledger and mempool now perform UTxO overlay-aware validation, including descendant pruning and new automated tests to cover these dependency scenarios."
  ],
  "💪 Improvements": [
    "It’s easier to understand how to run and deploy this project safely, so you can get started faster and avoid accidental production usage. The README was expanded with detailed usage, deployment, and DevNet documentation, and it now includes a prominent non-production warning.",
    "Build and publish automation is more up to date, which improves long-term maintainability and compatibility with current tooling. The publish workflow and related CI now use Node.js 24.x, actions/setup-node v6.3.0, docker/login-action v4, docker/metadata-action v6.0.0 (pinned by commit SHA), docker/build-push-action v7.0.0, and docker/setup-buildx-action v4.0.0.",
    "Dependency updates reduce the risk of incompatibilities and keep the codebase aligned with upstream improvements. This update bumps gouroboros to v0.160.1, upgrades github.com/blinklabs-io/bursa from v0.15.0 to v0.16.0, and updates aws-sdk-go-v2 core from 1.41.2 to 1.41.3.",
    "Release documentation is clearer and easier to follow, so you can quickly understand what changed in the version you’re using. RELEASE_NOTES.md now includes a documented v0.22.1 section, with clearer transaction-validation wording and additional notes covering the publish workflow and CI tooling updates."
  ],
  "🔧 Fixes": [
    "Network connection testing is more reliable and cleanup is safer, which reduces flaky failures and unexpected resource leaks during development and CI runs. The connection manager error test now uses a deterministic keepalive timeout, with safer channel closing and connection cleanup behavior.",
    "You’re less likely to hit confusing runtime errors during VRF-related operations, improving stability in scenarios that depend on VRF inputs. The code now adds explicit error handling around vrf.MkInputVrf calls alongside the gouroboros v0.160.1 update.",
    "Development mode now behaves more predictably, preventing misconfiguration that can lead to confusing storage behavior. When dev mode is enabled, the system forces storage mode to API and emits a log message indicating the override."
  ]
}

-->

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