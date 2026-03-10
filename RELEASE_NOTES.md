# Release Notes

## v0.23.0 (March 10, 2026)

**Title:** {
  "✨ What's New": [
    "Ledger and mempool validation now accounts for pending UTxO changes so transactions are checked the way they will actually land on-chain. This adds UTxO overlay-aware validation with descendant pruning in the ledger/mempool pipeline, plus new test coverage to confirm the behavior."
  ],
  "💪 Improvements": [
    "Project documentation is clearer about how to use and deploy the project, and it more explicitly sets expectations for safe usage. The README now includes expanded usage, deployment, and DevNet documentation along with a prominent non-production warning.",
    "Development mode now behaves more predictably by enforcing the correct storage setting automatically. When dev mode is enabled, the system forces storage mode to API, logs that it is overriding the configuration, and proceeds with the API-backed storage path.",
    "Release documentation is easier to follow and better reflects what actually shipped in the latest patch. The v0.22.1 entry in RELEASE_NOTES.md was added/updated with clearer transaction-validation wording and additional notes covering the publish workflow and CI tooling changes.",
    "Publishing and CI workflows were refreshed to align with newer supported tooling versions and improve reproducibility. The publish workflow now targets Node.js 24.x, upgrades docker/login-action to v4, moves docker/metadata-action to v6 pinned by SHA, and updates docker/build-push-action to v7.0.0 and docker/setup-buildx-action to v4.0.0, alongside bumping actions/setup-node to v6.3.0."
  ],
  "📋 What You Need to Know": [
    "If you rely on the GitHub publish workflow, make sure your environment and any pinned actions assumptions are compatible with the newer toolchain versions. The pipeline now uses Node.js 24.x and updated Docker GitHub Actions (including docker/metadata-action v6 pinned by commit SHA), which may affect downstream forks that pin older versions.",
    "If you run in dev mode, be aware that your configured storage mode may be overridden automatically. Dev mode forces storage mode to API and emits a log message indicating the override so you can spot it during local runs."
  ],
  "🔧 Fixes": [
    "Connection-related tests are now more stable and cleanup is less likely to leave resources behind. The connection manager error test now uses a deterministic keepalive timeout, and channel closing plus connection cleanup were made safer to reduce flakiness."
  ]
}


**Date:** March 10, 2026

**Version:** v0.23.0

Hi folks! Here’s what we shipped in v0.23.0.

### ✨ What's New

```json
{
  "✨ What's New": [
    "Ledger and mempool validation now accounts for pending UTxO changes so transactions are checked the way they will actually land on-chain. This adds UTxO overlay-aware validation with descendant pruning in the ledger/mempool pipeline, plus new test coverage to confirm the behavior."
  ],
  "💪 Improvements": [
    "Project documentation is clearer about how to use and deploy the project, and it more explicitly sets expectations for safe usage. The README now includes expanded usage, deployment, and DevNet documentation along with a prominent non-production warning.",
    "Development mode now behaves more predictably by enforcing the correct storage setting automatically. When dev mode is enabled, the system forces storage mode to API, logs that it is overriding the configuration, and proceeds with the API-backed storage path.",
    "Release documentation is easier to follow and better reflects what actually shipped in the latest patch. The v0.22.1 entry in RELEASE_NOTES.md was added/updated with clearer transaction-validation wording and additional notes covering the publish workflow and CI tooling changes.",
    "Publishing and CI workflows were refreshed to align with newer supported tooling versions and improve reproducibility. The publish workflow now targets Node.js 24.x, upgrades docker/login-action to v4, moves docker/metadata-action to v6 pinned by SHA, and updates docker/build-push-action to v7.0.0 and docker/setup-buildx-action to v4.0.0, alongside bumping actions/setup-node to v6.3.0."
  ],
  "📋 What You Need to Know": [
    "If you rely on the GitHub publish workflow, make sure your environment and any pinned actions assumptions are compatible with the newer toolchain versions. The pipeline now uses Node.js 24.x and updated Docker GitHub Actions (including docker/metadata-action v6 pinned by commit SHA), which may affect downstream forks that pin older versions.",
    "If you run in dev mode, be aware that your configured storage mode may be overridden automatically. Dev mode forces storage mode to API and emits a log message indicating the override so you can spot it during local runs."
  ],
  "🔧 Fixes": [
    "Connection-related tests are now more stable and cleanup is less likely to leave resources behind. The connection manager error test now uses a deterministic keepalive timeout, and channel closing plus connection cleanup were made safer to reduce flakiness."
  ]
}

```

### 💪 Improvements

```json
{
  "✨ What's New": [
    "Ledger and mempool validation now accounts for pending UTxO changes so transactions are checked the way they will actually land on-chain. This adds UTxO overlay-aware validation with descendant pruning in the ledger/mempool pipeline, plus new test coverage to confirm the behavior."
  ],
  "💪 Improvements": [
    "Project documentation is clearer about how to use and deploy the project, and it more explicitly sets expectations for safe usage. The README now includes expanded usage, deployment, and DevNet documentation along with a prominent non-production warning.",
    "Development mode now behaves more predictably by enforcing the correct storage setting automatically. When dev mode is enabled, the system forces storage mode to API, logs that it is overriding the configuration, and proceeds with the API-backed storage path.",
    "Release documentation is easier to follow and better reflects what actually shipped in the latest patch. The v0.22.1 entry in RELEASE_NOTES.md was added/updated with clearer transaction-validation wording and additional notes covering the publish workflow and CI tooling changes.",
    "Publishing and CI workflows were refreshed to align with newer supported tooling versions and improve reproducibility. The publish workflow now targets Node.js 24.x, upgrades docker/login-action to v4, moves docker/metadata-action to v6 pinned by SHA, and updates docker/build-push-action to v7.0.0 and docker/setup-buildx-action to v4.0.0, alongside bumping actions/setup-node to v6.3.0."
  ],
  "📋 What You Need to Know": [
    "If you rely on the GitHub publish workflow, make sure your environment and any pinned actions assumptions are compatible with the newer toolchain versions. The pipeline now uses Node.js 24.x and updated Docker GitHub Actions (including docker/metadata-action v6 pinned by commit SHA), which may affect downstream forks that pin older versions.",
    "If you run in dev mode, be aware that your configured storage mode may be overridden automatically. Dev mode forces storage mode to API and emits a log message indicating the override so you can spot it during local runs."
  ],
  "🔧 Fixes": [
    "Connection-related tests are now more stable and cleanup is less likely to leave resources behind. The connection manager error test now uses a deterministic keepalive timeout, and channel closing plus connection cleanup were made safer to reduce flakiness."
  ]
}

```

### 🔧 Fixes

```json
{
  "✨ What's New": [
    "Ledger and mempool validation now accounts for pending UTxO changes so transactions are checked the way they will actually land on-chain. This adds UTxO overlay-aware validation with descendant pruning in the ledger/mempool pipeline, plus new test coverage to confirm the behavior."
  ],
  "💪 Improvements": [
    "Project documentation is clearer about how to use and deploy the project, and it more explicitly sets expectations for safe usage. The README now includes expanded usage, deployment, and DevNet documentation along with a prominent non-production warning.",
    "Development mode now behaves more predictably by enforcing the correct storage setting automatically. When dev mode is enabled, the system forces storage mode to API, logs that it is overriding the configuration, and proceeds with the API-backed storage path.",
    "Release documentation is easier to follow and better reflects what actually shipped in the latest patch. The v0.22.1 entry in RELEASE_NOTES.md was added/updated with clearer transaction-validation wording and additional notes covering the publish workflow and CI tooling changes.",
    "Publishing and CI workflows were refreshed to align with newer supported tooling versions and improve reproducibility. The publish workflow now targets Node.js 24.x, upgrades docker/login-action to v4, moves docker/metadata-action to v6 pinned by SHA, and updates docker/build-push-action to v7.0.0 and docker/setup-buildx-action to v4.0.0, alongside bumping actions/setup-node to v6.3.0."
  ],
  "📋 What You Need to Know": [
    "If you rely on the GitHub publish workflow, make sure your environment and any pinned actions assumptions are compatible with the newer toolchain versions. The pipeline now uses Node.js 24.x and updated Docker GitHub Actions (including docker/metadata-action v6 pinned by commit SHA), which may affect downstream forks that pin older versions.",
    "If you run in dev mode, be aware that your configured storage mode may be overridden automatically. Dev mode forces storage mode to API and emits a log message indicating the override so you can spot it during local runs."
  ],
  "🔧 Fixes": [
    "Connection-related tests are now more stable and cleanup is less likely to leave resources behind. The connection manager error test now uses a deterministic keepalive timeout, and channel closing plus connection cleanup were made safer to reduce flakiness."
  ]
}

```

### 📋 What You Need to Know

```json
{
  "✨ What's New": [
    "Ledger and mempool validation now accounts for pending UTxO changes so transactions are checked the way they will actually land on-chain. This adds UTxO overlay-aware validation with descendant pruning in the ledger/mempool pipeline, plus new test coverage to confirm the behavior."
  ],
  "💪 Improvements": [
    "Project documentation is clearer about how to use and deploy the project, and it more explicitly sets expectations for safe usage. The README now includes expanded usage, deployment, and DevNet documentation along with a prominent non-production warning.",
    "Development mode now behaves more predictably by enforcing the correct storage setting automatically. When dev mode is enabled, the system forces storage mode to API, logs that it is overriding the configuration, and proceeds with the API-backed storage path.",
    "Release documentation is easier to follow and better reflects what actually shipped in the latest patch. The v0.22.1 entry in RELEASE_NOTES.md was added/updated with clearer transaction-validation wording and additional notes covering the publish workflow and CI tooling changes.",
    "Publishing and CI workflows were refreshed to align with newer supported tooling versions and improve reproducibility. The publish workflow now targets Node.js 24.x, upgrades docker/login-action to v4, moves docker/metadata-action to v6 pinned by SHA, and updates docker/build-push-action to v7.0.0 and docker/setup-buildx-action to v4.0.0, alongside bumping actions/setup-node to v6.3.0."
  ],
  "📋 What You Need to Know": [
    "If you rely on the GitHub publish workflow, make sure your environment and any pinned actions assumptions are compatible with the newer toolchain versions. The pipeline now uses Node.js 24.x and updated Docker GitHub Actions (including docker/metadata-action v6 pinned by commit SHA), which may affect downstream forks that pin older versions.",
    "If you run in dev mode, be aware that your configured storage mode may be overridden automatically. Dev mode forces storage mode to API and emits a log message indicating the override so you can spot it during local runs."
  ],
  "🔧 Fixes": [
    "Connection-related tests are now more stable and cleanup is less likely to leave resources behind. The connection manager error test now uses a deterministic keepalive timeout, and channel closing plus connection cleanup were made safer to reduce flakiness."
  ]
}

```

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