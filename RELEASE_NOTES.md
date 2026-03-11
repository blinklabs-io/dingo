# Release Notes


## v0.24.0 (March 11, 2026)

**Title:** Release updates

**Date:** March 11, 2026

**Version:** v0.24.0

Hi folks! Here’s what we shipped in v0.24.0.

### ✨ What's New

- **Generated release notes:** See the generated notes data in the details block below.

### 💪 Improvements

- **Generated release notes:** See the generated notes data in the details block below.

### 🔧 Fixes

- **Generated release notes:** See the generated notes data in the details block below.

### 📋 What You Need to Know

- **Generated release notes:** See the generated notes data in the details block below.

### 🙏 Thank You

Thank you for trying!

<details>
<summary>Generated notes data (internal)</summary>

```json
{
  "✨ What's New": [
    "You can now control whether your node shares peers, which makes it easier to run in environments with strict networking rules. A new configuration flag is passed through node initialization to enable or disable peer sharing explicitly.",
    "You now get clearer, more guided command-line help so it’s easier to understand how to run the tool and what each mode does. The root CLI command includes expanded Short and Long help text to improve discoverability and self-service usage.",
    "You can now track long-running data setup steps more easily, with progress updates that make large imports feel less like a black box. The system adds structured, rate-limited progress reporting for Mithril download and extraction, ledger and UTxO import, UTxO offset post-processing, and block copying, along with supporting helpers and tests.",
    "You can now measure performance more consistently and spot regressions earlier when evaluating ingestion and storage behavior. The tooling introduces a benchmark baseline plus detailed block and storage-mode ingestion benchmarks with improved benchmark utilities."
  ],
  "💪 Improvements": [
    "Documentation is clearer and safer to follow, so it’s easier to evaluate the project without accidentally treating it as production-ready. The README now includes expanded usage, deployment, and DevNet documentation along with a prominent non-production warning.",
    "Development runs behave more predictably, reducing confusion when local settings don’t match what the system actually supports. Dev mode now forces storage mode to the API option and logs when it overrides the configured value.",
    "Connections are more resilient and logs are less noisy, so transient network edge cases are less likely to look like failures. The connection manager treats existing inbound connections as the bidirectional link, does not count outbound address-in-use dial failures as peer failures, and classifies protocol transition timeouts as expected close events while keeping inbound connections open if chainsync client startup fails (with a warning-level log).",
    "Storage behavior is more transparent and better tuned out of the box, which helps with troubleshooting and performance tuning. Badger DB cache defaults were reset and the blob store now logs configured cache sizes on open, alongside profile-based Badger tuning, fixed 8-byte commit timestamp encoding, and several batched/optimized SQLite metadata operations with optional extended block offsets and rate-limited warnings.",
    "Validation and ingestion are more accurate across eras and more informative during big imports, reducing surprises when moving between protocol versions. The node now performs era-aware protocol parameter extraction and validation, improves UTxO import progress reporting, and updates governance-state decoding to use raw-element parsing.",
    "Transaction acceptance is more consistent when recent changes haven’t been fully committed yet, which helps avoid confusing “valid vs invalid” flips during active processing. The ledger and mempool now perform UTxO overlay-aware validation with descendant pruning and accompanying tests.",
    "Build and publish automation is more up to date and repeatable, which helps keep releases reliable. CI and publish workflows now use Node.js 24.x, newer Docker GitHub Actions (including docker/login-action v4 and pinned docker/metadata-action v6), updated buildx/build-push actions, updated actions/setup-node, and an additional pipeline that builds, tags, pushes, and attests an amd64-only antithesis image variant.",
    "Project dependencies are current, which improves compatibility with newer environments and upstream fixes. Updates include OpenTelemetry Go 1.42.0, golang.org/x/sys v0.42.0, golang.org/x/net v0.51.0, aws-sdk-go-v2 core 1.41.3, github.com/blinklabs-io/bursa v0.16.0, and gouroboros v0.160.1 with added error handling around vrf.MkInputVrf.",
    "The toolchain now targets a newer Go baseline so builds and CI better match modern Go ecosystems. The minimum Go version was raised to 1.25 and CI matrices and related tooling were updated accordingly.",
    "Release documentation is more complete, so it’s easier to understand what changed between versions. RELEASE_NOTES.md now includes expanded entries for v0.22.1, v0.23.0, and v0.23.1, with clearer wording and additional notes about workflow and CI tooling updates."
  ],
  "🔧 Fixes": [
    "Network-related tests are less flaky and shutdown behavior is safer, reducing false failures in CI and local runs. The connection manager error test now relies on a deterministic keepalive timeout and improves channel closing and connection cleanup safety.",
    "Block handling is more reliable under caching and reconciliation, which helps avoid subtle mismatches during synchronization. Block caching was refactored to use fixed-size typed hash keys, and chain block-addition and reconciliation now compare hashes via bytes.Equal using shared locked helpers while centralizing iterator notifications and updating tests accordingly."
  ]
}

```

</details>

---

## v0.23.1 (March 11, 2026)

**Title:** Clearer release notes and rock-solid Docker publishing

**Date:** March 11, 2026

**Version:** v0.23.1

Hi folks! Here’s what we shipped in v0.23.1.

### ✨ What's New

- **Release notes:** Release notes are easier to scan because `RELEASE_NOTES.md` now includes a complete set of notes for v0.23.0.

### 💪 Improvements

- **Docker publishing (Antithesis image):** Publishing is more consistent because the build pipeline now builds, tags, pushes, and generates an attestation for a Linux amd64-only Antithesis Docker image variant.

### 🔧 Fixes

- **No fixes:** No user-facing fixes shipped in this patch.

### 📋 What You Need to Know

- **Upgrade:** You’re all set—no required configuration changes for this release.

### 🙏 Thank You

Thank you for trying!

---

## v0.23.0 (March 10, 2026)

**Title:** Overlay-aware validation and a smoother dev mode

**Date:** March 10, 2026

**Version:** v0.23.0

Hi folks! Here’s what we shipped in v0.23.0.

### ✨ What's New

- **Transaction validation:** Transaction validation is more accurate because ledger and mempool checks now account for pending, in-flight changes with a temporary UTxO overlay.

### 💪 Improvements

- **Docs:** Setup is easier because `README.md` now includes expanded usage, deployment, and DevNet guidance plus a clear “not for production” warning.
- **Publish workflow:** Publishing is more rock-solid because the release workflow now targets Node.js `24.x` and pins key GitHub Actions versions.
- **Release notes:** Release notes are easier to scan because `RELEASE_NOTES.md` now includes v0.22.1 and tightens up transaction validation wording.

### 🔧 Fixes

- **Connection cleanup:** Connection-related tests are less flaky because keepalive timeouts are deterministic and connection cleanup is safer.

### 📋 What You Need to Know

- **Dev mode:** If you run dev mode, Dingo will automatically switch the storage mode to API, so check logs if you expected a different mode.

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