# Release Notes


## v0.40.0 (May 5, 2026)

**Title:** Governance DRep APIs and safer rollback recovery

**Date:** May 5, 2026

**Version:** v0.40.0

Hi folks! Here’s what we shipped in v0.40.0.

### ✨ What's New

* Added **Blockfrost compatible governance DRep lookups:** Blockfrost compatible services can now look up governance DReps by hex or bech32 identifier and retrieve registration, voting power, and activity details.

### 💪 Improvements

* Improved **newer Blockfrost client compatibility:** Blockfrost backed integrations now stay aligned with the newer `blockfrost-go` client release for smoother API compatibility.
* Refreshed **protocol and UTxO RPC compatibility:** Core protocol and UTxO RPC support now stay aligned with newer upstream releases, including Conway version 11 expectations, for steadier interoperability.
* Restored **release history continuity:** Release tracking now includes the v0.39.3 notes entry in `RELEASE_NOTES.md`, which keeps recent history easier to scan.

### 🔧 Fixes

* Corrected **rollback loop recovery:** Nodes now force a fresh chainsync resync when the same rollback repeats, which helps recovery move forward instead of circling on the same point.
* Stabilized **shutdown during catch up:** Nodes now finish in flight replay work before closing storage, which prevents `DB Closed` shutdown failures during catch up.
* Realigned **ledger state after rollbacks:** Rollback recovery now rewinds ledger state together with the chain, which prevents repeated validation failures after peers resend the same blocks.
* Enabled **faster source port reuse:** TCP reconnects now reuse configured source ports more reliably, which reduces reconnect failures after connection churn.

### 📋 What You Need to Know

* Clarified **patch release upgrade guidance:** This is a patch release, and normal upgrade procedures are generally sufficient.
* Highlighted **new governance DRep lookups:** Blockfrost compatible API users can now look up governance DReps through the new endpoint.
* Summarized **safer rollback and resync recovery:** Rollback handling now detects loops, rewinds state correctly, and resyncs more cleanly after recovery problems.
* Emphasized **steadier shutdowns and reconnects:** Catch up shutdowns and source port reuse now behave more reliably, which reduces avoidable interruptions.
* Reviewed **dependency and release history updates:** Blockfrost, protocol, and UTxO RPC dependencies were refreshed, and release history now includes the v0.39.3 notes entry.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.39.3 (May 4, 2026)

**Title:** Correct era transitions, Bark metadata, and peer recovery

**Date:** May 4, 2026

**Version:** v0.39.3

Hi folks! Here’s what we shipped in v0.39.3.

### ✨ What's New

* Noted **no new features:** This patch release focuses on improvements and fixes.

### 💪 Improvements

* Improved **refreshed bundled Cardano network settings:** Synced bundled network settings now include newer peer snapshots, updated guardrails scripts, refreshed topology values, and newer node version expectations for smoother setup.
* Updated **bundled cardano configs package:** Docker and bundled network settings now use `cardano-configs` `20260430-1` to stay aligned with the refreshed upstream configuration set.
* Refined **AWS SDK config compatibility:** AWS backed configuration handling now stays aligned with the latest patch level for steadier cloud integration behavior.
* Modernized **MySQL driver support:** MySQL backed deployments now stay current with `v1.10.0` for ongoing compatibility and maintenance.
* Refreshed **Google API client support:** Google API integrations now stay current with `v0.277.0` for routine compatibility maintenance.
* Advanced **AWS S3 SDK maintenance:** S3 backed storage support now includes the latest patch level for steadier ongoing compatibility.
* Streamlined **devnet contributor workflow:** Contributors can work with the devnet test harness more easily because the end to end test support now lives under `internal/test/devnet` in a clearer shared location.
* Restored **release history continuity:** Release tracking now includes the v0.39.2 notes entry in `RELEASE_NOTES.md`, which keeps recent history easier to scan.

### 🔧 Fixes

* Corrected **cross era transition correctness:** Dingo now keeps chain selection, nonce handling, leader schedule checks, slot battle resolution, and protocol parameter reads aligned across Shelley to Conway boundaries so peer validation and block production remain correct through hard fork transitions.
* Preserved **Bark archive metadata for historical lookups:** Bark backed historical block access now keeps the metadata needed to find archived, tombstoned, or pruned blocks through indexed lookups.
* Realigned **peer recovery after local tip plateaus:** Nodes now resync peer positions after plateau recovery so plateau triggered peer switching is less likely to stay stuck on misaligned peers.

### 📋 What You Need to Know

* Clarified **patch release upgrade guidance:** This is a patch release, and normal upgrade procedures are generally sufficient.
* Highlighted **safer Shelley to Conway transitions:** The most important runtime change is more reliable cross era validation and block production during Shelley to Conway transitions.
* Summarized **Bark historical metadata availability:** Bark backed historical block access now returns the metadata needed for archived and tombstoned block lookups.
* Emphasized **safer plateau recovery behavior:** Plateau recovery now realigns peers after a stalled active peer is recycled, which reduces the chance of getting stuck.
* Reviewed **config and dependency refreshes:** Bundled Cardano configs now use the newer sync set, and several dependencies were updated for compatibility and maintenance.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.39.2 (May 2, 2026)

**Title:** Improve fork recovery and hard fork readiness

**Date:** May 2, 2026

**Version:** v0.39.2

Hi folks! Here’s what we shipped in v0.39.2.

### ✨ What's New

* Added **hard fork validation plumbing:** Dingo now includes early validation infrastructure for scheduled era transitions and hard fork behavior, which improves confidence before future network changes take effect.

### 💪 Improvements

* Improved **era transition protocol version handling:** Dingo now advances scheduled era transitions with the right protocol version, which makes era changes more reliable and reduces the chance of stalling at a fork boundary.
* Refined **slot aware post fork protocol handling:** Dingo now selects the right protocol rules for the slot it is forging, which reduces the chance of producing era incompatible boundary blocks during scheduled forks.

### 🔧 Fixes

* Restored **safer at tip fork recovery:** Dingo now backs out more effectively from repeated validation failures at tip, which gives chain selection more room to escape a bad fork instead of getting trapped on the same failing path.
* Corrected **VRF nonce handling at epoch boundaries:** Dingo now carries forward the right nonce values across epoch boundaries, which keeps leader checks and boundary validation aligned with the active chain.

### 📋 What You Need to Know

* Clarified **patch release upgrade guidance:** This is a patch release, and normal upgrade procedures are generally sufficient.
* Highlighted **safer tip recovery behavior:** Nodes are less likely to remain stuck on the same failing fork after a tip validation problem.
* Emphasized **safer era transitions and hard forks:** Scheduled era changes now progress more reliably, with lower risk of stalls or boundary blocks that do not match the active era.
* Summarized **corrected nonce behavior at epoch boundaries:** Epoch boundary validation and leader related behavior now stay aligned with the correct nonce progression.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.39.1 (May 1, 2026)

**Title:** Safer pruning, rollback recovery, and snapshot nonces

**Date:** May 1, 2026

**Version:** v0.39.1

Hi folks! Here’s what we shipped in v0.39.1.

### ✨ What's New

* Noted **no new features:** This patch release focuses on improvements and fixes.

### 💪 Improvements

* Refined **archive aware Bark cleanup behavior:** Archived blocks now remain resolvable during prune and cleanup work because Bark leaves archive aware tombstones in place and reads honor them.

### 🔧 Fixes

* Preserved **safer rollback cleanup near the Mithril boundary:** Rollback cleanup now keeps gap UTxOs intact near the Mithril boundary, which makes rollback recovery safer.
* Hardened **archive aware Bark cleanup handling:** Bark pruning and archival handling now preserve the resolvability of archived blocks during cleanup.
* Corrected **snapshot bootstrap nonce handling across state layouts:** Snapshot bootstrap now reads epoch and snapshot nonces correctly across supported state layouts, which keeps epoch boundary validation correct after bootstrap.

### 📋 What You Need to Know

* Clarified **patch release upgrade guidance:** This is a patch release, and normal upgrade procedures are sufficient for most operators and users.
* Highlighted **safer pruning and rollback recovery:** Bark pruning now preserves resolvable live UTxOs, and Mithril boundary rollback recovery now preserves the required gap block state for safe recovery.
* Corrected **snapshot nonce handling after bootstrap:** Snapshot bootstrapped nodes now read the right nonce data across supported snapshot layouts so epoch boundary header and VRF validation continue correctly after bootstrap.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.39.0 (April 30, 2026)

**Title:** Era-aware forging, Bark tuning, and safer diagnostics

**Date:** April 30, 2026

**Version:** v0.39.0

Hi folks! Here’s what we shipped in v0.39.0.

### ✨ What's New

* Added **dedicated profiling listener controls:** Operators can now enable runtime profiling on `debugPort` without exposing those endpoints on the metrics listener, and profiling stays disabled unless explicitly turned on.
* Expanded **Bark cleanup control and custom S3 endpoint support:** Bark operators can now tune how often cleanup runs, and S3 compatible Bark storage now works more smoothly with custom endpoints.
* Introduced **ledger driven Bark safety windows:** Bark now derives its safety window from live ledger state, which removes the need to manage a separate `barkSecurityWindow` setting.

### 💪 Improvements

* Improved **near tip sync pacing:** Nodes now gate backup block requests against median peer latency and expose new gate metrics, which keeps near tip sync steadier with less duplicate work.
* Refined **slot timing tolerance:** Busy nodes and catch up runs now tolerate more normal clock drift before raising noise about timing issues.
* Enhanced **devnet validation workflow:** Development teams can now run `make test-devnet` for a more consistent devnet validation flow.
* Modernized **Docker build cleanliness:** Container builds now avoid worktree and bot metadata directories, which keeps build contexts cleaner.
* Updated **Bark download cancellation handling:** Bark block downloads now honor request cancellation and timeout context more cleanly, and lint coverage now checks that path again.
* Refreshed **release history continuity:** Release history now includes the v0.38.0 notes entry in `RELEASE_NOTES.md`, which keeps recent changes easier to scan.

### 🔧 Fixes

* Fixed **candidate nonce fallback safety:** Epoch and leader election calculations now stay protected when stored nonce rows trail behind block ingestion.
* Corrected **Shelley to Allegra verification behavior:** Historical era verification now stays correct across the Shelley to Allegra boundary, which restores successful post fork VRF verification.
* Hardened **multi era block forging:** Block producers can now forge correctly across Shelley through Conway era transitions instead of assuming Conway only block structure.
* Stabilized **devnet config access for non root containers:** Generated configs and keys now remain easier for non root containers to read during devnet runs.
* Repaired **txpump devnet build context:** Devnet and test transaction pump builds now resolve the correct Docker Compose build context more reliably.

### 📋 What You Need to Know

* Clarified **Bark tuning changes:** Bark operators should stop expecting a separate `barkSecurityWindow` setting and should use `barkPrunerFrequency` when cleanup cadence needs tuning.
* Highlighted **profiling opt in behavior:** Profiling users can enable runtime `pprof` on `debugPort`, while metrics stay isolated from debug endpoints by default.
* Emphasized **safer era transition behavior:** Historical era and era transition runs should see safer behavior from the candidate nonce fallback, the Shelley to Allegra VRF correction, and pre Conway forging support.
* Summarized **calmer sync and timing behavior:** Near tip sync and timing should feel steadier because shadow blockfetch gating is smarter and slot clock tolerance now allows 500ms of drift.
* Simplified **devnet and container workflows:** Devnet and test workflows are easier because `make test-devnet`, the txpump build context correction, cleaner Docker contexts, and relaxed test config permissions reduce setup friction.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---