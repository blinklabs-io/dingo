# Dingo Releases

* Stabilized **align Blockfrost stake account responses with the published schema:** Blockfrost stake account responses now match the published OpenAPI 0.1.88 fields and behavior more closely, which improves compatibility for API clients that depend on that schema. ([#2634](https://github.com/blinklabs-io/dingo/pull/2634))

* Hardened **retry transient Mithril download failures more safely:** Mithril v2 immutable archive and snapshot downloads now recover more reliably from temporary TLS, network, and server failures. ([#2645](https://github.com/blinklabs-io/dingo/pull/2645))

* Repaired **verify valid headers without stale VRF result mismatches:** Header verification now uses the correct VRF result data so valid headers are not rejected because of stale decoded values. ([#2651](https://github.com/blinklabs-io/dingo/pull/2651))

### 📋 What You Need to Know

* Clarified **run Midnight indexing in API mode for cNIGHT and registration events:** API-mode deployments can now persist Midnight cNIGHT and registration event data and keep it current as blocks are indexed.
* Highlighted **expect steadier synchronization near same tip peer competition:** Same tip peer pinning and chainsync race handling now reduce unnecessary switching and help synchronization behave more consistently during recovery.
* Emphasized **trust broader Cardano compatibility across validation and governance paths:** Restrictive Plutus validation, DRep state queries, transaction building, and header verification now align more closely with expected Cardano behavior.
* Summarized **expect smoother API and bootstrap compatibility paths:** Blockfrost stake account responses now match the published schema more closely, and Mithril downloads should recover more reliably from temporary failures.
* Reviewed **use the published v0.58.0 notes as the latest prior release context:** The in repository release history now includes the prior v0.58.0 entry for standard release review.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.58.0 (June 25, 2026)

**Title:** Forged-block self-validation, historical Leios backfill, and governance correctness

**Date:** June 25, 2026

**Version:** v0.58.0

Hi folks! Here’s what we shipped in v0.58.0.

### ✨ What's New

* Added **enable opt in forged block checks before local adoption and broadcast:** Operators can now enable forged block checks with `--validate-forged-block` or `DINGO_VALIDATE_FORGED_BLOCK`, which rejects invalid self forged blocks before broadcast and adds new validation metrics for visibility. ([#2633](https://github.com/blinklabs-io/dingo/pull/2633))
* Introduced **backfill historical Leios endorser blocks during catch up:** From scratch sync and catch up can now recover missing historical endorser blocks, which builds more complete replay state and reduces stalls during historical recovery. ([#2638](https://github.com/blinklabs-io/dingo/pull/2638))

### 💪 Improvements

* Improved **correct Conway DRep vote tallying across full stake credentials:** Governance tallying now counts voting power correctly across full stake credentials, and SQLite backed runs complete that work more efficiently. ([#2637](https://github.com/blinklabs-io/dingo/pull/2637))
* Refined **use the intended SQLite live UTxO index for pool stake snapshots:** Pool stake snapshot aggregation now uses the intended SQLite live UTxO index, which avoids poor query plans and keeps stake calculations steadier. ([#2641](https://github.com/blinklabs-io/dingo/pull/2641))
* Updated **keep the published v0.57.0 release history current:** The in repository release history now includes the structured v0.57.0 notes, which improves continuity across recent releases. ([#2635](https://github.com/blinklabs-io/dingo/pull/2635))

### 🔧 Fixes

* Fixed **prevent duplicate reward journal entries during replay and crash recovery:** Governance, MIR, and POOLREAP reward updates now stay consistent during replay and crash recovery, which prevents duplicate reward account journal entries. ([#2640](https://github.com/blinklabs-io/dingo/pull/2640))
* Corrected **store nullable genesis UTxO foreign key references as SQL NULL in SQLite:** SQLite genesis initialization now stores nullable genesis UTxO foreign key references as SQL NULL, which prevents initialization failures and keeps repeated initialization idempotent. ([#2642](https://github.com/blinklabs-io/dingo/pull/2642))
* Strengthened **recover single peer sync after a local tip plateau:** Nodes that rely on a single eligible upstream peer can now reconnect and resync after a plateau instead of remaining stuck. ([#2639](https://github.com/blinklabs-io/dingo/pull/2639))

### 📋 What You Need to Know

* Clarified **treat forged block checks as an opt in safety path:** Forged block checks remain optional, reject invalid self forged blocks before broadcast, and expose validation metrics for operators.
* Highlighted **expect more reliable historical Leios catch up backfill:** Historical catch up now backfills missing endorser blocks more reliably, which helps replay state stay more complete.
* Emphasized **trust more accurate SQLite governance and stake calculations:** SQLite backed governance tallying and stake snapshot work should be more accurate and less likely to degrade because vote tallying is corrected and pool stake snapshot aggregation now uses the intended SQLite live UTxO index to avoid poor query plans.
* Summarized **expect steadier SQLite genesis initialization and single peer recovery:** SQLite genesis initialization now stores nullable genesis UTxO foreign key references as SQL NULL, which prevents initialization failures and preserves idempotency, and single peer sync recovery should reconnect and resync more reliably after a plateau.
* Reviewed **use the published v0.57.0 notes as the latest prior release context:** The published release history now includes the v0.57.0 entry for smoother recent release review.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.57.0 (June 24, 2026)

**Title:** Midnight gRPC lifecycle, Leios forged-body serving, and faster governance tallying

**Date:** June 24, 2026

**Version:** v0.57.0

Hi folks! Here’s what we shipped in v0.57.0.

### ✨ What's New

* Added **enable a native MidnightState endpoint in API storage mode:** Operators can now run a MidnightState gRPC listener when API storage mode is enabled, and the service includes reflection, health checks, optional TLS when both certificate and key are configured, and a scaffolded handler set that currently returns `Unimplemented`; setting `midnight.port=0` disables the listener. ([#2623](https://github.com/blinklabs-io/dingo/pull/2623))
* Introduced **serve locally forged Leios endorser block transaction bodies through `leios-fetch`:** Peers and downstream consumers can now retrieve transaction bodies for locally forged endorser blocks immediately after announcement. ([#2625](https://github.com/blinklabs-io/dingo/pull/2625))

### 💪 Improvements

* Improved **speed governance tally work at epoch boundaries and surface slow tallies in logs:** Per epoch caching now reduces repeated tally denominator work, and tally duration logging makes slow governance tallies easier to spot during operations. ([#2626](https://github.com/blinklabs-io/dingo/pull/2626))
* Refined **keep Conway validation fixtures consistent with the shared `ouroboros-mock` chain:** Validation now uses the shared Conway chain fixture and the newer `v0.14.0` dependency, which improves test and validation consistency and maintainability. ([#2632](https://github.com/blinklabs-io/dingo/pull/2632))
* Updated **keep the published v0.56.0 release history current:** The in repository release history now includes the prior v0.56.0 entry, which keeps recent release documentation current. ([#2621](https://github.com/blinklabs-io/dingo/pull/2621))

### 🔧 Fixes

* Fixed **avoid inaccurate SPO reward-account auto-vote resolution after Mithril snapshot restores:** Current epoch auto-votes now resolve only when the required pool and reward-account state is present, and historical rows remain unresolved instead of being stored inaccurately when that state is missing. ([#2622](https://github.com/blinklabs-io/dingo/pull/2622))

### 📋 What You Need to Know

* Clarified **run MidnightState only in API storage mode and disable it with `midnight.port=0`:** The MidnightState listener is available only in API storage mode, `midnight.port=0` turns the listener off, and the current service scaffold returns `Unimplemented` until future handlers arrive.
* Highlighted **fetch locally forged Leios endorser block bodies through `leios-fetch`:** Leios peers can now retrieve transaction bodies for locally forged endorser blocks through `leios-fetch` as soon as those blocks are announced.
* Emphasized **expect faster governance tallying and clearer slow tally visibility:** Governance tally processing should run more efficiently at epoch boundaries, and the new logging makes slow tallies visible to operators.
* Summarized **trust safer Mithril imported governance state during auto-vote recovery:** Mithril imported snapshot state no longer marks SPO reward-account auto-votes as resolved when the required source state is missing.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.56.0 (June 23, 2026)

**Title:** Checkpoint validation, Musashi Leios interoperability, and Mithril catch-up correctness

**Date:** June 23, 2026

**Version:** v0.56.0

Hi folks! Here’s what we shipped in v0.56.0.

### ✨ What's New

* Added **validate chain progress against configured checkpoints:** Nodes can now check chain progress against configured checkpoints as they process blocks, which gives operators a stronger trust anchor and stops misaligned runs earlier. ([#2613](https://github.com/blinklabs-io/dingo/pull/2613))
* Introduced **apply Musashi endorsement data earlier and keep batched fetches moving:** Musashi style runs now apply endorsement data ahead of ranking blocks and recover batched fetches more reliably, which improves interoperability during active testing. ([#2617](https://github.com/blinklabs-io/dingo/pull/2617))
* Renamed **switch the Leios testnet name to Musashi:** Operators now use `-n musashi` instead of `-n leios`, while network magic `164` remains the relevant identifier for these runs. ([#2615](https://github.com/blinklabs-io/dingo/pull/2615))

### 💪 Improvements

* Improved **build optional storage plugins with clearer operator guidance:** Optional storage plugins now fit more cleanly into tailored builds, and startup guidance now explains more clearly when a required plugin is missing. ([#2616](https://github.com/blinklabs-io/dingo/pull/2616))
* Refined **keep the Sundae preview example current with TypeScript 6.0.3:** The Sundae preview example now uses TypeScript 6.0.3, which helps local example work stay aligned with current tooling. ([#2598](https://github.com/blinklabs-io/dingo/pull/2598))
* Updated **keep the Blockfrost explorer example current with TypeScript 6.0.3:** The Blockfrost explorer example now uses TypeScript 6.0.3, which helps local example work stay aligned with current tooling. ([#2596](https://github.com/blinklabs-io/dingo/pull/2596))
* Enhanced **enforce architecture boundaries during validation workflows:** Validation now checks package boundaries more strictly, which helps development changes stay aligned with the intended system structure. ([#2605](https://github.com/blinklabs-io/dingo/pull/2605))
* Modernized **reduce repeated peer governor skip noise in logs:** Peer governor logging now emits fewer repeated skip messages, which makes routine logs easier to scan for meaningful events. ([#2592](https://github.com/blinklabs-io/dingo/pull/2592))

### 🔧 Fixes

* Fixed **preserve newer primary chain progress during Mithril catch up:** Mithril catch up now keeps newer primary chain blocks instead of rewinding them away, which protects useful sync progress. ([#2618](https://github.com/blinklabs-io/dingo/pull/2618))
* Corrected **skip waits for stale Musashi endorsement data during catch up:** Catch up now moves past old endorsement data without waiting on it, which reduces avoidable stalls during Musashi style runs. ([#2620](https://github.com/blinklabs-io/dingo/pull/2620))
* Strengthened **fail fast when genesis start times disagree:** Startup now stops immediately when Byron and Shelley genesis start times do not match, which prevents a misaligned run from progressing. ([#2614](https://github.com/blinklabs-io/dingo/pull/2614))
* Stabilized **report Mithril immutable-download progress accurately:** Mithril progress reporting now shows immutable download totals and percentages more accurately, which makes long catch up work easier to follow. ([#2602](https://github.com/blinklabs-io/dingo/pull/2602))
* Hardened **recover Musashi sync more cleanly across prototype interoperability edge cases:** Musashi style sync now handles prototype interoperability edge cases more reliably, including single relay recovery and local rollback cleanup. ([#2600](https://github.com/blinklabs-io/dingo/pull/2600))

### 📋 What You Need to Know

* Clarified **expect checkpoints to enforce validation earlier and stop bad configurations fast:** Checkpoints now validate chain progress against configured values during block processing, and mismatched genesis start times now stop startup before an incorrect run continues. ([#2613](https://github.com/blinklabs-io/dingo/pull/2613), [#2614](https://github.com/blinklabs-io/dingo/pull/2614))
* Highlighted **switch Musashi runs to `-n musashi` and keep network magic `164` in view:** Operators moving from earlier Leios naming should update network selection to `musashi` while continuing to treat `164` as the relevant network magic. ([#2615](https://github.com/blinklabs-io/dingo/pull/2615))
* Emphasized **trust steadier Mithril catch up progress and rewind behavior:** Mithril catch up now preserves useful chain progress, skips stale waits, and reports immutable download progress more accurately. ([#2618](https://github.com/blinklabs-io/dingo/pull/2618), [#2620](https://github.com/blinklabs-io/dingo/pull/2620), [#2602](https://github.com/blinklabs-io/dingo/pull/2602))
* Summarized **expect Musashi interoperability to behave more smoothly during prototype style runs:** Musashi endorsement handling, batched fetch recovery, single relay recovery, and rollback cleanup now align more cleanly with current prototype expectations. ([#2617](https://github.com/blinklabs-io/dingo/pull/2617), [#2600](https://github.com/blinklabs-io/dingo/pull/2600))

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.55.0 (June 22, 2026)

**Title:** cardano-cli build compatibility, ledger accounting correctness, and configurable chainsync

**Date:** June 22, 2026

**Version:** v0.55.0

Hi folks! Here’s what we shipped in v0.55.0.

### ✨ What's New

* Added **build Conway transactions through cardano-cli against Dingo:** `cardano-cli conway transaction build` can now complete the local state queries it needs against Dingo, which improves direct build compatibility. ([#2547](https://github.com/blinklabs-io/dingo/pull/2547))
* Introduced **select chainsync header ingress strategies by configuration:** Operators can now choose primary, parallel, or round robin header ingress behavior for chainsync, which gives them more control over how peer headers are admitted. ([#2577](https://github.com/blinklabs-io/dingo/pull/2577))
* Expanded **configure Midnight services with a dedicated config section:** Midnight settings now live under their own config section, which makes service wiring and deployment choices easier to manage. ([#2568](https://github.com/blinklabs-io/dingo/pull/2568))
* Delivered **store Midnight indexer data in dedicated metadata tables:** Midnight indexer deployments can now persist asset, governance, registration, and epoch candidate data in dedicated tables for easier querying. (`1dd5ba0`)

### 💪 Improvements

* Improved **scale block caching for heavier concurrent workloads:** Block cache reads and writes now spread across shards, which helps busy nodes serve concurrent work more smoothly. ([#2590](https://github.com/blinklabs-io/dingo/pull/2590))
* Refined **align the Sundae preview example with newer Node type definitions:** The Sundae preview example now tracks newer Node development types, which helps local example builds stay current. (`1e5e9f6`)
* Updated **align the Blockfrost explorer example with newer Node type definitions:** The Blockfrost explorer example now tracks newer Node development types, which helps local example builds stay current. (`db5fc0e`)
* Enhanced **keep Google API support current across the project:** Google API backed integrations now stay aligned with a newer client release for steadier compatibility. (`2168972`)
* Modernized **keep the governance lens example aligned with newer Postgres client support:** The governance lens example now tracks a newer Postgres client release, which supports smoother example maintenance. (`121ec85`)
* Strengthened **keep Ouroboros protocol support aligned with newer upstream behavior:** Core protocol handling now tracks a newer upstream release, which supports ongoing compatibility work. (`b309afa`)
* Advanced **expand dependency update coverage across maintained examples:** Example projects now receive routine dependency update coverage alongside the main project paths. (`ebe80ad`)
* Polished **refresh published benchmark reports with current performance baselines:** Benchmark documentation now reflects newer throughput and load measurements, which makes performance review easier to follow. (`e36b260`)
* Refreshed **keep Google API integrations aligned with newer client support:** Google API backed integrations now track a newer client release, which supports steadier compatibility across those paths. (`5a67ea8`)
* Streamlined **keep Badger storage support current across the project:** Badger storage now tracks a newer patch release, which supports steadier storage compatibility and maintenance. (`c9d5854`)
* Sharpened **keep core networking support current:** Core networking support now tracks a newer Go networking release, which helps compatibility stay current across network-facing paths. (`0ef11ce`)
* Simplified **keep AWS S3 integration support current:** AWS S3 backed storage now tracks a newer SDK release, which supports steadier compatibility for object storage deployments. (`ac73226`)
* Cleaned **update build automation to the latest checkout workflow generation:** Project automation now uses the newer checkout workflow generation across CI and publish workflows. (`474ca48`)
* Stabilized **keep AWS configuration support aligned with newer upstream releases:** AWS configuration handling now tracks a newer SDK release for smoother compatibility. (`9bfc1b5`)
* Balanced **keep AWS credential handling current:** AWS credential support now tracks the latest SDK patch level for steadier authenticated integrations. (`c9b1afd`)
* Organized **keep Google cloud storage support current:** Google cloud storage integrations now track a newer client patch release for steadier maintenance. (`7b5326c`)
* Tuned **keep the Midnight example build tooling current:** The Midnight preview example now uses a newer Vite release for smoother local builds. (`ec55789`)
* Tightened **keep the Blockfrost explorer example build tooling current:** The Blockfrost explorer example now uses a newer Vite release for smoother local builds. (`067d47d`)
* Stabilized **tighten nil safety checks around fetch and account history paths:** Several runtime paths now handle missing values more defensively, which helps validation and metadata fetch behavior fail more safely. (`d89d00e`)
* Clarified **separate connection recycling signals from validation checks:** Connection recycle requests now move through clearer event boundaries, which reduces coupling between validation and connection management. (`553ee4c`)
* Realigned **extract Praos selection logic into dedicated helpers:** Praos chain selection logic now lives in focused comparison helpers, which makes selection behavior easier to reuse and verify. (`fcadf48`)
* Harmonized **keep crypto and core Go support libraries current:** Core cryptography and supporting Go libraries now track newer upstream releases for steadier compatibility. (`2ef6168`)
* Reworked **decouple chainsync state from direct ledger iteration ownership:** Chainsync now uses a narrower chain access contract, which keeps client state handling more modular. (`dd9a1d3`)
* Streamlined **separate ledger block building from mempool-specific transaction views:** Block building now relies on a narrower pending transaction contract, which keeps ledger and forging wiring more modular. (`9cfbfd4`)
* Restructured **decouple chain selection from peer governance events:** Chain selection now receives eligibility and priority updates through direct setters, which keeps selection logic less tightly bound to peer governance internals. (`a3038d0`)
* Organized **move peer relay adaptation to the node boundary:** Stake pool relay discovery now passes through a dedicated node layer adapter, which keeps peer governance wiring cleaner. (`f8e68c5`)
* Simplified **narrow API adapters to the capabilities each server needs:** Mesh and UTxO RPC now depend on smaller adapter contracts, which makes API wiring more focused and easier to validate. (`a3e715c`)
* Updated **publish the v0.54.0 release history entry:** The release history now includes the v0.54.0 notes, which keeps recent release context easier to review. (`eb9ebf1`)
* Revised **align endorser block support with newer protocol layouts after Conway:** Endorser block support now follows updated upstream era organization without changing endorser block behavior. (`9a377d7`)

### 🔧 Fixes

* Fixed **recognize stake and DRep identities with the correct credential tags:** Stake and DRep lookups now respect credential tag differences, which keeps identity handling aligned across key and script credentials. ([#2520](https://github.com/blinklabs-io/dingo/pull/2520))
* Corrected **return hard misses immediately for block by hash lookups:** Block by hash requests now return a direct miss when the hash index has no entry, which avoids unnecessary full store scans. ([#2167](https://github.com/blinklabs-io/dingo/pull/2167))
* Strengthened **clear reward balances safely when withdrawals exceed projected metadata rewards:** Reward withdrawals now clear the local reward balance without failing when projected metadata totals trail ledger valid withdrawals, and rollback restores the prior balance correctly. (`a4d10df`)
* Stabilized **handle empty Mithril snapshots without disrupting restore flows:** Mithril restore logic now handles empty snapshot responses more cleanly, which keeps restore automation moving through this edge case. ([#2551](https://github.com/blinklabs-io/dingo/pull/2551))
* Hardened **avoid lock order stalls during live chain reconciliation:** Chain reconciliation now acquires primary chain state in a safer order, which reduces the chance of deadlock during live recovery work. (`9cdcf67`)
* Repaired **credit Conway donations to treasury accounting:** Treasury donation handling now keeps Conway donation effects aligned with treasury balances. ([#2565](https://github.com/blinklabs-io/dingo/pull/2565))
* Resolved **restore POOLREAP refunds at epoch boundaries:** POOLREAP processing now returns the expected refunds at epoch boundaries, which keeps stake account balances aligned. ([#2571](https://github.com/blinklabs-io/dingo/pull/2571))
* Balanced **return orphaned governance refunds and remove expired actions correctly:** Expired governance actions now refund deposits and clear the orphaned records they leave behind, which keeps governance accounting cleaner. ([#2574](https://github.com/blinklabs-io/dingo/pull/2574))
* Secured **apply MIR rewards and pot changes to the right ledger state:** MIR processing now updates reward and Ada pot state more accurately, which keeps reward accounting aligned. ([#2576](https://github.com/blinklabs-io/dingo/pull/2576))
* Refined **apply transaction reward withdrawals to persisted account balances:** Reward withdrawals from valid transactions now update stored reward balances directly, which keeps reward account queries more accurate. (`bd50123`)
* Updated **align Blockfrost transaction details with the published schema:** Blockfrost transaction detail responses now match the published OpenAPI shape more closely, including resolved redeemer information instead of placeholders. ([#2554](https://github.com/blinklabs-io/dingo/pull/2554))

### 📋 What You Need to Know

* Clarified **expect cardano-cli Conway build support to rely on broader chain state coverage:** `cardano-cli conway transaction build` now depends on the expanded chain state coverage in this release, and that path now works against Dingo.
* Highlighted **choose chainsync header ingress strategy explicitly when the default is not enough:** Chainsync now supports configurable header ingress strategies, and the default remains the primary strategy unless operators choose parallel or round robin.
* Emphasized **configure Midnight through its dedicated section and note the API storage dependency:** Midnight now uses its own config section, it depends on API storage mode, and setting the Midnight port to `0` turns off the Midnight gRPC listener.
* Summarized **expect materially tighter ledger accounting around rewards, refunds, and identities:** Treasury donations, POOLREAP refunds, orphaned governance refunds, MIR effects, credential tag aware identities, and reward withdrawals now stay aligned more closely with ledger behavior.
* Reviewed **expect Blockfrost transaction details to return more complete resolved data:** Blockfrost transaction detail responses now align more closely with the published schema, and clients should expect real resolved redeemer metadata instead of placeholder values.
* Noted **plan for sharper block by hash misses and possible historical hash index backfill:** Hash index misses now return hard misses without scanning the store, and older databases that lack historical hash index entries may need a one time backfill.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.54.0 (June 12, 2026)

**Title:** Experimental Leios EB forging, Mithril v2 database restore, and off-chain metadata caching

**Date:** June 12, 2026

**Version:** v0.54.0

Hi folks! Here’s what we shipped in v0.54.0.

### ✨ What's New

* Added **cache metadata through API storage:** API storage can now fetch off chain metadata, verify fetched content against the on chain hash, and keep cached results available through the configured offchain metadata settings. ([#2544](https://github.com/blinklabs-io/dingo/pull/2544))
* Introduced **restore Mithril databases through the v2 backend by default:** Mithril restores now use the v2 database backend by default, which gives operators the current restore path without extra setup. ([#2548](https://github.com/blinklabs-io/dingo/pull/2548))
* Expanded **generate Midnight state services from bundled protocol definitions:** Midnight integrations can now use bundled state definitions with generated service support, which makes integration easier to keep aligned. ([#2518](https://github.com/blinklabs-io/dingo/pull/2518))
* Delivered **forge experimental Leios endorser blocks during leader turns:** Experimental Leios mode can now forge endorser blocks during leader turns, which moves Leios testing closer to a fuller production path. ([#2540](https://github.com/blinklabs-io/dingo/pull/2540))
* Enabled **time experimental Leios pipeline stages more precisely:** Experimental Leios mode now tracks pipeline timing and stage progression more explicitly, which makes its endorser block flow easier to observe. ([#2531](https://github.com/blinklabs-io/dingo/pull/2531))

### 💪 Improvements

* Improved **move public Go API packages under the api namespace:** Go integrators now get a clearer public package layout under `api/`, which makes library use easier to follow. ([#2545](https://github.com/blinklabs-io/dingo/pull/2545))
* Refined **keep the published v0.53.0 release history current:** The release history now includes the finalized v0.53.0 entry, which keeps recent release context easier to review. ([#2539](https://github.com/blinklabs-io/dingo/pull/2539))

### 🔧 Fixes

* Fixed **recover from stale sync peers more decisively:** Sync can now move away from stale peers more reliably, which helps nodes keep progress moving. ([#2549](https://github.com/blinklabs-io/dingo/pull/2549))
* Corrected **resume sync after peer disconnects without stalling:** Peer recovery now keeps sync moving after a disconnect, which reduces the chance of stalled recovery. ([#2543](https://github.com/blinklabs-io/dingo/pull/2543))
* Strengthened **reject unsafe experimental Leios votes before they proceed:** Experimental Leios vote handling now stops endorser block hash and slot mismatches before voting, which makes vote checks stricter. ([#2532](https://github.com/blinklabs-io/dingo/pull/2532))

### 📋 What You Need to Know

* Clarified **expect Mithril v2 as the default restore path:** Mithril now uses the v2 backend by default, operators who need legacy behavior must explicitly choose v1, and `mithril show` now expects the v2 CardanoDatabase artifact hash when the default backend is in use.
* Highlighted **update Go import paths for public API packages:** Go integrators should move imports from the former root level package names to the new paths under `api/`.
* Emphasized **configure off chain metadata fetching through API settings:** Off chain metadata fetching runs in `storageMode: api`, verifies fetched content against on chain hashes, and follows the configured offchain metadata settings.
* Summarized **expect broader experimental Leios timing, forging, and vote safety checks:** Experimental Leios mode now includes endorser block pipeline timing, endorser block forging, and stricter vote safety checks.
* Reviewed **use the finalized v0.53.0 notes as the latest prior release context:** The repository release history now includes the finalized v0.53.0 entry for recent release review.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.53.0 (June 10, 2026)

**Title:** Experimental Leios quorum voting, public Mithril sync API, and Blockfrost network accuracy

**Date:** June 10, 2026

**Version:** v0.53.0

Hi folks! Here’s what we shipped in v0.53.0.

### ✨ What's New

* Added **enable experimental Leios quorum voting and certificate handling:** Leios mode can now enforce committee voting and quorum handling for experimental voting flows, and operators can turn it on with the required voting credentials and public key settings. ([#2509](https://github.com/blinklabs-io/dingo/pull/2509))
* Introduced **open the Mithril sync API for direct public use:** Mithril users and integrators can now call the sync API directly, which makes snapshot and sync workflows easier to integrate. ([#2524](https://github.com/blinklabs-io/dingo/pull/2524))

### 💪 Improvements

* Improved **speed fresh Mithril backfills by skipping consumed input recovery:** Fresh Mithril backfills now avoid recovery work that is not needed on that path, which helps historical catch up finish sooner. ([#2508](https://github.com/blinklabs-io/dingo/pull/2508))
* Refined **stop publishing ancillary txpump, configurator, and analysis images in this release train:** Release automation now publishes only the main release images in this release train, which keeps published outputs aligned with the supported artifacts. ([#2538](https://github.com/blinklabs-io/dingo/pull/2538))
* Updated **keep untagged publish runs moving when build attestations are unavailable:** Untagged publish runs now continue when attestation steps are unavailable, while tagged releases still require successful attestations. ([#2536](https://github.com/blinklabs-io/dingo/pull/2536))
* Enhanced **strengthen Antithesis startup and validation checks:** Antithesis runs now load network bootstrap data more reliably and validate newer era data more safely, which makes test environments steadier. ([#2535](https://github.com/blinklabs-io/dingo/pull/2535))
* Corrected **report expired transaction metrics accurately in tests:** Validation now checks expired transaction metrics against the intended behavior, which keeps monitoring coverage aligned with runtime results. ([#2537](https://github.com/blinklabs-io/dingo/pull/2537))
* Modernized **keep the published v0.52.1 release history current:** The release history now includes the prior v0.52.1 entry, which keeps recent release context easier to review. ([#2529](https://github.com/blinklabs-io/dingo/pull/2529))

### 🔧 Fixes

* Fixed **return accurate Blockfrost network supply and script details:** Blockfrost network responses now report locked and circulating supply more accurately, carry script details through metadata, and align the network eras response with the supported schema. ([#2510](https://github.com/blinklabs-io/dingo/pull/2510))
* Repaired **connect Leios endorser and ranking blocks correctly:** Leios block handling now connects endorser and ranking blocks through the intended paths, which keeps experimental Leios processing aligned. ([#2507](https://github.com/blinklabs-io/dingo/pull/2507))
* Hardened **protect static analysis from malformed Mithril data and unknown eras:** Static analysis now rejects malformed Mithril data and unknown era data more safely, which prevents avoidable failures during analysis. ([#2534](https://github.com/blinklabs-io/dingo/pull/2534))
* Stabilized **use the correct slot in Leios vote handling:** Leios vote handling now uses the intended slot and applies stronger empty value guards, which keeps experimental voting behavior more reliable. ([#2533](https://github.com/blinklabs-io/dingo/pull/2533))

### 📋 What You Need to Know

* Clarified **configure Leios voting before enabling experimental quorum handling:** The largest runtime addition is experimental Leios quorum voting and certificate handling, and operators who enable it need the required voting key and voter public key configuration. ([#2509](https://github.com/blinklabs-io/dingo/pull/2509), [#2533](https://github.com/blinklabs-io/dingo/pull/2533))
* Highlighted **expect more accurate Blockfrost network data and review older metadata stores:** Blockfrost API users should now see accurate `supply.locked` and `supply.circulating` values, they must stop relying on the `era` field in `/api/v0/network/eras`, and older metadata databases may need a rebuild or reimport to fully reflect `payment_script` on existing UTxOs. ([#2510](https://github.com/blinklabs-io/dingo/pull/2510))
* Emphasized **call the public Mithril sync API directly and expect faster fresh backfills:** Mithril users and integrators can now use the public sync API directly, and fresh Mithril backfills should finish faster because this path skips consumed input recovery. ([#2524](https://github.com/blinklabs-io/dingo/pull/2524), [#2508](https://github.com/blinklabs-io/dingo/pull/2508))
* Summarized **expect ancillary txpump, configurator, and analysis images to stay unpublished while tagged releases stay strict:** Release automation no longer publishes ancillary txpump, configurator, and analysis images in this release train, while tagged releases still require successful attestations. ([#2538](https://github.com/blinklabs-io/dingo/pull/2538), [#2536](https://github.com/blinklabs-io/dingo/pull/2536))

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.52.1 (June 9, 2026)

**Title:** Mithril snapshot reuse, steadier epoch timing, and Blockfrost explorer improvements

**Date:** June 9, 2026

**Version:** v0.52.1

Hi folks! Here’s what we shipped in v0.52.1.

### ✨ What's New

* Noted **no new features in this patch release:** This patch release focuses on improvements, fixes, example polish, testing, and release history maintenance.

### 💪 Improvements

* Improved **keep protocol support aligned and steady Leios cache behavior:** Protocol and transaction handling now stay aligned with newer upstream support, and Leios endorser block caching now behaves more reliably. ([#2526](https://github.com/blinklabs-io/dingo/pull/2526))
* Refined **refresh the Blockfrost explorer example for easier local evaluation:** The Blockfrost explorer example now offers a clearer dashboard flow, search, compose setup, fallback metrics proxy handling, and broader port exposure, which makes local API evaluation easier to use. ([#2527](https://github.com/blinklabs-io/dingo/pull/2527))
* Updated **keep the published v0.52.0 release history current:** The release history now includes the prior v0.52.0 entry, which keeps recent release context easier to review. ([#2522](https://github.com/blinklabs-io/dingo/pull/2522))
* Strengthened **let metadata tests run safely in parallel:** Metadata test coverage now runs with better isolation, which makes routine validation steadier and reduces test interference. ([#2521](https://github.com/blinklabs-io/dingo/pull/2521))

### 🔧 Fixes

* Fixed **reuse a valid Mithril snapshot window during API startup:** API mode startup now reuses an existing post Mithril snapshot window instead of rebuilding that state again, which makes startup smoother after Mithril based recovery. ([#2525](https://github.com/blinklabs-io/dingo/pull/2525))
* Corrected **keep epoch timing moving through reconnects more reliably:** Epoch timing now clears stale upstream tip state and matches connection identity more safely across reconnects, which helps nodes avoid timing stalls. ([#2523](https://github.com/blinklabs-io/dingo/pull/2523))

### 📋 What You Need to Know

* Clarified **expect more reliable protocol behavior and steadier Leios caching:** Protocol handling now stays aligned with newer upstream support, and Leios endorser block caching should behave more reliably where it is in use. ([#2526](https://github.com/blinklabs-io/dingo/pull/2526))
* Highlighted **use the refreshed Blockfrost explorer example for smoother local API evaluation:** Teams using the example can expect an easier dashboard flow, search, compose setup, fallback metrics proxy handling, and broader port exposure. ([#2527](https://github.com/blinklabs-io/dingo/pull/2527))
* Emphasized **review the published v0.52.0 notes alongside this patch:** The release history now includes the prior v0.52.0 entry, which makes recent patch context easier to follow. ([#2522](https://github.com/blinklabs-io/dingo/pull/2522))
* Summarized **trust steadier parallel metadata test coverage around this release:** Validation now runs with better metadata test isolation, which supports more dependable routine verification. ([#2521](https://github.com/blinklabs-io/dingo/pull/2521))
* Reviewed **expect smoother API startup after Mithril based recovery:** API mode startup now reuses a valid post Mithril snapshot window when it is available, which avoids unnecessary rebuild work. ([#2525](https://github.com/blinklabs-io/dingo/pull/2525))
* Noted **expect steadier epoch timing across reconnect events:** Nodes should avoid timing stalls more reliably because stale upstream tip state no longer lingers across reconnects. ([#2523](https://github.com/blinklabs-io/dingo/pull/2523))

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.52.0 (June 8, 2026)

**Title:** Configurable history expiry, corrected Conway fees, and safer near-tip sync

**Date:** June 8, 2026

**Version:** v0.52.0

Hi folks! Here’s what we shipped in v0.52.0.

### ✨ What's New

* Added **configure history expiry separately from Bark pruning:** Operators can now enable local history expiry with dedicated settings while still using Bark as a fallback for archived block access when a remote archive is configured. ([#2519](https://github.com/blinklabs-io/dingo/pull/2519))

### 💪 Improvements

* Improved **refresh AWS credential support for authenticated integrations:** AWS authenticated integrations now stay aligned with newer upstream credential handling, which helps related storage and service connections behave more reliably. ([#2517](https://github.com/blinklabs-io/dingo/pull/2517))
* Refined **keep core AWS integration support current:** AWS backed storage and transfer paths now stay aligned with newer upstream SDK behavior, which supports steadier compatibility for those integrations. ([#2515](https://github.com/blinklabs-io/dingo/pull/2515))
* Updated **keep Google integration support current:** Google backed integrations now stay aligned with a newer client release, which supports ongoing compatibility and maintenance. ([#2516](https://github.com/blinklabs-io/dingo/pull/2516))
* Modernized **keep AWS protocol support aligned with upstream changes:** AWS related protocol and serialization handling now stays aligned with a newer upstream release, which supports steadier compatibility across those integration paths. ([#2513](https://github.com/blinklabs-io/dingo/pull/2513))
* Strengthened **keep the published v0.51.0 release history current:** The release history now includes the prior v0.51.0 entry, which keeps recent release context easier to review. ([#2512](https://github.com/blinklabs-io/dingo/pull/2512))

### 🔧 Fixes

* Fixed **correct Conway transaction checks for fees and required witnesses:** Conway transaction validation now includes reference-script costs in minimum-fee checks and enforces required redeemers locally, which helps reduce the chance of accepting invalid transactions. ([#2462](https://github.com/blinklabs-io/dingo/pull/2462))
* Corrected **avoid stalls while syncing close to the tip:** Near-tip sync now waits for a usable non-bootstrap successor path before leaving bootstrap mode, which helps nodes continue syncing instead of stalling near the tip. ([#2511](https://github.com/blinklabs-io/dingo/pull/2511))

### 📋 What You Need to Know

* Clarified **move history expiry settings to the new opt-in configuration:** Operators who previously used `barkPrunerFrequency` must move to `historyExpiry.enabled` and `historyExpiry.frequency`, and history expiry remains off until it is explicitly enabled.
* Highlighted **expect stricter Conway transaction checks on this release:** Minimum-fee validation now includes reference-script costs, and required redeemers must be present during local validation.
* Emphasized **expect more reliable near tip sync and bootstrap exit behavior:** Near-tip sync and bootstrap exit should now behave more reliably, which reduces the chance of stalls close to the tip.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.51.0 (June 7, 2026)

**Title:** Advance Leios support, expand protocol APIs, and smooth sync and backfill

**Date:** June 7, 2026

**Version:** v0.51.0

Hi folks! Here’s what we shipped in v0.51.0.

### ✨ What's New

* Added **try experimental next era support with early Leios groundwork:** Early support for the next era expands preview testing, which helps upcoming Leios work move forward with less setup friction. ([#2495](https://github.com/blinklabs-io/dingo/pull/2495))
* Introduced **relay Leios votes across the network path:** Leios mode can now diffuse votes through the dedicated relay flow, which makes early vote handling behave more like a complete network path. ([#2502](https://github.com/blinklabs-io/dingo/pull/2502))
* Expanded **read Conway protocol settings through Blockfrost epoch endpoints:** Blockfrost compatible epoch responses now expose Conway era protocol parameters, which gives API users a clearer view of current network settings. ([#2504](https://github.com/blinklabs-io/dingo/pull/2504))
* Delivered **request ledger based peer snapshots directly:** Peer snapshot consumers can now fetch ledger based snapshot data directly, which makes peer selection and network insight easier to use with live ledger state. ([#2496](https://github.com/blinklabs-io/dingo/pull/2496))
* Enabled **run a local Blockfrost explorer example for API testing:** A local explorer example now helps teams try Dingo’s Blockfrost compatible APIs in a more realistic workflow before deploying elsewhere. ([#2482](https://github.com/blinklabs-io/dingo/pull/2482))

### 💪 Improvements

* Improved **speed SQLite writes during heavy API backfill:** API backfill can now write SQLite data more efficiently on busy paths, which helps long historical catch up finish more smoothly. ([#2499](https://github.com/blinklabs-io/dingo/pull/2499))
* Refined **rebuild the most important indexes first after API backfill:** Critical indexes now return sooner while the remaining maintenance continues in the background, which makes recovered services more usable earlier and shutdown behavior safer. ([#2497](https://github.com/blinklabs-io/dingo/pull/2497))
* Updated **refresh bundled peer snapshots and seed startup from them:** Bundled topology data now stays fresher and startup can seed peers from embedded peer snapshots, which makes initial network connections more dependable. ([#2506](https://github.com/blinklabs-io/dingo/pull/2506))
* Modernized **use stronger compression controls when compression is on:** Compressed storage can now use a more efficient compression mode with a selectable compression level, which gives operators more direct control over storage behavior. ([#2501](https://github.com/blinklabs-io/dingo/pull/2501))
* Strengthened **keep the published v0.50.2 release history current:** The release history now includes the prior v0.50.2 entry, which keeps recent release context easier to review. ([#2498](https://github.com/blinklabs-io/dingo/pull/2498))

### 🔧 Fixes

* Fixed **avoid near tip stalls during bootstrap exit and tip detection:** Near tip sync now leaves bootstrap mode more safely and detects tip conditions more reliably, which helps nodes avoid stalling close to the tip. ([#2511](https://github.com/blinklabs-io/dingo/pull/2511))
* Corrected **match devnet startup with protocol version 11:** Devnet now starts with the intended minimum protocol version, which keeps test environments aligned with current network expectations. ([#2505](https://github.com/blinklabs-io/dingo/pull/2505))

### 📋 What You Need to Know

* Clarified **start bundled Leios testnets without separate network files:** Leios testnet configuration, topology, peer snapshot, and genesis data now ship inside the binary, which makes bundled Leios startup simpler. ([#2503](https://github.com/blinklabs-io/dingo/pull/2503))
* Highlighted **set JSON logging explicitly when machine readable logs are required:** The default log format is now text, and JSON log consumers must set `logging.format=json` or the matching environment variable or flag to keep machine readable JSON logs. ([#2500](https://github.com/blinklabs-io/dingo/pull/2500))

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.50.2 (June 3, 2026)

**Title:** Faster leader scheduling, sturdier backfill provenance, and safer chainsync intersections

**Date:** June 3, 2026

**Version:** v0.50.2

Hi folks! Here’s what we shipped in v0.50.2.

### ✨ What's New

* Noted **no new features in this patch release:** This patch release focuses on improvements, fixes, testing coverage, and release history updates.

### 💪 Improvements

* Improved **speed leader scheduling checks during block production planning:** Leader schedule lookups now finish faster, which keeps block production planning more responsive. ([#2493](https://github.com/blinklabs-io/dingo/pull/2493))
* Refined **strengthen recovery coverage with real chain replay paths:** Regression checks now cover real chain replay paths more directly, which increases confidence in recovery behavior. ([#2479](https://github.com/blinklabs-io/dingo/pull/2479))
* Updated **keep the published v0.50.1 release history current:** The release history now includes the v0.50.1 notes, which makes recent release context easier to review. ([#2480](https://github.com/blinklabs-io/dingo/pull/2480))

### 🔧 Fixes

* Fixed **preserve backfill history more reliably during historical recovery:** Historical backfill now keeps output history aligned more reliably during resumed recovery, which improves tracking accuracy after interruptions. ([#2481](https://github.com/blinklabs-io/dingo/pull/2481))
* Corrected **fail oversized sync intersection requests earlier:** Oversized sync intersection requests now stop sooner, which avoids unnecessary intersection work and keeps sync behavior safer. ([#2494](https://github.com/blinklabs-io/dingo/pull/2494))

### 📋 What You Need to Know

* Clarified **follow normal patch release upgrade steps for this release:** Normal patch release upgrade steps are generally sufficient for this release.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.50.1 (June 2, 2026)

**Title:** Stuck-peer recovery, corrected chain density, and maintenance polish

**Date:** June 2, 2026

**Version:** v0.50.1

Hi folks! Here’s what we shipped in v0.50.1.

### ✨ What's New

* Noted **no new features in this patch release:** This patch release focuses on improvements and fixes.

### 💪 Improvements

* Improved **keep sibling worktrees out of routine project commands:** Routine format and build commands now ignore sibling `.worktrees` directories, which keeps local maintenance behavior more predictable. ([#2478](https://github.com/blinklabs-io/dingo/pull/2478))
* Updated **publish the v0.50.0 release history entry:** The published release history now includes the v0.50.0 notes, which keeps recent release context easier to review. ([#2474](https://github.com/blinklabs-io/dingo/pull/2474))

### 🔧 Fixes

* Fixed **recover more cleanly from peers that stop serving blocks:** Nodes can now disconnect or slow peers that repeat empty block responses for the same point, which helps sync escape stuck-peer loops more reliably. ([#2473](https://github.com/blinklabs-io/dingo/pull/2473))
* Corrected **avoid repeated next epoch readiness work after it already completes:** Next epoch readiness now avoids repeating the same work after readiness has already been emitted, which keeps slot progress steadier. ([#2475](https://github.com/blinklabs-io/dingo/pull/2475))
* Stabilized **report chain density more accurately across restart and rollback:** Chain density now stays aligned with the active chain, which keeps this health signal steadier after restart and rollback events. ([#2477](https://github.com/blinklabs-io/dingo/pull/2477))

### 📋 What You Need to Know

* Clarified **expect smoother local maintenance when using multiple worktrees:** Local format and build commands now avoid picking up files from sibling `.worktrees` directories, which keeps routine maintenance behavior more predictable. ([#2478](https://github.com/blinklabs-io/dingo/pull/2478))
* Highlighted **review the published v0.50.0 notes alongside this patch:** The release history now includes the v0.50.0 entry, which makes recent patch context easier to follow. ([#2474](https://github.com/blinklabs-io/dingo/pull/2474))
* Emphasized **expect safer recovery when a peer keeps returning no blocks:** Nodes can now move away from peers that get stuck in repeated empty block loops, which helps sync resume more reliably. ([#2473](https://github.com/blinklabs-io/dingo/pull/2473))
* Summarized **trust steadier next epoch readiness handling during slot progress:** Next epoch readiness work no longer repeats after the node has already signaled readiness, which reduces avoidable repeated work during slot progress. ([#2475](https://github.com/blinklabs-io/dingo/pull/2475))
* Reviewed **rely on more accurate chain density signals after recovery events:** Chain density reporting now stays more accurate through restart and rollback paths, which makes health monitoring easier to trust. ([#2477](https://github.com/blinklabs-io/dingo/pull/2477))

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.50.0 (June 1, 2026)

**Title:** Add browser-friendly APIs, improve catch-up visibility, and strengthen reliability

**Date:** June 1, 2026

**Version:** v0.50.0

Hi folks! Here’s what we shipped in v0.50.0.

### ✨ What's New

* Added **configure CORS behavior for Dingo APIs:** API operators can now manage cross origin access more directly, which makes browser based integrations easier to expose with the intended policy. ([#2456](https://github.com/blinklabs-io/dingo/pull/2456))
* Introduced **measure backfill progress with clearer timing signals:** Operators can now see timing counters for critical backfill work, which makes catch up behavior easier to observe and tune. ([#2449](https://github.com/blinklabs-io/dingo/pull/2449))
* Expanded **speed large Mithril imports by delaying heavy lookup preparation:** Large Mithril imports now postpone expensive lookup preparation until the right point in the load process, which helps imports move more smoothly. ([#2450](https://github.com/blinklabs-io/dingo/pull/2450))
* Delivered **show clearer diagnostics when block production checks fail:** Forging logs now surface clearer diagnostics when a newly forged block fails a final verification step, which makes failure analysis faster. ([#2464](https://github.com/blinklabs-io/dingo/pull/2464))

### 💪 Improvements

* Improved **refresh example dependencies for smoother setup:** Example projects now use refreshed npm dependencies, which helps local setup and repeatable example runs stay current. ([#2451](https://github.com/blinklabs-io/dingo/pull/2451))
* Refined **use production-style storage in the governance example stack:** The governance example now uses Postgres for stored chain data, which makes the example closer to practical deployments. ([#2447](https://github.com/blinklabs-io/dingo/pull/2447))
* Enhanced **wait for Mithril before starting the governance example stack:** The governance example compose flow now waits for Mithril readiness, which makes example startup more dependable. ([#2454](https://github.com/blinklabs-io/dingo/pull/2454))
* Updated **clarify archive and pruning modes with fresher usage guidance:** Archive and pruning documentation now explains operational choices more clearly, which makes deployment planning easier. ([#2455](https://github.com/blinklabs-io/dingo/pull/2455))
* Modernized **streamline publish automation around supported artifacts:** Release automation no longer packages npm artifacts, which keeps publish work aligned with the current distribution model. ([#2452](https://github.com/blinklabs-io/dingo/pull/2452))
* Strengthened **refresh routine dependency maintenance across the project:** Routine dependency updates keep build, runtime, and integration support current for steadier ongoing compatibility. ([#2465](https://github.com/blinklabs-io/dingo/pull/2465), [#2466](https://github.com/blinklabs-io/dingo/pull/2466), [#2467](https://github.com/blinklabs-io/dingo/pull/2467), [#2468](https://github.com/blinklabs-io/dingo/pull/2468), [#2469](https://github.com/blinklabs-io/dingo/pull/2469), [#2470](https://github.com/blinklabs-io/dingo/pull/2470), [#2471](https://github.com/blinklabs-io/dingo/pull/2471))

### 🔧 Fixes

* Fixed **delay asset indexing more safely during historical catch up:** Asset indexing now follows a safer delayed path, which reduces indexing trouble during heavy historical processing. ([#2461](https://github.com/blinklabs-io/dingo/pull/2461))
* Corrected **keep transaction execution limits aligned with actual work:** Transaction budget handling now tracks execution costs more accurately, which improves result correctness for affected transactions. ([#2453](https://github.com/blinklabs-io/dingo/pull/2453))
* Stabilized **recover local storage more reliably after problems:** Local storage recovery now handles damaged or interrupted states more defensively, which improves restart reliability. ([#2472](https://github.com/blinklabs-io/dingo/pull/2472))

### 📋 What You Need to Know

* Clarified **follow normal upgrade steps for this release:** Normal upgrade steps are generally sufficient for this release.
* Highlighted **configure browser access for API deployments more directly:** API users can now configure CORS support, which makes browser based integrations easier to expose with the intended access policy. ([#2456](https://github.com/blinklabs-io/dingo/pull/2456))
* Emphasized **expect better visibility and smoother behavior during Mithril and API catch up:** Large Mithril imports now delay heavy lookup preparation, and API backfill exposes timing counters that make import and catch up work easier to understand. ([#2450](https://github.com/blinklabs-io/dingo/pull/2450), [#2449](https://github.com/blinklabs-io/dingo/pull/2449))
* Summarized **review refreshed examples and archive guidance before deployment changes:** The governance example stack, archive and pruning documentation, and publish workflow guidance were refreshed, which makes current operational guidance easier to follow. ([#2447](https://github.com/blinklabs-io/dingo/pull/2447), [#2454](https://github.com/blinklabs-io/dingo/pull/2454), [#2455](https://github.com/blinklabs-io/dingo/pull/2455), [#2452](https://github.com/blinklabs-io/dingo/pull/2452))

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.49.0 (May 27, 2026)

**Title:** Leios merged delivery, reward-state persistence, and sturdier sync handling

**Date:** May 27, 2026

**Version:** v0.49.0

Hi folks! Here’s what we shipped in v0.49.0.

### ✨ What's New

* Added **deliver merged Leios data to local integrations:** Integrators can now receive merged Leios input data through local integrations, which makes Leios data access more complete. ([#2392](https://github.com/blinklabs-io/dingo/pull/2392))
* Introduced **keep reward state across restarts:** Nodes now preserve reward state across restarts, which makes recovery and ongoing operations more dependable. ([#2426](https://github.com/blinklabs-io/dingo/pull/2426))
* Expanded **honor CIP-1694 reward account delegation semantics:** Governance and reward handling now follow CIP-1694 stake pool operator reward account delegation semantics more closely, which keeps delegation behavior aligned with current expectations. ([#2378](https://github.com/blinklabs-io/dingo/pull/2378))

### 💪 Improvements

* Improved **show Dingoswap flows with a practical integration example:** Example guidance now demonstrates a Dingoswap integration path more clearly, which makes common integration patterns easier to understand and reuse. ([#2390](https://github.com/blinklabs-io/dingo/pull/2390))
* Refined **keep the preview example stack current:** The Dingo Sundae preview example now uses refreshed build dependencies, which helps example runs stay smoother and easier to maintain. ([#2431](https://github.com/blinklabs-io/dingo/pull/2431))
* Enhanced **publish the v0.48.0 release history entry:** The published release history now includes the previous v0.48.0 entry, which keeps recent release context easier to review. ([#2423](https://github.com/blinklabs-io/dingo/pull/2423))

### 🔧 Fixes

* Fixed **keep transaction submission responses compatible with cardano cli 10.x:** Transaction submission rejections now return a format that works more reliably with cardano cli 10.x, which makes submission troubleshooting clearer. ([#2427](https://github.com/blinklabs-io/dingo/pull/2427))
* Corrected **clear stale header tracking during normal chainsync work:** Normal chainsync operation now clears stale header tracking more consistently, which helps sync behavior stay steadier over time. ([#2428](https://github.com/blinklabs-io/dingo/pull/2428))
* Strengthened **surface async chainsync failures more clearly:** Roll forward and roll backward failures now surface more directly, which makes chainsync problems easier to detect and diagnose. ([#2419](https://github.com/blinklabs-io/dingo/pull/2419))

### 📋 What You Need to Know

* Clarified **expect normal upgrade steps for this release:** Normal upgrade steps are generally sufficient for this release.
* Highlighted **expect better cardano cli 10.x submission compatibility:** Transaction submission rejection handling now works more cleanly with cardano cli 10.x, which reduces avoidable submission confusion. ([#2427](https://github.com/blinklabs-io/dingo/pull/2427))
* Emphasized **trust sturdier chainsync behavior and clearer failure reporting:** Chainsync now clears stale header tracking during normal operation and surfaces async sync failures more clearly, which supports steadier recovery and troubleshooting. ([#2428](https://github.com/blinklabs-io/dingo/pull/2428), [#2419](https://github.com/blinklabs-io/dingo/pull/2419))
* Summarized **rely on persisted rewards and updated delegation semantics:** Reward state now persists across restarts, and reward account delegation handling now follows CIP-1694 semantics more closely. ([#2426](https://github.com/blinklabs-io/dingo/pull/2426), [#2378](https://github.com/blinklabs-io/dingo/pull/2378))
* Reviewed **use the refreshed example and release history updates:** The Dingoswap example and the published v0.48.0 release history entry make current integration guidance and recent release context easier to follow. ([#2390](https://github.com/blinklabs-io/dingo/pull/2390), [#2423](https://github.com/blinklabs-io/dingo/pull/2423), [#2431](https://github.com/blinklabs-io/dingo/pull/2431))

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.48.0 (May 26, 2026)

**Title:** Backfill tuning, Mithril diagnostics, and sturdier sync recovery

**Date:** May 26, 2026

**Version:** v0.48.0

Hi folks! Here’s what we shipped in v0.48.0.

### ✨ What's New

* Added **tune historical backfill batch size more directly:** Operators can now adjust backfill batch size to better balance throughput and memory usage during historical indexing. ([#2389](https://github.com/blinklabs-io/dingo/pull/2389))
* Introduced **inspect Mithril sync with a dedicated debug port:** Operators can now use a debug and profiling port during Mithril sync, which makes long running sync behavior easier to inspect and troubleshoot. ([#2413](https://github.com/blinklabs-io/dingo/pull/2413))

## v0.48.0 (May 26, 2026)

**Title:** Backfill tuning, Mithril diagnostics, and sturdier sync recovery

**Date:** May 26, 2026

**Version:** v0.48.0

Hi folks! Here’s what we shipped in v0.48.0.

### ✨ What's New

* Added **tune historical backfill batch size more directly:** Operators can now adjust backfill batch size to better balance throughput and memory usage during historical indexing. ([#2389](https://github.com/blinklabs-io/dingo/pull/2389))
* Introduced **inspect Mithril sync with a dedicated debug port:** Operators can now use a debug and profiling port during Mithril sync, which makes long running sync behavior easier to inspect and troubleshoot. ([#2413](https://github.com/blinklabs-io/dingo/pull/2413))

### 💪 Improvements

* Improved **keep architecture guidance aligned with the current system:** Refreshed architecture documentation makes contributor and operator guidance easier to follow as the system evolves. ([#2391](https://github.com/blinklabs-io/dingo/pull/2391))
* Refined **keep HTTP handling aligned with current upstream expectations:** Updated HTTP handling now stays aligned with current upstream behavior, which supports steadier service operation. ([#2417](https://github.com/blinklabs-io/dingo/pull/2417))
* Enhanced **refresh build, runtime, and release dependencies:** Routine maintenance updates keep core build and runtime components current, which supports steadier compatibility across release workflows and supported integrations. ([#2405](https://github.com/blinklabs-io/dingo/pull/2405), [#2411](https://github.com/blinklabs-io/dingo/pull/2411), [#2406](https://github.com/blinklabs-io/dingo/pull/2406), [#2407](https://github.com/blinklabs-io/dingo/pull/2407), [#2412](https://github.com/blinklabs-io/dingo/pull/2412), [#2410](https://github.com/blinklabs-io/dingo/pull/2410), [#2409](https://github.com/blinklabs-io/dingo/pull/2409), [#2345](https://github.com/blinklabs-io/dingo/pull/2345))

### 🔧 Fixes

* Fixed **make Mithril downloads recover more defensively:** Mithril download handling now responds more safely to failure conditions, which helps long running sync work recover more reliably. ([#2387](https://github.com/blinklabs-io/dingo/pull/2387))
* Corrected **reduce memory pressure during heavier backfill work:** Database backfill now uses memory more efficiently, which helps heavier historical indexing runs behave more smoothly. ([#2415](https://github.com/blinklabs-io/dingo/pull/2415))
* Strengthened **keep ledger state reads more reliable during table parsing:** Ledger UTxO table parsing now behaves more reliably, which improves ledger state correctness during recovery and indexing flows. ([#2414](https://github.com/blinklabs-io/dingo/pull/2414))
* Stabilized **extract invalid transaction indexes during streaming work:** Invalid transaction index extraction now completes more reliably during streamed processing. ([#2416](https://github.com/blinklabs-io/dingo/pull/2416))
* Hardened **avoid races near the chain tip iterator:** Near tip chain traversal now behaves more steadily, which improves stability during recovery and indexing close to the tip. ([#2422](https://github.com/blinklabs-io/dingo/pull/2422))

### 📋 What You Need to Know

* Clarified **follow normal upgrade steps for this release:** Normal upgrade steps are generally sufficient for this release.
* Highlighted **tune backfill work for smoother historical indexing:** Operators can now adjust backfill batch size to balance resource usage more directly, and reduced backfill memory pressure should help heavier historical runs stay smoother. ([#2389](https://github.com/blinklabs-io/dingo/pull/2389), [#2415](https://github.com/blinklabs-io/dingo/pull/2415))
* Emphasized **troubleshoot Mithril sync with clearer visibility:** Mithril sync is easier to inspect because a dedicated debug port is available during sync, and download handling is more defensive during recovery. ([#2413](https://github.com/blinklabs-io/dingo/pull/2413), [#2387](https://github.com/blinklabs-io/dingo/pull/2387))
* Summarized **expect steadier ledger, backfill, and near tip recovery behavior:** Iterator, ledger parsing, and invalid index extraction fixes reduce several common sources of recovery and indexing trouble. ([#2422](https://github.com/blinklabs-io/dingo/pull/2422), [#2414](https://github.com/blinklabs-io/dingo/pull/2414), [#2416](https://github.com/blinklabs-io/dingo/pull/2416))

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.48.0 (May 26, 2026)

**Title:** Backfill tuning, Mithril diagnostics, and sturdier sync recovery

**Date:** May 26, 2026

**Version:** v0.48.0

Hi folks! Here’s what we shipped in v0.48.0.

### ✨ What's New

* Added **tune historical backfill batch size more directly:** Operators can now adjust backfill batch size to better balance throughput and memory usage during historical indexing. ([#2389](https://github.com/blinklabs-io/dingo/pull/2389))
* Introduced **inspect Mithril sync with a dedicated debug port:** Operators can now use a debug and profiling port during Mithril sync, which makes long running sync behavior easier to inspect and troubleshoot. ([#2413](https://github.com/blinklabs-io/dingo/pull/2413))

## v0.48.0 (May 26, 2026)

**Title:** Backfill tuning, Mithril diagnostics, and sturdier sync recovery

**Date:** May 26, 2026

**Version:** v0.48.0

Hi folks! Here’s what we shipped in v0.48.0.

### ✨ What's New

* Added **tune historical backfill batch size more directly:** Operators can now adjust backfill batch size to better balance throughput and memory usage during historical indexing. ([#2389](https://github.com/blinklabs-io/dingo/pull/2389))
* Introduced **inspect Mithril sync with a dedicated debug port:** Operators can now use a debug and profiling port during Mithril sync, which makes long running sync behavior easier to inspect and troubleshoot. ([#2413](https://github.com/blinklabs-io/dingo/pull/2413))

### 💪 Improvements

* Improved **keep architecture guidance aligned with the current system:** Refreshed architecture documentation keeps contributor and operator guidance aligned with the current system. ([#2391](https://github.com/blinklabs-io/dingo/pull/2391))
* Refined **keep HTTP handling aligned with current upstream expectations:** Updated HTTP handling stays aligned with current upstream expectations, which supports steadier service behavior. ([#2417](https://github.com/blinklabs-io/dingo/pull/2417))
* Enhanced **refresh build and runtime maintenance dependencies:** Dependency and CI maintenance updates keep important build and runtime components current, which supports steadier maintenance and release workflows. ([#2405](https://github.com/blinklabs-io/dingo/pull/2405), [#2411](https://github.com/blinklabs-io/dingo/pull/2411), [#2406](https://github.com/blinklabs-io/dingo/pull/2406), [#2407](https://github.com/blinklabs-io/dingo/pull/2407), [#2412](https://github.com/blinklabs-io/dingo/pull/2412), [#2410](https://github.com/blinklabs-io/dingo/pull/2410), [#2409](https://github.com/blinklabs-io/dingo/pull/2409), [#2345](https://github.com/blinklabs-io/dingo/pull/2345))

### 🔧 Fixes

* Fixed **make Mithril downloads more defensive during recovery:** Mithril download handling now behaves more defensively, which helps long running sync work recover more reliably. ([#2387](https://github.com/blinklabs-io/dingo/pull/2387))
* Corrected **reduce memory pressure during heavier backfill work:** Database backfill now uses memory more efficiently, which helps heavier historical indexing runs behave more smoothly. ([#2415](https://github.com/blinklabs-io/dingo/pull/2415))
* Strengthened **keep ledger state parsing more reliable during recovery:** Ledger UTxO table parsing now behaves more reliably, which improves ledger state correctness during recovery and indexing work. ([#2414](https://github.com/blinklabs-io/dingo/pull/2414))
* Stabilized **extract invalid transaction indexes during streamed processing:** Invalid transaction index extraction now behaves more reliably during streamed processing. ([#2416](https://github.com/blinklabs-io/dingo/pull/2416))
* Hardened **avoid races near the chain tip iterator:** Near tip chain traversal now behaves more steadily, which improves stability during recovery and indexing close to the tip. ([#2422](https://github.com/blinklabs-io/dingo/pull/2422))

### 📋 What You Need to Know

* Clarified **follow normal upgrade steps for this release:** Normal upgrade steps are generally sufficient for this release.
* Highlighted **tune backfill work to balance resource usage more directly:** Operators can now adjust backfill batch size to better balance resource usage during historical indexing, and reduced backfill memory pressure should help heavier runs behave more smoothly. ([#2389](https://github.com/blinklabs-io/dingo/pull/2389), [#2415](https://github.com/blinklabs-io/dingo/pull/2415))
* Emphasized **inspect Mithril sync with clearer troubleshooting visibility:** Mithril troubleshooting is easier because a dedicated debug and profiling port is available during sync, and download handling is more defensive during recovery. ([#2413](https://github.com/blinklabs-io/dingo/pull/2413), [#2387](https://github.com/blinklabs-io/dingo/pull/2387))
* Summarized **expect steadier near tip and ledger backfill behavior:** Iterator, parsing, and invalid-index extraction fixes reduce several common sources of recovery and indexing trouble around near tip and ledger backfill work. ([#2422](https://github.com/blinklabs-io/dingo/pull/2422), [#2414](https://github.com/blinklabs-io/dingo/pull/2414), [#2416](https://github.com/blinklabs-io/dingo/pull/2416))

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.47.1 (May 22, 2026)

**Title:** Safer plateau recovery, correct governance refunds, and hydrated snapshot imports

**Date:** May 22, 2026

**Version:** v0.47.1

Hi folks! Here’s what we shipped in v0.47.1.

### ✨ What's New

* Noted **no new features in this patch release:** This patch release focuses on reliability and correctness updates. ([#2377](https://github.com/blinklabs-io/dingo/pull/2377))

### 💪 Improvements

* Improved **keep governance rewards and refunds on active reward accounts only:** Governance reward payouts and expired proposal deposit refunds now credit only active existing reward accounts, while unclaimed amounts return safely to treasury and treasury withdrawal capacity stays enforced across the epoch. ([#2371](https://github.com/blinklabs-io/dingo/pull/2371))

### 🔧 Fixes

* Corrected **preserve snapshot imported transaction output history during bulk import:** Snapshot imports now keep the origin and added slot details for imported transaction outputs, which makes historical lookups and ordering more accurate after bulk import. ([#2364](https://github.com/blinklabs-io/dingo/pull/2364))
* Strengthened **recover same slot fork plateaus without manual restarts:** Nodes now reconcile live ledger divergence before recycling peers, which helps same slot fork stalls recover more safely without requiring a manual restart. ([#2377](https://github.com/blinklabs-io/dingo/pull/2377))

### 📋 What You Need to Know

* Clarified **follow normal patch release upgrade steps:** Normal patch release upgrade steps are generally sufficient for this release.
* Highlighted **expect safer recovery from same slot fork plateaus:** Nodes can now recover from same slot fork plateaus more safely without manual restarts because live ledger reconciliation happens before peer recycling. ([#2377](https://github.com/blinklabs-io/dingo/pull/2377))
* Emphasized **trust corrected governance refunds and treasury handling:** Missing or inactive reward accounts no longer receive governance rewards or expired proposal deposit refunds, and unclaimed amounts return to treasury while treasury withdrawal limits stay enforced across the epoch. ([#2371](https://github.com/blinklabs-io/dingo/pull/2371))
* Summarized **expect more accurate snapshot imported history after bulk import:** Snapshot imported transaction outputs now keep provenance and added slot details more accurately, which improves historical behavior after bulk import. ([#2364](https://github.com/blinklabs-io/dingo/pull/2364))

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.47.0 (May 21, 2026)

**Title:** Mithril sync resilience, safer chainsync handling, and reverse chain traversal

**Date:** May 21, 2026

**Version:** v0.47.0

Hi folks! Here’s what we shipped in v0.47.0.

### ✨ What's New

* Added **traverse chain history in reverse order:** Operators and integrators can now walk chain history backward more directly, which makes historical inspection and recovery work easier to manage. ([#2318](https://github.com/blinklabs-io/dingo/pull/2318))
* Introduced **tune chainsync stall timeouts:** Operators can now configure how quickly stalled chainsync work triggers recovery, which makes sync behavior easier to adapt to different network conditions. ([#2319](https://github.com/blinklabs-io/dingo/pull/2319))
* Expanded **recover Mithril downloads after idle stalls:** Mithril sync can now detect idle download stalls and resume work more safely, which makes long running bootstrap and backfill flows more resilient. ([#2365](https://github.com/blinklabs-io/dingo/pull/2365))

### 💪 Improvements

* Improved **speed address lookup work during API backfill:** API mode backfill now finds input addresses with less overhead, which helps historical indexing move more efficiently. ([#2363](https://github.com/blinklabs-io/dingo/pull/2363))
* Refined **surface SQLite planning details before and during resumed backfill:** Operators can now see SQLite planning statistics before API mode backfill starts and when it resumes, which makes backfill behavior easier to understand and troubleshoot. ([#2367](https://github.com/blinklabs-io/dingo/pull/2367))
* Enhanced **avoid duplicate transaction output offset writes during Mithril backfill:** Mithril API mode backfill now skips repeated produced transaction output offset writes, which reduces unnecessary storage work during historical catch up. ([#2368](https://github.com/blinklabs-io/dingo/pull/2368))
* Updated **broaden governance tally and enactment regression coverage:** Governance related validation now checks tally and enactment behavior more thoroughly, which increases confidence in governance tracking during upgrades. ([#2370](https://github.com/blinklabs-io/dingo/pull/2370))
* Modernized **publish the v0.46.4 release history entry:** The repository now includes the v0.46.4 release notes entry in its published release history, which keeps recent patch context easier to review. ([#2362](https://github.com/blinklabs-io/dingo/pull/2362))

### 🔧 Fixes

* Fixed **preserve ordering for ledger sync events:** Ordering critical ledger sync events now arrive through blocking delivery, which makes downstream consumers less likely to miss important sequence changes. ([#2366](https://github.com/blinklabs-io/dingo/pull/2366))
* Corrected **end stalled transaction wait streams with a clear deadline:** Transaction wait requests now stop with a server side timeout instead of hanging indefinitely, which makes stalled request handling easier to detect. ([#2379](https://github.com/blinklabs-io/dingo/pull/2379))
* Strengthened **choose Mithril trust boundary intersect points more reliably:** Mithril recovery now includes the trust boundary intersect point when picking where to resume, which makes restore and recovery decisions safer. ([#2358](https://github.com/blinklabs-io/dingo/pull/2358))
* Stabilized **retain spent transaction outputs for historical queries during pruning:** API storage mode now keeps spent transaction outputs available through pruning, which helps historical queries remain reliable over time. ([#2361](https://github.com/blinklabs-io/dingo/pull/2361))

### 📋 What You Need to Know

* Clarified **follow normal upgrade steps for this release:** Normal upgrade steps are generally sufficient for this release.
* Highlighted **expect steadier Mithril and API backfill behavior:** Mithril and API mode backfill should be more resilient and more efficient because downloads recover from stalls, SQLite planning improves before backfill, and redundant transaction output offset writes are avoided.
* Emphasized **trust safer sync recovery and event handling:** Operators and integrators should expect safer sync behavior because chainsync stall detection is configurable, ordering critical ledger sync events no longer rely on lossy delivery, and Mithril trust boundary point selection is more reliable.
* Summarized **rely on stronger historical query behavior:** API mode historical queries should remain more reliable because spent transaction outputs stay available through pruning, and transaction wait requests now fail with a clear server side deadline instead of hanging indefinitely.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.46.4 (May 18, 2026)

**Title:** Genesis bootstrap expansion, Mithril sync visibility, and sturdier SQLite metadata handling

**Date:** May 18, 2026

**Version:** v0.46.4

Hi folks! Here’s what we shipped in v0.46.4.

### ✨ What's New

* Added **expand genesis bootstrap across MySQL and Postgres deployments:** Networks that use MySQL or Postgres can now start with genesis staking and governance state already available, which gives non mainnet deployments a more complete view on networks that depend on genesis rooted state. ([#2331](https://github.com/blinklabs-io/dingo/pull/2331))
* Introduced **surface Mithril sync progress through metrics visibility:** Operators can now follow long running Mithril sync work through Mithril metrics and a dedicated metrics server, which makes sync progress easier to observe. ([#2341](https://github.com/blinklabs-io/dingo/pull/2341))

### 💪 Improvements

* Improved **speed batched SQLite UTxO lookups and updates:** SQLite heavy lookup and update paths now use the `tx_id_output_idx` path more consistently, which helps batched UTxO work finish more smoothly. ([#2342](https://github.com/blinklabs-io/dingo/pull/2342))
* Refined **keep S3 integrations aligned with newer client support:** S3 backed workflows now stay aligned with the refreshed AWS S3 client, which supports steadier compatibility and maintenance. ([#2348](https://github.com/blinklabs-io/dingo/pull/2348))
* Enhanced **keep Google API integrations current:** Google API integrations now stay aligned with a newer client release, which supports ongoing compatibility and maintenance. ([#2347](https://github.com/blinklabs-io/dingo/pull/2347))
* Updated **keep bitcoin utility support current:** Bitcoin related utility support now stays aligned with a newer upstream release, which improves compatibility maintenance. ([#2346](https://github.com/blinklabs-io/dingo/pull/2346))
* Modernized **publish the v0.46.3 release history entry:** The repository now includes the v0.46.3 release notes entry in its published release history, which makes recent patch release context easier to review. ([#2340](https://github.com/blinklabs-io/dingo/pull/2340))

### 🔧 Fixes

* Fixed **keep genesis rooted history consistent across supported databases:** Account history and rollback related paths now preserve visibility for genesis rooted accounts and delegations more consistently across MySQL, Postgres, and SQLite backends. ([#2344](https://github.com/blinklabs-io/dingo/pull/2344))
* Corrected **handle high value transaction metadata labels more reliably:** Supported databases now keep high bit transaction metadata labels readable, and SQLite handles the affected metadata and lookup paths more reliably. ([#2357](https://github.com/blinklabs-io/dingo/pull/2357))

### 📋 What You Need to Know

* Clarified **follow normal patch release upgrade steps:** Normal patch release upgrade steps are generally sufficient for this release.
* Highlighted **expect broader genesis bootstrap support on non mainnet SQL deployments:** Non mainnet networks that use MySQL or Postgres now gain genesis bootstrap support and more consistent genesis history handling.
* Emphasized **watch Mithril sync progress and trust steadier SQLite query paths:** Mithril sync now exposes Prometheus visible progress, and SQLite heavy backfill and query workloads should see better behavior from the indexing and metadata fixes.
* Summarized **review refreshed integrations and updated release history:** Dependency refreshes keep S3, Google API, and bitcoin utility support current, and the repository release history now includes the v0.46.3 notes.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.46.3 (May 17, 2026)

**Title:** Faster governance and stake queries, sturdier plateau recovery, and broader rollback test coverage

**Date:** May 17, 2026

**Version:** v0.46.3

Hi folks! Here’s what we shipped in v0.46.3.

### ✨ What's New

* Noted **no new features:** This patch release focuses on improvements, fixes, and documentation updates.

### 💪 Improvements

* Improved **expand rollback conformance coverage with ouroboros-mock v0.11.0:** Rollback validation now covers a synthetic rollback path and replay fixes, which broadens rollback coverage without requiring adapter changes. ([#2330](https://github.com/blinklabs-io/dingo/pull/2330))
* Refined **clarify RTS metrics guidance in place:** The code comment now explains how Go memory reporting maps to the matching Haskell runtime view, which makes the metrics guidance more accurate and self contained. ([#2334](https://github.com/blinklabs-io/dingo/pull/2334))
* Updated **surface the v0.46.2 release details more clearly:** Operators can now review the v0.46.2 release information alongside newer patch releases for clearer recent upgrade context. ([#2329](https://github.com/blinklabs-io/dingo/pull/2329))

### 🔧 Fixes

* Fixed **reconnect plateau recovery with a fresh chainsync session:** Post-plateau peer realignment now starts a fresh chainsync connection, so nodes do not reuse stale cursors during recovery. ([#2337](https://github.com/blinklabs-io/dingo/pull/2337))
* Corrected **speed governance voting-power and delegate lookups:** New composite indexes reduce query cost for governance voting-power requests and related DRep and staking-key lookups. ([#2332](https://github.com/blinklabs-io/dingo/pull/2332))
* Strengthened **accelerate stake-by-pool lookups:** A new composite account index speeds active pool and staking-key lookups used by stake-by-pool queries. ([#2335](https://github.com/blinklabs-io/dingo/pull/2335))

### 📋 What You Need to Know

* Clarified **follow normal patch release upgrade steps:** Normal upgrade procedures are generally sufficient for this release.
* Highlighted **expect more efficient governance and stake by pool queries:** New composite indexes help governance and stake by pool lookups run more efficiently.
* Emphasized **trust safer recovery and broader rollback validation:** Plateau recovery now reconnects with a fresh chainsync session, and the `ouroboros-mock` update broadens rollback conformance coverage.
* Summarized **expect clearer metrics guidance and recent patch context:** Metrics guidance is now more accurate in place, and v0.46.2 release information stays available for recent patch context.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.46.2 (May 15, 2026)

**Title:** Mithril trust-boundary protection, safer peer recovery, and sturdier rollback handling

**Date:** May 15, 2026

**Version:** v0.46.2

Hi folks! Here’s what we shipped in v0.46.2.

### ✨ What's New

* Added **record forged-block signals before chain adoption completes:** Operators can now see forged-block metrics and events as soon as a block is built, which improves observability even when chain adoption later fails. ([#2323](https://github.com/blinklabs-io/dingo/pull/2323))
* Introduced **tune chainsync block timeout and safer reconnect resets:** Operators can now configure the NtN chainsync block timeout, and peer reconnect handling now clears connection timing and outbound retry state only when the connection path truly requires it. ([#2316](https://github.com/blinklabs-io/dingo/pull/2316))

### 💪 Improvements

* Improved **refresh upstream compatibility with plutigo v0.1.13:** Deployments now stay aligned with the newer upstream support in `plutigo`, which keeps compatibility and routine maintenance steadier. ([#2327](https://github.com/blinklabs-io/dingo/pull/2327))
* Refined **tighten default era-test coverage selection:** Default erastest runs now exclude vanRossem unless an explicit run filter is supplied, which keeps routine validation focused unless broader coverage is intentionally requested. ([#2325](https://github.com/blinklabs-io/dingo/pull/2325))
* Enhanced **refresh protocol-library compatibility with gouroboros v0.170.1:** Protocol support now stays aligned with the newer upstream `gouroboros` release, which supports ongoing compatibility and maintenance work. ([#2321](https://github.com/blinklabs-io/dingo/pull/2321))
* Updated **modernize dependency-policy checks in CI:** Continuous integration now uses the newer `gomodguard_v2` policy checks, which keeps dependency-policy maintenance current. ([#2320](https://github.com/blinklabs-io/dingo/pull/2320))

### 🔧 Fixes

* Fixed **enforce Mithril trust boundaries during rollback recovery:** Mithril-restored nodes now treat the trusted snapshot boundary as a hard recovery limit, skip header-only verification at or below that boundary, and rebuild post-restore transaction metadata safely after restore. ([#2326](https://github.com/blinklabs-io/dingo/pull/2326))
* Corrected **deny divergent peers before they can loop recovery:** Nodes now temporarily deny-list divergent peers, close denied inbound peers, and enforce those denials across more connection paths, which reduces rollback loops and reconnect churn. ([#2324](https://github.com/blinklabs-io/dingo/pull/2324))
* Strengthened **recover live rollback divergence from a safe shared ancestor:** Live rollback reconciliation can now bring the primary chain and ledger back to a safe common ancestor, which improves recovery from over-K rollback and fork-divergence scenarios. ([#2322](https://github.com/blinklabs-io/dingo/pull/2322))
* Stabilized **close databases safely during repeated shutdown paths:** Repeated shutdown or cleanup calls no longer trigger duplicate-close failures because `Database.Close` now behaves safely when called more than once. ([#2315](https://github.com/blinklabs-io/dingo/pull/2315))

### 📋 What You Need to Know

* Clarified **trust the Mithril restore boundary during recovery:** Mithril-restored nodes now treat the trusted snapshot boundary as a hard rollback and replay limit, and they rebuild post-restore transaction metadata from restored block data after recovery completes.
* Highlighted **safer divergent-peer recovery paths:** Nodes now temporarily deny bad peers during divergence recovery, and live rollback reconciliation can recover more safely by returning the primary chain and ledger to a shared safe ancestor.
* Emphasized **stronger forging visibility and connection control:** Forged-block metrics and events now appear as soon as a block is built, and operators can tune the chainsync block timeout more directly.
* Summarized **broader maintenance refreshes:** Dependency compatibility, validation defaults, dependency-policy checks, and database shutdown handling all received updates in this release.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.46.1 (May 14, 2026)

**Title:** Conway genesis governance bootstrap, safer startup recovery, and networking fixes

**Date:** May 14, 2026

**Version:** v0.46.1

Hi folks! Here’s what we shipped in v0.46.1.

### ✨ What's New

* Added **bootstrap Conway governance from genesis settings:** Networks that define initial governance delegates and voter registrations now start with that governance state already in place, which gives governance aware services a more complete view from the first block.

### 💪 Improvements

* Improved **surface clearer governance stability signals:** Operators can now see when stored governance proposals fail to decode during stability checks, which makes governance troubleshooting easier and keeps transition monitoring clearer.
* Refined **pick up devnet configurator changes at startup:** Devnet runs now rebuild the configurator image before launch, which helps environment setup reflect the latest configuration behavior more reliably.
* Enhanced **steady hard-fork readiness checks in tests:** Release validation now waits for hard-fork readiness checks to finish before asserting results, which makes test outcomes more dependable.
* Updated **clarify mempool and UTxO RPC package guidance:** Embedded package guidance now explains transaction handling and UTxO filtering more clearly, which makes the exposed behavior easier to understand.

### 🔧 Fixes

* Fixed **stop block producers when genesis snapshots fail:** Block producers now stop immediately if startup cannot build the required initial snapshot, which helps prevent unsafe forging starts.
* Corrected **recover startup state from the right common chain point:** Startup recovery now rolls back to the latest shared chain point when local state no longer matches the selected chain, which helps nodes resume more cleanly after interrupted or divergent starts.
* Strengthened **honor peer-sharing settings consistently:** Nodes now apply peer-sharing choices correctly across connections, which makes network behavior match the configured role more reliably.
* Stabilized **return startup errors instead of crashing on missing services:** Startup now reports missing required services as ordinary errors, which makes failed launches easier to diagnose and recover.
* Hardened **respect real epoch boundaries during leader scheduling:** Block production scheduling now follows the active epoch layout more accurately, which helps leaders avoid incorrect scheduling around changing epoch boundaries.
* Repaired **restore pool state details more accurately:** Pool state recovery now reads stored pool details more reliably, which helps stake pool views stay complete after startup and rollback work.
* Prevented **misread offset data from Byron boundary blocks:** Nodes now skip offset indexing for Byron epoch boundary blocks that carry no transactions, which avoids false indexing results during historical processing.
* Secured **start UTxO RPC TLS sessions without reloading files:** UTxO RPC now keeps the loaded TLS certificate in memory for server startup, which makes secure startup behavior more reliable.

### 📋 What You Need to Know

* Clarified **genesis governance now starts in place:** Networks that define initial governance delegates and voter registrations now begin with that governance state already loaded, which makes early governance visibility more complete from startup.
* Highlighted **safer startup recovery and clearer failure handling:** Block producers now stop when required startup snapshots fail, startup reports missing required services as ordinary errors, and divergent startup state now rolls back to the right shared chain point more cleanly.
* Emphasized **more reliable scheduling and peer behavior:** Block production scheduling now respects the active epoch layout more accurately, and peer sharing behavior now follows the configured role more consistently across connections.
* Summarized **steadier operational support paths:** Governance observability, pool state recovery, historical offset handling, secure UTxO RPC startup, and devnet validation all received updates that make operations easier to trust.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.46.0 (May 13, 2026)

**Title:** Stake account APIs, cardano-node Praos selection, and safer sync recovery

**Date:** May 13, 2026

**Version:** v0.46.0

Hi folks! Here’s what we shipped in v0.46.0.

### ✨ What's New

* Added **expand Blockfrost stake-account coverage:** Blockfrost compatible services can now return stake-account details, associated addresses, delegation history, registration history, and reward history through stake-address endpoints.
* Introduced **surface chain block-proposed events:** Locally forged blocks now follow a dedicated proposal path before the chain accepts them, which gives operators clearer behavior around block adoption.
* Expanded **track the highest observed pool certificate sequence:** Block producers can now compare the newest observed pool certificate sequence during startup, which makes outdated certificate counters easier to catch before they cause trouble.

### 💪 Improvements

* Improved **raise the default Leios mempool capacity:** Leios mode now starts with a larger default mempool capacity, which gives transaction-heavy runs more room before the queue fills.
* Refined **reduce SQLite batch allocation churn:** SQLite metadata backfill now reuses batch storage more efficiently, which helps sustained indexing work run more smoothly.
* Enhanced **quiet routine NoBlocks log noise:** Routine blockfetch responses that return no blocks now stay at debug level, which keeps normal logs easier to scan for real issues.
* Updated **strengthen chain-iterator regression coverage:** Regression checks now cover more iterator ordering, rollback, and waiting paths, which increases confidence in chain recovery behavior.
* Clarified **explain Mithril API-mode backfill behavior more clearly:** API mode guidance now states that historical metadata backfill runs after Mithril snapshot import, which makes full-history expectations easier to understand.

### 🔧 Fixes

* Fixed **align Praos chain selection with cardano-node behavior:** Equal-height chain choices now follow cardano-node tie breaking more closely, which helps nodes converge more reliably during competing forges.
* Corrected **refresh connections after unrecoverable sync failures:** Nodes now drop affected sync connections and start fresh after security-window or unrecoverable recovery failures, which gives sync recovery a cleaner restart path.
* Strengthened **use safer chain intersects across snapshot gaps:** Recovery now prefers better chain intersect points across snapshot gaps, which helps sync continue more reliably when history is incomplete.
* Stabilized **restore account history in deterministic order:** Account recovery now uses block order as well as certificate order, which keeps restored account state consistent when multiple transactions share a slot.
* Hardened **initialize ledger metrics earlier:** Ledger metrics now exist as soon as ledger state starts, which avoids failures in paths that read metrics before full startup completes.
* Repaired **propagate rollback recovery errors correctly:** Rollback recovery now returns ancestor lookup and rollback errors directly, which makes serious recovery failures easier to detect and handle.

### 📋 What You Need to Know

* Clarified **broader Blockfrost stake-account API coverage:** Blockfrost compatible services now cover stake-account details, address listings, delegation history, registration history, and reward history through stake-address endpoints.
* Highlighted **cardano-node aligned Praos tie breaking and safer fresh sync recovery:** Equal-height Praos decisions now follow cardano-node behavior more closely, and unrecoverable sync failures now restart from fresh connections more aggressively.
* Emphasized **larger Leios mempool defaults and stronger startup checks:** Leios mode now uses a larger default mempool, and pool certificate tracking gives block producers a clearer startup check for stale certificate counters.
* Summarized **operational polish and documentation updates:** Logging, SQLite batching, regression coverage, and Mithril API-mode guidance all received changes that make operations easier to follow.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.45.0 (May 12, 2026)

**Title:** Batched backfill, ledger safety fixes, and stronger PV11 readiness

**Date:** May 12, 2026

**Version:** v0.45.0

Hi folks! Here’s what we shipped in v0.45.0.

### ✨ What's New

* Added **batch historical backfill work for smoother replay progress:** Historical metadata replay now writes work in larger batches, which helps long backfills move forward more smoothly and keeps checkpoint recovery safer when replay resumes.

### 💪 Improvements

* Improved **verify upcoming protocol-step readiness on a live multi-node path:** Release validation now checks the Conway to vanRossem progression more directly, which increases confidence that mixed-node networks can cross that protocol step cleanly.
* Refined **expand fuzz-driven regression coverage across core behaviors:** More parsing, storage, networking, and configuration paths now receive broader stress coverage, which helps surface edge cases before they reach operators and integrators.
* Enhanced **keep publish workflows moving while package indexing catches up:** Release publishing now avoids a broken package refresh step, which keeps releases moving even when package index updates take longer to appear.

### 🔧 Fixes

* Fixed **recover sparse chain intersections more reliably after missing history gaps:** Synchronization can now fall back to safer recent chain points more consistently, which helps recovery continue when recent ledger history is incomplete.
* Corrected **reject headers that run ahead of safe ledger rules:** Ledger validation now stops headers that claim a protocol step too far beyond the active rules, which reduces the risk of unsafe acceptance during version transitions.
* Strengthened **skip rollback work that moves past the known ledger tip:** Recovery now avoids applying rollbacks that run ahead of recorded ledger progress, which keeps replay and catch-up behavior safer when metadata trails the raw chain.

### 📋 What You Need to Know

* Clarified **backfill batching and safer replay progress:** Historical replay now batches more backfill work and keeps checkpoint progress safer, which helps long catch-up runs resume more smoothly after interruptions.
* Highlighted **safer ledger validation, rollback handling, and sparse recovery:** Ledger validation now rejects headers that jump too far ahead, rollback handling no longer pushes past the known ledger tip, and sparse intersect recovery finds safer restart points more reliably.
* Emphasized **stronger PV11 and fuzz regression coverage:** Release validation now exercises PV11 crossing behavior more directly, and broader stress coverage checks more parsing, storage, networking, and configuration paths for regressions.
* Summarized **publish maintenance and package-index timing:** Release publishing keeps moving even while package index refreshes may lag because the refresh pull remains disabled.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.44.0 (May 11, 2026)

**Title:** Blockfrost transaction endpoints, steadier Mithril backfill, and safer chain recovery

**Date:** May 11, 2026

**Version:** v0.44.0

Hi folks! Here’s what we shipped in v0.44.0.

### ✨ What's New

* Added **expand Blockfrost transaction access:** Blockfrost compatible services can now look up transaction details, metadata, UTxOs, certificates, redeemers, required signers, and signed CBOR, and they can also submit transactions with clearer submit responses.

### 💪 Improvements

* Improved **preserve Mithril backfill progress across interruptions:** Mithril bootstrap now keeps more recovery state and resumes metadata backfill more safely, which helps interrupted runs continue without losing important progress.
* Refined **give full devnet validation more time:** Devnet validation now has a longer default timeout, which reduces avoidable failures during full environment checks.
* Enhanced **recover forked chains across more rollback paths:** Chain reconciliation now handles more rollback and fork combinations without dropping useful chain history, which makes recovery steadier after difficult fork scenarios.
* Updated **align operational tooling with newer cardano-cli support:** Operational tooling now stays aligned with the newer `cardano-cli` release for smoother maintenance.
* Modernized **align data handling with newer CBOR support:** Data handling now stays aligned with the newer CBOR library release for steadier compatibility maintenance.
* Strengthened **align secrets handling with newer sops support:** Encrypted configuration and secrets handling now stay aligned with the newer `sops` release for steadier maintenance.
* Advanced **align compression support with newer library updates:** Compression support now stays aligned with the newer `klauspost/compress` release for routine compatibility maintenance.
* Polished **align system support with newer platform updates:** System level compatibility now stays aligned with the newer `golang.org/x/sys` release for steadier maintenance.

### 🔧 Fixes

* Fixed **return safer intersect points from sparse ledger history:** Chains can now find safer meeting points even when recent ledger history is sparse, which helps sync and recovery continue more reliably.

### 📋 What You Need to Know

* Clarified **broader Blockfrost transaction coverage and submit handling:** Blockfrost compatible integrations can now use transaction detail and submit endpoints with clearer coverage across metadata, UTxOs, certificates, redeemers, required signers, signed CBOR, and submit outcomes.
* Highlighted **safer Mithril backfill and resume handling:** Mithril bootstrap now preserves more recovery state and resumes metadata backfill more safely, which lowers the chance of losing progress after an interrupted run.
* Emphasized **safer fork and intersect recovery behavior:** Chain recovery now handles sparse intersect points and more fork reconciliation scenarios more safely, which helps nodes recover from difficult rollback paths with less risk of getting stuck.
* Summarized **maintenance, validation, and dependency updates:** Validation timing and dependency compatibility were refreshed to keep operations and maintenance steadier around this release.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.43.0 (May 9, 2026)

**Title:** Safer block-producer startup, deeper observability, and easier dashboard imports

**Date:** May 9, 2026

**Version:** v0.43.0

Hi folks! Here’s what we shipped in v0.43.0.

### ✨ What's New

* Added **catch block-producer credential problems during startup:** Block producers now stop early when forging credentials are missing, mismatched, expired, or not yet valid, which prevents later surprises after launch.
* Expanded **track protocol activity and peer state changes:** Operators can now see clearer network behavior signals, which makes it easier to spot unhealthy connections and shifting peer conditions.
* Introduced **prompt for the Prometheus datasource during dashboard imports:** Bundled Grafana dashboards now ask for the right Prometheus datasource during import, which makes dashboard setup smoother across different Grafana environments.

### 💪 Improvements

* Improved **show epoch progress against wall-clock time in dashboards:** Operators can now compare current epoch slot progress with a wall-clock reference line, which makes slot stalls easier to notice.
* Refined **track compatibility with gouroboros 0.168.0:** Protocol compatibility now stays aligned with the newer `gouroboros` `0.168.0` release for steadier maintenance and integration tracking.

### 🔧 Fixes

* Updated **limit slow fallback block lookups in large databases:** Large databases and fork recovery paths now avoid long lookup stalls because fallback scans stop quickly and move on to safer recovery paths.
* Modernized **restart ledger recovery after stale rollback iterators:** Rollback recovery now restarts cleanly when an iterator points at stale chain data, which helps forked chains continue without getting stuck on the wrong branch.

### 📋 What You Need to Know

* Clarified **fail-fast forging startup checks:** Block-producer startup now validates VRF, KES, and operational certificate inputs against Shelley genesis data and available ledger state, so misconfigured or expired credentials stop the node early instead of failing later.
* Highlighted **broader observability and easier dashboard imports:** Operators now get new protocol and peer observability metrics, and bundled dashboards are easier to import because Grafana prompts for the Prometheus datasource.
* Emphasized **safer rollback recovery and block-hash lookups:** Rollback recovery and block-hash lookups now behave more safely on forked chains and very large databases because stale iterators restart cleanly and expensive fallback scans stop quickly.
* Summarized **dependency tracking updates:** Maintenance tracking now includes the `gouroboros` `0.168.0` update.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.42.0 (May 8, 2026)

**Title:** Batch stake account lookups for faster delegation queries

**Date:** May 8, 2026

**Version:** v0.42.0

Hi folks! Here’s what we shipped in v0.42.0.

### ✨ What's New

* Added **batch stake account lookups for faster delegation queries:** Stake-key-heavy delegation and reward account requests now return more smoothly because Dingo can load many stake accounts in one pass.

### 💪 Improvements

* Improved **catch-up checks that no longer stall sync work:** Catch-up runs now stay smoother because hard-fork readiness checks evaluate in the background instead of pausing the main sync path.
* Refined **larger deep catch-up block batches:** Nodes now pull much larger block batches during deep catch-up, which helps long syncs move forward faster with less stop-and-go behavior.
* Enhanced **quieter inbound listener logging:** Routine inbound connection handling now produces calmer logs, which makes real connection problems easier to spot.
* Updated **test environments aligned with cardano-node 11.0.1:** Validation and demo environments now track `cardano-node` `11.0.1`, which keeps operational testing closer to current network expectations.
* Modernized **project alignment with Go 1.26.x:** Build, test, and release workflows now align on Go `1.26.x`, which keeps maintenance and contributor environments current.
* Strengthened **Mithril bootstrap rollover coverage:** Regression coverage now checks Mithril bootstrap and candidate nonce rollover paths more thoroughly, which improves confidence around snapshot recovery.

### 🔧 Fixes

* Fixed **governance imports that now preserve proposal history:** Mithril-bootstrapped nodes now keep parent action links, full proposal details, and ratified hard-fork timing so governance stays aligned after snapshot startup.
* Corrected **archive demo teardown that no longer hangs on pruning shutdown:** The archive demonstration now cleans up pruning data with the right permissions, which makes teardown finish reliably.
* Stabilized **clearer Bark and UTxO RPC startup failures:** Bark and UTxO RPC now return deterministic startup errors for TLS and port-binding problems, which makes failed launches easier to diagnose.
* Preserved **Mithril governance roots across snapshot startup:** Mithril bootstrap now seeds governance roots so chained governance proposals keep progressing instead of silently expiring after startup.
* Hardened **safer peer-sharing defaults by node role:** Block producers now keep peer sharing off by default, while non-block producers fall back to `cardano-node` behavior when the setting is unset.
* Prevented **rollback recovery from replaying applied headers:** Chainsync rollback recovery now skips headers the node already applied, which prevents recovery from wedging after rollback scenarios.
* Repaired **benchmark runs that no longer leave stray cloud paths in the worktree:** Benchmark tests now keep temporary cloud-backend metadata out of the worktree, which leaves local checkouts cleaner.
* Eliminated **pinned-tip rollback loops during no-op rollbacks:** No-op rollbacks at the current tip no longer trigger repeated local resync loops, which keeps chainsync from wedging at a pinned tip.
* Renamed **Dingo-owned metrics under the dingo_metrics prefix:** Dingo-owned metric families moved from `cardano_node_metrics_*` names to `dingo_metrics_*` names, bundled dashboards now use the new names, and operators who use any affected metrics must update alerts, dashboards, and monitoring queries.

### 📋 What You Need to Know

* Clarified **faster stake account queries:** Batched account lookups now improve delegation and reward account queries for stake-key-heavy workloads.
* Highlighted **faster and smoother catch-up behavior:** Catch-up behavior is faster and smoother because blockfetch batching scales much more aggressively, and hard-fork initiation checks no longer block the ledger path the same way.
* Emphasized **safer governance continuity after Mithril bootstrap:** Mithril-bootstrapped nodes should see safer governance continuity because governance roots, parent action links, and ratification timing are now preserved more accurately after snapshot import.
* Reviewed **operator updates for startup, peer sharing, and metrics:** Operators using Bark, UTxO RPC, PeerSharing, or any affected metrics must update alerts, dashboards, and monitoring queries for the Dingo-owned metric families that moved from `cardano_node_metrics_*` names to `dingo_metrics_*` names:
  * `cardano_node_metrics_txsEvictedNum_int` -> `dingo_metrics_txsEvictedNum_int`
  * `cardano_node_metrics_txsExpiredNum_int` -> `dingo_metrics_txsExpiredNum_int`
  * `cardano_node_metrics_slotBattlesTotal_int` -> `dingo_metrics_slotBattlesTotal_int`
  * `cardano_node_metrics_blockForgingLatency_seconds` -> `dingo_metrics_blockForgingLatency_seconds`
  * `cardano_node_metrics_forgedBlockSize_bytes` -> `dingo_metrics_forgedBlockSize_bytes`
  * `cardano_node_metrics_forgedBlockTxCount_int` -> `dingo_metrics_forgedBlockTxCount_int`
  * `cardano_node_metrics_leader_slot_checks_total` -> `dingo_metrics_leader_slot_checks_total`
  * `cardano_node_metrics_leader_slot_won_total` -> `dingo_metrics_leader_slot_won_total`
  * `cardano_node_metrics_leader_slot_not_won_total` -> `dingo_metrics_leader_slot_not_won_total`
  * `cardano_node_metrics_leader_vrf_eval_duration_seconds` -> `dingo_metrics_leader_vrf_eval_duration_seconds`
  * `cardano_node_metrics_leader_stake_lookup_duration_seconds` -> `dingo_metrics_leader_stake_lookup_duration_seconds`
  * `cardano_node_metrics_leader_last_epoch_slots_checked_int` -> `dingo_metrics_leader_last_epoch_slots_checked_int`
  * `cardano_node_metrics_leader_last_epoch_slots_won_int` -> `dingo_metrics_leader_last_epoch_slots_won_int`
  * `cardano_node_metrics_leader_last_epoch_slots_not_won_int` -> `dingo_metrics_leader_last_epoch_slots_not_won_int`
  * `cardano_node_metrics_leader_last_evaluated_epoch_int` -> `dingo_metrics_leader_last_evaluated_epoch_int`
  * `cardano_node_metrics_stake_snapshot_capture_duration_seconds` -> `dingo_metrics_stake_snapshot_capture_duration_seconds`
  * `cardano_node_metrics_stake_snapshot_capture_success_total` -> `dingo_metrics_stake_snapshot_capture_success_total`
  * `cardano_node_metrics_stake_snapshot_capture_failure_total` -> `dingo_metrics_stake_snapshot_capture_failure_total`
  * `cardano_node_metrics_stake_snapshot_pool_count_int` -> `dingo_metrics_stake_snapshot_pool_count_int`
  * `cardano_node_metrics_stake_snapshot_total_active_stake_lovelace` -> `dingo_metrics_stake_snapshot_total_active_stake_lovelace`
  * `cardano_node_metrics_stake_snapshot_last_successful_epoch_int` -> `dingo_metrics_stake_snapshot_last_successful_epoch_int`
  * `cardano_node_metrics_peerSelection_peers_by_source` -> `dingo_metrics_peerSelection_peers_by_source`
  * `cardano_node_metrics_peerSelection_churn_demotions_by_source` -> `dingo_metrics_peerSelection_churn_demotions_by_source`
  * `cardano_node_metrics_peerSelection_churn_promotions_by_source` -> `dingo_metrics_peerSelection_churn_promotions_by_source`
  * `cardano_node_metrics_peerSelection_InboundWarmTarget` -> `dingo_metrics_peerSelection_InboundWarmTarget`
  * `cardano_node_metrics_peerSelection_InboundHotQuota` -> `dingo_metrics_peerSelection_InboundHotQuota`
  * `cardano_node_metrics_peerSelection_InboundWarmHeld` -> `dingo_metrics_peerSelection_InboundWarmHeld`
  * `cardano_node_metrics_peerSelection_InboundHotHeld` -> `dingo_metrics_peerSelection_InboundHotHeld`
  * `cardano_node_metrics_peerSelection_InboundPruned` -> `dingo_metrics_peerSelection_InboundPruned`
  * `cardano_node_metrics_peerSelection_InboundArrivalsTotal` -> `dingo_metrics_peerSelection_InboundArrivalsTotal`
  * `cardano_node_metrics_peerSelection_InboundTopologyMatched` -> `dingo_metrics_peerSelection_InboundTopologyMatched`
  * `cardano_node_metrics_peerSelection_InboundDuplexHeld` -> `dingo_metrics_peerSelection_InboundDuplexHeld`
  * `cardano_node_metrics_peerSelection_InboundPrunedByReason` -> `dingo_metrics_peerSelection_InboundPrunedByReason`
  * `cardano_node_metrics_peerSelection_InboundLifecycleTotal` -> `dingo_metrics_peerSelection_InboundLifecycleTotal`
  * `cardano_node_metrics_peerSelection_InboundHotQuotaUsage` -> `dingo_metrics_peerSelection_InboundHotQuotaUsage`
  * `cardano_node_metrics_peerSelection_InboundWarmTargetOccupancy` -> `dingo_metrics_peerSelection_InboundWarmTargetOccupancy`
* Summarized **refreshed tooling and validation:** Tooling and validation were refreshed through Go `1.26.x` alignment, `cardano-node` `11.0.1` test coverage, stronger Mithril rollover regression coverage, and archive-demo reliability improvements.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.41.0 (May 6, 2026)

**Title:** Batched metadata writes, leaner memory use, and safer node behavior

**Date:** May 6, 2026

**Version:** v0.41.0

Hi folks! Here’s what we shipped in v0.41.0.

### ✨ What's New

* Added **batched metadata persistence across supported databases:** Metadata transaction data can now flush in batches across sqlite, MySQL, and Postgres backends, which makes high-volume metadata persistence easier to manage.
* Expanded **archive node demonstration tooling:** Teams can now run an archive node demonstration with MinIO-backed archive storage, a pruning node, helper scripts, and an end-to-end runnable flow.

### 💪 Improvements

* Improved **leaner default subscriber buffering:** Default subscriber buffers are now much smaller, while high-burst consumers keep large queues where needed, which reduces idle memory use.
* Refined **era test harness organization:** The era test harness now lives in a clearer shared location, which makes validation work easier to follow.
* Enhanced **Conway and PV11 readiness confidence:** Readiness coverage now checks Conway and PV11 behavior more clearly, which improves validation confidence.
* Cleaned **stale rate limit maintenance:** The project now drops an outdated rate-limit constant, which keeps maintenance cleaner without changing runtime behavior.
* Strengthened **archive node validation coverage:** The archive node demonstration now checks the full runnable flow more thoroughly, which makes the operational demo easier to trust.

### 🔧 Fixes

* Fixed **safer Mithril startup around recoverable metadata gaps:** Missing blob commit-timestamp keys now resolve as empty values, and recoverable mismatches no longer block Mithril or related startup paths.
* Corrected **iterator handling after storage closes:** Iterator creation now returns an error after Badger closes, which prevents a shutdown panic.
* Synchronized **stability window reads during era changes:** Stability-window reads now stay aligned with concurrent era updates, which avoids race-driven failures.
* Released **pruning locks earlier:** Pruning now releases the metadata and prune lock sooner, which reduces contention and improves diagnostics.
* Preserved **eligible peer continuity during local tip plateaus:** Nodes no longer disconnect their only eligible peer when a local tip plateau appears.
* Unified **peer identity handling across DNS fallbacks:** Hostname and IP variants now stay consistent for deduplication, deny lists, and DNS-failure logging.

### 📋 What You Need to Know

* Clarified **batched metadata persistence availability:** Batched metadata persistence is now available across sqlite, MySQL, and Postgres backends.
* Highlighted **lower idle heap usage:** Idle heap usage should be lower because default event subscriber buffers are smaller.
* Emphasized **safer startup and shutdown paths:** Mithril startup and shutdown behavior is safer around recoverable database edge cases.
* Summarized **more robust sync and peer management:** Sync and peer-management behavior is safer because plateau recovery and DNS-fallback peer handling are more robust.
* Reviewed **archive demo updates:** The project now includes an archive-node demonstration plus stronger validation coverage.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

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
* Reviewed **dependency updates:** Blockfrost, protocol, and UTxO RPC dependencies were refreshed.

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

## v0.38.0 (April 29, 2026)

**Title:** Smarter sync decisions, clearer operations, and ready made dashboards

**Date:** April 29, 2026

**Version:** v0.38.0

Hi folks! Here’s what we shipped in v0.38.0.

### ✨ What's New

* Added **inbound peer controls and visibility:** Operators can now track how inbound peers are accepted, denied, warmed, promoted, cooled down, and pruned, which makes public relays easier to tune and unsolicited traffic easier to manage.
* Expanded **dashboard based monitoring setup:** Operators can now deploy a full Grafana dashboard bundle with Prometheus scrape settings, alert rules, provisioning files, and setup guidance, which makes node monitoring faster to stand up.
* Introduced **parallel backup block delivery near tip:** Nodes can now request the same near tip block range from a second suitable peer and prefer the faster response, which keeps sync steadier when multiple peers report the same tip.
* Delivered **structured era mismatch errors:** Integrations can now detect era mismatch failures through consistent error details instead of relying on plain text parsing.
* Surfaced **earlier era transition stability signals:** Operators and integrators can now see when an upcoming era transition has effectively stabilized before the epoch boundary arrives, which makes planning safer around governance driven changes.

### 💪 Improvements

* Improved **one step era advancement safety:** Era changes now advance one boundary at a time, which avoids skipping required transition handling when the chain crosses into a new era.
* Refined **future protocol setting audit guidance:** Maintainers now have a clearer note about a future audit at era boundaries, which makes upcoming governance work easier to track.
* Enhanced **Windows and macOS build reliability:** Automation now avoids platform specific native linker issues on Windows and macOS, which keeps routine build verification steadier.
* Modernized **Windows test stability on slower runners:** Automation now gives slower Windows runners more room to finish successfully, which reduces avoidable test failures.
* Updated **OpenTelemetry stdout tracing support:** Tracing support now stays current with the newer `go.opentelemetry.io/otel/exporters/stdout/stdouttrace` release.
* Refreshed **Connect API compatibility:** Connect based integrations now stay current with the newer `connectrpc.com/connect` release.
* Advanced **AWS S3 integration currency:** S3 backed workflows now stay aligned with the newer `github.com/aws/aws-sdk-go-v2/service/s3` release.
* Polished **AWS client compatibility:** AWS backed components now stay aligned with the newer `github.com/aws/smithy-go` release.

### 🔧 Fixes

* Fixed **active block download continuity during peer switches:** Near tip sync can now keep a block download moving even when the best peer changes, which reduces the chance of repeated restarts during equal tip switching.
* Corrected **stale nonce cleanup after rollbacks:** Rollback recovery now removes outdated nonce data after fork changes and while loading tip state, which keeps nonce dependent behavior aligned with the active chain.
* Hardened **async block delivery failure reporting:** Connection error handling now surfaces asynchronous block delivery failures, which makes stuck or failed block delivery easier to detect.
* Stabilized **connection setup diagnostics:** Connection failure logs now include the remote peer address, which makes troubleshooting failed handshakes easier.

### 📋 What You Need to Know

* Adopt the new Grafana and Prometheus dashboard bundle and alert rules to monitor Dingo nodes more quickly.
* Expect steadier near-tip sync and smoother equal-tip peer switching because shadow blockfetch, latency aware peer choice, preserved in-flight batches, and clearer blockfetch failure reporting now work together.
* Note more explicit era and hard-fork handling because Dingo now surfaces upcoming transition stability earlier, advances only one era at a time, and returns typed era mismatch errors.
* Review the dependency refreshes when maintaining integrations.

### Recommended Network Compatibility ⚠️

| Network             | Compatible |
|---------------------|------------|
| mainnet             | ⛔         |
| preprod-testnet     | ⛔         |
| preview-testnet     | ✅         |

### 🙏 Thank You

Thank you for trying!

---

## v0.37.0 (April 27, 2026)

**Title:** Genesis bootstrap, governance, and steadier Mithril recovery

**Date:** April 27, 2026

**Version:** v0.37.0

Hi folks! Here’s what we shipped in v0.37.0.

### ✨ What's New

* Added **Ouroboros Genesis bootstrap support:** Operators can now start from Genesis bootstrap flows, which gives early sync a clearer path before the node reaches the current chain tip.
* Expanded **Genesis mode chain selection:** Nodes can now choose the chain more safely during Genesis mode, which keeps early sync aligned with the right history.
* Introduced **bootstrap aware peer promotion:** Peer promotion now accounts for bootstrap state, which helps early sync keep useful peers active while the node is still catching up.
* Delivered **Conway governance ratification support:** Governance aware services can now track ratified actions more completely as Conway governance advances.
* Enabled **Conway governance enactment handling:** Governance state now follows enacted actions more accurately, which gives operators and integrators a clearer view of active governance outcomes.
* Preserved **committee quorum persistence:** Committee quorum information now persists across restarts, which keeps governance state steadier after interruptions.
* Opened **broader governance state handling:** Governance aware integrations can now read and handle more of the Conway governance state without losing important context.
* Unlocked **new Blockfrost compatible block endpoints:** Blockfrost compatible users can now retrieve additional block details through new block endpoints.
* Extended **new Blockfrost compatible network endpoints:** Network status integrations can now access new network endpoints in a Blockfrost compatible shape.
* Introduced **new Blockfrost compatible era endpoints:** Era aware clients can now request era information through new Blockfrost compatible endpoints.
* Added **new Blockfrost compatible genesis endpoints:** Genesis data is now available through new Blockfrost compatible endpoints for services that need network bootstrap details.

### 💪 Improvements

* Improved **newer Mithril snapshot compatibility:** Bootstrap runs now accept newer snapshot shapes more reliably, which makes setup and recovery smoother across current Mithril data.
* Refined **interrupted Mithril resume safety:** Resuming after an interrupted Mithril run now behaves more safely, which reduces the chance of a broken recovery path.
* Enhanced **bootstrap aware peer governance:** Peer promotion and selection now align more closely with bootstrap state, which helps early sync keep useful peers active.
* Modernized **bootstrap and governance observability:** Operators can now see more metrics and event signals around bootstrap and governance behavior.
* Updated **operator and integrator guidance:** Documentation and workflow refreshes make recent bootstrap, governance, and API behavior easier to follow.

### 🔧 Fixes

* Fixed **Mithril bootstrap resume reliability:** Mithril bootstrap and resume handling now recover more reliably when a previous run stopped before completion.
* Corrected **snapshot bootstrap handling:** Snapshot based bootstrap flows now handle current snapshot layouts correctly, which reduces failed setup paths.
* Hardened **early sync recovery behavior:** Recovery during early sync now follows Genesis mode chain selection and bootstrap aware promotion more safely after interruptions.
* Stabilized **governance state continuity:** Governance state now keeps ratification, enactment, and committee quorum handling steadier across restarts and replay.

### 📋 What You Need to Know

* Clarified **Genesis bootstrap rollout:** Ouroboros Genesis bootstrap changes early sync behavior, including Genesis mode chain selection and bootstrap aware peer promotion.
* Highlighted **governance support growth:** Conway governance coverage now reaches ratification, enactment, committee quorum persistence, and broader governance state handling.
* Emphasized **steadier Mithril recovery:** Mithril bootstrap and resume handling now work more reliably with newer snapshot shapes and safer interrupted resume behavior.
* Summarized **expanded Blockfrost compatibility:** Blockfrost compatible users can now use new block, network, era, and genesis endpoints.

### 🙏 Thank You

Thank you for trying!

---

## v0.36.1 (April 21, 2026)

**Title:** Steadier catch-up sync and refreshed integrations

**Date:** April 21, 2026

**Version:** v0.36.1

Hi folks! Here’s what we shipped in v0.36.1.

### 💪 Improvements

* Improved **catch-up sync stability:** Nodes now stay steadier while catching up because stall checks wait longer, plateau recovery allows more time, and recycle cooldowns stay longer until the node reaches tip, which reduces unnecessary connection recycling and socket churn during large syncs.
* Updated **AWS integration currency:** AWS backed integrations stay current because the AWS SDK config package and related AWS modules were refreshed.
* Refreshed **core AWS compatibility:** AWS integrated behavior stays aligned with upstream fixes because the core AWS SDK and `aws/smithy-go` were updated.
* Modernized **Google API client support:** Google API client support stays current because `google.golang.org/api` now uses a newer release.

### 🙏 Thank You

Thank you for trying!

---

## v0.36.0 (April 20, 2026)

**Title:** Asset endpoints and steadier chain operations

**Date:** April 20, 2026

**Version:** v0.36.0

Hi folks! Here’s what we shipped in v0.36.0.

### ✨ What's New

* Added **asset queries for Blockfrost API users:** Blockfrost API users can now query native assets through `/api/v0/assets/{asset}` with the combined asset identifier format.
* Expanded **era transition visibility:** Era history now shows confirmed upcoming change points more clearly, so slot and time forecasts stay aligned with known transitions.
* Clarified **safe epoch boundaries:** Era history now reports when an era change cannot happen in the current epoch, which makes forecast results steadier near the epoch boundary.

### 💪 Improvements

* Improved **contributor setup guidance:** Development work is easier to follow because the project now includes clearer guidance for testing, architecture, and profiling.
* Refreshed **build cache stability:** Automation now uses a newer cache action release for steadier build runs.
* Updated **protocol support alignment:** Network compatibility stays current with newer upstream protocol updates.
* Modernized **operator tooling support:** Operational workflows stay aligned with newer Cardano command line tooling.
* Streamlined **era rollover handling:** Era changes now follow one transition path, which makes chain operations more consistent during rollovers.

### 🔧 Fixes

* Fixed **single relay sync stalls:** Single-relay block producers now keep chain progress steadier when the relay reconnects first after an interruption.
* Hardened **ledger startup safeguards:** Startup and rollback related paths now stop immediately when the Ouroboros security parameter K is missing or zero.
* Corrected **fork recovery error reporting:** Fork recovery now returns ancestor lookup failures directly, which makes recovery issues easier to detect.
* Prevented **Blockfrost startup crashes:** Blockfrost startup now returns a clear error when ledger state is missing instead of crashing.
* Set **forged block version markers:** Forged blocks now carry the expected minor protocol version for steadier interoperability.

### 📋 What You Need to Know

* Blockfrost API users can now query native assets through /api/v0/assets/{asset} using the concatenated {policy_id}{asset_name_hex} identifier format.
* Operators should ensure ledger initialization provides a positive Ouroboros security parameter K, because startup and rollback-related paths now fail fast when K is unset or zero.
* Most users can upgrade normally to v0.36.0; single-relay block producers should see steadier sync behavior, and Blockfrost startup now reports missing ledger state as an error instead of panicking.

### 🙏 Thank You

Thank you for trying!

---

## v0.35.3 (April 19, 2026)

**Title:** Improve peer sharing compatibility

**Date:** April 19, 2026

**Version:** v0.35.3

Hi folks! Here’s what we shipped in v0.35.3.

### ✨ What's New

* Noted **no new features:** This patch focuses on improvements and fixes.

### 🔧 Fixes

* Corrected **safer peer sharing behavior:** Dingo now avoids asking peers to share more peers when they do not offer that capability, so connections stay steadier and compatibility is smoother across mixed peer setups.

### 📋 What You Need to Know

* Simplified **upgrade guidance:** Most users only need to upgrade to v0.35.3, and peer sharing now behaves more safely with peers that do not advertise sharing.

### 🙏 Thank You

Thank you for trying!

---

## v0.35.2 (April 18, 2026)

**Title:** Safer era forecasts and refreshed dependencies

**Date:** April 18, 2026

**Version:** v0.35.2

Hi folks! Here’s what we shipped in v0.35.2.

### ✨ What's New

* Noted **no new features:** This patch focuses on improvements and fixes.

### 💪 Improvements

* Improved **protocol library compatibility:** Dingo now uses gouroboros v0.165.1 to stay current with upstream protocol library updates.

### 🔧 Fixes

* Corrected **safer era forecasts:** Hard fork era history now stays within the safe forecast horizon, so slot and time lookups do not promise certainty too far past the current ledger tip.

### 📋 What You Need to Know

* Simplified **upgrade guidance:** No action is required for most users, and upgrading to v0.35.2 is sufficient.

### 🙏 Thank You

Thank you for trying!

---

## v0.35.1 (April 17, 2026)

**Title:** Steadier peers and faster schedule calculations

**Date:** April 17, 2026

**Version:** v0.35.1

Hi folks! Here’s what we shipped in v0.35.1.

### ✨ What's New

- **No new features:** This patch focuses on improvements and fixes.

### 💪 Improvements

- **Faster epoch nonce computation:** Ledger processing moves faster because epoch nonce calculation now uses stored nonce data instead of decoding every block again.
- **Quicker leader schedule generation:** Schedule calculation finishes dramatically faster, especially on lower-powered hardware, while keeping the same scheduling behavior.
- **Current upstream protocol compatibility:** Cardano protocol support stays current because key upstream dependencies were refreshed for smoother compatibility.
- **Stronger PostgreSQL compatibility:** PostgreSQL-backed setups are more robust because upstream database coverage and compatibility were improved.
- **More predictable Antithesis defaults:** Test environments are easier to reason about because the default `IMAGE_TAG` now resolves to `main` instead of `latest`.

### 🔧 Fixes

- **Steadier peer selection near tip:** Nodes are less likely to bounce between near-tip peers because chain selection now compares the tips it has actually observed.
- **Safer UTxO RPC predicate handling:** Deep or cyclic request predicates no longer recurse indefinitely, so requests do not hang or risk crash-like behavior.
- **More reliable UTxO RPC queries:** SQLite-backed address queries now handle the reserved `transaction` table name correctly, and broader Connect RPC coverage helps harden query, submit, sync, and watch behavior.

### 📋 What You Need to Know

- **No action required for most users:** You're all set—just upgrade to v0.35.1.
- **Antithesis users:** If you rely on the default IMAGE_TAG in the dingo-praos testnet compose file, it now resolves to `main` instead of `latest`.

### 🙏 Thank You

Thank you for trying!

---


## v0.35.0 (April 15, 2026)

**Title:** Smoother sync and faster block lookups

**Date:** April 15, 2026

**Version:** v0.35.0

Hi folks! Here’s what we shipped in v0.35.0.

### ✨ What's New

- **Blockfrost-compatible metadata-label transaction endpoints:** Building Blockfrost-style integrations is easier because you can now query transaction metadata labels using Blockfrost-compatible endpoints.

### 💪 Improvements

- **More complete Mithril bootstrap snapshots:** Startup is smoother because nodes now prepare more of the snapshot data needed for the current epoch window after a Mithril bootstrap.
- **More resilient peer retry behavior:** Sync is more rock-solid because chainsync and blockfetch retry using the active best peer when target connections aren’t available.
- **Faster block lookup by hash:** Hash-based block queries are faster because block-by-hash resolution is streamlined across blob storage backends.
- **Larger event queue capacity:** Bursty workloads are easier to handle because the event queue capacity was increased from 10,000 to 100,000.
- **Stronger connection direction tracking:** Networking is more rock-solid because chainsync tracks outbound-started clients and the connection manager detects inbound/outbound `ConnectionId` collisions.
- **Simpler SQLite metadata batching:** Batched metadata writes are easier to manage because a `BatchAccumulator` streamlines collecting and resetting SQLite metadata model batches.

### 🔧 Fixes

- **No user-facing fixes:** This release focuses on new features and improvements.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.35.0.

### 🙏 Thank You

Thank you for trying!

---


## v0.34.0 (April 14, 2026)

**Title:** Blockfrost-compatible metadata label endpoints

**Date:** April 14, 2026

**Version:** v0.34.0

Hi folks! Here’s what we shipped in v0.34.0.

### ✨ What's New

- **Blockfrost-compatible metadata-label transaction endpoints:** Building Blockfrost-style integrations is easier because you can now retrieve transactions by metadata label using endpoints that match Blockfrost conventions.

### 💪 Improvements

- **Ledger validation guardrails:** Ledger validation and processing is more consistent in edge cases, which helps avoid repeated work and makes failures easier to understand.
- **Safer Unix-domain socket startup:** Local Unix socket startup is safer and more reliable, reducing the chance of startup failures due to leftover files.
- **Clearer submit errors (`utxorpc/submit.go`):** Error messages now provide clearer context when a submission fails, making troubleshooting faster.
- **Shared SQL address filtering and sentinel errors:** SQL plugins now share a consistent way to filter UTxO addresses and report common failure cases, which improves maintainability and debugging.
- **Dependency refresh:** Dependencies were refreshed to keep the project current and compatible with upstream fixes and improvements.
- **More adaptive Antithesis workflow defaults:** CI workflow behavior is now more adaptive, helping reduce unintended pinning to outdated behavior in automation runs.

### 🔧 Fixes

- **No user-facing fixes:** This release focuses on new features and improvements.

### 📋 What You Need to Know

- **Blockfrost integrations:** If you rely on Blockfrost-compatible behavior, you can start using the new metadata-label transaction endpoints immediately without changing existing endpoints.

### 🙏 Thank You

Thank you for trying!

---


## v0.33.0 (April 13, 2026)

**Title:** Blockfrost-compatible address endpoints

**Date:** April 13, 2026

**Version:** v0.33.0

Hi folks! Here’s what we shipped in v0.33.0.

### ✨ What's New

- **Blockfrost-compatible address endpoints:** Plugging Dingo into existing Blockfrost-based tooling is easier because you can now query address UTxOs and transactions via Blockfrost-compatible HTTP endpoints.
- **Epoch-scoped protocol parameters:** Clients can fetch the exact settings they need for historical or upcoming validation because protocol parameters can now be requested for a specific epoch.

### 💪 Improvements

- **Node internals split into focused files:** The codebase is easier to navigate because chainsync recycling, forging, and shutdown logic moved out of `node.go` into dedicated files.
- **Makefile help and lint targets:** Build and contributor workflows are easier to run consistently because the Makefile now includes `help` and `lint` targets with inline target descriptions.
- **Dependency and CI refresh:** Compatibility stays smoother because Go module dependencies (including `golang.org/x`), key CI actions, and the txtop Docker image were updated.
- **Clearer package docs and licensing:** Development is simpler because core packages now include Apache 2.0 headers, richer package docs, and conformance test suite docs under `internal/test/conformance`.
- **More complete `dingo.yaml.example`:** Getting started is easier because the example config now includes a default `databasePath` that matches the expected on-disk layout.

### 🔧 Fixes

- **Safer chainsync recycling on small topologies:** Small networks are less likely to lose their only workable connection because the recycler now avoids recycling the only eligible peer.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.33.0.

### 🙏 Thank You

Thank you for trying!

---


## v0.32.3 (April 10, 2026)

**Title:** Certificate-aware transaction matching

**Date:** April 10, 2026

**Version:** v0.32.3

Hi folks! Here’s what we shipped in v0.32.3.

### ✨ What's New

- **Certificate-aware transaction pattern matching (`has_certificate`):** Filtering certificate-related transactions is easier because you can now match and validate Cardano certificates directly in transaction pattern rules.

### 💪 Improvements

- **More reliable persistent-fork recovery:** Sync is more rock-solid because chain sync now closes the affected connection when it detects a persistent fork.

### 🔧 Fixes

- **No user-facing fixes:** This patch focuses on new features and improvements.

### 📋 What You Need to Know

- **Transaction-pattern configs:** If you maintain custom transaction-pattern rules, you can start using `has_certificate` to filter or detect certificate-related transactions.

### 🙏 Thank You

Thank you for trying!

---


## v0.32.2 (April 9, 2026)

**Title:** Stability and polish updates

**Date:** April 9, 2026

**Version:** v0.32.2

Hi folks! Here’s what we shipped in v0.32.2.

### ✨ What's New

- **No new features:** This patch focuses on improvements and fixes.

### 💪 Improvements

- **More reliable connection cleanup:** Network operations stay more rock-solid because the node now detects inactive connections and purges leftover state tied to closed links.
- **Refreshed telemetry and runtime dependencies:** Observability and compatibility stay smoother because core dependencies (including OpenTelemetry, gRPC, and gonum) were refreshed.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.32.2.

### 🙏 Thank You

Thank you for trying!

---


## v0.32.1 (April 7, 2026)

**Title:** Smoother submission controls and matching

**Date:** April 7, 2026

**Version:** v0.32.1

Hi folks! Here’s what we shipped in v0.32.1.

### ✨ What's New

- **No new features:** This patch focuses on improvements and fixes.

### 💪 Improvements

- **Disable transaction submission throttling:** High-throughput workloads are easier to run because setting the transaction submission rate limit to `0` now fully disables throttling.
- **Refined transaction pattern matching:** Filtering rules are easier to reason about because transaction pattern matching now uses dedicated helpers for `consumes`, `produces`, `has_address`, and asset filters.

### 🔧 Fixes

- **AWS SDK patch updates:** AWS S3 integrations are more rock-solid because AWS SDK for Go v2 S3 modules were bumped to newer patch versions.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.32.1.

### 🙏 Thank You

Thank you for trying!

---


## v0.32.0 (April 7, 2026)

**Title:** Background pruning and scalable UTxO search

**Date:** April 7, 2026

**Version:** v0.32.0

Hi folks! Here’s what we shipped in v0.32.0.

### ✨ What's New

- **Periodic Bark block pruning:** Long-running nodes stay smooth because a nifty background task now periodically prunes no-longer-needed Bark blocks and starts and stops cleanly with node startup and shutdown.
- **Structured `SearchUtxos` pagination and asset filtering:** Finding and listing UTxOs is more consistent and scalable because `SearchUtxos` pagination and asset filtering now use a structured query model handled in the database layer.

### 💪 Improvements

- **Slot-aware transaction validation:** Transaction validation is more rock-solid across slot-clock changes because the ledger and mempool now validate against the current-or-tip slot and track slot-clock fallbacks.
- **Clearer external interfaces docs:** Integration decisions are simpler because docs now describe client-facing **external interfaces**, clarify Bark’s role, and expand port and configuration tables with defaults and role descriptions.
- **Updated CI and Docker Go toolchains:** Maintenance is smoother because workflows now use actions/setup-go v6.4.0 and the Dockerfile build image moved from Go 1.25.8-1 to 1.26.1-1.
- **Dependency refresh:** Compatibility stays rock-solid because we updated fxamacker/cbor to v2.9.1, plutigo to v0.1.1, AWS SDK for Go v2 S3 modules, OpenTelemetry Go to v1.43.0, and google.golang.org/api to v0.274.0.

### 🔧 Fixes

- **No user-facing fixes:** This release focuses on new features and improvements.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.32.0.

### 🙏 Thank You

Thank you for trying!

---


## v0.31.1 (April 4, 2026)

**Title:** Clearer rollback recovery outcomes

**Date:** April 4, 2026

**Version:** v0.31.1

Hi folks! Here’s what we shipped in v0.31.1.

### ✨ What's New

- **Clearer rollback recovery status:** Recovery is simpler to understand because chain recovery now reports clearer outcomes after a local rollback.

### 💪 Improvements

- **More consistent recovery staleness checks:** Recovery decisions are more rock-solid because staleness checks are now based on the primary chain tip.

### 🔧 Fixes

- **Rollback-aware recovery test coverage:** Reliability is more rock-solid because recovery and chainsync tests now validate rollback-aware behavior.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.31.1.

### 🙏 Thank You

Thank you for trying!

---


## v0.31.0 (April 3, 2026)

**Title:** Storage safeguards and smarter peer sync

**Date:** April 3, 2026

**Version:** v0.31.0

Hi folks! Here’s what we shipped in v0.31.0.

### ✨ What's New

- **Immutable storage settings:** Upgrades are more rock-solid because the node now persists storage mode and network metadata and refuses to open a database if they don’t match your current `--storage-mode` configuration.
- **Earlier ledger peer discovery:** Peer discovery starts sooner because reconciliation now invokes ledger-based discovery earlier with debug logs and configurable target and bound settings.
- **Shared transaction filtering (`TxPredicate`):** Transaction submit and watch are more consistent because filtering now uses a shared `TxPredicate` evaluation engine.

### 💪 Improvements

- **Quieter near-tip chain sync logs:** Routine operation is less noisy because chain-sync progress logging now pauses once you’re at least 99.9% synced.
- **Smarter peer lag filtering:** Sync catch-up is smoother because peer selection now skips nodes that are far behind the best known tip using a `securityParam`-based filter.
- **Smoother Prometheus defaults and cache visibility:** Metrics setup is simpler because the node now uses the default Prometheus registry when none is provided and exports Badger cache gauges.
- **More accurate SQLite disk reporting:** Troubleshooting is easier because SQLite disk size reporting and error diagnostics are now more accurate.
- **Clearer CI workflow documentation:** Release automation is easier to follow because workflow behavior and related documentation were refined.

### 🔧 Fixes

- **Safer fork extension handling:** Fork recovery is smoother because block fetching now restarts cleanly on forks and skips unnecessary rollbacks when a fork extends from your local tip.
- **Earlier validity-interval rejection:** Mempool processing is more efficient because transactions that can’t be valid yet are rejected up front.
- **Security dependency update:** Security stays solid because `github.com/go-jose/go-jose/v4` was updated to v4.1.4.

### 📋 What You Need to Know

- **No action required for most users:** You're all set—just upgrade to v0.31.0.
- **Operators changing storage settings:** If you change storage mode or network for an existing database, the node will now fail fast to prevent opening it with incompatible settings.

### 🙏 Thank You

Thank you for trying!

---


## v0.30.0 (April 2, 2026)

**Title:** Paginated pools API and steadier automation

**Date:** April 2, 2026

**Version:** v0.30.0

Hi folks! Here’s what we shipped in v0.30.0.

### ✨ What's New

- **Paginated extended stake pool details (HTTP API):** Pool queries are easier to work with because you can now request extended stake pool data in smaller pages via `/api/v0/pools/extended`.

### 💪 Improvements

- **Dependency refresh:** Builds stay more rock-solid because we updated a key upstream dependency.
- **More predictable Antithesis runs:** Automation is easier to reason about because the Antithesis GitHub Actions workflow now runs with the expected default argument.

### 🔧 Fixes

- **No surprise automation runs:** CI noise is lower because affected workflows no longer auto-trigger unexpectedly.

### 📋 What You Need to Know

- **Pools API users:** If you use the pools API, consider switching to `/api/v0/pools/extended` to page through extended pool data.

### 🙏 Thank You

Thank you for trying!

---


## v0.29.1 (April 2, 2026)

**Title:** Safer APIs and steadier sync

**Date:** April 2, 2026

**Version:** v0.29.1

Hi folks! Here’s what we shipped in v0.29.1.

### ✨ What's New

- **Rolled out expanded transaction builders and txpump testing:** End-to-end testing is easier because you can now build more transaction types and run a randomized txpump loop.
- **Handy devnet and testnet setup tooling:** Spinning up local and testnet environments is simpler because you now have additional helper scripts, specs, and Antithesis wiring.

### 💪 Improvements

- **Safer API opt-in defaults:** Service exposure is clearer because APIs now only activate when you explicitly opt in.
- **Steadier rollback iteration:** Catch-up and restart behavior is more predictable because chain iteration is now safer under rollbacks.
- **Relaxed peer-tip validation during catch-up:** Sync is smoother because peer tip checks are less likely to reject useful peers while you’re catching up.
- **More reproducible CI and release automation:** Builds are easier to operate because CI and Antithesis automation were hardened.
- **Refreshed dependency set:** Compatibility stays rock-solid because a key dependency was updated.

### 🔧 Fixes

- **Iterator cleanup in ledger iteration:** Long-running processes are steadier because ledger iteration now avoids potential resource leaks.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.29.1.
- **API users:** If you rely on Dingo APIs, make sure your configuration explicitly opts in so the endpoints are enabled.

### 🙏 Thank You

Thank you for trying!

---


## v0.29.0 (March 31, 2026)

**Title:** Smoother operations and API refinements

**Date:** March 31, 2026

**Version:** v0.29.0

Hi folks! Here’s what we shipped in v0.29.0.

### ✨ What's New

- **Conway-era transaction builders and txpump:** End-to-end testing is easier because you can now generate and run newer-era transactions in automated runs.
- **Devnet and testnet helpers:** Spinning up test environments is simpler because you now have wallet-focused tests, a configurator script, local devnet helpers, a Dingo testnet spec, and Antithesis `docker-compose` wiring.
- **Storage disk-usage metrics:** Capacity planning is easier because the blob store and metadata store now export disk-usage (`DiskSize`) Prometheus gauge metrics.

### 💪 Improvements

- **Faster post-snapshot replay:** Restarts are faster because chain-sync now uses a Mithril trust-boundary slot to skip replay work already covered by a snapshot.
- **Configurable CBOR cache sizing:** Performance tuning is simpler because you can now set the CBOR cache size in configuration.
- **More predictable Badger defaults:** Deployments are more consistent because storage-mode-specific defaults only apply when values are truly unset.
- **More accurate Blockfrost responses:** Block and epoch data is more reliable because latest block, epoch, and protocol-parameter responses are now sourced from ledger state and the database.

### 🔧 Fixes

- **Safer rollbacks behind snapshots:** Rollback behavior is more rock-solid because the Mithril trust-boundary now resets when a rollback crosses it.
- **Rock-solid rollback recovery:** Recovery is smoother because “rollback point not found” now follows the same handling path as `local_tip_plateau`.
- **Rock-solid WatchTx rollbacks:** Transaction watching is more reliable because WatchTx now supports undo and rollback during chain reorganizations.

### 📋 What You Need to Know

- **No action required:** You're all set—just upgrade to v0.29.0.
- **Replay and rollback near snapshots:** Testing runs may look different because chain-sync replay now respects the Mithril trust-boundary and resets it on rollback.

### 🙏 Thank You

Thank you for trying!

---


## v0.28.0 (March 30, 2026)

**Title:** Network-aware metrics and steadier block production

**Date:** March 30, 2026

**Version:** v0.28.0

Hi folks! Here's what we shipped in v0.28.0.

### ✨ What's New

- **Network-labelled Prometheus metrics and build info:** Dashboard setup is easier because every Prometheus metric now carries a `network` label automatically, and a new `dingo_build_info` gauge exposes version, commit, and Go version at a glance.
- **Tip gap and epoch gauges:** Sync monitoring is simpler because three new gauges track tip gap (wall-clock slot minus chain tip), Shelley genesis start time, and epoch length.
- **Transaction metadata label index (Blockfrost):** Metadata queries are faster because a new transaction metadata label table powers indexed lookups with deterministic pagination and rollback-safe cleanup.

### 💪 Improvements

- **Smarter chain selection near tip:** Tip following is steadier because chain selection now prefers the actually-delivered chainsync tip over the advertised remote tip and sticks with the incumbent peer when two peers report the same frontier.
- **Paginated DumpHistory (UTxO RPC):** Large history queries are more reliable because `DumpHistory` now uses a chain iterator with `next_token` pagination and sensible defaults when `max_items` is omitted.
- **Leaner TxSubmission pipeline:** CPU usage under load is lower because the unnecessary rate limiter on the pull-based TxSubmission protocol was removed, eliminating a tight retry loop that starved chainsync and blockfetch.
- **Leios protocols gated behind config:** Compatibility with non-Leios peers is more rock-solid because Leios mini-protocols are now only registered when `EnableLeios` is set, preventing muxer errors from peers that reject unknown protocols.

### 🔧 Fixes

- **Post-Mithril leader election:** Block production after a Mithril bootstrap is more reliable because `EpochNonce()` now falls through to the database when the in-memory nonce is empty, and `CaptureGenesisSnapshot()` falls back to the latest epoch start slot when slot 0 yields no pools.
- **Forged-block rollback exemption:** Fork resolution after slot battles is correct because the rollback loop detector now exempts rollbacks through slots where the node forged a block.
- **Richer UTxO RPC responses:** Script evaluation and data queries are more complete because `evalTx` now includes redeemer payloads and `readData` now returns parsed datums.

### 📋 What You Need to Know

- **Metrics dashboards:** If you run Prometheus dashboards, all metrics now include a `network` label — update your queries or selectors if you filter by metric name alone.
- **Leios users:** If you use Leios mode, make sure `EnableLeios` is set in your configuration; the protocols are no longer registered by default.
- **Default make target:** `make` no longer runs tests by default — use `make test` explicitly.

### 🙏 Thank You

Thank you for trying!

---


## v0.27.7 (March 24, 2026)

**Title:** Steadier sync and leaner storage

**Date:** March 24, 2026

**Version:** v0.27.7

Hi folks! Here’s what we shipped in v0.27.7.

### ✨ What's New

- **Configurable network ID:** Connecting to the right Cardano network is more rock-solid because you can now set the network identifier in configuration.
- **Optional blob-store compression:** Disk usage can be lower because you can now enable compression for blob storage in some environments.
- **Devnet transaction pump:** Testing transaction flow is easier because development networks now include an additional transaction pump service.

### 💪 Improvements

- **More stable chain following:** Sync is more rock-solid because chain following and recovery is now more stable during tip changes and temporary peer issues.
- **Safer rollbacks and replay:** Reorg handling is more predictable because rollback and ledger replay behavior is now safer.
- **Lighter rewind pruning:** Larger cleanups are smoother because rewind and pruning operations now put less pressure on storage backends.
- **Stickier best-peer selection:** Peer churn can be lower because peer selection now better preserves stable connections when multiple peers report equivalent tips.

### 🔧 Fixes

- **More consistent blob deletion:** Cleanup is more reliable because blob deletion now better matches transaction behavior.
- **Graceful missing-blob recovery:** Processing is more resilient because missing blob data is now handled more gracefully in some cases.

### 📋 What You Need to Know

- **Multi-network deployments:** If you run against different Cardano networks, review your config to ensure the correct network is selected.
- **Docker builds:** If you rely on custom Docker build caching, Docker builds may behave differently.
- **Tooling refreshes:** Routine updates to build tooling and libraries may land as part of keeping the project secure and compatible.

### 🙏 Thank You

Thank you for trying!

---


## v0.27.5 (March 19, 2026)

**Title:** Faster UTxO lookups and steadier sync

**Date:** March 19, 2026

**Version:** v0.27.5

Hi folks! Here’s what we shipped in v0.27.5.

### ✨ What's New

- **Stable UTxO ordering and address queries:** Wallet and explorer-style queries are more predictable because you can now query UTxOs with a stable ordering and look them up efficiently by address.
- **Observability-only chain-sync clients:** Metrics are clearer because you can now run observability-only chain-sync clients that are counted in metrics without affecting normal client operation.
- **Automatic chain realignment on startup:** Recoveries after interruptions are easier because ledger startup can now realign chain state automatically without noisy side effects.

### 💪 Improvements

- **More consistent rollback scheduling:** Recovery after chain reorganizations is more reliable because rollback handling now keeps scheduling state more consistent.
- **Sliding-window chain density:** Chain health signals are more representative because chain density calculations now use a sliding window of recent slots and blocks.
- **Safer raw block copy resume:** Resuming raw/direct block copying is safer because resume checks are now stricter and reduce accidental skipping or duplication.
- **Smoother peer governance convergence:** Early runtime is steadier because peer governance now converges faster after startup and follows configuration defaults more consistently.
- **Better default peer targets:** Peer configuration works out of the box more often because Dingo now falls back to Cardano P2P peer target values when Dingo peer targets aren’t set.
- **Richer UTxO RPC chain references:** Downstream indexing is easier because UTxO RPC responses now include a more complete reference to chain position.
- **Clearer stake snapshot errors (SQLite):** Operational debugging is easier because stake snapshot maintenance errors now include clearer context.
- **Dependency refresh:** Builds are more rock-solid because dependencies were refreshed to keep compatibility and security posture current.
- **Clearer tests and documentation:** Maintenance is easier because tests and documentation were clarified for long-term readability.

### 🔧 Fixes

- **Resilient ledger block processing:** The node is less likely to go down on transient ledger errors because block processing now restarts on non-fatal errors instead of exiting.
- **Safer block fetch flushing:** Block downloads recover more cleanly after flush failures because block fetch now cleans up state when flushing pending blocks fails.

### 📋 What You Need to Know

- **Client-count metrics may shift:** Capacity planning may look different because observability-only chain-sync clients are now tracked separately from eligible clients.

### 🙏 Thank You

Thank you for trying!

---


## v0.27.4 (March 18, 2026)

**Title:** Reliability and usability refinements

**Date:** March 18, 2026

**Version:** v0.27.4

Hi folks! Here’s what we shipped in v0.27.4.

### ✨ What's New

- **Rock-solid chain-sync recovery:** Sync stays more rock-solid because chain-sync recovers more reliably from stalled or unstable network connections.
- **Solid chain selection:** Sync wastes less time on poor candidates because chain selection now takes peer suitability into account.

### 💪 Improvements

- **Simpler config loading:** Configuration stays under your control because config loading no longer fills in a default Cardano configuration path.
- **Sleeker block downloads:** Block downloads are more consistent because header processing no longer gets blocked by duplicate headers.
- **More robust intersections:** Synchronization starts from the right place more often because intersection point selection is now more robust.

### 🔧 Fixes

- **No missing boundary blocks:** Historical imports are more complete because older-era boundary blocks are no longer skipped during block loading.

### 📋 What You Need to Know

- **Inbound peers excluded from chain choice:** Chain selection behavior may change in mixed inbound/outbound topologies because inbound peers no longer influence which chain is selected.

### 🙏 Thank You

Thank you for trying!

---

## v0.27.3 (March 17, 2026)

**Title:** Safer rollbacks and steadier leader election

**Date:** March 17, 2026

**Version:** v0.27.3

Hi folks! Here’s what we shipped in v0.27.3.

### ✨ What's New

- **Follow-tip reset and rollback:** Tip tracking is easier to manage because the follow-tip API now supports safe reset and rollback with clearer metadata about what changed.
- **Leader election readiness:** Block producer readiness is easier to track because leader election now surfaces epoch-nonce readiness and carries schedule state more reliably across restarts.

### 💪 Improvements

- **More consistent cache sizing:** Sizing runs are easier to tune because cache defaults are more consistent and the BP/PI sizing script now supports explicit memory limits and optional cache overrides.
- **Predictable KES period semantics:** Operational certificate (KES) periods are more predictable because KES endpoints now standardize on absolute periods while translating internally from the certificate start period.
- **Preserved original block bytes:** Downstream tooling can retain exact block bytes because API block objects now include the original encoded bytes.
- **Standard network identifiers from genesis:** Network identification is simpler because genesis reads now return a standard CAIP-2 network identifier derived from network magic.
- **Smoother peer switching:** Sync stays more rock-solid because chain-sync now preserves its state while only swapping the active connection during a peer switch.
- **More consistent Mithril imports:** Mithril snapshot imports are more consistent across epochs because imports now normalize snapshot types and centralize persistence and epoch-summary handling.
- **Protocol dependency validation:** Modern-era transaction handling is more reliable because protocol dependencies were updated and regression tests now guard transaction size behavior.
- **Safer default containers:** Default containers are safer because the main Docker image now runs as a non-root `dingo` user.

### 🔧 Fixes

- **Resilient background monitoring:** Long-running monitoring is more rock-solid because the stall checker now recovers from panics instead of crashing its background loop.
- **Robust block fetch batching:** Block fetch serving is more robust because batching now handles iterator errors and connection closes correctly.
- **Clearer unexpected event handling:** Event processing is easier to debug because chain-sync and block fetch now log unexpected event payload types instead of failing silently.
- **Reliable shutdown error reporting:** Shutdowns are easier to troubleshoot because node and metrics server shutdown now propagates errors instead of exiting abruptly.
- **Clear tx-submission failures:** Transaction submission fails more clearly because the tx-submission handlers now return an explicit error when no mempool consumer exists.

### 📋 What You Need to Know

- **Docker volume permissions:** If you run Dingo in Docker with mounted volumes, make sure the data directory is writable by the `dingo` user inside the container.
- **API integrations:** If you integrate with follow-tip or KES APIs, give your client code a quick check for the updated reset/rollback and period semantics.

### 🙏 Thank You

Thank you for trying!

---

## v0.27.2 (March 16, 2026)

**Title:** Snapshot events and safer services

**Date:** March 16, 2026

**Version:** v0.27.2

Hi folks! Here’s what we shipped in v0.27.2.

### ✨ What's New

- **Snapshot event publishing and clean shutdown:** Relay operation is more reliable because the relay now publishes events from snapshots and shuts down cleanly without dropping in-flight work.

### 💪 Improvements

- **HTTP timeouts for public APIs:** Network-facing services are more resilient under slow or stalled connections thanks to new write/read/idle timeouts on the Bark, Blockfrost, and UTxO RPC HTTP servers.
- **Streamlined peer and connection management:** Peer and connection management is faster and uses fewer resources on constrained machines thanks to quicker inbound host lookups, tighter Badger cache defaults, and expanded benchmarks and sizing guidance.
- **More consistent key-period handling:** Block production key period handling is more consistent across configurations thanks to improved key-period calculations with added validation and tests.
- **Race detection in CI:** Test runs catch concurrency bugs earlier because the Linux test job now runs with the Go race detector enabled.

### 🔧 Fixes

- **Safer concurrent chain reads:** Reads are more consistent under load because primary chain and protocol-parameter access are now protected with read locks.
- **Validated epoch nonce reuse:** Nonce reuse is safer because cached epoch nonce entries are now validated against the nonce provided for the current run before reuse.
- **Graceful invalid hash handling:** Malformed block metadata no longer crashes encoding because previous-hash length issues now return errors instead of panicking.
- **SQLite VACUUM actually runs:** Database maintenance now completes as intended because SQLite VACUUM is now executed rather than only prepared.

### 📋 What You Need to Know

- **Build provenance updates:** Supply-chain attestations are easier to verify because build provenance now uses `actions/attest` and updated Docker Hub image subjects.

### 🙏 Thank You

Thank you for trying!

---

## v0.27.1 (March 16, 2026)

**Title:** Smoother reconnects and safer chain-sync

**Date:** March 16, 2026

**Version:** v0.27.1

Hi folks! Here’s what we shipped in v0.27.1.

### ✨ What's New

- **Better chain-sync intersection:** Resuming sync is easier and faster because chain-sync uses a denser, wider set of intersect points to improve `ChainSync` intersection behavior.

### 💪 Improvements

- **Inbound connection reuse and `TxSubmission`:** Networking is more rock-solid on reconnect because peer reuse and governance now normalize exact peer addresses, require client-capable connections for reuse, and start `TxSubmission` on duplex inbound connections.
- **Stake snapshot and epoch summary upserts:** Data storage is more consistent across supported databases because write paths now upsert across DB backends and report errors more clearly.
- **More robust delegation parsing:** Delegation reads are more reliable because parsing now handles multiple account encodings with expanded tests.

### 🔧 Fixes

- **Dependency refresh:** Upgrades are less error-prone because Go modules were refreshed (including AWS SDK v2/S3, `golang.org/x/*`, `plutigo` v0.0.27, `go-ethereum` v1.17.1, and `google.golang.org/api` v0.271.0).

### 📋 What You Need to Know

- **Go module sync (some builds):** You’re all set for most setups, but if you vendor dependencies or run reproducible builds you may need to re-sync Go modules (update `go.mod`/`go.sum`) to pick up refreshed versions.

### 🙏 Thank You

Thank you for trying!

---

## v0.27.0 (March 15, 2026)

**Title:** S3-backed CI tests and embedded network configs

**Date:** March 15, 2026

**Version:** v0.27.0

Hi folks! Here’s what we shipped in v0.27.0.

### ✨ What's New

- **S3-backed CI storage tests:** Storage testing is more rock-solid because CI now spins up a MinIO S3-compatible service and runs coverage across all supported storage backends, including S3.
- **Embedded network config bundles:** Getting started is easier because preview, preprod, mainnet, and devnet network configs are now embedded and loaded via `EmbeddedConfigFS`.

### 💪 Improvements

- **Snapshot epoch transitions:** Snapshot handling is more reliable because the snapshot manager now processes every epoch transition event during rapid chain progress.
- **Tip ingestion fast paths:** Sync near the chain tip is sleeker because blockfetch and block insertion reuse queued header data and caller-supplied points to cut redundant work.

### 🔧 Fixes

- **No fixes:** No user-facing fixes shipped in this release.

### 📋 What You Need to Know

- **Compact block metadata format (Badger):** Disk usage can be smaller because you can opt into an optional Badger setting that stores block metadata in a compact binary format.

### 🙏 Thank You

Thank you for trying!

---

## v0.26.0 (March 14, 2026)

**Title:** Trusted Mithril downloads and tuned peers

**Date:** March 14, 2026

**Version:** v0.26.0

Hi folks! Here’s what we shipped in v0.26.0.

### ✨ What's New

- **End-to-end Mithril verification:** Mithril downloads are more rock-solid because Dingo now verifies the full certificate chain and signatures using genesis keys from your Cardano configuration.

### 💪 Improvements

- **Peer governor tuning:** Peer connectivity is easier to tune because you can now adjust peer-governor settings, including hot-peer promotion behavior and its defaults.

### 🔧 Fixes

- **No fixes:** No user-facing fixes shipped in this release.

### 🙏 Thank You

Thank you for trying!

---

## v0.25.1 (March 13, 2026)

**Title:** Configurable storage and smoother sync

**Date:** March 13, 2026

**Version:** v0.25.1

Hi folks! Here’s what we shipped in v0.25.1.

### ✨ What's New

- **Configurable data directory:** Running in different environments is easier because you can now choose where the node stores its data on disk with `--data-dir`.
- **Built-in Mithril bootstrap:** Containerized bootstrapping is simpler because Dingo can now spin up Mithril sync without an external client.
- **Expanded architecture docs:** Understanding the system is easier because `ARCHITECTURE.md` now includes clearer diagrams and explanations.

### 💪 Improvements

- **Idle connection stability:** Long-running connections are more rock-solid because idle sessions are less likely to be dropped unexpectedly.
- **Resilient sync across reconnections:** Sync is more rock-solid because chainsync and blockfetch handle connection switches more reliably.
- **Blockfetch overhead:** Observability is sleeker because blockfetch avoids extra work when no one is listening for events.
- **Dependencies:** Builds are more solid because dependencies were refreshed for consistency.

### 📋 What You Need to Know

- **Upgrade:** You’re all set—no required configuration changes for this release.
- **Custom storage path:** If you want to store node data somewhere else, pass `--data-dir <path>` at startup.

### 🙏 Thank You

Thank you for trying!

---

## v0.25.0 (March 12, 2026)

**Title:** Trusted replay verification and peer counts

**Date:** March 12, 2026

**Version:** v0.25.0

Hi folks! Here’s what we shipped in v0.25.0.

### ✨ What's New

- **Trusted chain verification:** Startup and recovery runs are more rock-solid because your node can now verify the chain from a trusted, immutable replay path.

### 💪 Improvements

- **Peer metrics accuracy:** Peer limits and metrics are more accurate because local client connections are now tracked separately.

### 🔧 Fixes

- **Mithril key defaults (Docker):** Docker deployments are less error-prone because the correct Mithril genesis verification key is now picked up automatically when you don’t provide one.

### 📋 What You Need to Know

- **Peer counts:** If you rely on peer-count metrics or peer-limit tuning, expect local client connections to no longer be included in those counts.

### 🙏 Thank You

Thank you for trying!

---

## v0.24.1 (March 12, 2026)

**Title:** Stability and polish

**Date:** March 12, 2026

**Version:** v0.24.1

Hi folks! Here’s what we shipped in v0.24.1.

### 💪 Improvements

- **Deeper rollback scanning:** Sync is more rock-solid because deep rollbacks are only enabled when needed, and block fetch can scan a wider window to find the data it needs.

### 🔧 Fixes

- **Shutdown reliability:** Shutdowns are more rock-solid because node and ledger shutdown paths are less likely to hang, and logs now show how long shutdown took.
- **Epoch cache consistency checks:** Diagnosing cache issues is easier because epoch cache validation is stricter and catches inconsistencies earlier.

### 🙏 Thank You

Thank you for trying!

---

## v0.24.0 (March 11, 2026)

**Title:** Peer sharing controls and import visibility

**Date:** March 11, 2026

**Version:** v0.24.0

Hi folks! Here’s what we shipped in v0.24.0.

### ✨ What's New

- **Peer sharing controls:** Networking is easier to lock down because you can now explicitly enable or disable peer sharing.
- **CLI help text:** Running the tool is easier because the root command now includes clearer Short and Long help text.
- **Import progress reporting:** Large data setup runs feel less like a black box because Mithril and ledger/UTxO imports now emit rate-limited progress updates.
- **Benchmark suite:** Performance testing is more consistent because the benchmark tooling now includes a baseline and detailed ingestion benchmarks.

### 💪 Improvements

- **Docs:** Evaluating Dingo is safer because the README now includes expanded usage, deployment, and DevNet guidance plus a clear “not for production” warning.
- **Dev mode:** Local runs are less surprising because dev mode now forces storage mode to API and logs when it overrides your configured value.
- **Connection manager:** Peer connectivity is more rock-solid because inbound connections are treated as the bidirectional link and expected transition timeouts are handled without tearing down peers.
- **Storage tuning and logging:** Troubleshooting storage is easier because cache defaults were reset and configured cache sizes are now logged on open, with additional Badger and SQLite tuning.
- **Era-aware validation:** Cross-era validation is more accurate because protocol parameter extraction is now era-aware and governance-state decoding uses raw-element parsing.
- **Overlay-aware acceptance:** Transaction validity is more consistent because ledger and mempool validation now uses a temporary UTxO overlay with descendant pruning.
- **CI and publishing:** Releases are more repeatable because CI and publishing workflows were refreshed with Node.js 24.x and newer pinned Docker and GitHub Actions.
- **Dependencies:** Compatibility is better because key dependencies were updated, including OpenTelemetry Go and gouroboros.
- **Go toolchain:** Builds are more up to date because the minimum Go version is now 1.25 and CI was updated to match.

### 🔧 Fixes

- **Connection cleanup tests:** Network tests are less flaky because keepalive timeouts are deterministic and connection cleanup is safer.
- **Block caching and reconciliation:** Sync is more reliable because block caching now uses fixed-size typed hash keys and reconciliation compares hashes consistently.

### 📋 What You Need to Know

- **Go version:** If you build from source, you’ll need Go 1.25 or newer.
- **Dev mode:** If you run dev mode, Dingo will force storage mode to the API option.

### 🙏 Thank You

Thank you for trying!

---

## v0.23.1 (March 11, 2026)

**Title:** Rock-solid Docker publishing

**Date:** March 11, 2026

**Version:** v0.23.1

Hi folks! Here’s what we shipped in v0.23.1.

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
