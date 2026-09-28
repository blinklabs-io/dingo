# Dingo Mithril API Backfill Benchmark Results

Focused Mithril API-mode metadata backfill results are tracked here separately
from the broader ledger and database benchmark table. This is a historical
snapshot, not a measurement of the current branch or current releases.

## May 29-30, 2026 Preview Backfill

This local Kubernetes Preview run measured commit `d73407b6`. At that revision,
Mithril ledger-state UTxO asset import required a local diagnostic workaround:
the deferred-index manifest dropped the asset uniqueness required by the
import's `ON CONFLICT` target. The
[deferred asset index fix](https://github.com/blinklabs-io/dingo/pull/2461)
removed that index from the deferred manifest; this historical blocker is
resolved in the current source and released builds.

| Metric | Value |
|--------|-------|
| Blocks processed | 4,312,604 |
| Transactions stored | 6,567,610 |
| Backfill elapsed time | 13h45m16s |
| Average backfill throughput | 87.1 blocks/sec |
| Throughput needed for same range under 6h | 199.7 blocks/sec |
| Remaining gap | about 2.3x |
| Deferred metadata index rebuild | 26 indexes in 32m28s |
| Whole pod command wall time | about 14h32m33s |
