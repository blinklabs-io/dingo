# Dingo Mithril API Backfill Benchmark Results

Focused Mithril API-mode metadata backfill results are tracked here separately
from the broader ledger and database benchmark table. Each section identifies
the measured release or commit; the figures apply only to those runs.

## v0.89.0 batch-size comparison

These full Mithril API syncs used the v0.89.0 release binary, a fresh database
and cache for each run, and the same pinned snapshot and host within each
network. Batch size was the only changed setting. Each network has one batch-
100 control and repeated batch-200 runs; the positive result is consistent,
but a single control per network does not measure control-run variation.

| Network | Batch-100 control | Batch-200 runs | Batch-200 mean | Mean change |
| --- | ---: | --- | ---: | ---: |
| Preview | 3h18m37.118s | 3h13m09.320s; 3h14m06.301s | 3h13m37.811s | 4m59.307s faster (2.512%) |
| Preprod | 4h30m10.063s | 4h11m39.175s; 4h15m45.012s; 4h23m14.841s | 4h16m53.009s | 13m17.054s faster (4.917%) |

Every batch-200 run completed sooner than its network's shared batch-100
control. This supports changing the default to 200 as an additive improvement;
it does not establish the 20% target. The captures do not include comparable
resource samples, and the shared control leaves run-to-run variation
unmeasured.

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
