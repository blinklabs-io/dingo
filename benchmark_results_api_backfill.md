# Dingo Mithril API Backfill Benchmark Results

Focused Mithril API-mode metadata backfill results are tracked here separately
from the broader ledger and database benchmark table.

## May 29-30, 2026 Preview Backfill

The latest local Kubernetes preview run measured `origin/main` commit
`d73407b6`. Clean current `main` failed during Mithril ledger-state UTxO asset
import because deferred SQLite index handling drops the asset uniqueness needed
by `ON CONFLICT (utxo_id, policy_id, name) DO NOTHING`; that blocker is tracked
as #2457. The completed measurement used only a local diagnostic workaround for
#2457, keeping `asset.policy_id` out of the deferred-index manifest.

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
