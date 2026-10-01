# Dingo Timed Chunk Load Results

Results of `make test-load-timing` (see [benchmarks](../benchmarks.md)). Each
row is one run on the named host and commit; compare only runs from the same
host.

Targets: 300 chunks under 5 seconds, 3,000 chunks under 1 minute, 24,000
chunks under 15 minutes.

| Date | Commit | Host | Chunks | Blocks | Wall time | Blocks/s | Peak RSS | Target met |
|------|--------|------|--------|--------|-----------|----------|----------|------------|
| 2026-09-30 | `9839227f3` plus timing script | linux/arm64, 128 cores, Go 1.26.7 | 300 | 59,315 | 154s | 385.2 | 679 MiB | no |

The 3,000 and 24,000 chunk sets need a Mithril snapshot's `immutable/`
directory and have not been timed yet.
