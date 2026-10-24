# Mithril Core bootstrap benchmark

The duration ends at the Mithril bootstrap complete event. It excludes later catch-up to the network tip.

## v0.87.1 Preview

Runs used release commit 8d3796436788d4d05f11abc3a27ad51bf4a15895, snapshot b3d1c0169fba08a3598f3a4cb125ecbebfe2068df663f06c5ffe16a0e84e34a1, the same machine, and a 512 MiB Badger memtable. Both used the PGO-enabled v0.87.1 release binary and had an effective GOMAXPROCS of 8.

| Database workers | Bootstrap durations | Mean | Change from default |
| --- | ---: | ---: | ---: |
| 5 (release default) | 20m54.004s | 20m54.004s | baseline |
| 8 | 19m14.504s, 18m00.035s | 18m37.270s | 10.9% faster |

The 8-worker runs used the --db-workers setting. They did not reach the 20% target of 16m43.203s; their mean was 1m54.067s above it. This result covers Core mode. API-mode performance with eight workers has not been measured.

The serving-window resource samples averaged 129.8% CPU and peaked at 11.7 GiB RSS with the default. The two 8-worker captures averaged 134.4% and 134.9% CPU and peaked at 12.4 GiB and 13.1 GiB RSS, respectively. These resource samples are separate from the bootstrap timing.

## Order-balanced repeat (2026-10-24)

This repeat used the same pinned Preview snapshot and an 8-CPU host. All runs used the PGO-enabled v0.87.1 release binary, `GOMAXPROCS=8`, and the default 512 MiB Badger memtable. The 8-worker arm set `--db-workers 8`; the automatic arm left it unset. The pair order was reversed for the second comparison.

| Pair | Order | 8 workers | Automatic (default 5) | Reduction |
| --- | --- | ---: | ---: | ---: |
| 1 | 8 workers, then automatic | 20m57.684s | 23m04.865s | 9.184% |
| 2 | Automatic, then 8 workers | 21m34.042s | 22m46.487s | 5.302% |
| Mean | — | 21m15.863s | 22m55.676s | 7.256% |

The two order-balanced pairs show a repeatable full-bootstrap gain, below the 20% target. Pair 2's immutable block copy took 9m24.217s with eight workers and 10m46.935s with automatic workers. Snapshot acquisition was 6.376s slower with eight workers, while UTxO import was effectively unchanged. An additional unpaired eight-worker run took 21m57.001s and is excluded from the paired mean.

Peak sampled RSS across the paired eight-worker runs was 14.680–14.821 GiB, versus 14.361–14.664 GiB for controls. These configuration comparisons used the release binary with the worker count overridden explicitly; they measure the full-bootstrap impact of the setting, not a binary built from the pull request commit. API-mode impact is not measured here.
