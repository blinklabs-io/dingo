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
