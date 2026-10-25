# Mithril Core sync: Badger memtable

This comparison measures Preprod Core bootstrap on the Dingo v0.87.1 release
binary. Duration ends at the `Mithril bootstrap complete` log event. The
matched runs used the same snapshot and topology on the same hardware.

| Run | Badger memtable | Bootstrap time |
| --- | ---: | ---: |
| Release-default control | 512 MiB | 18m54.542s |
| Candidate | 1 GiB | 17m13.306s |

The candidate completed 1m41.236s sooner (8.92%). Two earlier matched pairs
averaged 25m16.563s for the 1 GiB candidate, 9.27% faster than their controls.
Absolute times are comparable only within a matched pair, so that mean is not
compared with the table above.

Peak process RSS during the latest candidate import was 538.6 MiB higher than
its matched control. RSS includes mapped files and is not a Go-heap measurement.
