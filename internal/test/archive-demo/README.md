# Archive Node Demo

Runnable demonstration of Dingo's archive-node + pruning-node + Bark
proxy. Mirrors `internal/test/devnet/` in shape but adds Minio (S3) and
shows the S3 blob plugin and Bark proxy working end to end.

## Stack

- `cardano-producer` - sole block producer (cardano-node 11.0.1)
- `dingo-archive` - Dingo with `blobPlugin: s3`, Bark server on port 3003
- `dingo-pruning` - Dingo with `blobPlugin: badger`,
  `barkBaseUrl` pointing at `dingo-archive`,
  `barkPrunerFrequency: 5s`. The security window is derived from the
  ledger's stability window (3k/f = 300 slots for testnet.yaml).
- `minio` - S3-compatible blob storage; bucket `dingo-archive`

## Usage

```
./demo.sh          # operator-facing guided demo (~5 min, prints periodic stats)
./start.sh         # just bring the stack up
./run-tests.sh     # run integration tests (build tag archive_demo)
./stop.sh          # tear down
```

`demo.sh` is the right entry point for showing this off. It builds the
helper binary, brings the stack up, prints a stats line every 10 seconds
(chain tip, Minio object count, local Badger size, pruner activity), and
once the chain has crossed the security window it runs a scripted
BlockFetch from outside the stack against a pre-window block on the
pruning node and prints the byte count and timing.

## What it shows

The demo and the integration test exercise the same three claims about a
block at slot ~50, well behind the 300-slot stability window:

1. Returned successfully when BlockFetched from `dingo-pruning` (the
   bark proxy fetches it from the archive on the fly).
2. Absent from `dingo-pruning`'s local Badger blob store (the pruner
   really did delete it).
3. Present in the Minio `dingo-archive` bucket (the archive holds the
   only copy).

`demo.sh` makes claims 2 and 3 visible as a live stats stream, then runs
a scripted BlockFetch and prints the byte count and timing for claim 1.
`run-tests.sh` asserts all three programmatically and exits non-zero if
any fail.


## Ports

| Component | Default host port | Env override |
|---|---|---|
| cardano-producer NtN | 3110 | `ARCHIVEDEMO_CARDANO_PORT` |
| dingo-archive NtN    | 3111 | `ARCHIVEDEMO_DINGO_ARCHIVE_PORT` |
| dingo-archive Bark   | 3112 | `ARCHIVEDEMO_BARK_PORT` |
| dingo-pruning NtN    | 3113 | `ARCHIVEDEMO_DINGO_PRUNING_PORT` |
| Minio API            | 9100 | `ARCHIVEDEMO_MINIO_PORT` |
| Minio console        | 9101 | `ARCHIVEDEMO_MINIO_CONSOLE_PORT` |

Minio credentials: `demo` / `demodemo`.

## Implementation notes

- `tmp/dingo-pruning-data` is a host bind mount. The integration test
  stops the dingo-pruning container before opening that directory with
  the `inspect-blob` helper (Badger is single-writer, so the container
  must release the lockfile first).
- `cmd/inspect-blob/` is a small Go binary that imports Dingo's blob
  plugin and reports whether a (slot, hash) pair is present in a Badger
  store. `run-tests.sh` builds it on demand.
- The pruner's run frequency is configurable via `barkPrunerFrequency`
  in `dingo.yaml` or the `DINGO_BARK_PRUNER_FREQUENCY` env var. Default
  is 1 hour; the demo uses 5 seconds.
