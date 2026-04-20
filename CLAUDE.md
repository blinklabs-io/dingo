# CLAUDE.md

Go-based Cardano node implementing Ouroboros. Most of what you need is derivable from code (`Makefile`, `go.mod`, `node.go`, `dingo.yaml.example`). Below is what isn't.

## Testing rules

- **Never use `time.Sleep()` for synchronization.** Use helpers in `internal/test/testutil/`:
  - `WaitForCondition` / `require.Eventually` for polling (2–5s timeout, 5–10ms interval, lock shared state inside the condition fn)
  - `RequireReceive` / `RequireNoReceive` for channel assertions
  - `context.WithTimeout` for graceful shutdown — not sleep
- Race detection is on by default in `make test`.
- Integration tests in `internal/integration/` load real blocks from `database/immutable/testdata/`.

## Before committing

```bash
golangci-lint run ./...   # .golangci.yml configured
nilaway ./...
modernize ./...           # --fix to auto-apply
```

## Non-obvious architecture

- **CBOR offset storage**: UTxOs/txs store 52-byte `CborOffset` refs (magic `"DOFF"`, slot, hash, offset, length), not full CBOR. Resolved via `TieredCborCache` (hot → block LRU → cold extract from blob store). See `database/cbor_offset.go`, `database/cbor_cache.go`, `database/block_indexer.go`.
- **Plugins** (`database/plugin/`): blob = `badger` (default) / `gcs` / `s3`; metadata = `sqlite` (default) / `mysql` / `postgres`.
- **Event bus**: components communicate via `event.EventBus.SubscribeFunc()`, not direct calls.

## Profiling

```bash
./dingo --cpuprofile=cpu.prof --memprofile=mem.prof load database/immutable/testdata
go tool pprof cpu.prof
```
