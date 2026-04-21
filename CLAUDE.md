# CLAUDE.md

Go Cardano node (Ouroboros). Derivable info (build targets, flags, package layout) lives in `Makefile`, `go.mod`, `node.go`, `dingo.yaml.example`. This file is only what isn't derivable.

## Testing

- No `time.Sleep()` for sync. Use `internal/test/testutil/`:
  - `WaitForCondition` / `require.Eventually` — 2–5s timeout, 5–10ms interval, lock shared state inside the condition fn
  - `RequireReceive` / `RequireNoReceive` for channel assertions
  - `context.WithTimeout` for graceful shutdown
- `make test` runs with `-race`.
- Integration tests in `internal/integration/` load real blocks from `database/immutable/testdata/`.
- Mock fixtures come from `github.com/blinklabs-io/ouroboros-mock` (`fixtures/`, `ledger/`, `conformance/`). Never add local ledger/consensus/network mocks — extend the shared library.

## Pre-commit

```shell
golangci-lint run ./...
nilaway ./...
modernize ./...   # --fix to auto-apply
```

## Non-obvious invariants

- CBOR offsets: UTxOs/txs store 52-byte `CborOffset` refs (magic `"DOFF"` + slot + hash + offset + length), not full CBOR. Resolved via `TieredCborCache` (hot → block LRU → cold blob extract). See `database/cbor_offset.go`, `database/cbor_cache.go`, `database/block_indexer.go`.
- EventBus for async cross-component notifications: block/chain/mempool/peer events go through `event.EventBus.SubscribeFunc()`. Synchronous state queries between components still use direct method calls.
- Cert ordering: multiple certs per slot — tie-break with `Order("added_slot DESC, block_index DESC, cert_index DESC")` (`cert_index` resets per tx, so `block_index` is required to disambiguate across txs in the same block).
- Rollbacks: `Chain.Rollback(point)` emits `ChainRollbackEvent`; check `TransactionEvent.Rollback` for undo.
- Plugins (`database/plugin/`): blob = `badger` (default) | `gcs` | `s3`; metadata = `sqlite` (default) | `mysql` | `postgres`.
- Config priority: CLI > env > YAML > defaults.

## Profiling

```shell
./dingo --cpuprofile=cpu.prof --memprofile=mem.prof load database/immutable/testdata
go tool pprof cpu.prof
```
