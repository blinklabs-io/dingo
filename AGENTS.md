# AGENTS.md

Go Cardano node (Ouroboros). See `CLAUDE.md` for detailed rules; this file has additional content (Build/test section, `make golines` in pre-commit, Key events table) and a different structure — the two are related but not exact copies. Package layout, targets, and flags are derivable from `Makefile`, `go.mod`, `node.go`.

## Build / test

```
make              # fmt, test, build
make test         # tests with -race
go test -v -race -run TestName ./path/to/pkg/
```

## Pre-commit

```
golangci-lint run ./...
nilaway ./...
modernize ./...
make golines
```

## Testing rules

- No `time.Sleep()` for sync — use `internal/test/testutil/` (`WaitForCondition`, `RequireReceive`, `context.WithTimeout`).
- Integration tests: `internal/integration/` + `database/immutable/testdata/` (real blocks, slots 0–1.3M).
- Mock fixtures come from `github.com/blinklabs-io/ouroboros-mock` (`fixtures/`, `ledger/`, `conformance/`). Never duplicate mocks inside dingo — extend the shared library so every Blink Labs app (dingo, gouroboros, adder, ...) reuses the same test surface.

## Non-obvious invariants

- EventBus for async cross-component notifications: use `event.EventBus.SubscribeFunc()` for block/chain/mempool/peer events. Synchronous state queries between components still use direct method calls.
- CBOR offsets: UTxOs/txs stored as 52-byte refs (magic `"DOFF"` + slot + hash + offset + length), resolved by `TieredCborCache` (hot → block LRU → cold extract). See `database/cbor_offset.go`.
- Cert ordering: `Order("added_slot DESC, block_index DESC, cert_index DESC")` — `cert_index` resets per tx, so `block_index` is required to disambiguate across txs in the same block.
- Rollbacks: delivered on `chain.update` as a `chain.ChainRollbackEvent` payload (no separate `chain.rollback` topic); also subscribe to `chain.fork_detected` for fork metrics. Check `TransactionEvent.Rollback` for undo.
- Stake snapshots: mark/set/go rotation at epoch boundaries (Praos). `LedgerView.GetStakeDistribution(epoch)` for leader election. Per-pool stake in `PoolStakeSnapshot`; aggregates in `EpochSummary`.
- Plugins (`database/plugin/`): blob = `badger` | `gcs` | `s3`; metadata = `sqlite` | `mysql` | `postgres`.

## Key events

| Event | Meaning |
|---|---|
| `chain.update` | block added |
| `chain.fork_detected` | fork |
| `chainselection.chain_switch` | active peer changed |
| `epoch.transition` | epoch boundary — triggers stake snapshot |
| `mempool.add_tx` / `mempool.remove_tx` | tx lifecycle |
| `connmanager.conn_closed` | connection closed |
| `peergov.peer_churn` | peer rotation |

## Config

Priority: CLI > env > YAML > defaults. Key env vars: `CARDANO_NETWORK`, `CARDANO_DATABASE_PATH`, `DINGO_DATABASE_BLOB_PLUGIN`, `DINGO_DATABASE_METADATA_PLUGIN`.
