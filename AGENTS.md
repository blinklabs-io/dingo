# AGENTS.md

Concise guide for AI agents working with Dingo. For full details, see `ARCHITECTURE.md`.

## Quick Reference

**Dingo**: Go-based Cardano blockchain node by Blink Labs (Ouroboros protocol, UTxO tracking, pluggable storage).

```bash
make              # Format, test, build
make test         # Tests with race detection
make bench        # Run benchmarks
go run ./cmd/dingo/         # Run without building
go test -v -race -run TestName ./path/to/package/  # Single test
```

## Key Packages

| Package | Purpose |
|---------|---------|
| `chain/` | Blockchain state, fork detection, rollback |
| `chainselection/` | Multi-peer chain comparison (Ouroboros Praos) |
| `chainsync/` | Block sync state tracking |
| `connmanager/` | Network connection lifecycle |
| `database/` | Storage plugins (badger, sqlite, postgres, mysql, gcs, s3) |
| `event/` | Event bus (async pub/sub, 4 workers) |
| `ledger/` | UTxO state, protocol params, Plutus execution |
| `mempool/` | Transaction pool & validation |
| `ouroboros/` | Protocol handlers (N2N, N2C) |
| `peergov/` | Peer governance, churn, quotas |
| `utxorpc/` | UTxO RPC gRPC server |

## Code Quality (Run Before Committing)

```bash
golangci-lint run ./...    # Linters (39 via .golangci.yml)
nilaway ./...              # Nil-safety analyzer
modernize ./...            # Modern Go idioms
make golines               # 80 char line limit
```

## Essential Patterns

### Error Handling
```go
// Wrap errors with context
return fmt.Errorf("failed to process block: %w", err)
```

### Event-Driven Communication
```go
// Subscribe to events (never call components directly)
eventBus.SubscribeFunc(chain.ChainForkEventType, handler)
```

### Database Queries
```go
// GOOD: Batch fetch (single query)
db.Where("id IN ?", ids).Find(&records)

// BAD: N+1 queries
for _, id := range ids {
    db.Where("id = ?", id).First(&record)
}

// Certificate ordering: use cert_index as tie-breaker
Order("added_slot DESC, cert_index DESC")
```

### Rollback Handling
- `Chain.Rollback(point)` emits `ChainRollbackEvent`
- State restoration in `database/plugin/metadata/sqlite/`
- Use `TransactionEvent.Rollback` field to detect undo operations

### CBOR Storage
- UTxOs/TXs stored as 52-byte `CborOffset` (magic "DOFF" + slot + hash + offset + length)
- `TieredCborCache` resolves: hot cache → block LRU → cold extraction

### Stake Snapshots
- Mark/Set/Go rotation at epoch boundaries (Ouroboros Praos)
- `LedgerView.GetStakeDistribution(epoch)` for leader election queries
- `PoolStakeSnapshot` model stores per-pool stake
- `EpochSummary` stores network aggregates

## Key Event Types

| Event | Purpose |
|-------|---------|
| `chain.update` | Block added to chain |
| `chain.fork-detected` | Fork detected |
| `chainselection.chain_switch` | Active peer changed |
| `epoch.transition` | Epoch boundary crossed (triggers stake snapshot) |
| `mempool.add_tx` / `mempool.remove_tx` | Transaction lifecycle |
| `connmanager.conn-closed` | Connection lifecycle |
| `peergov.peer-churn` | Peer rotation |

## Configuration

Priority: CLI flags > Env vars > YAML config > Defaults

Key env vars: `CARDANO_NETWORK`, `CARDANO_DATABASE_PATH`, `DINGO_DATABASE_BLOB_PLUGIN`, `DINGO_DATABASE_METADATA_PLUGIN`

## Critical Anti-Patterns

1. **Don't bypass EventBus** - Components communicate via events, not direct calls
2. **Don't use N+1 queries** - Batch fetch with `WHERE id IN ?`
3. **Don't forget cert_index** - Multiple certs in same slot need ordering
4. **Don't ignore rollbacks** - Check `TransactionEvent.Rollback` field
5. **Don't skip linting** - Run `golangci-lint`, `nilaway`, `modernize` before commits

## Testing

- Integration tests: `internal/integration/`
- Test data: `database/immutable/testdata/` (real blocks slots 0-1.3M)
- Benchmarks: `make bench` (28+ benchmarks)
