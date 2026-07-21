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
make import-boundaries
make golines
```

## Testing rules

- No `time.Sleep()` for sync — use `internal/test/testutil/` (`WaitForCondition`, `RequireReceive`, `context.WithTimeout`).
- Integration tests: `internal/integration/` + `database/immutable/testdata/` (real blocks, slots 0–1.3M).
- Mock fixtures come from `github.com/blinklabs-io/ouroboros-mock` (`fixtures/`, `ledger/`, `conformance/`). Never duplicate mocks inside dingo — extend the shared library so every Blink Labs app (dingo, gouroboros, adder, ...) reuses the same test surface.
- DevNet end-to-end (`internal/test/devnet/run-tests.sh`): default run is an all-dingo network (three Dingo producers + relay) with `txpump` driving the mempool; it validates the generic consensus and liveness suite dingo-vs-dingo and hosts dingo-only feature tests (CIP-50 pledge leverage) that have no cardano-node reference. Use it for any change touching consensus, block production, header/VRF/KES/OpCert verification, chain selection, mempool, tx submission, NtN/NtC protocols, epoch boundaries, or nonce computation. `./run-tests.sh --conformance` runs Dingo beside `cardano-node` for compatibility and conformance with the reference. Conformance tests in `internal/test/conformance/` are still mandatory after every change.

## Documentation requirements

- Treat `DATABASE.md` and `ARCHITECTURE.md` as part of the change bar, like tests. Before finishing any code change, decide whether either document needs an update; update it in the same change when it does.
- Update `DATABASE.md` for any change to GORM models, migrated tables, table relationships, SQL query/API surfaces in `metadata.MetadataStore`, blob-store key layout, CBOR/offset encodings, storage plugins, pruning/tombstone behavior, or anything external Postgres/MySQL/SQLite/blob users rely on.
- Update `ARCHITECTURE.md` for any change to component responsibilities, package boundaries, startup/composition, EventBus topics/payloads, plugin interfaces, lifecycle/concurrency behavior, or cross-component flows among ledger, database, mempool, networking, API, and node wiring.
- In final responses, report documentation status the same way tests are reported: either list the docs updated or explicitly state that `DATABASE.md`/`ARCHITECTURE.md` were checked and not affected.

## Non-obvious invariants

- EventBus for async cross-component notifications: use `event.EventBus.SubscribeFunc()` for block/chain/mempool/peer events. Synchronous state queries between components still use direct method calls.
- CBOR offsets: UTxOs/txs stored as 52-byte refs (magic `"DOFF"` + slot + hash + offset + length), resolved by `TieredCborCache` (hot → block LRU → cold extract). See `database/cbor_offset.go`.
- Cert ordering: `Order("added_slot DESC, block_index DESC, cert_index DESC")` — `cert_index` resets per tx, so `block_index` is required to disambiguate across txs in the same block.
- Rollbacks: delivered on `chain.update` as a `chain.ChainRollbackEvent` payload (no separate `chain.rollback` topic); also subscribe to `chain.fork_detected` for fork metrics. Check `TransactionEvent.Rollback` for undo.
- Stake snapshots: mark/set/go rotation at epoch boundaries (Praos). `LedgerView.GetStakeDistribution(epoch)` for leader election. Per-pool stake in `PoolStakeSnapshot`; aggregates in `EpochSummary`.
- Plugins (`database/plugin/`): blob = `badger` | `gcs` | `s3`; metadata = `sqlite` | `mysql` | `postgres`.

## Isolation requirements

- Composition belongs in `node.go`, root-package helpers, `cmd/dingo`, or `internal/node`; do not make domain packages start, stop, or configure unrelated subsystems.
- Use EventBus for async notifications only. For synchronous state reads, inject narrow interfaces or callbacks through constructors instead of importing a sibling package just to call one method.
- Keep dependency direction stable: `database/` and storage plugins must not import ledger, mempool, networking, node, or API packages; `connmanager/` must not know Ouroboros or ledger semantics; `chainselection/` must not validate blocks or mutate ledger state.
- Ledger owns validation, rollback repair, nonce/epoch logic, and ledger queries. It should not directly trigger connection-manager or peer-governance actions; emit a neutral event/callback and let node/network wiring translate it.
- Peer governance policy types should not leak into ledger data extraction. Put adapters at the composition boundary or behind neutral DTOs/interfaces.
- API packages should expose server behavior through package-local interfaces. Keep concrete adapters to `ledger`, `database`, and `mempool` narrow, and require an explicit architecture review before adding new root-level API packages.
- Tests may use `internal/test/testutil`, but production packages must not import `internal/test` or duplicate fixtures that belong in `ouroboros-mock`.

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
