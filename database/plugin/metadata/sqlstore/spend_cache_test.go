// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func spendCacheAddress(
	t *testing.T,
	seed byte,
) (lcommon.Address, models.StakeCredentialRef) {
	t.Helper()
	payment := bytes.Repeat([]byte{seed}, 28)
	staking := bytes.Repeat([]byte{seed + 0x80}, 28)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		payment,
		staking,
	)
	require.NoError(t, err)
	return addr, models.NewStakeCredentialRef(0, staking)
}

// spendCacheProducer builds a transaction with len(seeds) outputs; output i
// pays amount (1_000_000 + i) to the address derived from seeds[i].
func spendCacheProducer(
	t *testing.T,
	txID []byte,
	seeds []byte,
) lcommon.Transaction {
	t.Helper()
	outputs := make([]lcommon.TransactionOutput, len(seeds))
	for i, seed := range seeds {
		addr, _ := spendCacheAddress(t, seed)
		out, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(addr.String()).
			WithLovelace(uint64(1_000_000 + i)).
			Build()
		require.NoError(t, err)
		outputs[i] = out
	}
	funding, err := mockledger.NewTransactionInputBuilder().
		WithTxId(bytes.Repeat([]byte{0xff}, 32)).
		WithIndex(0).
		Build()
	require.NoError(t, err)
	tx, err := mockledger.NewTransactionBuilder().
		WithId(txID).
		WithInputs(funding).
		WithOutputs(outputs...).
		WithValid(true).
		Build()
	require.NoError(t, err)
	return tx
}

func spendCacheSpender(
	t *testing.T,
	txID []byte,
	refs ...models.UtxoId,
) lcommon.Transaction {
	t.Helper()
	inputs := make([]lcommon.TransactionInput, len(refs))
	for i, ref := range refs {
		in, err := mockledger.NewTransactionInputBuilder().
			WithTxId(ref.Hash).
			WithIndex(ref.Idx).
			Build()
		require.NoError(t, err)
		inputs[i] = in
	}
	out, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(spendCacheSink(t)).
		WithLovelace(1).
		Build()
	require.NoError(t, err)
	tx, err := mockledger.NewTransactionBuilder().
		WithId(txID).
		WithInputs(inputs...).
		WithOutputs(out).
		WithValid(true).
		Build()
	require.NoError(t, err)
	return tx
}

type spendCacheRow struct {
	hash        string
	idx         int64
	deletedSlot int64
	spentByHash string
}

// spendCacheRows lists the outputs of every transaction but the spender,
// whose own sink output is not under test.
func spendCacheRows(
	t *testing.T,
	store *Store,
	spenderID []byte,
) []spendCacheRow {
	t.Helper()
	rows, err := store.instrumentedQueryer(store.writeDB).QueryContext(
		t.Context(), `
SELECT p.hash, u.output_idx, u.deleted_slot, s.hash
FROM utxo u
JOIN "transaction" p ON p.id = u.tx_id
LEFT JOIN "transaction" s ON s.id = u.spent_at_tx_id
WHERE p.hash <> ?
ORDER BY p.hash, u.output_idx`, spenderID)
	require.NoError(t, err)
	defer rows.Close()
	var out []spendCacheRow
	for rows.Next() {
		var (
			r              spendCacheRow
			hash, spenders []byte
		)
		require.NoError(t, rows.Scan(
			&hash, &r.idx, &r.deletedSlot, &spenders,
		))
		r.hash = hex.EncodeToString(hash)
		r.spentByHash = hex.EncodeToString(spenders)
		out = append(out, r)
	}
	require.NoError(t, rows.Err())
	return out
}

// spendCacheStake reads a credential's running live-stake total.
func spendCacheStake(
	t *testing.T,
	store *Store,
	ref models.StakeCredentialRef,
) uint64 {
	t.Helper()
	var raw string
	require.NoError(t, store.instrumentedQueryer(store.writeDB).QueryRowContext(
		t.Context(), `
SELECT COALESCE((
    SELECT utxo_stake FROM reward_live_stake
    WHERE credential_tag = ? AND staking_key = ?
), '0')`,
		int64(ref.Tag), ref.Key,
	).Scan(&raw))
	value, err := parseUint64("test utxo stake", raw)
	require.NoError(t, err)
	return value
}

func spendCachePoint(slot uint64, txID []byte) ocommon.Point {
	return ocommon.Point{Slot: slot, Hash: txID}
}

// TestSpendCachedOutputsMatchByReference proves a spend served from the cache
// leaves the same utxo rows and live-stake totals as the by-reference path.
// The ten outputs exceed utxoSpendBatchSize, so the spend spans batches, and
// one extra input names an output the store never inserted.
func TestSpendCachedOutputsMatchByReference(t *testing.T) {
	t.Parallel()
	testSpendCachedOutputsMatchByReference(
		t, func(t *testing.T) *Store { return newMigratedSQLiteStore(t) },
	)
}

func testSpendCachedOutputsMatchByReference(
	t *testing.T,
	newStore func(*testing.T) *Store,
) {
	t.Helper()
	seeds := []byte{1, 2, 3, 1, 2, 3, 1, 2, 3, 4}
	producerID := bytes.Repeat([]byte{0xa1}, 32)
	spenderID := bytes.Repeat([]byte{0xa2}, 32)
	absentID := bytes.Repeat([]byte{0xa3}, 32)
	refs := make([]models.UtxoId, 0, len(seeds)+1)
	for i := range seeds {
		refs = append(refs, models.UtxoId{Hash: producerID, Idx: uint32(i)})
	}
	refs = append(refs, models.UtxoId{Hash: absentID, Idx: 0})

	run := func(t *testing.T, cached bool) ([]spendCacheRow, map[uint8]uint64) {
		t.Helper()
		store := newStore(t)
		for _, seed := range []byte{1, 2, 3, 4} {
			_, ref := spendCacheAddress(t, seed)
			establishRunningTotal(t, store, ref, 1)
		}
		require.NoError(t, store.SetTransaction(
			spendCacheProducer(t, producerID, seeds),
			spendCachePoint(10, producerID), 0, nil, false, nil, 0,
		))
		if cached {
			require.Len(t, store.spends.entries, len(seeds))
		} else {
			store.forgetSpendCache()
		}
		require.NoError(t, store.SetTransaction(
			spendCacheSpender(t, spenderID, refs...),
			spendCachePoint(20, spenderID), 0, nil, false, nil, 0,
		))
		// Only the spender's own output remains cached.
		require.Len(t, store.spends.entries, 1)
		stake := make(map[uint8]uint64)
		for _, seed := range []byte{1, 2, 3, 4} {
			_, ref := spendCacheAddress(t, seed)
			stake[seed] = spendCacheStake(t, store, ref)
			want, err := store.sumCredentialUtxoStake(
				context.Background(), store.writeDB, ref,
			)
			require.NoError(t, err)
			require.Equal(t, want, stake[seed])
		}
		return spendCacheRows(t, store, spenderID), stake
	}
	cachedRows, cachedStake := run(t, true)
	refRows, refStake := run(t, false)
	require.Len(t, cachedRows, len(seeds))
	for _, row := range cachedRows {
		require.EqualValues(t, 20, row.deletedSlot)
		require.NotEmpty(t, row.spentByHash)
	}
	require.Equal(t, refRows, cachedRows)
	require.Equal(t, refStake, cachedStake)
	for _, total := range cachedStake {
		require.Zero(t, total)
	}
}

// TestSpendCachedOutputsResolveByRowID renames the producer's hash after its
// outputs are inserted. A spend that resolved the input through the hash
// would miss the row; one that holds the row id spends it.
func TestSpendCachedOutputsResolveByRowID(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	producerID := bytes.Repeat([]byte{0xb1}, 32)
	spenderID := bytes.Repeat([]byte{0xb2}, 32)
	require.NoError(t, store.SetTransaction(
		spendCacheProducer(t, producerID, []byte{1, 2}),
		spendCachePoint(10, producerID), 0, nil, false, nil, 0,
	))
	renamedID := bytes.Repeat([]byte{0xee}, 32)
	_, err := store.writeDB.Exec(
		`UPDATE "transaction" SET hash = ? WHERE hash = ?`,
		renamedID, producerID,
	)
	require.NoError(t, err)
	require.NoError(t, store.SetTransaction(
		spendCacheSpender(t, spenderID,
			models.UtxoId{Hash: producerID, Idx: 0},
			models.UtxoId{Hash: producerID, Idx: 1},
		),
		spendCachePoint(20, spenderID), 0, nil, false, nil, 0,
	))
	var live int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM utxo u JOIN "transaction" p ON p.id = u.tx_id
WHERE u.deleted_slot = 0 AND p.hash = ?`, renamedID,
	).Scan(&live))
	require.Zero(t, live, "cached outputs must be spent by row id")
}

// TestSpendCacheClearedByRollback proves a rolled-back output's cached row id
// cannot spend the unrelated output that later reuses the id.
func TestSpendCacheClearedByRollback(t *testing.T) {
	t.Parallel()
	for name, useSavepoint := range map[string]bool{
		"rollback":              false,
		"rollback to savepoint": true,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStore(t)
			rolledID := bytes.Repeat([]byte{0xc1}, 32)
			keptID := bytes.Repeat([]byte{0xc2}, 32)
			spenderID := bytes.Repeat([]byte{0xc3}, 32)

			txn := store.Transaction(t.Context())
			savepointer, ok := txn.(interface {
				SavePoint(string) error
				RollbackTo(string) error
			})
			require.True(t, ok)
			if useSavepoint {
				require.NoError(t, savepointer.SavePoint("sp"))
			}
			require.NoError(t, store.SetTransaction(
				spendCacheProducer(t, rolledID, []byte{1}),
				spendCachePoint(10, rolledID), 0, nil, false, txn, 0,
			))
			rolledRowID := spendCacheTxnRowID(t, txn)
			if useSavepoint {
				require.NoError(t, savepointer.RollbackTo("sp"))
				require.NoError(t, store.SetTransaction(
					spendCacheProducer(t, keptID, []byte{2}),
					spendCachePoint(11, keptID), 0, nil, false, txn, 0,
				))
				require.NoError(t, txn.Commit())
			} else {
				require.NoError(t, txn.Rollback())
				require.NoError(t, store.SetTransaction(
					spendCacheProducer(t, keptID, []byte{2}),
					spendCachePoint(11, keptID), 0, nil, false, nil, 0,
				))
			}
			var keptRowID int64
			require.NoError(t, store.writeDB.QueryRow(
				`SELECT id FROM utxo`,
			).Scan(&keptRowID))
			require.Equal(
				t, rolledRowID, keptRowID,
				"the rollback must free the id for the next insert",
			)

			require.NoError(t, store.SetTransaction(
				spendCacheSpender(t, spenderID,
					models.UtxoId{Hash: rolledID, Idx: 0},
				),
				spendCachePoint(20, spenderID), 0, nil, false, nil, 0,
			))
			rows := spendCacheRows(t, store, spenderID)
			require.Len(t, rows, 1)
			require.Zero(
				t, rows[0].deletedSlot,
				"an output that was never rolled back must stay live",
			)
		})
	}
}

// spendCacheSink is an address with no staking part, so spenders add no stake.
func spendCacheSink(t *testing.T) string {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x77}, 28),
		nil,
	)
	require.NoError(t, err)
	return addr.String()
}

// TestSpendCacheClearedByStoreReplacement proves Reset and RestoreFrom, which
// replace the rows a cached id was taken from, leave no id that can spend an
// unrelated output inserted afterwards under the same id.
func TestSpendCacheClearedByStoreReplacement(t *testing.T) {
	t.Parallel()
	// wipe stands in for a provider hook that empties utxo and rewinds its
	// id sequence, as a schema drop and recreate does.
	wipe := func(store *Store) func() error {
		return func() error {
			_, err := store.writeDB.Exec(`DELETE FROM utxo`)
			if err != nil {
				return err
			}
			_, err = store.writeDB.Exec(
				`DELETE FROM sqlite_sequence WHERE name = 'utxo'`,
			)
			return err
		}
	}
	for name, replace := range map[string]func(*Store) error{
		"reset": func(store *Store) error {
			store.reset = func(context.Context) error {
				return wipe(store)()
			}
			return store.Reset(t.Context())
		},
		"restore": func(store *Store) error {
			store.restoreFrom = func(context.Context, string) error {
				return wipe(store)()
			}
			return store.RestoreFrom(t.Context(), "backup")
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStore(t)
			replacedID := bytes.Repeat([]byte{0xd1}, 32)
			keptID := bytes.Repeat([]byte{0xd2}, 32)
			spenderID := bytes.Repeat([]byte{0xd3}, 32)
			require.NoError(t, store.SetTransaction(
				spendCacheProducer(t, replacedID, []byte{1}),
				spendCachePoint(10, replacedID), 0, nil, false, nil, 0,
			))
			replacedRowID := store.spends.entries[utxoIdentityKey(replacedID, 0)].id
			require.NotZero(t, replacedRowID)
			require.NoError(t, replace(store))
			require.NoError(t, store.SetTransaction(
				spendCacheProducer(t, keptID, []byte{2}),
				spendCachePoint(11, keptID), 0, nil, false, nil, 0,
			))
			var keptRowID int64
			require.NoError(t, store.writeDB.QueryRow(
				`SELECT id FROM utxo`,
			).Scan(&keptRowID))
			require.Equal(
				t, replacedRowID, keptRowID,
				"the replacement must free the id for the next insert",
			)
			require.NoError(t, store.SetTransaction(
				spendCacheSpender(t, spenderID,
					models.UtxoId{Hash: replacedID, Idx: 0},
				),
				spendCachePoint(20, spenderID), 0, nil, false, nil, 0,
			))
			rows := spendCacheRows(t, store, spenderID)
			require.Len(t, rows, 1)
			require.Zero(
				t, rows[0].deletedSlot,
				"an output inserted after the replacement must stay live",
			)
		})
	}
}

// spendCacheTxnRowID reads the one utxo row id visible inside txn.
func spendCacheTxnRowID(t *testing.T, txn types.Txn) int64 {
	t.Helper()
	sqlTransaction, ok := txn.(*sqlTxn)
	require.True(t, ok)
	var id int64
	require.NoError(t, sqlTransaction.tx.QueryRowContext(
		context.Background(), `SELECT id FROM utxo`,
	).Scan(&id))
	return id
}

// TestSpendCacheIgnoresUncommittedOutputs proves an output whose transaction
// never committed leaves no cached row id. Cancelling the context rolls the
// transaction back inside database/sql without calling the wrapper's Rollback,
// and a Commit after that fails; in both cases the next insert reuses the
// freed id, and a spend of the uncommitted output must not reach that row.
func TestSpendCacheIgnoresUncommittedOutputs(t *testing.T) {
	t.Parallel()
	for name, commit := range map[string]bool{
		"cancelled context": false,
		"failed commit":     true,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStore(t)
			rolledID := bytes.Repeat([]byte{0xe1}, 32)
			keptID := bytes.Repeat([]byte{0xe2}, 32)
			spenderID := bytes.Repeat([]byte{0xe3}, 32)

			ctx, cancel := context.WithCancel(t.Context())
			txn := store.Transaction(ctx)
			t.Cleanup(func() { _ = txn.Rollback() })
			require.NoError(t, store.SetTransaction(
				spendCacheProducer(t, rolledID, []byte{1}),
				spendCachePoint(10, rolledID), 0, nil, false, txn, 0,
			))
			rolledRowID := spendCacheTxnRowID(t, txn)
			cancel()
			if commit {
				require.Error(t, txn.Commit())
			}
			require.NoError(t, store.SetTransaction(
				spendCacheProducer(t, keptID, []byte{2}),
				spendCachePoint(11, keptID), 0, nil, false, nil, 0,
			))
			var keptRowID int64
			require.NoError(t, store.writeDB.QueryRow(
				`SELECT id FROM utxo`,
			).Scan(&keptRowID))
			require.Equal(
				t, rolledRowID, keptRowID,
				"the rollback must free the id for the next insert",
			)

			require.NoError(t, store.SetTransaction(
				spendCacheSpender(t, spenderID,
					models.UtxoId{Hash: rolledID, Idx: 0},
				),
				spendCachePoint(20, spenderID), 0, nil, false, nil, 0,
			))
			rows := spendCacheRows(t, store, spenderID)
			require.Len(t, rows, 1)
			require.Zero(
				t, rows[0].deletedSlot,
				"an output inserted after the rollback must stay live",
			)
		})
	}
}

// TestSpendCachedOutputsResolveByRowIDInTransaction is
// TestSpendCachedOutputsResolveByRowID with the producer and spender in one
// transaction: the spender finds the producer's outputs among the entries the
// transaction has not yet committed.
func TestSpendCachedOutputsResolveByRowIDInTransaction(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	producerID := bytes.Repeat([]byte{0xf1}, 32)
	spenderID := bytes.Repeat([]byte{0xf2}, 32)
	renamedID := bytes.Repeat([]byte{0xef}, 32)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	require.NoError(t, store.SetTransaction(
		spendCacheProducer(t, producerID, []byte{1, 2}),
		spendCachePoint(10, producerID), 0, nil, false, txn, 0,
	))
	sqlTransaction, ok := txn.(*sqlTxn)
	require.True(t, ok)
	_, err := sqlTransaction.tx.ExecContext(
		context.Background(),
		`UPDATE "transaction" SET hash = ? WHERE hash = ?`,
		renamedID, producerID,
	)
	require.NoError(t, err)
	require.NoError(t, store.SetTransaction(
		spendCacheSpender(t, spenderID,
			models.UtxoId{Hash: producerID, Idx: 0},
			models.UtxoId{Hash: producerID, Idx: 1},
		),
		spendCachePoint(20, spenderID), 0, nil, false, txn, 0,
	))
	require.NoError(t, txn.Commit())
	var live int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM utxo u JOIN "transaction" p ON p.id = u.tx_id
WHERE u.deleted_slot = 0 AND p.hash = ?`, renamedID,
	).Scan(&live))
	require.Zero(t, live, "uncommitted outputs must be spent by row id")
}
