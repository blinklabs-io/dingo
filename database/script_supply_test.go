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

package database

import (
	"context"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func supplyTestUtxo(
	id byte,
	slot, deleted, amount uint64,
	script bool,
) *models.Utxo {
	txID := make([]byte, 32)
	txID[0] = id
	return &models.Utxo{
		TxId:          txID,
		PaymentKey:    make([]byte, 28),
		AddedSlot:     slot,
		DeletedSlot:   deleted,
		Amount:        types.Uint64(amount),
		PaymentScript: script,
	}
}

// deferredImporter is the snapshot importer's batched UTxO load, which the
// metadata store offers outside the MetadataStore interface.
func deferredImporter(t *testing.T, db *Database) interface {
	ImportUtxosDeferredRewardLiveStakeRefresh([]models.Utxo, types.Txn) error
} {
	t.Helper()
	importer, ok := db.Metadata().(interface {
		ImportUtxosDeferredRewardLiveStakeRefresh(
			[]models.Utxo, types.Txn,
		) error
	})
	require.True(t, ok)
	return importer
}

func scriptSupply(t *testing.T, db *Database) uint64 {
	t.Helper()
	got, err := db.Metadata().GetScriptLockedSupply(nil)
	require.NoError(t, err)
	return got
}

// The rollback sweep drops the supply rows above the rollback slot and the
// restored UTxO rows agree with what remains.
func TestTruncateAfterSlotRestoresScriptLockedSupply(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	target := testIndexedBlock(50, 1, 0x15)
	require.NoError(t, db.BlockCreate(target, nil))
	for _, u := range []*models.Utxo{
		supplyTestUtxo(1, 40, 0, 300, true),
		supplyTestUtxo(2, 60, 0, 500, true),
		supplyTestUtxo(3, 60, 0, 900, false),
	} {
		require.NoError(t, db.Metadata().CreateUtxo(nil, u))
	}
	require.NoError(
		t,
		db.MetadataTxn(context.Background(), true).Do(func(txn *Txn) error {
			return db.Metadata().MarkUtxosDeletedAtSlot(
				txn.Metadata(),
				[]types.UtxoKey{{TxId: supplyTestUtxo(1, 0, 0, 0, true).TxId}},
				70,
			)
		}),
	)
	require.EqualValues(t, 500, scriptSupply(t, db))

	require.NoError(t, db.Transaction(context.Background(), true).Do(
		func(txn *Txn) error {
			_, _, err := db.TruncateAfterSlot(
				context.Background(),
				ocommon.Point{Slot: 50, Hash: target.Hash},
				0, txn,
			)
			return err
		},
	))
	require.EqualValues(t, 300, scriptSupply(t, db))
}

// A recovered consumed input is imported live through ImportUtxos and spent
// later by the maintained primitives, so the import must credit it or the
// spend debits lovelace that was never counted.
func TestImportUtxosCreditsLiveScriptOutputs(t *testing.T) {
	t.Parallel()
	for _, deferred := range []bool{false, true} {
		t.Run(map[bool]string{false: "per-row", true: "batched"}[deferred],
			func(t *testing.T) {
				t.Parallel()
				db := newTestDB(t)
				require.NoError(t, db.Metadata().CreateUtxo(
					nil, supplyTestUtxo(1, 10, 0, 100, true),
				))
				imported := []models.Utxo{
					*supplyTestUtxo(2, 20, 0, 50, true),
					*supplyTestUtxo(3, 20, 0, 900, false),
					*supplyTestUtxo(4, 20, 25, 300, true),
				}
				require.NoError(t, db.MetadataTxn(
					context.Background(), true,
				).Do(func(txn *Txn) error {
					if deferred {
						return deferredImporter(t, db).
							ImportUtxosDeferredRewardLiveStakeRefresh(
								imported, txn.Metadata(),
							)
					}
					return db.Metadata().ImportUtxos(
						imported, txn.Metadata(),
					)
				}))
				require.EqualValues(t, 150, scriptSupply(t, db))
				require.NoError(t, db.MetadataTxn(
					context.Background(), true,
				).Do(func(txn *Txn) error {
					return db.Metadata().MarkUtxosDeletedAtSlot(
						txn.Metadata(),
						[]types.UtxoKey{{TxId: imported[0].TxId}},
						30,
					)
				}))
				require.EqualValues(t, 100, scriptSupply(t, db))
			})
	}
}

// Re-importing an output as live clears a spend recorded locally, which puts
// the output back in the live set and its lovelace back in the total.
func TestImportUtxosClearedSpendRestoresScriptLockedSupply(t *testing.T) {
	t.Parallel()
	for _, deferred := range []bool{false, true} {
		t.Run(map[bool]string{false: "per-row", true: "batched"}[deferred],
			func(t *testing.T) {
				t.Parallel()
				db := newTestDB(t)
				out := supplyTestUtxo(1, 10, 0, 100, true)
				require.NoError(t, db.Metadata().CreateUtxo(nil, out))
				require.NoError(t, db.MetadataTxn(
					context.Background(), true,
				).Do(func(txn *Txn) error {
					return db.Metadata().MarkUtxosDeletedAtSlot(
						txn.Metadata(),
						[]types.UtxoKey{{TxId: out.TxId}},
						20,
					)
				}))
				require.EqualValues(t, 0, scriptSupply(t, db))
				live := []models.Utxo{*supplyTestUtxo(1, 10, 0, 100, true)}
				require.NoError(t, db.MetadataTxn(
					context.Background(), true,
				).Do(func(txn *Txn) error {
					if deferred {
						return deferredImporter(t, db).
							ImportUtxosDeferredRewardLiveStakeRefresh(
								live, txn.Metadata(),
							)
					}
					return db.Metadata().ImportUtxos(live, txn.Metadata())
				}))
				require.EqualValues(t, 100, scriptSupply(t, db))
			})
	}
}

// Contributions are summed per slot before they reach the aggregate, so the
// sum itself must be range-checked: math.MaxInt64 and 1 would otherwise wrap
// to a negative delta the aggregate accepts.
func TestImportUtxosRejectsScriptSupplyOverflow(t *testing.T) {
	t.Parallel()
	for _, deferred := range []bool{false, true} {
		t.Run(map[bool]string{false: "per-row", true: "batched"}[deferred],
			func(t *testing.T) {
				t.Parallel()
				db := newTestDB(t)
				outs := []models.Utxo{
					*supplyTestUtxo(1, 10, 0, math.MaxInt64, true),
					*supplyTestUtxo(2, 10, 0, 1, true),
				}
				err := db.MetadataTxn(context.Background(), true).Do(
					func(txn *Txn) error {
						if deferred {
							return deferredImporter(t, db).
								ImportUtxosDeferredRewardLiveStakeRefresh(
									outs, txn.Metadata(),
								)
						}
						return db.Metadata().ImportUtxos(
							outs, txn.Metadata(),
						)
					},
				)
				require.ErrorContains(t, err, "out of range")
				require.EqualValues(t, 0, scriptSupply(t, db))
			})
	}
}

// Deleting the outputs created above a slot without the rest of the rollback
// sweep (the completed-import cleanup) takes their contribution out of every
// supply row they were live at, so the total matches the remaining rows and a
// replay that re-creates one counts it once.
func TestUtxosDeleteRolledbackRemovesScriptLockedSupply(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	for _, u := range []*models.Utxo{
		supplyTestUtxo(1, 40, 0, 300, true),
		supplyTestUtxo(2, 40, 0, 200, true),
		supplyTestUtxo(3, 60, 0, 500, true),
		supplyTestUtxo(4, 60, 0, 700, true),
		supplyTestUtxo(5, 60, 0, 900, false),
	} {
		require.NoError(t, db.Metadata().CreateUtxo(nil, u))
	}
	spend := func(id byte, slot uint64) {
		require.NoError(
			t,
			db.MetadataTxn(context.Background(), true).Do(func(txn *Txn) error {
				return db.Metadata().MarkUtxosDeletedAtSlot(
					txn.Metadata(),
					[]types.UtxoKey{{TxId: supplyTestUtxo(id, 0, 0, 0, true).TxId}},
					slot,
				)
			}),
		)
	}
	spend(2, 70)
	spend(4, 80)
	require.EqualValues(t, 800, scriptSupply(t, db))

	require.NoError(t, db.UtxosDeleteRolledback(context.Background(), 50, nil))
	require.EqualValues(t, 300, scriptSupply(t, db))

	require.NoError(
		t,
		db.Metadata().CreateUtxo(nil, supplyTestUtxo(3, 60, 0, 500, true)),
	)
	require.EqualValues(t, 800, scriptSupply(t, db))
}

// The trim keeps the base row at the floor and drops the rows below it, and
// refuses a transaction that could commit outside the caller's.
func TestPruneScriptLockedSupplyBefore(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{DataDir: dataDir})
	require.NoError(t, err)
	for _, u := range []*models.Utxo{
		supplyTestUtxo(1, 10, 0, 100, true),
		supplyTestUtxo(2, 20, 0, 200, true),
		supplyTestUtxo(3, 30, 0, 400, true),
	} {
		require.NoError(t, db.Metadata().CreateUtxo(nil, u))
	}
	require.NoError(t, db.MetadataTxn(context.Background(), true).Do(
		func(txn *Txn) error {
			return db.PruneScriptLockedSupplyBefore(25, txn)
		},
	))
	raw, err := openMetadataSQLite(dataDir)
	require.NoError(t, err)
	defer raw.Close()
	var oldest, rows int
	require.NoError(t, raw.QueryRow(
		"SELECT MIN(slot), COUNT(*) FROM script_locked_supply",
	).Scan(&oldest, &rows))
	require.Equal(t, 20, oldest, "the base row at the floor stays")
	require.Equal(t, 2, rows)
	require.EqualValues(t, 700, scriptSupply(t, db))

	for name, txn := range map[string]*Txn{
		"none":      nil,
		"blob":      db.BlobTxn(true),
		"read-only": db.MetadataTxn(context.Background(), false),
	} {
		err := db.PruneScriptLockedSupplyBefore(25, txn)
		require.ErrorIs(t, err, types.ErrTxnWrongType, name)
		if txn != nil {
			txn.Rollback() //nolint:errcheck
		}
	}
}
