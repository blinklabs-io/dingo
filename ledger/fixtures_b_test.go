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

package ledger

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// fxAttachLedgerDB gives a ledger built as a struct literal what
// NewLedgerState gives one: the database's LedgerDB set, and the ledger as
// the database's source for the families that set keeps.
func fxAttachLedgerDB(t testing.TB, ls *LedgerState) {
	t.Helper()
	if ls.config.UtxoSet == nil {
		attachTestLiveSet(t, ls)
	}
	require.NoError(t, ls.db.SetLiveUtxoSource(ls))
	ls.installNonceSource()
	ls.installEpochSource()
	ls.installPParamsSource()
	ls.installPoolSource()
}

// fxOpenDB is a test database whose LedgerDB set is already open, so every
// seeding helper writes to the set.
func fxOpenDB(t *testing.T) *database.Database {
	t.Helper()
	db := newTestDB(t)
	OpenLiveSetForTest(t, db)
	return db
}

// fxSeed writes changes to db's LedgerDB set outside any block, as an import
// does.
func fxSeed(t testing.TB, db *database.Database, changes LedgerChanges) {
	t.Helper()
	seedLedgerOf(t, db).stageLedgerChanges(nil, changes)
}

// fxSeedSnapshot saves one pool stake snapshot row in db's LedgerDB set.
func fxSeedSnapshot(
	t testing.TB,
	db *database.Database,
	snapshot *models.PoolStakeSnapshot,
) error {
	t.Helper()
	fxSeed(t, db, LedgerChanges{Snapshots: []SnapshotChange{{
		Kind: SnapshotSave, Snapshot: snapshot,
	}}})
	return nil
}

// fxCommitAt commits changes to db's LedgerDB set as the block at point, so
// the set's journal can answer a read as of that point and undo the commit
// on a rollback below it.
func fxCommitAt(
	t testing.TB,
	db *database.Database,
	point ocommon.Point,
	changes LedgerChanges,
) {
	t.Helper()
	w, ok := OpenLiveSetForTest(t, db).(LedgerChangesWriter)
	require.True(t, ok, "the fixture's set must keep ledger state")
	if len(changes.Utxo) == 0 {
		changes.Utxo = []UtxoEvent{{Point: point}}
	}
	require.NoError(t, w.ApplyLedgerChanges(&point, changes))
}

// fxSeedPParamUpdate stores a protocol parameter update proposal in db's
// LedgerDB set.
func fxSeedPParamUpdate(
	t testing.TB,
	db *database.Database,
	genesis, params []byte,
	slot, epoch uint64,
) error {
	t.Helper()
	fxSeed(t, db, LedgerChanges{PParamUpdates: []PParamUpdateChange{{
		Update: models.PParamUpdate{
			GenesisHash: genesis, Cbor: params,
			AddedSlot: slot, Epoch: epoch,
		},
	}}})
	return nil
}

// requireSpentAt requires ls's LedgerDB set to record the spend of
// txID#index at at and no earlier: a rollback to at leaves the output spent,
// and one to before restores it. A point without a hash stands for every
// change through its slot, so slots before and at bound the spend exactly.
func requireSpentAt(
	t testing.TB,
	ls *LedgerState,
	txID []byte,
	index uint32,
	before, at ocommon.Point,
) {
	t.Helper()
	restores := func(point ocommon.Point) bool {
		evt := previewSetRollback(t, ls, point)
		return slices.ContainsFunc(evt.Added, func(out UtxoOutput) bool {
			return bytes.Equal(out.Ref.Hash, txID) && out.Ref.Idx == index
		})
	}
	require.False(
		t, restores(at), "%x#%d is not yet spent at slot %d",
		txID, index, at.Slot,
	)
	require.True(
		t, restores(before), "%x#%d was spent at or before slot %d",
		txID, index, before.Slot,
	)
}

// requireCreatedAt is requireSpentAt for the creation of txID#index: a
// rollback to at keeps the output, and one to before removes it.
func requireCreatedAt(
	t testing.TB,
	ls *LedgerState,
	txID []byte,
	index uint32,
	before, at ocommon.Point,
) {
	t.Helper()
	removes := func(point ocommon.Point) bool {
		evt := previewSetRollback(t, ls, point)
		return slices.ContainsFunc(evt.Removed, func(id models.UtxoId) bool {
			return bytes.Equal(id.Hash, txID) && id.Idx == index
		})
	}
	require.False(
		t, removes(at), "%x#%d is not yet created at slot %d",
		txID, index, at.Slot,
	)
	require.True(
		t, removes(before), "%x#%d was created at or before slot %d",
		txID, index, before.Slot,
	)
}

func previewSetRollback(
	t testing.TB,
	ls *LedgerState,
	point ocommon.Point,
) UtxoEvent {
	t.Helper()
	history, ok := ls.config.UtxoSet.(UtxoSetRollbacker)
	require.True(t, ok, "the ledger's set keeps no history")
	evt, err := history.PreviewUtxoSetRollback(point)
	require.NoError(t, err)
	return evt
}

// fxSeedOutput makes encoded the live output txID#index in db's LedgerDB
// set, created at slot outside any block, with its body in the blob store
// where a block's outputs keep theirs.
func fxSeedOutput(
	t testing.TB,
	db *database.Database,
	txID []byte,
	index uint32,
	slot uint64,
	encoded []byte,
) {
	t.Helper()
	require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
		return db.Blob().SetUtxo(txn.Blob(), txID, index, encoded)
	}))
	require.NoError(t, OpenLiveSetForTest(t, db).ApplyUtxoChanges(
		nil, []UtxoEvent{{
			Point: ocommon.Point{Slot: slot},
			Added: []UtxoOutput{{
				Ref:  models.UtxoId{Hash: bytes.Clone(txID), Idx: index},
				Cbor: bytes.Clone(encoded),
			}},
		}},
	))
}

// fxImporter writes ledger state into db's LedgerDB set as a Mithril import
// does.
func fxImporter(t testing.TB, db *database.Database) *ImportLedgerDB {
	t.Helper()
	return NewImportLedgerDB(db, OpenLiveSetForTest(t, db), testLogger())
}

// fxImportAccountBaselines gives every active account in db's LedgerDB set
// the import baseline a Mithril import writes, so a rollback has the
// account's imported delegation to restore.
func fxImportAccountBaselines(t testing.TB, db *database.Database) {
	t.Helper()
	ldb := seedDB(t, db)
	refs, err := ldb.GetActiveAccountCredentials(nil)
	require.NoError(t, err)
	importer := fxImporter(t, db)
	for _, ref := range refs {
		account, err := ldb.GetAccountByCredential(
			t.Context(), ref.Tag, ref.Key, false, nil,
		)
		require.NoError(t, err)
		require.NoError(t, importer.ImportAccount(account, nil))
	}
}

// fxNetworkState reads the latest treasury and reserves from where the
// ledger keeps them.
func fxNetworkState(
	t testing.TB,
	db *database.Database,
) (treasury, reserves, slot uint64) {
	t.Helper()
	state, err := seedDB(t, db).GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	return uint64(state.Treasury), uint64(state.Reserves), state.Slot
}

// fxImportPool is seedImportPool that also records the registration in the
// set's pool history, as the certificate's own block does, so reads as of a
// slot find it.
func fxImportPool(
	t testing.TB,
	db *database.Database,
	ctx context.Context,
	txn *database.Txn,
	pool *models.Pool,
	reg *models.PoolRegistration,
) error {
	t.Helper()
	if err := seedImportPool(t, db, ctx, txn, pool, reg); err != nil {
		return err
	}
	fxSeed(t, db, LedgerChanges{PoolEvents: []PoolEventChange{{
		Pool: bytes.Clone(pool.PoolKeyHash),
		Event: PoolEvent{
			Slot:         reg.AddedSlot,
			Registration: CleanPoolRegistration(reg),
		},
	}}})
	return nil
}

// fxReregisterPool applies reg to a pool already in db's open LedgerDB set as
// a registration certificate of a registered pool does: the parameters wait
// beside the ones in force for the next epoch boundary to adopt, and the
// pool's history records the certificate.
func fxReregisterPool(
	t testing.TB,
	db *database.Database,
	reg *models.PoolRegistration,
) {
	t.Helper()
	ls := seedLedgerOf(t, db)
	state, err := ls.poolReader(nil).PoolState(reg.PoolKeyHash)
	require.NoError(t, err)
	require.NotNil(t, state, "the pool is not registered")
	state.Future = CleanPoolRegistration(reg)
	state.RetiringEpoch = nil
	fxSeed(t, db, LedgerChanges{
		Pools: []PoolChange{{Slot: reg.AddedSlot, State: *state}},
		PoolEvents: []PoolEventChange{{
			Pool: bytes.Clone(reg.PoolKeyHash),
			Event: PoolEvent{
				Slot:         reg.AddedSlot,
				Position:     uint64(reg.RegistrationIndex),
				Registration: CleanPoolRegistration(reg),
			},
		}},
	})
}

// fxRetirePool applies a retirement certificate at slot to a pool already in
// db's open LedgerDB set, leaving it retiring at epoch.
func fxRetirePool(
	t testing.TB,
	db *database.Database,
	pool []byte,
	epoch, slot uint64,
) {
	t.Helper()
	ls := seedLedgerOf(t, db)
	state, err := ls.poolReader(nil).PoolState(pool)
	require.NoError(t, err)
	require.NotNil(t, state, "the pool is not registered")
	state.RetiringEpoch = &epoch
	fxSeed(t, db, LedgerChanges{
		Pools: []PoolChange{{Slot: slot, State: *state}},
		PoolEvents: []PoolEventChange{{
			Pool:  bytes.Clone(pool),
			Event: PoolEvent{Slot: slot, RetiringEpoch: epoch},
		}},
	})
}

// fxPoolDeposit records deposit as the deposit a pool already in db's open
// LedgerDB set holds from slot on, as its registration certificate paying
// that deposit does.
func fxPoolDeposit(
	t testing.TB,
	db *database.Database,
	pool []byte,
	slot, deposit uint64,
) {
	t.Helper()
	ls := seedLedgerOf(t, db)
	state, err := ls.poolReader(nil).PoolState(pool)
	require.NoError(t, err)
	require.NotNil(t, state, "the pool is not registered")
	state.DepositHeld = &deposit
	fxSeed(t, db, LedgerChanges{Pools: []PoolChange{{
		Slot: slot, State: *state,
	}}})
}

// fxPoint is a chain point at slot whose hash is derived from the slot.
func fxPoint(slot uint64) ocommon.Point {
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash[24:], slot)
	hash[0] = 0xf7
	return ocommon.NewPoint(slot, hash)
}

// fxTip records point as the tip of ls's LedgerDB set, which a later
// rollback can return to.
func fxTip(t testing.TB, ls *LedgerState, point ocommon.Point) {
	t.Helper()
	ls.stageUtxoSet(nil, &point)
	require.False(t, ls.utxoSetFailed.Load(), "the set did not take the tip")
}

// fxJournalChain records every stored block up to the ledger's tip as a tip
// of ls's LedgerDB set, in slot order, as applying those blocks does. A
// fixture that places the ledger tip on a chain it stored without applying
// it calls this, so a rollback to any of those blocks finds its point in the
// set's journal.
func fxJournalChain(t testing.TB, ls *LedgerState) {
	t.Helper()
	tip := ls.currentTip.Point
	if tip.Slot == 0 && len(tip.Hash) == 0 {
		return
	}
	it := ls.db.BlocksInRange(0, tip.Slot)
	defer it.Close()
	journaledTip := false
	for {
		block, err := it.NextRaw()
		require.NoError(t, err)
		if block == nil {
			break
		}
		point := ocommon.NewPoint(block.Slot, bytes.Clone(block.Hash))
		if point.Slot == tip.Slot && !bytes.Equal(point.Hash, tip.Hash) {
			continue
		}
		journaledTip = journaledTip || point.Slot == tip.Slot
		fxTip(t, ls, point)
	}
	if !journaledTip {
		fxTip(t, ls, tip)
	}
}

// fxPruneJournal drops from the journal of ls's LedgerDB set every tip at or
// below slot, as the set does once those tips are a stability window behind
// its own. A rollback then reaches the block at slot and nothing below it.
func fxPruneJournal(t testing.TB, ls *LedgerState, slot uint64) {
	t.Helper()
	pruner, ok := ls.config.UtxoSet.(interface{ PruneHistory(uint64) error })
	require.True(t, ok, "the set cannot prune its journal")
	require.NoError(t, pruner.PruneHistory(slot+1))
}

// fxEmptyJournal records the ledger's tip as the tip of ls's LedgerDB set and
// drops the set's whole journal, as a set holds it once every earlier tip is
// a stability window behind: no rollback below the tip reaches.
func fxEmptyJournal(t testing.TB, ls *LedgerState) {
	t.Helper()
	tip := ls.currentTip.Point
	fxTip(t, ls, tip)
	fxPruneJournal(t, ls, tip.Slot)
}

// fxBlock commits what write stages as the block at point, so the set
// journals it under that point and a rollback below the point undoes it.
func fxBlock(
	t testing.TB,
	ls *LedgerState,
	point ocommon.Point,
	write func(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn) error,
) {
	t.Helper()
	txn := ls.db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		ls.stageUtxoSet(dbadapter.Txn(txn), &point)
		return write(ls.ledgerDB(), dbadapter.Txn(txn))
	}))
	require.False(t, ls.utxoSetFailed.Load(), "the set did not take the block")
}

// fxRollback rolls ls's LedgerDB set back to point along its journal, as a
// chain rollback does. point must be a tip the set has recorded.
func fxRollback(t testing.TB, ls *LedgerState, point ocommon.Point) {
	t.Helper()
	ls.stageUtxoSetRollback(nil, point)
	require.False(t, ls.utxoSetFailed.Load(), "the set must roll back")
}

// fxAddOutput makes txId#0 a live output of ls's LedgerDB set outside any
// block, as an output produced below the journal is.
func fxAddOutput(t testing.TB, ls *LedgerState, txId []byte) {
	t.Helper()
	tt, ok := t.(*testing.T)
	require.True(t, ok)
	require.NoError(t, ls.config.UtxoSet.ApplyUtxoChanges(nil, []UtxoEvent{{
		Added: []UtxoOutput{{
			Ref:  models.UtxoId{Hash: txId, Idx: 0},
			Cbor: boundaryOutputCbor(tt, 1_000_000),
		}},
	}}))
}

// fxSpendAt commits the spend of txId#0 as the block at point, so a rollback
// below point restores the output from the set's journal.
func fxSpendAt(
	t testing.TB,
	ls *LedgerState,
	point ocommon.Point,
	txId []byte,
) {
	t.Helper()
	fxCommitAt(t, ls.db, point, LedgerChanges{Utxo: []UtxoEvent{{
		Point:   point,
		Removed: []models.UtxoId{{Hash: txId, Idx: 0}},
	}}})
}

// fxOutputLive reports whether txId#0 is a live output of the ledger, by the
// lookup validation resolves inputs with.
func fxOutputLive(t testing.TB, ls *LedgerState, txId []byte) bool {
	t.Helper()
	_, err := ls.UtxoByRef(txId, 0)
	if errors.Is(err, database.ErrUtxoNotFound) {
		return false
	}
	require.NoError(t, err)
	return true
}
