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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// fxAttachLedgerDB gives a ledger built as a struct literal what
// NewLedgerState gives one: the database's LedgerDB set, and the ledger as
// the database's source for the families that set keeps.
func fxAttachLedgerDB(t testing.TB, ls *LedgerState) {
	t.Helper()
	if ls.config.UtxoSet == nil {
		if _, open := LiveSetOfForTest(ls.db); !open {
			fxStoreSeededUtxoCbor(t, ls.db)
		}
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
// does. The set must be open.
func fxSeed(t testing.TB, db *database.Database, changes LedgerChanges) {
	t.Helper()
	ls := seedLedgerOf(db)
	require.NotNil(t, ls, "the database has no open LedgerDB set")
	ls.stageLedgerChanges(nil, changes)
}

// fxSeedSnapshot saves one pool stake snapshot row in db's LedgerDB set, or
// in the metadata store for the set to take over when it opens later. It
// returns an error so it stands where the metadata store's save did.
func fxSeedSnapshot(
	t testing.TB,
	db *database.Database,
	snapshot *models.PoolStakeSnapshot,
) error {
	t.Helper()
	if seedLedgerOf(db) == nil {
		return db.Metadata().SavePoolStakeSnapshot(snapshot, nil)
	}
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
	set, ok := LiveSetOfForTest(db)
	require.True(t, ok, "the database has no open LedgerDB set")
	w, ok := set.(LedgerChangesWriter)
	require.True(t, ok, "the fixture's set must keep ledger state")
	if len(changes.Utxo) == 0 {
		changes.Utxo = []UtxoEvent{{Point: point}}
	}
	require.NoError(t, w.ApplyLedgerChanges(&point, changes))
}

// fxSeedPParamUpdate stores a protocol parameter update proposal in db's
// LedgerDB set, or in the metadata store for the set to take over when it
// opens later.
func fxSeedPParamUpdate(
	t testing.TB,
	db *database.Database,
	genesis, params []byte,
	slot, epoch uint64,
) error {
	t.Helper()
	if seedLedgerOf(db) == nil {
		return db.SetPParamUpdate(genesis, params, slot, epoch, nil)
	}
	fxSeed(t, db, LedgerChanges{PParamUpdates: []PParamUpdateChange{{
		Update: models.PParamUpdate{
			GenesisHash: genesis, Cbor: params,
			AddedSlot: slot, Epoch: epoch,
		},
	}}})
	return nil
}

// fxStoreSeededUtxoCbor gives every live metadata UTxO row that has no body
// in the blob store a Shelley output paying the row's amount to its staking
// key, so the LedgerDB set can take the rows over when it opens.
func fxStoreSeededUtxoCbor(t testing.TB, db *database.Database) {
	t.Helper()
	type row struct {
		txID, stakeKey []byte
		index          uint32
		amount         uint64
	}
	var seeded []row
	require.NoError(t, db.IterateLiveUtxoRefs(
		t.Context(), nil, func(u *models.Utxo) error {
			seeded = append(seeded, row{
				txID:     bytes.Clone(u.TxId),
				stakeKey: bytes.Clone(u.StakingKey),
				index:    u.OutputIdx,
				amount:   uint64(u.Amount),
			})
			return nil
		},
	))
	for _, r := range seeded {
		read := db.BlobTxn(false)
		_, err := db.Blob().GetUtxo(read.Blob(), r.txID, r.index)
		read.Release()
		if err == nil {
			continue
		}
		addrType := lcommon.AddressTypeKeyNone
		if len(r.stakeKey) > 0 {
			addrType = lcommon.AddressTypeKeyKey
		}
		addr, err := lcommon.NewAddressFromParts(
			uint8(addrType), lcommon.AddressNetworkTestnet,
			bytes.Repeat([]byte{0x37}, lcommon.AddressHashSize), r.stakeKey,
		)
		require.NoError(t, err)
		encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
			OutputAddress: addr, OutputAmount: r.amount,
		})
		require.NoError(t, err)
		require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
			return db.Blob().SetUtxo(txn.Blob(), r.txID, r.index, encoded)
		}))
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
	if seedLedgerOf(db) == nil {
		return nil
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
	ls := seedLedgerOf(db)
	require.NotNil(t, ls, "the database has no open LedgerDB set")
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
	ls := seedLedgerOf(db)
	require.NotNil(t, ls, "the database has no open LedgerDB set")
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
	ls := seedLedgerOf(db)
	require.NotNil(t, ls, "the database has no open LedgerDB set")
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
