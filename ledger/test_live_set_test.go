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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// OpenLiveSetForTest is installed by the external tests. It opens the
// LedgerDB set of a database as node startup does: ledger state the database
// holds in SQL moves into the set and is deleted from SQL. Every call for one
// database returns the same set.
var OpenLiveSetForTest func(testing.TB, *database.Database) UtxoSetWriter

// AdoptLiveSetForTest makes a set the caller opened the one OpenLiveSetForTest
// returns for db, after it takes over db's ledger state the same way.
var AdoptLiveSetForTest func(tb testing.TB, db *database.Database, set UtxoSetWriter)

// OpenLiveSetAtForTest is OpenLiveSetForTest for a set kept under dataDir,
// where a harness restarted over that directory finds it again; close
// releases it so the directory can be reopened.
var OpenLiveSetAtForTest func(
	tb testing.TB, db *database.Database, dataDir string,
) (set UtxoSetWriter, close func() error)

// LiveSetOfForTest returns the set OpenLiveSetForTest already opened for db.
var LiveSetOfForTest func(db *database.Database) (UtxoSetWriter, bool)

// seedLedgerOf returns a ledger over db's open set to seed state through, or
// nil while the set is not open yet.
func seedLedgerOf(db *database.Database) *LedgerState {
	set, ok := LiveSetOfForTest(db)
	if !ok {
		return nil
	}
	return &LedgerState{
		db:     db,
		config: LedgerStateConfig{UtxoSet: set, Logger: testLogger()},
	}
}

// seedDB returns the writer a fixture seeds ledger state through. Once db's
// LedgerDB set is open it is the ledger's own LedgerDB, whose writes outside
// a transaction go straight to the set. Before that it is db's SQL tables,
// the layout of a database written by an earlier release, which the set
// takes over when it opens.
func seedDB(t testing.TB, db *database.Database) ledgerdb.LedgerDB {
	t.Helper()
	if ls := seedLedgerOf(db); ls != nil {
		return stagingLedgerDB{LedgerDB: dbadapter.New(db), ls: ls}
	}
	return dbadapter.New(db)
}

// seedBlockNonce is seedDB for a block nonce, which the ledger stages itself
// rather than through its LedgerDB.
func seedBlockNonce(
	t testing.TB,
	db *database.Database,
	hash []byte,
	slot uint64,
	nonce []byte,
	checkpoint bool,
	txn *database.Txn,
) error {
	t.Helper()
	ls := seedLedgerOf(db)
	if ls == nil {
		return db.SetBlockNonce(hash, slot, nonce, checkpoint, txn)
	}
	ls.stageBlockNonces(nil, []models.BlockNonce{{
		Hash: hash, Slot: slot, Nonce: nonce, IsCheckpoint: checkpoint,
	}}, 0)
	return nil
}

// seedCreateAccount is seedDB for an account row.
func seedCreateAccount(
	t testing.TB,
	db *database.Database,
	ctx context.Context,
	txn *database.Txn,
	account *models.Account,
) error {
	t.Helper()
	ls := seedLedgerOf(db)
	if ls == nil {
		return db.CreateAccount(ctx, txn, account)
	}
	ls.stageLedgerChanges(nil, LedgerChanges{Accounts: []AccountChange{{
		Slot: account.AddedSlot, Account: *account,
	}}})
	return nil
}

// seedImportPool is seedDB for a pool and the registration in force.
func seedImportPool(
	t testing.TB,
	db *database.Database,
	ctx context.Context,
	txn *database.Txn,
	pool *models.Pool,
	reg *models.PoolRegistration,
) error {
	t.Helper()
	ls := seedLedgerOf(db)
	if ls == nil {
		return db.ImportPool(ctx, txn, pool, reg)
	}
	ls.stageLedgerChanges(nil, LedgerChanges{Pools: []PoolChange{{
		Slot: reg.AddedSlot,
		State: PoolState{
			Pool:   pool.PoolKeyHash,
			Params: CleanPoolRegistration(reg),
		},
	}}})
	return nil
}

// seedCreateDrep is seedDB for a DRep row.
func seedCreateDrep(
	t testing.TB,
	db *database.Database,
	ctx context.Context,
	txn *database.Txn,
	drep *models.Drep,
) error {
	t.Helper()
	ls := seedLedgerOf(db)
	if ls == nil {
		return db.CreateDrep(ctx, txn, drep)
	}
	ls.stageLedgerChanges(nil, LedgerChanges{DReps: []DRepChange{{
		Slot: drep.AddedSlot, Drep: *drep,
	}}})
	return nil
}

// commitTestLedgerChanges commits changes to ls's LedgerDB set as the block
// at tip and moves the stored tip there, so the set's journal records the
// state and a rollback below tip undoes it.
func commitTestLedgerChanges(
	t testing.TB,
	ls *LedgerState,
	tip ochainsync.Tip,
	changes LedgerChanges,
) {
	t.Helper()
	w, ok := ls.config.UtxoSet.(LedgerChangesWriter)
	require.True(t, ok, "the fixture's set must keep ledger state")
	require.NoError(t, w.ApplyLedgerChanges(&tip.Point, changes))
	require.NoError(t, ls.db.SetTip(tip, nil))
}

func attachTestLiveSet(t testing.TB, ls *LedgerState) {
	t.Helper()
	ls.config.UtxoSet = OpenLiveSetForTest(t, ls.db)
	if ls.config.Logger == nil {
		ls.config.Logger = testLogger()
	}
	closing := ls
	t.Cleanup(func() {
		target := closing
		closing = nil
		require.NoError(t, target.Close())
	})
}

func seedTestOutputCBOR(t *testing.T, db *database.Database, txID []byte, index uint32, amount uint64) []byte {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(lcommon.AddressTypeKeyNone, lcommon.AddressNetworkTestnet, bytes.Repeat([]byte{0x37}, lcommon.AddressHashSize), nil)
	require.NoError(t, err)
	encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{OutputAddress: addr, OutputAmount: amount})
	require.NoError(t, err)
	require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error { return db.Blob().SetUtxo(txn.Blob(), txID, index, encoded) }))
	return encoded
}

func seedTestLiveOutput(t *testing.T, ls *LedgerState, txID []byte, index uint32, amount uint64) {
	t.Helper()
	encoded := seedTestOutputCBOR(t, ls.db, txID, index, amount)
	require.NoError(t, ls.config.UtxoSet.ApplyUtxoChanges(nil, []UtxoEvent{{Added: []UtxoOutput{{Ref: models.UtxoId{Hash: txID, Idx: index}, Cbor: encoded}}}}))
}

// importLedgerStateForTest imports cfg through a production live UTxO set,
// as a Mithril import does, then gives its outputs to ls's set and hands the
// database's live UTxO reads back to ls. The families the import wrote stay
// in the metadata store, where a ledger on the in-memory set reads them.
func importLedgerStateForTest(t *testing.T, ls *LedgerState, cfg ledgerstate.ImportConfig) error {
	t.Helper()
	set := OpenLiveSetForTest(t, ls.db)
	loader, ok := set.(ledgerstate.UtxoSetLoader)
	require.True(t, ok)
	cfg.UtxoSet = loader
	if cfg.Database == nil {
		cfg.Database = ls.db
	}
	cfg.LedgerDB = NewImportLedgerDB(cfg.Database, set, cfg.Logger)
	err := ledgerstate.ImportLedgerState(t.Context(), cfg)
	if ls.config.UtxoSet == nil {
		ls.config.UtxoSet = set
	}
	var added []UtxoOutput
	require.NoError(t, set.(UtxoSetIterator).EachUtxo(
		func(txID []byte, index uint32, cbor []byte) error {
			added = append(added, UtxoOutput{
				Ref:  models.UtxoId{Hash: bytes.Clone(txID), Idx: index},
				Cbor: bytes.Clone(cbor),
			})
			return nil
		},
	))
	require.NoError(t, ls.config.UtxoSet.ApplyUtxoChanges(
		nil, []UtxoEvent{{Added: added}},
	))
	require.NoError(t, ls.db.SetLiveUtxoSource(ls))
	return err
}

// seedTestStakedOutput puts a live output of amount lovelace at a base
// address delegating to stakeKey into ls's live UTxO set. The account of
// stakeKey must exist.
func seedTestStakedOutput(t *testing.T, ls *LedgerState, txID []byte, index uint32, stakeKey []byte, amount uint64) {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x37}, lcommon.AddressHashSize), stakeKey,
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: addr, OutputAmount: amount,
	})
	require.NoError(t, err)
	require.NoError(t, ls.config.UtxoSet.ApplyUtxoChanges(nil, []UtxoEvent{{
		Added: []UtxoOutput{{
			Ref:  models.UtxoId{Hash: txID, Idx: index},
			Cbor: encoded,
		}},
	}}))
	// The store's live stake aggregate follows the outputs a block applies;
	// one seeded past the ledger is folded in here.
	require.NoError(t, ls.db.RefreshRewardLiveStakeRefs(
		[]models.StakeCredentialRef{models.NewStakeCredentialRef(0, stakeKey)},
		0, nil,
	))
}

// mirrorSeededUtxoRowsForTest gives ls's live UTxO set an output for every
// staked utxo row a fixture seeded in the metadata store, at the slot the row
// was added, as the startup migration moves rows into the set. The rows carry
// no output CBOR, so each output is rebuilt from its staking key and amount.
func mirrorSeededUtxoRowsForTest(t *testing.T, ls *LedgerState) {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, ls.db)
	require.NoError(t, err)
	rows, err := raw.Query(
		`SELECT u.tx_id, u.output_idx, c.hash, CAST(u.amount AS INTEGER),
		        u.added_slot
		 FROM utxo u JOIN credential c ON c.id = u.staking_key_id
		 WHERE u.deleted_slot = 0
		 ORDER BY u.added_slot, u.id`,
	)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var (
			txID, stakeKey []byte
			index          uint32
			amount, slot   uint64
		)
		require.NoError(t, rows.Scan(&txID, &index, &stakeKey, &amount, &slot))
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
			bytes.Repeat([]byte{0x37}, lcommon.AddressHashSize), stakeKey,
		)
		require.NoError(t, err)
		encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
			OutputAddress: addr, OutputAmount: amount,
		})
		require.NoError(t, err)
		require.NoError(t, ls.config.UtxoSet.ApplyUtxoChanges(nil, []UtxoEvent{{
			Point: ocommon.Point{Slot: slot, Hash: txID},
			Added: []UtxoOutput{{
				Ref:  models.UtxoId{Hash: txID, Idx: index},
				Cbor: encoded,
			}},
		}}))
	}
	require.NoError(t, rows.Err())
}

// The seeding writers, for the external tests.
var (
	SeedDBForTest            = seedDB
	SeedBlockNonceForTest    = seedBlockNonce
	SeedCreateAccountForTest = seedCreateAccount
	SeedImportPoolForTest    = seedImportPool
	SeedCreateDrepForTest    = seedCreateDrep
)
