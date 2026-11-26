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

// seedLedgerOf returns a ledger over db's LedgerDB set to seed state
// through, opening the set first if no fixture has yet.
func seedLedgerOf(t testing.TB, db *database.Database) *LedgerState {
	t.Helper()
	return &LedgerState{
		db: db,
		config: LedgerStateConfig{
			UtxoSet: OpenLiveSetForTest(t, db),
			Logger:  testLogger(),
		},
	}
}

// seedDB returns the writer a fixture seeds ledger state through: the
// ledger's own LedgerDB over db's set, whose writes outside a transaction go
// straight to the set.
func seedDB(t testing.TB, db *database.Database) ledgerdb.LedgerDB {
	t.Helper()
	return stagingLedgerDB{LedgerDB: dbadapter.New(db), ls: seedLedgerOf(t, db)}
}

// pruneRewardStakeInputsBefore stages the prune of reward stake inputs
// below epoch, leaving reward account outputs, as a test of a basis that
// has aged out needs.
func pruneRewardStakeInputsBefore(
	t testing.TB,
	db *database.Database,
	epoch uint64,
) {
	t.Helper()
	s := stagingLedgerDB{LedgerDB: dbadapter.New(db), ls: seedLedgerOf(t, db)}
	s.stageRewardInput(nil, RewardInputChange{
		Kind:       RewardInputPrune,
		PruneBelow: s.ls.rewardPruneBelow(epoch),
	})
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
	_ *database.Txn,
) error {
	t.Helper()
	seedLedgerOf(t, db).stageBlockNonces(nil, []models.BlockNonce{{
		Hash: hash, Slot: slot, Nonce: nonce, IsCheckpoint: checkpoint,
	}}, 0)
	return nil
}

// seedCreateAccount is seedDB for an account row.
func seedCreateAccount(
	t testing.TB,
	db *database.Database,
	_ context.Context,
	_ *database.Txn,
	account *models.Account,
) error {
	t.Helper()
	seedLedgerOf(t, db).stageLedgerChanges(nil, LedgerChanges{
		Accounts: []AccountChange{{Slot: account.AddedSlot, Account: *account}},
	})
	return nil
}

// seedImportPool is seedDB for a pool and the registration in force.
func seedImportPool(
	t testing.TB,
	db *database.Database,
	_ context.Context,
	_ *database.Txn,
	pool *models.Pool,
	reg *models.PoolRegistration,
) error {
	t.Helper()
	seedLedgerOf(t, db).stageLedgerChanges(nil, LedgerChanges{
		Pools: []PoolChange{{
			Slot: reg.AddedSlot,
			State: PoolState{
				Pool:   pool.PoolKeyHash,
				Params: CleanPoolRegistration(reg),
			},
		}},
	})
	return nil
}

// seedCreateDrep is seedDB for a DRep row.
func seedCreateDrep(
	t testing.TB,
	db *database.Database,
	_ context.Context,
	_ *database.Txn,
	drep *models.Drep,
) error {
	t.Helper()
	seedLedgerOf(t, db).stageLedgerChanges(nil, LedgerChanges{
		DReps: []DRepChange{{Slot: drep.AddedSlot, Drep: *drep}},
	})
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

// fxSetLedgerTip records tip as the ledger's tip in db and in db's LedgerDB
// set, as applying the block at tip does. A set left behind a database tip
// has no state to catch up from, so a ledger built on db would refuse it.
func fxSetLedgerTip(t testing.TB, db *database.Database, tip ochainsync.Tip) {
	t.Helper()
	w, ok := OpenLiveSetForTest(t, db).(LedgerChangesWriter)
	require.True(t, ok, "the fixture's set must keep ledger state")
	require.NoError(t, w.ApplyLedgerChanges(&tip.Point, LedgerChanges{}))
	require.NoError(t, db.SetTip(tip, nil))
}

// fxReleasedLedgerDB opens a v0.94.2 database in dataDir, lets seed write its
// metadata store, takes that state into the LedgerDB set kept in dataDir
// through OpenLiveSetAtForTest, and marks the block store migrated. It returns
// the ledger's database reopened over the directory, with that set open.
func fxReleasedLedgerDB(
	tb testing.TB,
	dataDir string,
	seed func(*database.Database),
) *database.Database {
	tb.Helper()
	released, err := dbtest.NewReleasedDatabase(
		tb, &database.Config{DataDir: dataDir},
	)
	require.NoError(tb, err)
	seed(released)
	_, closeSet := OpenLiveSetAtForTest(tb, released, dataDir)
	require.NoError(tb, closeSet())
	// The migration records the move last, so the block store only opens as
	// the ledger's once the set holds the state.
	require.NoError(tb, database.MarkLedgerMigrated(released.Blob()))
	require.NoError(tb, dbtest.CloseDatabase(released))
	db, err := dbtest.NewDatabase(tb, &database.Config{DataDir: dataDir})
	require.NoError(tb, err)
	OpenLiveSetAtForTest(tb, db, dataDir)
	return db
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
}

// The seeding writers, for the external tests.
var (
	SeedDBForTest            = seedDB
	SeedBlockNonceForTest    = seedBlockNonce
	SeedCreateAccountForTest = seedCreateAccount
	SeedImportPoolForTest    = seedImportPool
	SeedCreateDrepForTest    = seedCreateDrep
)
