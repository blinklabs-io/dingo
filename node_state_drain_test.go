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

package dingo

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// drainScratchTable gives the drain real rows to delete in the SQLite store
// without borrowing a ledger table.
func drainScratchTable(t *testing.T, db *database.Database, rows int) *sql.DB {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(
		`CREATE TABLE drain_scratch (id INTEGER PRIMARY KEY, v INTEGER)`,
	)
	require.NoError(t, err)
	for i := range rows {
		_, err = raw.Exec(
			`INSERT INTO drain_scratch (id, v) VALUES (?, ?)`,
			i+1,
			i,
		)
		require.NoError(t, err)
	}
	return raw
}

func scratchRows(t *testing.T, raw *sql.DB) int {
	t.Helper()
	var n int
	require.NoError(
		t,
		raw.QueryRow(`SELECT count(*) FROM drain_scratch`).Scan(&n),
	)
	return n
}

func newDrainDatabase(t *testing.T, dir string) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	return db
}

type setTip struct {
	point ocommon.Point
}

func (s setTip) Tip() (ocommon.Point, bool, error) { return s.point, true, nil }

func TestLedgerStateLatchPersistsAcrossRestartsAndOnlyWidens(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	db := newDrainDatabase(t, dir)

	mask, err := db.DrainedFamilies(ctx)
	require.NoError(t, err)
	require.Zero(t, mask, "a new database has drained nothing")

	require.NoError(t, db.RecordDrainedFamily(ctx, nodesettings.FamilyNonces))
	require.NoError(t, db.RecordDrainedFamily(ctx, nodesettings.FamilyOpCerts))
	require.NoError(
		t,
		db.RecordDrainedFamily(ctx, nodesettings.FamilyNonces),
		"idempotent",
	)
	require.NoError(t, dbtest.CloseDatabase(db))

	reopened := newDrainDatabase(t, dir)
	mask, err = reopened.DrainedFamilies(ctx)
	require.NoError(t, err)
	require.Equal(
		t,
		nodesettings.FamilyMask(0).
			With(nodesettings.FamilyNonces).
			With(nodesettings.FamilyOpCerts),
		mask,
	)

	// A later family widens the mask and keeps the earlier bits.
	require.NoError(
		t,
		reopened.RecordDrainedFamily(ctx, nodesettings.FamilyEpochs),
	)
	mask, err = reopened.DrainedFamilies(ctx)
	require.NoError(t, err)
	require.True(t, mask.Has(nodesettings.FamilyNonces))
	require.True(t, mask.Has(nodesettings.FamilyOpCerts))
	require.True(t, mask.Has(nodesettings.FamilyEpochs))
}

func TestLedgerStateLatchRefusesAValueItCannotDecode(t *testing.T) {
	t.Parallel()
	db := newDrainDatabase(t, t.TempDir())
	require.NoError(t, db.Metadata().SetNodeSettingsGates(
		context.Background(),
		map[string]string{nodesettings.LedgerStateDrainedGateName: "f9:zz"},
		0, 0,
	))
	_, err := db.DrainedFamilies(context.Background())
	require.ErrorIs(t, err, nodesettings.ErrLatchFormat)
	require.Error(t, db.RecordDrainedFamily(
		context.Background(), nodesettings.FamilyEpochs,
	))
}

func TestDeleteLedgerStateRowsDeletesInChunks(t *testing.T) {
	t.Parallel()
	db := newDrainDatabase(t, t.TempDir())
	raw := drainScratchTable(t, db, 7)
	ctx := context.Background()

	for _, want := range []struct {
		deleted int64
		left    int
	}{{3, 4}, {3, 1}, {1, 0}, {0, 0}} {
		n, err := db.DeleteLedgerStateRows(ctx, "drain_scratch", "id", 3)
		require.NoError(t, err)
		require.Equal(t, want.deleted, n)
		require.Equal(t, want.left, scratchRows(t, raw))
	}

	_, err := db.DeleteLedgerStateRows(ctx, "drain_scratch", "id", 0)
	require.Error(t, err)
	for _, protected := range []string{
		"schema_migrations", "node_settings_gate", "sync_state",
	} {
		_, err = db.DeleteLedgerStateRows(ctx, protected, "name", 10)
		require.ErrorContains(t, err, "not ledger state", protected)
	}
	_, err = db.DeleteLedgerStateRows(
		ctx,
		"drain_scratch; DROP TABLE x",
		"id",
		1,
	)
	require.ErrorContains(t, err, "invalid table")
}

// TestStateDrainRunsAgainstTheDatabase drives statedrain.Run through the real
// store: an interruption between chunks leaves the marker, a second run
// finishes, the bit survives a restart, and a third run is a no-op.
func TestStateDrainRunsAgainstTheDatabase(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	db := newDrainDatabase(t, dir)
	raw := drainScratchTable(t, db, 11)
	tip := ocommon.Point{Slot: 50, Hash: bytes.Repeat([]byte{9}, 32)}
	require.NoError(t, db.SetTip(ochainsync.Tip{Point: tip}, nil))

	entry := statedrain.Entry{
		Family: nodesettings.FamilyNonces,
		Tables: []statedrain.Table{{Name: "drain_scratch", Key: "id"}},
		Loaded: func() (bool, error) { return true, nil },
	}
	interrupt := &interruptingStore{drainStore: drainStore{db}, after: 2}
	env := statedrain.Env{
		Store: interrupt, Set: setTip{tip}, ChunkRows: 4,
	}
	err := statedrain.Run(ctx, env, []statedrain.Entry{entry})
	require.ErrorIs(t, err, errChunkInterrupted)
	require.Equal(t, 3, scratchRows(t, raw))
	marker, err := db.GetSyncState(statedrain.MarkerPrefix+"nonces", nil)
	require.NoError(t, err)
	require.NotEmpty(t, marker)
	mask, err := db.DrainedFamilies(ctx)
	require.NoError(t, err)
	require.Zero(t, mask)

	env.Store = drainStore{db}
	require.NoError(t, statedrain.Run(ctx, env, []statedrain.Entry{entry}))
	require.Zero(t, scratchRows(t, raw))
	marker, err = db.GetSyncState(statedrain.MarkerPrefix+"nonces", nil)
	require.NoError(t, err)
	require.Empty(t, marker)
	require.NoError(t, dbtest.CloseDatabase(db))

	reopened := newDrainDatabase(t, dir)
	mask, err = reopened.DrainedFamilies(ctx)
	require.NoError(t, err)
	require.True(t, mask.Has(nodesettings.FamilyNonces))
	env.Store = drainStore{reopened}
	require.NoError(t, statedrain.Run(ctx, env, []statedrain.Entry{entry}))
}

var errChunkInterrupted = errors.New("interrupted between chunks")

type interruptingStore struct {
	drainStore
	after int
	calls int
}

func (s *interruptingStore) DeleteLedgerStateRows(
	ctx context.Context,
	table, key string,
	limit int,
) (int64, error) {
	if s.calls >= s.after {
		return 0, errChunkInterrupted
	}
	s.calls++
	return s.drainStore.DeleteLedgerStateRows(ctx, table, key, limit)
}

// TestStatementGuardTripsOnADrainedFamilyRead reads a table through the
// LedgerDB adapter before and after its family is armed.
func TestStatementGuardTripsOnADrainedFamilyRead(t *testing.T) {
	t.Parallel()
	guard := statedrain.NewTripGuard()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config:         &database.Config{DataDir: t.TempDir()},
		StatementGuard: guard,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	ledgerDB := dbadapter.New(db)

	_, err = ledgerDB.GetBlockNoncesInSlotRange(0, 10, nil)
	require.NoError(t, err, "nothing is drained yet")
	require.Empty(t, guard.Trips())

	guard.Arm(nodesettings.FamilyNonces, []string{"block_nonce"})
	_, err = ledgerDB.GetBlockNoncesInSlotRange(0, 10, nil)
	require.ErrorIs(t, err, statedrain.ErrDrainedRead)
	require.Len(t, guard.Trips(), 1)
	require.Equal(t, nodesettings.FamilyNonces, guard.Trips()[0].Family)
	require.Equal(t, "block_nonce", guard.Trips()[0].Table)

	// Other tables stay readable.
	_, err = db.GetSyncState("anything", nil)
	require.NoError(t, err)
	require.Len(t, guard.Trips(), 1)
}

func TestDrainLedgerStateDoesNothingWhenNoFamilyGraduates(t *testing.T) {
	t.Parallel()
	db := newDrainDatabase(t, t.TempDir())
	n := &Node{db: db}
	require.NoError(t, n.drainLedgerState(context.Background()), "switch off")

	n.config.ledgerStateSetOnly = true
	require.ErrorContains(
		t, n.drainLedgerState(context.Background()), "live UTxO set",
	)
}

func TestLedgerStateSetOnlyNeedsAFreshOrMarkedDatabase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	node := func(db *database.Database, switchOn bool) *Node {
		n := &Node{db: db}
		n.config.ledgerUtxoBadgerOnly = true
		n.config.ledgerStateSetOnly = switchOn
		return n
	}
	tipAt10 := ochainsync.Tip{
		Point: ocommon.Point{Slot: 10, Hash: bytes.Repeat([]byte{1}, 32)},
	}

	synced := newDrainDatabase(t, t.TempDir())
	require.NoError(t, synced.SetTip(tipAt10, nil))
	err := node(synced, true).checkLedgerStateSetOnly(ctx)
	require.ErrorContains(t, err, "ledgerStateSetOnly needs a database synced")
	require.ErrorContains(t, err, "resync")
	require.NoError(
		t,
		node(synced, false).checkLedgerStateSetOnly(ctx),
		"without the switch the database is not this check's concern",
	)

	fresh := newDrainDatabase(t, t.TempDir())
	require.NoError(t, node(fresh, true).checkLedgerStateSetOnly(ctx))

	marked := newDrainDatabase(t, t.TempDir())
	require.NoError(
		t, marked.SetSyncState(database.UtxoSetOnlySyncKey, "1", nil),
	)
	require.NoError(t, marked.SetTip(tipAt10, nil))
	require.NoError(
		t,
		node(marked, true).checkLedgerStateSetOnly(ctx),
		"a database already synced set-only may take the switch",
	)

	// reconcileUtxoSet runs the check before the UTxO mode check, whose own
	// message would otherwise answer first.
	err = node(synced, true).reconcileUtxoSet(ctx)
	require.ErrorContains(t, err, "ledgerStateSetOnly needs a database synced")
}

func TestADrainedDatabaseRefusesToStartWithoutTheSwitch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newDrainDatabase(t, t.TempDir())
	require.NoError(
		t, db.RecordDrainedFamily(ctx, nodesettings.FamilyEpochs),
	)
	n := &Node{db: db}
	n.config.ledgerUtxoBadgerOnly = true
	err := n.checkLedgerStateSetOnly(ctx)
	require.ErrorContains(t, err, "start with ledgerStateSetOnly")
	require.ErrorContains(t, err, "epochs")
	require.ErrorContains(
		t, n.reconcileUtxoSet(ctx), "start with ledgerStateSetOnly",
	)

	n.config.ledgerStateSetOnly = true
	require.NoError(t, n.checkLedgerStateSetOnly(ctx))
}

func TestLedgerStateConfigCarriesTheSwitch(t *testing.T) {
	t.Parallel()
	for _, on := range []bool{true, false} {
		n := &Node{
			config:  Config{cfg: &internalconfig.Config{}},
			utxoSet: &utxoset.Set{},
		}
		n.config.ledgerStateSetOnly = on
		require.Equal(t, on, n.ledgerStateConfig().LedgerStateSetOnly)
	}
}

// TestReconcileUtxoSetRunsTheDrain requires the startup reconcile to end in
// the drain: with the switch on and no open set it fails there rather than
// returning with the SQL rows undrained.
func TestReconcileUtxoSetRunsTheDrain(t *testing.T) {
	t.Parallel()
	db := newDrainDatabase(t, t.TempDir())
	n := &Node{db: db}
	require.NoError(t, n.reconcileUtxoSet(context.Background()))
	require.True(t, n.utxoSetReconciled)

	n.config.ledgerStateSetOnly = true
	err := n.reconcileUtxoSet(context.Background())
	require.ErrorContains(t, err, "needs the live UTxO set")
}

// TestReconcileUtxoSetPublishesTheLatchBeforeTheLedgerStarts requires the
// startup reconcile to hand the persisted latch to the ledger and to the live
// UTxO set, which then refuses to rebuild itself from the store.
func TestReconcileUtxoSetPublishesTheLatchBeforeTheLedgerStarts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newDrainDatabase(t, t.TempDir())
	require.NoError(
		t, db.RecordDrainedFamily(ctx, nodesettings.FamilyAccounts),
	)
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	n := &Node{db: db, utxoSet: set, ledgerState: &ledger.LedgerState{}}
	n.config.ledgerUtxoBadgerOnly = true
	n.config.ledgerStateSetOnly = true

	err = n.reconcileUtxoSet(ctx)
	require.ErrorContains(t, err, "drained ledger-state families")
	require.Equal(
		t,
		nodesettings.FamilyMask(0).With(nodesettings.FamilyAccounts),
		n.ledgerState.DrainedFamilies(),
	)
}

// TestDrainPublishesEachFamilyItLatches requires the drain's store to hand the
// widened mask to its subscriber as soon as a family is latched.
func TestDrainPublishesEachFamilyItLatches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newDrainDatabase(t, t.TempDir())
	var seen []nodesettings.FamilyMask
	store := publishingStore{
		drainStore: drainStore{db},
		publish:    func(m nodesettings.FamilyMask) { seen = append(seen, m) },
	}
	require.NoError(
		t, store.RecordDrainedFamily(ctx, nodesettings.FamilyNonces),
	)
	require.NoError(
		t, store.RecordDrainedFamily(ctx, nodesettings.FamilyEpochs),
	)
	require.Equal(t, []nodesettings.FamilyMask{
		nodesettings.FamilyMask(0).With(nodesettings.FamilyNonces),
		nodesettings.FamilyMask(0).
			With(nodesettings.FamilyNonces).
			With(nodesettings.FamilyEpochs),
	}, seen)
}
