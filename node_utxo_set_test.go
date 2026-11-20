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
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func init() {
	utxoset.DefaultMemTableSize = 8 << 20
	utxoset.DefaultBlockCacheSize = 1 << 20
}

// TestCheckUtxoSetOnlyMarksAFreshDatabase marks a database at origin as
// holding its UTxO set only in the live UTxO set. checkUtxoSetOnly alone
// refuses an unmarked synced database; migrateUtxoRows is what marks one.
func TestCheckUtxoSetOnlyMarksAFreshDatabase(t *testing.T) {
	t.Parallel()
	node := func(db *database.Database) *Node {
		n := &Node{db: db}
		n.config.dataDir = t.TempDir()
		return n
	}

	fresh, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(fresh) })
	require.NoError(t, node(fresh).checkUtxoSetOnly())
	require.NoError(t, node(fresh).checkUtxoSetOnly(), "already marked")

	synced, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(synced) })
	require.NoError(t, synced.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 10, Hash: bytes.Repeat([]byte{1}, 32)},
	}, nil))
	require.Error(t, node(synced).checkUtxoSetOnly())
}

// TestMigrateUtxoRowsMarksASyncedDatabase moves a synced database that holds
// its UTxO set in the metadata store into the live UTxO set at startup instead
// of refusing it, and leaves a marked database that has no move pending alone.
func TestMigrateUtxoRowsMarksASyncedDatabase(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	tip := ocommon.Point{Slot: 10, Hash: bytes.Repeat([]byte{1}, 32)}
	require.NoError(t, db.SetTip(ochainsync.Tip{Point: tip}, nil))
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir(), Only: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	n := &Node{db: db, utxoSet: set}
	n.config.dataDir = t.TempDir()
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	require.Error(t, n.checkUtxoSetOnly(), "unmarked before the move")
	require.NoError(t, n.migrateUtxoRows(t.Context()))
	require.NoError(t, n.checkUtxoSetOnly())
	setTip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, tip, setTip)
	progress, err := db.GetSyncState(utxoset.MigrationSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, progress)
	require.NoError(t, n.migrateUtxoRows(t.Context()), "nothing pending")

	n.utxoSet = nil
	require.NoError(
		t,
		n.migrateUtxoRows(t.Context()),
		"marked: nothing to move",
	)
}

// TestMigrateUtxoRowsNeedsTheSet requires an unmarked synced database to name
// the missing live UTxO set rather than tell the operator to resync.
func TestMigrateUtxoRowsNeedsTheSet(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 10, Hash: bytes.Repeat([]byte{1}, 32)},
	}, nil))
	n := &Node{db: db}
	n.config.dataDir = t.TempDir()
	err = n.migrateUtxoRows(t.Context())
	require.ErrorContains(t, err, "live UTxO set")
	require.NotContains(t, err.Error(), "resync")
}

func TestUtxoMigrationCleanupJoinsBeforeStorageClose(t *testing.T) {
	original := cleanupMigratedUtxoRows
	t.Cleanup(func() { cleanupMigratedUtxoRows = original })
	started := make(chan struct{})
	stopped := make(chan struct{})
	cleanupMigratedUtxoRows = func(
		ctx context.Context,
		_ *database.Database,
		_ *utxoset.Set,
		_ *slog.Logger,
	) error {
		close(started)
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	n := &Node{ctx: ctx}
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	stop := n.startUtxoMigrationCleanup(ctx)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start")
	}
	stop()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not stop")
	}
	n.utxoMigrationCleanupMu.Lock()
	require.Nil(t, n.utxoMigrationCleanupCancel)
	require.Nil(t, n.utxoMigrationCleanupDone)
	n.utxoMigrationCleanupMu.Unlock()
}

// TestUtxoSetOnlyStakeRebuildWaitsForTheSet requires the startup
// reward_live_stake rebuild to wait until the live UTxO set is reconciled and
// then to read UTxO stake from the set.
func TestUtxoSetOnlyStakeRebuildWaitsForTheSet(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	stakeKey := bytes.Repeat([]byte{0x44}, 28)
	require.NoError(
		t,
		db.SetLiveUtxoSource(oneStakeSource{key: stakeKey, lovelace: 42}),
	)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	// A row from an older calculation makes the startup check rebuild.
	_, err = raw.Exec(`INSERT INTO reward_live_stake (staking_key,
credential_tag, utxo_stake, reward_stake, total_stake, registered,
updated_slot, calculation_version) VALUES (?, 0, '1', '0', '1', 0, 0, 0)`,
		bytes.Repeat([]byte{0x55}, 28))
	require.NoError(t, err)
	rows := func() []string {
		r, err := raw.Query(
			`SELECT staking_key, utxo_stake FROM reward_live_stake ORDER BY 1`,
		)
		require.NoError(t, err)
		defer r.Close()
		var out []string
		for r.Next() {
			var key []byte
			var stake string
			require.NoError(t, r.Scan(&key, &stake))
			out = append(out, fmt.Sprintf("%x=%s", key, stake))
		}
		require.NoError(t, r.Err())
		return out
	}
	before := rows()

	n := &Node{db: db}
	n.config.dataDir = t.TempDir()
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	require.NoError(t, n.backfillRewardLiveStake(t.Context()))
	require.Equal(t, before, rows(), "rebuilt before the set was reconciled")

	n.utxoSetReconciled = true
	require.NoError(t, n.backfillRewardLiveStake(t.Context()))
	require.Equal(t, []string{fmt.Sprintf("%x=42", stakeKey)}, rows())
}

// oneStakeSource is a live UTxO set holding lovelace for one credential with
// no account.
type oneStakeSource struct {
	key      []byte
	lovelace uint64
}

func (oneStakeSource) LiveUtxoCbor(
	types.Txn,
	[]byte,
	uint32,
) ([]byte, bool, error) {
	return nil, false, nil
}

func (s oneStakeSource) LiveUtxoStake(
	_ types.Txn,
	tag uint8,
	key []byte,
) (uint64, error) {
	if tag == 0 && bytes.Equal(key, s.key) {
		return s.lovelace, nil
	}
	return 0, nil
}

func (oneStakeSource) LivePointerStakes(
	types.Txn,
) ([]types.PointerStake, error) {
	return nil, nil
}

func (s oneStakeSource) EachLiveUtxoStake(
	_ types.Txn,
	fn func(uint8, []byte, uint64) error,
) error {
	return fn(0, s.key, s.lovelace)
}

// TestReinitializeCoreStorageClearsTheUtxoSetReconciledMark requires a live
// Restore or Truncate rebuild to invalidate the set's reconcile, so the
// startup stake backfill waits for the reconcile of the rebuilt ledger.
func TestReinitializeCoreStorageClearsTheUtxoSetReconciledMark(t *testing.T) {
	t.Parallel()
	n := &Node{utxoSetReconciled: true}
	n.config.cfg = &internalconfig.Config{}

	// The rebuild cannot complete for a Node with no resolved storage
	// plugins; the mark is cleared before that failure returns.
	require.Error(t, n.reinitializeCoreStorage(t.Context()))
	require.False(t, n.utxoSetReconciled)
}

func TestCheckpointPolicyWaitsForTheWallClockTip(t *testing.T) {
	t.Parallel()
	p := checkpointPolicy{every: 2}
	// Replaying history: nothing is written and no epoch is counted.
	for epoch := range uint64(50) {
		require.False(t, p.due(epoch, false, utxoSetCheckpointNearTipSlots+1))
	}
	// First pass near the tip with no checkpoint writes one at once.
	require.True(t, p.due(50, false, 0))
	require.False(t, p.due(51, true, 0))
	require.True(t, p.due(52, true, 0))
	// Falling behind again pauses it without losing the count.
	require.False(t, p.due(60, true, utxoSetCheckpointNearTipSlots+1))
	require.True(t, p.due(60, true, utxoSetCheckpointNearTipSlots))
}

func TestCheckpointPolicyCountsAnEarlierRunsCheckpointFromNow(t *testing.T) {
	t.Parallel()
	p := checkpointPolicy{every: 1}
	require.False(t, p.due(10, true, 0))
	require.True(t, p.due(11, true, 0))
}

// TestCloseUtxoSetStopsARunningCheckpointCopy requires closing the set to
// cancel a checkpoint copy and wait for its goroutine, so the copy never runs
// against a closed set.
func TestCloseUtxoSetStopsARunningCheckpointCopy(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{
		Dir: t.TempDir(), Only: true,
		CheckpointDir: t.TempDir(), CheckpointKeep: 2,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	entered := make(chan struct{})
	set.SetCheckpointBarrier(func(ctx context.Context) (func(), error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	n := &Node{utxoSet: set}
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	n.anchoredUtxoSet.Store(set)
	tip := ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{1}, 32)}
	require.NoError(t, set.LoadUtxoSet(tip,
		func(func([]byte, uint32, []byte) error) error { return nil }))
	ended := make(chan struct{})
	require.True(
		t,
		n.launchUtxoSetCheckpoint(t.Context(), set, func() { close(ended) }),
	)
	<-entered

	require.NoError(t, n.closeUtxoSet())

	select {
	case <-ended:
	default:
		t.Fatal("the checkpoint copy was still running when the set closed")
	}
	require.False(t, n.launchUtxoSetCheckpoint(t.Context(), set, func() {}),
		"a copy started after the set closed")
}

// TestStartupMovesALegacyDatabaseToTheLiveUtxoSet runs the startup sequence of
// a node over a database as a released version leaves it, with its UTxO set
// and ledger state in the metadata store, and requires the database to hold
// them in the live UTxO set afterwards: marked as such, every graduated family
// drained, and the set at the ledger's tip, which the startup reverted below
// the released tip so the set's rollback journal covers the blocks up to it.
// A second start, as after a crash at any point of the first, finds nothing
// left to do.
func TestStartupMovesALegacyDatabaseToTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	n, points := newLiveLifecycleTestNodeWithGenesis(
		t, 12, nil, disabledLiveLifecycleTestWorkerPoolCfg,
		withLegacyStorageFixture,
	)
	require.NoError(t, n.ledgerState.Close())
	tip := points[len(points)-1]

	require.NoError(t, n.openUtxoSet())
	require.NotNil(t, n.utxoSet, "the node opens the live UTxO set")
	t.Cleanup(func() { _ = n.closeUtxoSet() })
	ls, err := ledger.NewLedgerState(n.ledgerStateConfig())
	require.NoError(t, err)
	n.setLedgerState(ls)
	require.NoError(t, n.reconcileUtxoSet(t.Context()))

	marked, err := n.db.GetSyncState(database.UtxoSetOnlySyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "1", marked, "the metadata store holds no UTxO set")
	drained, err := n.db.EffectiveDrainedFamilies(t.Context())
	require.NoError(t, err)
	require.Equal(
		t, len(statedrain.Graduated(drainNoSource{})), len(drained.Families()),
		"every graduated family is drained",
	)
	setTip, ok, err := n.utxoSet.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	ledgerTip, err := n.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, ledgerTip.Point, setTip)
	require.Less(t, setTip.Slot, tip.Slot,
		"the state moves below the released tip")
	_, err = database.BlockByPoint(t.Context(), n.db, tip)
	require.NoError(t, err, "the blocks up to the released tip are kept")

	require.NoError(t, n.reconcileUtxoSet(t.Context()), "a second start")
	again, err := n.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, ledgerTip, again, "a second start reverts nothing")
}
