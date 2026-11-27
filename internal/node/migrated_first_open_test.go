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

package node

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/nodestorage"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// migratedLedger is a ledger over the consensus database of an API node's
// data directory, reconciled with its live UTxO set as a node's is at startup.
type migratedLedger struct {
	db    *database.Database
	set   *utxoset.Set
	ls    *ledger.LedgerState
	bus   *event.EventBus
	fatal chan error
}

// openMigratedLedger opens the ledger over dir and reconciles it, reverting
// a database that still holds its ledger state in the metadata store rewind
// blocks below its tip. The ledger is not started.
func openMigratedLedger(
	t *testing.T,
	dir string,
	nodeCfg *cardano.CardanoNodeConfig,
	rewind uint64,
) *migratedLedger {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := &migratedLedger{fatal: make(chan error, 1)}
	var closeDB func()
	m.db, closeDB = openConsensusRewound(t, dir, rewind)
	var err error
	m.set, _, err = nodestorage.OpenUtxoSet(nodestorage.UtxoSetConfig{
		DataDir:           dir,
		Logger:            logger,
		CardanoNodeConfig: nodeCfg,
	})
	require.NoError(t, err)
	m.bus = event.NewEventBus(nil, logger)
	cm, err := chain.NewManager(ctx, m.db, m.bus)
	require.NoError(t, err)
	m.ls, err = ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:          m.db,
		UtxoSet:           m.set,
		ChainManager:      cm,
		EventBus:          m.bus,
		Logger:            logger,
		CardanoNodeConfig: nodeCfg,
		Network:           "preview",
		PromRegistry:      prometheus.NewRegistry(),
		FatalErrorFunc: func(err error) {
			select {
			case m.fatal <- err:
			default:
			}
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, m.ls.Close())
		require.NoError(t, m.set.Close())
		closeDB()
	})
	require.NoError(t, (nodestorage.Reconcile{
		DB:     m.db,
		Set:    m.set,
		Logger: logger,
		Resume: m.ls.ResumeUtxoSetFrom,
	}).Run(ctx))
	require.NoError(t, cm.SetLedger(m.ls))
	return m
}

// awaitTip waits for the ledger and the live UTxO set to be at slot.
func (m *migratedLedger) awaitTip(t *testing.T, slot uint64, what string) {
	t.Helper()
	var fatalErr error
	require.Eventually(t, func() bool {
		select {
		case fatalErr = <-m.fatal:
			return true
		default:
		}
		setTip, _, err := m.set.Tip()
		return err == nil && m.ls.Tip().Point.Slot == slot &&
			setTip.Slot == slot
	}, time.Minute, 10*time.Millisecond, what)
	require.NoError(t, fatalErr, what)
}

// `dingo load` is the first to open a database the released version wrote,
// and appends to it. The node that starts next follows a fork that leaves
// the chain below the released tip: the load reverted the metadata store
// below that tip before it moved the ledger state, and applied the blocks up
// to it through the live UTxO set, which journals each.
func TestLoadedReleasedDatabaseRollsBackBelowItsReleasedTip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodeCfg := previewNodeConfig(t)
	dir := unpackLegacyDatabase(t, legacyParityArchive)
	require.NoError(t, MigrateToIndexerDB(ctx, parityConfig(dir), logger))
	// One chunk past the released tip.
	require.NoError(t, Load(
		ctx, parityConfig(dir), logger,
		copyImmutableChunkPrefix(t, legacyRollbackChunks+2),
	))

	m := openMigratedLedger(t, dir, nodeCfg, previewSecurityParam(t))
	require.NoError(t, m.ls.Start(ctx))
	loaded := m.ls.Tip()
	require.Greater(t, loaded.Point.Slot, uint64(legacyParityTipSlot),
		"the load appended to the released chain")
	released, err := database.BlockBeforeSlot(
		ctx, m.db, legacyParityTipSlot+1,
	)
	require.NoError(t, err)
	require.EqualValues(t, legacyParityTipSlot, released.Slot)
	require.Less(t, loaded.BlockNumber-released.Number,
		previewSecurityParam(t),
		"the released tip is inside the security parameter of the loaded tip")
	target, err := database.BlockByNumber(ctx, m.db, released.Number-5)
	require.NoError(t, err)
	m.bus.Publish(ledger.ChainsyncEventType, event.NewEvent(
		ledger.ChainsyncEventType,
		ledger.ChainsyncEvent{
			Rollback: true,
			Point:    ocommon.NewPoint(target.Slot, target.Hash),
		},
	))
	m.awaitTip(t, target.Slot,
		"the ledger and the live UTxO set roll back below the released tip")
}

// A released database whose revert lands inside epoch 0 has no ledger state
// as of slot 0 in its live UTxO set, which the genesis snapshot capture reads
// on a database past its first block. The mark the released version captured
// at genesis is still recorded, so a block producer starts, whether the
// capture runs while the ledger is still in epoch 0 or after it has applied
// the reverted blocks again into epoch 1, where the boundary records the next
// mark. A database that holds no such mark was left without it by a failed
// capture of the released version, which is a warning on a node that does not
// produce blocks; a block producer is refused there, at either point, with
// what the database lacks and why it cannot be captured.
func TestMigratedDatabaseInEpochZeroKeepsItsGenesisSnapshot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		recorded bool
	}{
		{name: "recorded", recorded: true},
		{name: "missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			nodeCfg := previewNodeConfig(t)
			dir := unpackLegacyDatabase(t, legacyParityArchive)
			if !tc.recorded {
				raw, err := sql.Open(
					"sqlite", filepath.Join(dir, "metadata.sqlite"),
				)
				require.NoError(t, err)
				_, err = raw.Exec("DELETE FROM epoch_summary WHERE epoch=0")
				require.NoError(t, errors.Join(err, raw.Close()))
			}
			require.NoError(t, MigrateToIndexerDB(ctx, parityConfig(dir), logger))
			m := openMigratedLedger(t, dir, nodeCfg, previewSecurityParam(t))
			tip, err := m.db.GetTip(nil)
			require.NoError(t, err)
			require.Less(t, tip.Point.Slot, uint64(previewEpochLength),
				"the revert lands inside epoch 0")
			require.Positive(t, tip.Point.Slot)

			mgr := snapshot.NewManager(m.ls.LedgerDB(), nil, logger)
			m.ls.SetEpochBoundarySnapshotStakeHook(
				func(txn ledgerdb.Txn, evt event.EpochTransitionEvent) error {
					return mgr.ComputeEpochBoundarySnapshot(ctx, txn, evt)
				},
			)
			m.ls.SetEpochBoundarySnapshotHook(
				func(txn ledgerdb.Txn, evt event.EpochTransitionEvent) error {
					return mgr.CaptureEpochBoundarySnapshot(ctx, txn, evt)
				},
			)
			capture := func() error {
				return captureLoadGenesisSnapshot(
					ctx, mgr, &config.Config{BlockProducer: true}, logger,
				)
			}
			check := func(when string) {
				t.Helper()
				err := capture()
				if tc.recorded {
					require.NoError(t, err, when)
					return
				}
				require.ErrorIs(t, err, snapshot.ErrGenesisMarkUnavailable, when)
				require.ErrorIs(t, err, ledger.ErrLedgerStateAsOfUnavailable, when)
			}
			check("in epoch 0, before the reverted blocks are applied again")

			// The node captures after it starts the ledger, which applies the
			// reverted blocks again and crosses into epoch 1.
			require.NoError(t, m.ls.Start(ctx))
			m.awaitTip(t, legacyParityTipSlot,
				"the ledger applies the blocks up to the released tip again")
			epochs, err := m.ls.LedgerDB().GetEpochs(nil)
			require.NoError(t, err)
			require.EqualValues(t, 1, epochs[len(epochs)-1].EpochId)
			check("in epoch 1, after the reverted blocks are applied again")
		})
	}
}
