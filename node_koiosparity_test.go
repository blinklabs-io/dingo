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
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/livesettest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// stubRewardParitySource satisfies NewObserver's non-nil Source check; the
// observer is never started, so no method is called.
type stubRewardParitySource struct {
	koiosparity.RewardParitySource
}

// TestKoiosParityObserverRegistersThroughNodeRegistry builds the observer
// against the registry New leaves in n.config.promRegistry, which
// configWrapPromRegistry has wrapped with a constant "network" label. It
// builds it twice, as startKoiosParityObserver does again on a live
// restore/truncate, so the reuse path through the wrapper is covered too.
func TestKoiosParityObserverRegistersThroughNodeRegistry(t *testing.T) {
	registry := prometheus.NewRegistry()
	node, err := New(NewConfig(
		WithNetworkMagic(2),
		WithPrometheusRegistry(registry),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp", ListenAddress: "127.0.0.1:0",
		}),
	))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.Stop()) })

	cacheDir := t.TempDir()
	for i := range 2 {
		var observer *koiosparity.Observer
		require.NotPanics(t, func() {
			observer, err = koiosparity.NewObserver(koiosparity.ObserverConfig{
				Network:      "preview",
				CachePath:    filepath.Join(cacheDir, "cache"),
				Source:       stubRewardParitySource{},
				PromRegistry: node.config.promRegistry,
			})
		}, "observer construction %d", i)
		require.NoError(t, err)
		require.NoError(t, observer.Stop(context.Background()))
	}

	err = node.config.promRegistry.Register(prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_koiosparity_epoch_result_total",
			Help: "koios-parity epoch validation results, by network (a constant label from the node's shared registry), queue and status (pass/fail/error/not_checked)",
		},
		[]string{"queue", "status"},
	))
	_, ok := errors.AsType[prometheus.AlreadyRegisteredError](err)
	require.True(
		t,
		ok,
		"koios-parity collectors were not registered through the node "+
			"registry: %v",
		err,
	)
}

// TestKoiosParitySourceReadsTransactionHistoryFromTheIndexerDB verifies the
// source an API node builds samples transactions from its IndexerDB. The
// consensus database runs in core mode, where the sample is skipped, so a
// source built on it alone never compares a transaction. The IndexerDB's tip
// decides the answer here: pending while it trails the epoch, a sample after.
func TestKoiosParitySourceReadsTransactionHistoryFromTheIndexerDB(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()

	consensus, err := dbtest.NewDatabase(
		t, &database.Config{DataDir: t.TempDir()},
	)
	require.NoError(t, err)
	indexerDB, err := dbtest.NewIndexerDatabase(
		t, consensus, dbtest.IndexerOptions{},
	)
	require.NoError(t, err)
	set := livesettest.OpenLedgerDB(t, consensus)
	require.NoError(t, set.ApplyLedgerChanges(nil, ledger.LedgerChanges{
		Epochs: []models.Epoch{{
			EpochId: 3, StartSlot: 1000, EraId: 5,
			SlotLength: 1000, LengthInSlots: 500,
		}},
	}))
	// A node's ledger gives its database the set's epochs; this one has no
	// ledger.
	consensus.SetEpochSource(set)

	core, err := (&Node{db: consensus}).koiosParitySource()
	require.NoError(t, err)
	hashes, err := core.GetEpochTransactionHashes(ctx, 3, 20)
	require.NoError(t, err)
	require.Empty(t, hashes, "a core node retains no history to sample")

	api, err := (&Node{db: consensus, indexerDB: indexerDB}).
		koiosParitySource()
	require.NoError(t, err)
	_, err = api.GetEpochTransactionHashes(ctx, 3, 20)
	require.ErrorIs(t, err, koiosparity.ErrTransactionHistoryPending,
		"the sample must be read from the IndexerDB, which is still empty")

	require.NoError(t, indexerDB.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1500, bytes.Repeat([]byte{0x0a}, 32)),
	}, nil))
	_, err = api.GetEpochTransactionHashes(ctx, 3, 20)
	require.NoError(t, err)
}

// TestKoiosParityObserverLeavesTheLedgerDBRewardWindowAlone verifies a core
// node with the Koios parity observer enabled prunes reward outputs from its
// LedgerDB exactly as one without it does. Reward history older than the
// window is API data, so the observer must not keep it in the LedgerDB.
func TestKoiosParityObserverLeavesTheLedgerDBRewardWindowAlone(t *testing.T) {
	t.Parallel()

	n, stop := bootSetNode(t, t.TempDir(), "preview")
	defer stop()
	n.config.koiosParity.Enabled = true
	require.NoError(t, n.configureSnapshotManager())
	require.NoError(t, n.snapshotMgr.Start(t.Context()))
	defer func() { require.NoError(t, n.snapshotMgr.Stop()) }()

	const currentEpoch = 10
	for epoch := range uint64(currentEpoch + 1) {
		require.NoError(t, n.utxoSet.ApplyLedgerChanges(
			nil, ledger.LedgerChanges{
				RewardDerived: []ledger.RewardDerivedChange{{
					Kind:  ledger.RewardDerivedAccounts,
					Epoch: epoch,
					Accounts: []*models.RewardAccountOutput{{
						StakingKey:  bytes.Repeat([]byte{0x01}, 28),
						PoolKeyHash: bytes.Repeat([]byte{0x02}, 28),
						RewardType:  "member",
						Epoch:       epoch,
						Amount:      types.Uint64(epoch + 1),
						Spendable:   true,
					}},
				}},
			},
		))
	}
	ldb := n.ledgerState.LedgerDB()
	rows, err := ldb.GetRewardAccountOutputs(0, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	n.eventBus.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(
			event.EpochTransitionEventType,
			event.EpochTransitionEvent{
				PreviousEpoch: currentEpoch - 1,
				NewEpoch:      currentEpoch,
			},
		),
	)
	retainedFrom := uint64(currentEpoch - 3)
	testutil.WaitForCondition(t, func() bool {
		rows, err := ldb.GetRewardAccountOutputs(retainedFrom-1, nil)
		return err == nil && len(rows) == 0
	}, 5*time.Second,
		"the LedgerDB must prune reward outputs below the window with the "+
			"observer enabled")
	for epoch := range retainedFrom {
		rows, err := ldb.GetRewardAccountOutputs(epoch, nil)
		require.NoError(t, err)
		require.Empty(t, rows, "epoch %d is below the window", epoch)
	}
	for epoch := retainedFrom; epoch <= currentEpoch; epoch++ {
		rows, err := ldb.GetRewardAccountOutputs(epoch, nil)
		require.NoError(t, err)
		require.Len(t, rows, 1, "epoch %d is inside the window", epoch)
	}
}

// TestKoiosParitySourceReadsRewardStateFromTheIndexerDB verifies the source
// an API node builds reads the reward state from its IndexerDB, which holds
// epochs the LedgerDB's window no longer does. The LedgerDB here holds no
// reward rows at all.
func TestKoiosParitySourceReadsRewardStateFromTheIndexerDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	consensus, err := dbtest.NewDatabase(
		t, &database.Config{DataDir: t.TempDir()},
	)
	require.NoError(t, err)
	indexerDB, err := dbtest.NewIndexerDatabase(
		t, consensus, dbtest.IndexerOptions{},
	)
	require.NoError(t, err)
	set := livesettest.OpenLedgerDB(t, consensus)
	consensus.SetEpochSource(set)

	poolKeyHash := bytes.Repeat([]byte{0x02}, 28)
	const stakeEpoch = 2
	txn := indexerDB.Transaction(ctx, true)
	require.NoError(t, indexerDB.Metadata().SaveRewardPoolOutputs(
		[]*models.RewardPoolOutput{{
			Epoch:             stakeEpoch,
			PoolKeyHash:       poolKeyHash,
			MemberRewardTotal: types.Uint64(7),
		}},
		txn.Metadata(),
	))
	require.NoError(t, indexerDB.Metadata().SaveRewardAccountOutputs(
		[]*models.RewardAccountOutput{{
			StakingKey:  bytes.Repeat([]byte{0x01}, 28),
			PoolKeyHash: poolKeyHash,
			RewardType:  "member",
			Epoch:       stakeEpoch,
			Amount:      types.Uint64(7),
			Spendable:   true,
		}},
		txn.Metadata(),
	))
	require.NoError(t, txn.Commit())

	api, err := (&Node{db: consensus, indexerDB: indexerDB}).
		koiosParitySource()
	require.NoError(t, err)
	rows, err := api.GetRewardAccountOutputs(ctx, stakeEpoch)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	pools, err := api.GetPoolEpochDataMap(ctx, stakeEpoch, stakeEpoch+1)
	require.NoError(t, err)
	pool := pools[hex.EncodeToString(poolKeyHash)]
	require.NotNil(t, pool)
	require.True(t, pool.MemberRewardPresent)
	require.Equal(t, "7", pool.MemberRewardTotal)
	require.True(t, pool.SpendableMemberRewardPresent)
	require.Equal(t, "7", pool.SpendableMemberRewardTotal)
}
