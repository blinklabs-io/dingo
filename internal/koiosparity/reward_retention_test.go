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

package koiosparity

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// setLedgerTipInEpoch10 gives db an epoch 10 spanning [10_000, 11_000) and a
// tip inside it, so the LedgerDB's reward window starts at stake epoch 7.
func setLedgerTipInEpoch10(t *testing.T, db *database.Database) {
	t.Helper()
	require.NoError(t, db.SetEpoch(
		10_000, 10, nil, nil, nil, nil, 5, 1, 1_000, nil,
	))
	setSourceTip(t, db, bytes.Repeat([]byte{0x0a}, 32), 10_500)
}

// startStrictObserver runs a strict observer over source against a fake
// Koios serving epoch 5, and publishes the transition that closes it.
func startStrictObserver(
	t *testing.T,
	source *DatabaseSource,
	reg prometheus.Registerer,
) (<-chan *EpochCompareResult, *atomic.Int32, *Observer) {
	t.Helper()
	srv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{
		5: {activeStake: "1000000", treasury: "10", reserves: "20", fees: "30"},
	})
	results := make(chan *EpochCompareResult, 8)
	fatalCount := &atomic.Int32{}
	o, err := NewObserver(ObserverConfig{
		BaseURL:               srv.URL,
		AllowInsecureHTTP:     true,
		AllowPrivateAddresses: true,
		Network:               "preview",
		CachePath:             filepath.Join(t.TempDir(), "cache.db"),
		Source:                source,
		Strict:                true,
		PromRegistry:          reg,
		Logger:                slog.New(slog.DiscardHandler),
		FetchRetryAttempts:    3,
		FetchRetryDelay:       5 * time.Millisecond,
		OnResult:              func(r *EpochCompareResult) { results <- r },
		FatalFunc:             func(error) { fatalCount.Add(1) },
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = o.Stop(context.Background()) })
	require.NoError(t, o.Start(context.Background()))
	eb := event.NewEventBus(nil, nil)
	t.Cleanup(eb.Stop)
	eb.SubscribeFunc(
		event.EpochTransitionEventType,
		o.HandleEpochTransitionEvent,
	)
	publishEpochTransition(eb, 5)
	return results, fatalCount, o
}

// TestObserverReportsAPrunedEpochAsNotChecked covers a node without an
// IndexerDB: the LedgerDB keeps reward outputs only for its window, so Koios
// epoch 5 (stake epoch 4) is gone once the tip is in epoch 10. Reading the
// absent rows as a mismatch stopped a strict node; the epoch is not checked,
// with a reason the metrics carry, and nothing is stored as its verdict.
func TestObserverReportsAPrunedEpochAsNotChecked(t *testing.T) {
	t.Parallel()

	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	setLedgerTipInEpoch10(t, db)

	reg := prometheus.NewRegistry()
	results, fatalCount, o := startStrictObserver(t, source, reg)

	result := testutil.RequireReceive(
		t, results, 5*time.Second, "epoch 5 result",
	)
	require.Equal(t, uint64(5), result.Epoch)
	require.Equal(t, "NOT_CHECKED", result.Status)
	require.Empty(t, result.Mismatches)
	require.Zero(t, fatalCount.Load(), "not checked must not stop the node")

	count, err := promtestutil.GatherAndCount(
		reg, "dingo_koiosparity_epoch_not_checked_total",
	)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	families, err := reg.Gather()
	require.NoError(t, err)
	var reason string
	for _, f := range families {
		if f.GetName() != "dingo_koiosparity_epoch_not_checked_total" {
			continue
		}
		for _, l := range f.GetMetric()[0].GetLabel() {
			if l.GetName() == "reason" {
				reason = l.GetValue()
			}
		}
	}
	require.Equal(t, "reward_state_pruned", reason)

	mismatches, err := o.cache.GetMismatches("preview", 5, "")
	require.NoError(t, err)
	require.Empty(t, mismatches)
	statuses, err := o.cache.GetStatusSummary("preview")
	require.NoError(t, err)
	require.Empty(t, statuses, "a not-checked epoch stores no verdict")
}

// TestObserverValidatesAnEpochOutsideTheLedgerWindowFromTheIndexerDB covers
// an API node: the LedgerDB no longer holds Koios epoch 5's reward state, but
// the IndexerDB does, so the epoch is compared and passes.
func TestObserverValidatesAnEpochOutsideTheLedgerWindowFromTheIndexerDB(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDatabaseSourceDB(t)
	indexer, err := dbtest.NewReleasedDatabase(
		t, &database.Config{DataDir: t.TempDir()},
	)
	require.NoError(t, err)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	source = source.WithIndexerDB(indexer)

	ledgerSQL := sourceSQLDB(t, db)
	seedDingoBabbageProtocolParams(t, ledgerSQL)
	require.NoError(t, db.SetEpoch(
		5_000, 5, nil, nil, nil, nil, 5, 1, 1_000, nil,
	))
	require.NoError(t, db.SetEpoch(
		6_000, 6, nil, nil, nil, nil, 5, 1, 1_000, nil,
	))
	setLedgerTipInEpoch10(t, db)

	indexerSQL := sourceSQLDB(t, indexer)
	require.NoError(t, indexerSQL.Create(&models.EpochSummary{
		Epoch:            4,
		TotalActiveStake: types.Uint64(1_000_000),
		SnapshotReady:    true,
	}).Error)
	require.NoError(t, indexerSQL.Create(&models.RewardAdaPots{
		Epoch:    5,
		Treasury: types.Uint64(10),
		Reserves: types.Uint64(20),
		Fees:     types.Uint64(30),
	}).Error)
	require.NoError(t, indexer.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(6_500, bytes.Repeat([]byte{0x06}, 32)),
	}, nil))

	retainedFrom, bounded, err := source.RewardStateRetainedFrom(
		context.Background(),
	)
	require.NoError(t, err)
	require.False(t, bounded, "the IndexerDB holds every epoch")
	require.Zero(t, retainedFrom)

	results, fatalCount, _ := startStrictObserver(t, source, nil)
	result := testutil.RequireReceive(
		t, results, 5*time.Second, "epoch 5 result",
	)
	require.Equal(t, uint64(5), result.Epoch)
	require.Equal(t, StatusPass, result.Status, "%+v", result.Mismatches)
	require.Zero(t, fatalCount.Load())
}

// TestRewardHistoryReadyWaitsForTheIndexerDB pins the gate an API node's
// check waits on: the IndexerDB is filled behind the ledger, and reading the
// reward state before it has applied the boundary closing the epoch would see
// rows that are not there yet.
func TestRewardHistoryReadyWaitsForTheIndexerDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	db := newTestDatabaseSourceDB(t)
	indexer, err := dbtest.NewReleasedDatabase(
		t, &database.Config{DataDir: t.TempDir()},
	)
	require.NoError(t, err)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	require.NoError(t, source.rewardHistoryReady(ctx, 5),
		"a source reading the ledger alone never waits")
	source = source.WithIndexerDB(indexer)

	require.ErrorIs(t, source.rewardHistoryReady(ctx, 5),
		ErrRewardHistoryPending, "epoch 5 has not closed on the ledger")
	require.NoError(t, db.SetEpoch(
		6_000, 6, nil, nil, nil, nil, 5, 1, 1_000, nil,
	))
	require.ErrorIs(t, source.rewardHistoryReady(ctx, 5),
		ErrRewardHistoryPending, "the IndexerDB has no tip")
	require.NoError(t, indexer.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(5_999, bytes.Repeat([]byte{0x05}, 32)),
	}, nil))
	require.ErrorIs(t, source.rewardHistoryReady(ctx, 5),
		ErrRewardHistoryPending, "the IndexerDB is still inside epoch 5")
	require.NoError(t, indexer.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(6_000, bytes.Repeat([]byte{0x06}, 32)),
	}, nil))
	require.NoError(t, source.rewardHistoryReady(ctx, 5))
}
