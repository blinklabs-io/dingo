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
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// newDiskTestLedger builds a ledger over an on-disk database. A QueryView's
// isolation comes from a held read transaction, which the in-memory SQLite
// used by newTestDB cannot provide: its shared cache gives readers table
// locks, not a snapshot, so a concurrent writer would block instead of being
// hidden from the view.
func newDiskTestLedger(t *testing.T) (*LedgerState, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	return newPoolDistr2Ledger(t, db), db
}

func shelleyBlockQuery(q any) *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{Query: q},
	}
}

func queryViewTestAddress(t *testing.T) lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	return addr
}

// utxoMapLen unwraps a UtxoByAddress/UtxoByTxin reply and returns how many
// outputs it carries.
func utxoMapLen(t *testing.T, result any) int {
	t.Helper()
	arr, ok := result.([]any)
	require.True(t, ok, "expected []any result, got %T", result)
	require.NotEmpty(t, arr)
	m, ok := arr[len(arr)-1].(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok, "expected UtxoId map, got %T", arr[len(arr)-1])
	return len(m)
}

// TestQueryViewIsolatedFromLaterCommits proves a view answers from the state
// that existed when it was acquired: a spend and a pool registration
// committed afterwards stay invisible to the view while a direct, unacquired
// query already sees them. The two queries read through different handlers,
// one that already took a transaction and one that did not.
func TestQueryViewIsolatedFromLaterCommits(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	addr := queryViewTestAddress(t)
	txId := seedBabbageUtxo(t, db, 0xC1, 0, addr, 5_000_000)
	txIn := ledger.NewShelleyTransactionInput(hex.EncodeToString(txId), 0)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1_000, repeatedBytes(32, 0x0B)),
	}, nil))
	require.NoError(t, db.SetEpoch(
		0, 0, nil, nil, nil, nil, 0, 1, 100_000, nil,
	))
	seedPoolDistr2Fixture(
		t, db, repeatedBytes(28, 0x11), repeatedBytes(32, 0x21), 1_000, 0,
	)

	utxoQuery := shelleyBlockQuery(&olocalstatequery.ShelleyUtxoByTxinQuery{
		TxIns: []ledger.ShelleyTransactionInput{txIn},
	})
	poolsQuery := shelleyBlockQuery(&olocalstatequery.ShelleyStakePoolsQuery{})
	utxoCount := func(result any) int {
		return utxoMapLen(t, result)
	}
	poolCount := func(result any) int {
		set, ok := result.([]any)[0].(cbor.Set)
		require.True(t, ok)
		return len(set)
	}

	view, err := ls.AcquireQueryView(t.Context(), QueryPoint{})
	require.NoError(t, err)
	t.Cleanup(view.Close)

	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		context.Background(),
		nil,
		[]dbtypes.UtxoKey{{TxId: txId, OutputIdx: 0}},
		500,
	))
	seedPoolDistr2Fixture(
		t, db, repeatedBytes(28, 0x12), repeatedBytes(32, 0x22), 1_000, 0,
	)

	for _, tc := range []struct {
		name            string
		query           any
		count           func(any) int
		wantLive, wantV int
	}{
		{"UtxoByTxin", utxoQuery, utxoCount, 0, 1},
		{"StakePools", poolsQuery, poolCount, 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			live, err := ls.Query(context.Background(), tc.query, QueryPoint{})
			require.NoError(t, err)
			require.Equal(
				t, tc.wantLive, tc.count(live),
				"a direct query must see the later commit",
			)

			viewed, err := view.Query(t.Context(), tc.query, 0)
			require.NoError(t, err)
			require.Equal(
				t, tc.wantV, tc.count(viewed),
				"the view must not see a commit made after acquire",
			)
		})
	}
}

// TestQueryViewChainTipIsAcquireTip proves chain-tip queries report the tip
// the view was acquired at rather than the live one.
func TestQueryViewChainTipIsAcquireTip(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	acquiredHash := bytes.Repeat([]byte{0x01}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.NewPoint(10, acquiredHash),
		BlockNumber: 3,
	}, nil))

	view, err := ls.AcquireQueryView(t.Context(), QueryPoint{})
	require.NoError(t, err)
	t.Cleanup(view.Close)

	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.NewPoint(11, bytes.Repeat([]byte{0x02}, 32)),
		BlockNumber: 4,
	}, nil))

	point, err := view.Query(t.Context(), &olocalstatequery.ChainPointQuery{}, 0)
	require.NoError(t, err)
	require.Equal(t, ocommon.NewPoint(10, acquiredHash), point)

	blockNo, err := view.Query(t.Context(), &olocalstatequery.ChainBlockNoQuery{}, 0)
	require.NoError(t, err)
	require.Equal(t, []any{1, uint64(3)}, blockNo)
}

// TestQueryViewPinnedPointSurvivesRollback proves a view acquired at a
// specific point keeps answering after a rollback moves the live tip below
// that point, which a direct query against the same point refuses. It also
// proves a point that is not on the chain never opens a view.
func TestQueryViewPinnedPointSurvivesRollback(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	addr := queryViewTestAddress(t)
	txId := seedBabbageUtxo(t, db, 0xC2, 0, addr, 1_000_000)
	pointHash := bytes.Repeat([]byte{0xAB}, 32)
	seedBlockAtSlot(t, ls, 300, pointHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(300, pointHash),
	}, nil))
	require.NoError(t, db.SetEpoch(
		0, 0, nil, nil, nil, nil, 0, 1, 1000, nil,
	))
	// GetAccountState needs a network_state row at or before the point;
	// genesis sync writes this slot-0 baseline on a real node.
	require.NoError(t, db.Metadata().SetNetworkState(0, 0, 0, nil))
	point := QueryPoint{Slot: 300, Hash: pointHash}

	_, err := ls.AcquireQueryView(
		t.Context(),
		QueryPoint{Slot: 300, Hash: bytes.Repeat([]byte{0xCD}, 32)},
	)
	require.ErrorIs(t, err, ErrPointNotOnChain)

	view, err := ls.AcquireQueryView(t.Context(), point)
	require.NoError(t, err)
	t.Cleanup(view.Close)

	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(200, bytes.Repeat([]byte{0xEF}, 32)),
	}, nil))

	txIn := ledger.NewShelleyTransactionInput(hex.EncodeToString(txId), 0)
	query := shelleyBlockQuery(&olocalstatequery.ShelleyUtxoByTxinQuery{
		TxIns: []ledger.ShelleyTransactionInput{txIn},
	})
	_, err = ls.Query(context.Background(), query, point)
	require.ErrorIs(
		t, err, ErrPointNotOnChain,
		"a direct query must see the rolled-back tip",
	)

	result, err := view.Query(t.Context(), query, 0)
	require.NoError(t, err)
	require.Equal(t, 1, utxoMapLen(t, result))
}

// TestQueryViewClose covers the close contract: queries are refused once
// closed, Close is idempotent, and a query still in flight keeps the snapshot
// until it finishes.
func TestQueryViewClose(t *testing.T) {
	t.Parallel()

	ls, _ := newDiskTestLedger(t)
	view, err := ls.AcquireQueryView(t.Context(), QueryPoint{})
	require.NoError(t, err)

	released := make(chan struct{})
	var releasedCount int
	view.txn.OnFinish(func() {
		releasedCount++
		close(released)
	})

	view.mu.Lock()
	view.inFlight++
	view.mu.Unlock()
	view.Close()
	select {
	case <-released:
		t.Fatal("Close released the snapshot under an in-flight query")
	default:
	}

	_, err = view.Query(t.Context(), &olocalstatequery.ChainPointQuery{}, 0)
	require.ErrorIs(t, err, ErrQueryViewClosed)

	view.finishQuery()
	// Release runs synchronously inside finishQuery, so the channel is
	// already closed here; a receive that blocks fails the test on its own.
	<-released

	view.Close()
	require.Equal(t, 1, releasedCount)
}

// freezeEpochThenCrossBoundary seeds epoch 3 (era3) and epoch 6 (era6),
// acquires an unpinned view while the tip is in epoch 3, then moves the tip
// and the live consensus snapshot into epoch 6, the way an epoch boundary
// does after a client has already acquired.
func freezeEpochThenCrossBoundary(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	era3, era6 eras.EraDesc,
) *QueryView {
	t.Helper()
	require.NoError(t, db.SetEpoch(
		300, 3, nil, nil, nil, nil, era3.Id, 1, 100, nil,
	))
	require.NoError(t, db.SetEpoch(
		600, 6, nil, nil, nil, nil, era6.Id, 1, 100, nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, repeatedBytes(32, 0x0A)),
	}, nil))
	ls.currentEra = era3
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	view, err := ls.AcquireQueryView(t.Context(), QueryPoint{})
	require.NoError(t, err)
	t.Cleanup(view.Close)

	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))
	ls.currentEra = era6
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()
	return view
}

// TestQueryViewEpochAndEraAreThoseFrozenAtAcquire proves the current epoch
// number and era, which the ledger otherwise serves from a live in-memory
// snapshot, describe the epoch the view froze after a boundary passes.
func TestQueryViewEpochAndEraAreThoseFrozenAtAcquire(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	view := freezeEpochThenCrossBoundary(
		t, ls, db, eras.ShelleyEraDesc, eras.ConwayEraDesc,
	)
	epochQuery := shelleyBlockQuery(&olocalstatequery.ShelleyEpochNoQuery{})
	eraQuery := &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.HardForkQuery{
			Query: &olocalstatequery.HardForkCurrentEraQuery{},
		},
	}

	live, err := ls.Query(context.Background(), epochQuery, QueryPoint{})
	require.NoError(t, err)
	require.Equal(t, []any{uint64(6)}, live)
	viewed, err := view.Query(t.Context(), epochQuery, 0)
	require.NoError(t, err)
	require.Equal(t, []any{uint64(3)}, viewed)

	live, err = ls.Query(context.Background(), eraQuery, QueryPoint{})
	require.NoError(t, err)
	require.Equal(t, eras.ConwayEraDesc.Id, live)
	viewed, err = view.Query(t.Context(), eraQuery, 0)
	require.NoError(t, err)
	require.Equal(t, eras.ShelleyEraDesc.Id, viewed)
}

// TestQueryViewProtocolParametersAreThoseFrozenAtAcquire proves the view
// answers current protocol parameters from the epoch it froze once a boundary
// passes, and falls back to the live value when that epoch has no persisted
// row rather than failing the session.
func TestQueryViewProtocolParametersAreThoseFrozenAtAcquire(t *testing.T) {
	t.Parallel()

	ppQuery := shelleyBlockQuery(
		&olocalstatequery.ShelleyCurrentProtocolParamsQuery{},
	)
	costModel := func(t *testing.T, result any) []int64 {
		t.Helper()
		params, ok := result.([]any)[0].(*conway.ConwayProtocolParameters)
		require.True(t, ok)
		return params.CostModels[0]
	}
	setup := func(t *testing.T) (*LedgerState, *database.Database) {
		ls, db := newDiskTestLedger(t)
		ls.currentPParams = conwayPParamsWithCostModels(
			map[uint][]int64{0: {9, 9, 9}},
		)
		return ls, db
	}

	t.Run("persisted row", func(t *testing.T) {
		t.Parallel()
		ls, db := setup(t)
		persisted, err := cbor.Encode(
			conwayPParamsWithCostModels(map[uint][]int64{0: {1, 1, 1}}),
		)
		require.NoError(t, err)
		require.NoError(t, db.SetPParams(
			persisted, 300, 3, eras.ConwayEraDesc.Id, nil,
		))
		view := freezeEpochThenCrossBoundary(
			t, ls, db, eras.ConwayEraDesc, eras.ConwayEraDesc,
		)

		live, err := ls.Query(context.Background(), ppQuery, QueryPoint{})
		require.NoError(t, err)
		require.Equal(t, []int64{9, 9, 9}, costModel(t, live))
		viewed, err := view.Query(t.Context(), ppQuery, 0)
		require.NoError(t, err)
		require.Equal(t, []int64{1, 1, 1}, costModel(t, viewed))
	})

	t.Run("no persisted row falls back to live", func(t *testing.T) {
		t.Parallel()
		ls, db := setup(t)
		view := freezeEpochThenCrossBoundary(
			t, ls, db, eras.ConwayEraDesc, eras.ConwayEraDesc,
		)

		viewed, err := view.Query(t.Context(), ppQuery, 0)
		require.NoError(t, err)
		require.Equal(t, []int64{9, 9, 9}, costModel(t, viewed))
	})
}

// TestQueryViewStakeSnapshotsUseFrozenEpoch proves GetStakeSnapshots reports
// the mark stake of the epoch the view froze, not the live epoch's.
func TestQueryViewStakeSnapshotsUseFrozenEpoch(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	pool := repeatedBytes(28, 0x11)
	require.NoError(t, db.Metadata().SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			{
				Epoch: 3, SnapshotType: snapshotTypeMark, PoolKeyHash: pool,
				TotalStake: 111, DelegatorCount: 1, CapturedSlot: 300,
			},
			{
				Epoch: 6, SnapshotType: snapshotTypeMark, PoolKeyHash: pool,
				TotalStake: 666, DelegatorCount: 1, CapturedSlot: 600,
			},
		},
		nil,
	))
	view := freezeEpochThenCrossBoundary(
		t, ls, db, eras.ConwayEraDesc, eras.ConwayEraDesc,
	)
	query := shelleyBlockQuery(&olocalstatequery.ShelleyStakeSnapshotsQuery{})
	markStake := func(result any) uint64 {
		t.Helper()
		snapshots, ok := result.([]any)[0].(olocalstatequery.StakeSnapshotsResult)
		require.True(t, ok)
		snapshot, ok := snapshots.PoolSnapshots[lcommon.NewBlake2b224(pool)]
		require.True(t, ok)
		return snapshot.StakeMark
	}

	live, err := ls.Query(context.Background(), query, QueryPoint{})
	require.NoError(t, err)
	require.Equal(t, uint64(666), markStake(live))
	viewed, err := view.Query(t.Context(), query, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(111), markStake(viewed))
}

// TestQueryViewEraHistoryUsesFrozenTip proves GetEraHistory is computed from
// the tip the view froze: a live tip that has since moved near the epoch end
// extends the forecast, and the view must not follow it.
func TestQueryViewEraHistoryUsesFrozenTip(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	require.NoError(t, db.SetEpoch(
		100_000, 500, nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, 1_000, 432_000, nil,
	))
	setTip := func(slot uint64) {
		tip := ochainsync.Tip{
			Point: ocommon.NewPoint(slot, repeatedBytes(32, 0x0C)),
		}
		require.NoError(t, db.SetTip(tip, nil))
		ls.currentTip = tip
		ls.publishSnapshotsLocked()
	}
	ls.currentEra = eras.ConwayEraDesc
	ls.transitionInfo = hardfork.NewTransitionUnknown()
	setTip(200_000)
	query := &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.HardForkQuery{
			Query: &olocalstatequery.HardForkEraHistoryQuery{},
		},
	}

	before, err := ls.Query(context.Background(), query, QueryPoint{})
	require.NoError(t, err)
	view, err := ls.AcquireQueryView(t.Context(), QueryPoint{})
	require.NoError(t, err)
	t.Cleanup(view.Close)
	setTip(530_000)

	after, err := ls.Query(context.Background(), query, QueryPoint{})
	require.NoError(t, err)
	require.NotEqual(
		t, before, after,
		"the live tip must change the forecast for this test to mean anything",
	)
	viewed, err := view.Query(t.Context(), query, 0)
	require.NoError(t, err)
	require.Equal(t, before, viewed)
}

// TestQueryViewLeavesAReadConnectionForOtherReaders proves acquired views
// cannot occupy the whole metadata read pool. A view holds its read
// connection for its whole lifetime, so without a cap as many clients as
// there are connections would block every other reader in the node -- chain
// sync, APIs, rollback -- until their views expire. Once the cap is reached
// an Acquire fails when its context ends instead of taking the last
// connection. The context passed to AcquireQueryView bounds only that wait.
func TestQueryViewLeavesAReadConnectionForOtherReaders(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1_000, repeatedBytes(32, 0x0B)),
	}, nil))

	// Every connection but one can hold a view. The acquire contexts end as
	// soon as each view is open.
	views := make([]*QueryView, 0, sqlite.DefaultMaxConnections-1)
	for range sqlite.DefaultMaxConnections - 1 {
		ctx, cancel := context.WithCancel(t.Context())
		view, err := ls.AcquireQueryView(ctx, QueryPoint{})
		cancel()
		require.NoError(t, err)
		t.Cleanup(view.Close)
		views = append(views, view)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	_, err := ls.AcquireQueryView(ctx, QueryPoint{})
	cancel()
	require.ErrorIs(
		t, err, context.DeadlineExceeded,
		"the last read connection must stay free",
	)

	// The context only bounds the wait to acquire: a view outlives it.
	for _, view := range views {
		_, err := view.Query(t.Context(), &olocalstatequery.ChainPointQuery{}, 0)
		require.NoError(t, err)
	}

	ctx, cancel = context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	txn := db.TransactionContext(ctx, false)
	defer txn.Release()
	_, err = db.GetTip(txn)
	require.NoError(t, err, "a reader outside the views must still get a connection")
}

func seedPendingRatification(
	t *testing.T,
	db *database.Database,
	rec pendingRatificationRecord,
) {
	t.Helper()
	raw, err := json.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, db.SetSyncState(
		pendingRatificationSyncKey, string(raw), nil,
	))
}

// TestAcquireQueryViewRetriesPastPendingBoundary proves a snapshot that
// froze a boundary whose deferred work had not finished is dropped and
// reopened once the pending record is consumed.
func TestAcquireQueryViewRetriesPastPendingBoundary(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	seedPendingRatification(t, db, pendingRatificationRecord{
		Epoch: 4, BoundarySlot: 400, ID: 1,
	})

	type acquired struct {
		view *QueryView
		err  error
	}
	done := make(chan acquired, 1)
	go func() {
		view, err := ls.AcquireQueryView(t.Context(), QueryPoint{})
		done <- acquired{view, err}
	}()

	select {
	case res := <-done:
		if res.view != nil {
			res.view.Close()
		}
		t.Fatalf("acquire returned with a boundary pending: %v", res.err)
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, db.DeleteSyncState(pendingRatificationSyncKey, nil))
	select {
	case res := <-done:
		require.NoError(t, res.err)
		require.NotNil(t, res.view)
		res.view.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("acquire did not retry after the pending record cleared")
	}
}

// TestAcquireQueryViewPendingBoundaryHonorsDeadline proves the retry wait is
// inside the caller's context and releases the snapshot it dropped.
func TestAcquireQueryViewPendingBoundaryHonorsDeadline(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	seedPendingRatification(t, db, pendingRatificationRecord{
		Epoch: 4, BoundarySlot: 400, ID: 1,
	})

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	view, err := ls.AcquireQueryView(ctx, QueryPoint{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, view)

	// The dropped snapshots must not leak admission slots.
	require.NoError(t, db.DeleteSyncState(pendingRatificationSyncKey, nil))
	view, err = ls.AcquireQueryView(t.Context(), QueryPoint{})
	require.NoError(t, err)
	view.Close()
}

// TestQueryViewQueryRecoversPanic proves a panic in a query handler fails
// that query instead of reaching the connection's handler goroutine, and
// leaves the view usable and releasable.
func TestQueryViewQueryRecoversPanic(t *testing.T) {
	t.Parallel()

	ls, _ := newDiskTestLedger(t)
	view, err := ls.AcquireQueryView(t.Context(), QueryPoint{})
	require.NoError(t, err)
	t.Cleanup(view.Close)

	var nilQuery *olocalstatequery.ShelleyStakeSnapshotsQuery
	require.NotPanics(t, func() {
		_, err = view.Query(t.Context(), shelleyBlockQuery(nilQuery), 0)
	})
	require.Error(t, err)
	require.ErrorIs(t, err, database.ErrTxnPanic)

	_, err = view.Query(t.Context(), &olocalstatequery.ChainPointQuery{}, 0)
	require.NoError(t, err)
}

// TestQueryViewStakeSnapshotsOmitZeroPoolsUsesFrozenProtocolVersion proves a
// view that froze a pre-PV11 epoch keeps returning an explicitly requested
// zero-stake pool after the live epoch moves to PV11.
func TestQueryViewStakeSnapshotsOmitZeroPoolsUsesFrozenProtocolVersion(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	pool := repeatedBytes(28, 0x22)
	frozen := conwayPParamsWithCostModels(nil)
	frozen.ProtocolVersion.Major = 10
	live := conwayPParamsWithCostModels(nil)
	live.ProtocolVersion.Major = 11
	frozenCbor, err := cbor.Encode(frozen)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		frozenCbor, 300, 3, eras.ConwayEraDesc.Id, nil,
	))
	view := freezeEpochThenCrossBoundary(
		t, ls, db, eras.ConwayEraDesc, eras.ConwayEraDesc,
	)
	ls.currentPParams = live
	ls.publishSnapshotsLocked()

	poolID := lcommon.NewBlake2b224(pool)
	query := shelleyBlockQuery(&olocalstatequery.ShelleyStakeSnapshotsQuery{
		Pools: []cbor.SetType[ledger.PoolId]{
			cbor.NewSetType([]ledger.PoolId{ledger.PoolId(poolID)}, true),
		},
	})
	liveResult, err := ls.Query(context.Background(), query, QueryPoint{})
	require.NoError(t, err)
	require.Empty(t, liveResult.([]any)[0].(olocalstatequery.StakeSnapshotsResult).PoolSnapshots)

	viewed, err := view.Query(t.Context(), query, 0)
	require.NoError(t, err)
	require.Contains(t,
		viewed.([]any)[0].(olocalstatequery.StakeSnapshotsResult).PoolSnapshots,
		poolID,
	)
}
