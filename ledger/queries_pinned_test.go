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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

func seedAddressUtxoAt(
	t *testing.T,
	db *database.Database,
	addr lcommon.Address,
	txIdSeed byte,
	addedSlot uint64,
	amount uint64,
) []byte {
	t.Helper()
	txId := bytes.Repeat([]byte{txIdSeed}, 32)
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       txId,
		OutputIdx:  0,
		PaymentKey: addr.PaymentKeyHash().Bytes(),
		AddedSlot:  addedSlot,
		Amount:     dbtypes.Uint64(amount),
	}))
	encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  amount,
	})
	require.NoError(t, err)
	require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
		return db.Blob().SetUtxo(txn.Blob(), txId, 0, encoded)
	}))
	return txId
}

func utxoByAddressAmounts(
	t *testing.T,
	ls *LedgerState,
	addr lcommon.Address,
	at QueryPoint,
) []uint64 {
	t.Helper()
	result, err := ls.queryShelleyUtxoByAddress(
		[]ledger.Address{addr}, at, nil,
	)
	require.NoError(t, err)
	reply := result.([]any)[0]
	m, ok := reply.(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok)
	amounts := make([]uint64, 0, len(m))
	for _, out := range m {
		amounts = append(amounts, out.Amount().Uint64())
	}
	return amounts
}

func TestQueryShelleyUtxoByAddress_PinnedPointAnswersAtThatPoint(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1_000, repeatedBytes(32, 0x0B)),
	}, nil))
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xA7}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)

	spentLater := seedAddressUtxoAt(t, db, addr, 0x01, 100, 1_000_000)
	seedAddressUtxoAt(t, db, addr, 0x02, 100, 2_000_000)
	seedAddressUtxoAt(t, db, addr, 0x03, 600, 3_000_000)
	spentBefore := seedAddressUtxoAt(t, db, addr, 0x04, 100, 4_000_000)
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		nil,
		[]dbtypes.UtxoKey{{TxId: spentLater, OutputIdx: 0}},
		500,
	))
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		nil,
		[]dbtypes.UtxoKey{{TxId: spentBefore, OutputIdx: 0}},
		200,
	))

	require.ElementsMatch(
		t,
		[]uint64{1_000_000, 2_000_000},
		utxoByAddressAmounts(t, ls, addr, QueryPoint{Slot: 300}),
		"at slot 300: the output spent at 500 is still live, the one "+
			"created at 600 does not exist yet, the one spent at 200 is gone",
	)
	require.ElementsMatch(
		t,
		[]uint64{2_000_000, 3_000_000},
		utxoByAddressAmounts(t, ls, addr, QueryPoint{}),
	)
}

func TestQueryShelleyUtxoByAddress_PinnedPastRetentionFloorRejected(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	// The default stability window is 50_000, so the floor is 150_000.
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(200_000, repeatedBytes(32, 0x0B)),
	}, nil))
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xA8}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)

	_, err = ls.queryShelleyUtxoByAddress(
		[]ledger.Address{addr}, QueryPoint{Slot: 149_999}, nil,
	)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
	_, err = ls.queryShelleyUtxoByAddress(
		[]ledger.Address{addr}, QueryPoint{Slot: 150_000}, nil,
	)
	require.NoError(t, err)
}

func accountState(t *testing.T, result any) olocalstatequery.AccountState {
	t.Helper()
	st, ok := result.([]any)[0].(olocalstatequery.AccountState)
	require.True(t, ok)
	return st
}

func TestQueryShelleyAccountState_PinnedPointReadsRowInEffect(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.Metadata().SetNetworkState(10, 900, 100, nil))
	require.NoError(t, db.Metadata().SetNetworkState(20, 800, 500, nil))

	for _, tc := range []struct {
		at       QueryPoint
		treasury int64
		reserves int64
	}{
		{QueryPoint{Slot: 100}, 10, 900},
		{QueryPoint{Slot: 499}, 10, 900},
		{QueryPoint{Slot: 500}, 20, 800},
		{QueryPoint{}, 20, 800},
	} {
		result, err := ls.queryShelleyAccountState(tc.at, nil)
		require.NoError(t, err)
		got := accountState(t, result)
		require.Equal(t, tc.treasury, got.Treasury, "slot %d", tc.at.Slot)
		require.Equal(t, tc.reserves, got.Reserves, "slot %d", tc.at.Slot)
	}
}

func TestQueryShelleyAccountState_PinnedBeforeFirstRowRejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.Metadata().SetNetworkState(10, 900, 100, nil))

	_, err := ls.queryShelleyAccountState(QueryPoint{Slot: 99}, nil)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// newStakeSnapshotsLedger seeds Conway epochs 3-6 at slots 300-600 with the
// live tip in epoch 6. pinnedMajor is the protocol version persisted at
// epoch 3, which later epochs inherit until a change, and liveMajor the one
// in the live snapshot. pool has mark stake 100*epoch in epochs 3-6.
func newStakeSnapshotsLedger(
	t *testing.T,
	pool []byte,
	pinnedMajor, liveMajor uint,
) *LedgerState {
	t.Helper()
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	conwayEraId := uint(eras.ConwayEraDesc.Id)
	pinned := conwayPParamsWithCostModels(nil)
	pinned.ProtocolVersion.Major = pinnedMajor
	pinnedCbor, err := cbor.Encode(pinned)
	require.NoError(t, err)
	var snapshots []*models.PoolStakeSnapshot
	for epoch := uint64(3); epoch <= 6; epoch++ {
		require.NoError(t, db.SetEpoch(
			epoch*100, epoch, nil, nil, nil, nil, conwayEraId, 1, 100, nil,
		))
		if epoch == 3 {
			require.NoError(t, db.SetPParams(
				pinnedCbor, epoch*100, epoch, conwayEraId, nil,
			))
		}
		snapshots = append(snapshots, &models.PoolStakeSnapshot{
			Epoch:          epoch,
			SnapshotType:   snapshotTypeMark,
			PoolKeyHash:    pool,
			TotalStake:     dbtypes.Uint64(100 * epoch),
			DelegatorCount: 1,
			CapturedSlot:   epoch * 100,
		})
	}
	require.NoError(t, db.Metadata().SavePoolStakeSnapshots(snapshots, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))
	live := conwayPParamsWithCostModels(nil)
	live.ProtocolVersion.Major = liveMajor
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = live
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()
	return ls
}

func stakeSnapshotsQuery(
	pools ...[]byte,
) *olocalstatequery.ShelleyStakeSnapshotsQuery {
	if len(pools) == 0 {
		return &olocalstatequery.ShelleyStakeSnapshotsQuery{}
	}
	ids := make([]ledger.PoolId, len(pools))
	for i, pool := range pools {
		ids[i] = ledger.PoolId(lcommon.NewBlake2b224(pool))
	}
	return &olocalstatequery.ShelleyStakeSnapshotsQuery{
		Pools: []cbor.SetType[ledger.PoolId]{cbor.NewSetType(ids, true)},
	}
}

func stakeSnapshotsResult(
	t *testing.T,
	result any,
) olocalstatequery.StakeSnapshotsResult {
	t.Helper()
	got, ok := result.([]any)[0].(olocalstatequery.StakeSnapshotsResult)
	require.True(t, ok)
	return got
}

func TestQueryShelleyStakeSnapshots_PinnedPointReadsItsEpoch(t *testing.T) {
	t.Parallel()

	pool := repeatedBytes(28, 0x21)
	ls := newStakeSnapshotsLedger(t, pool, 10, 10)
	key := lcommon.NewBlake2b224(pool)

	for _, tc := range []struct {
		name           string
		at             QueryPoint
		mark, set, gos uint64
	}{
		{"pinned in epoch 5", QueryPoint{Slot: 550}, 500, 400, 300},
		{"live epoch 6", QueryPoint{}, 600, 500, 400},
	} {
		result, err := ls.queryShelleyStakeSnapshots(
			stakeSnapshotsQuery(), tc.at, nil,
		)
		require.NoError(t, err, tc.name)
		got := stakeSnapshotsResult(t, result)
		require.Contains(t, got.PoolSnapshots, key, tc.name)
		snapshot := got.PoolSnapshots[key]
		require.Equal(t, tc.mark, snapshot.StakeMark, tc.name)
		require.Equal(t, tc.set, snapshot.StakeSet, tc.name)
		require.Equal(t, tc.gos, snapshot.StakeGo, tc.name)
		require.Equal(t, tc.mark, got.TotalStakeMark, tc.name)
		require.Equal(t, tc.gos, got.TotalStakeGo, tc.name)
	}
}

// TestQueryShelleyStakeSnapshots_PinnedGoSnapshotPrunedRejected covers an
// epoch whose own mark snapshot is still retained but whose go snapshot,
// two epochs older, is not: live epoch 6 keeps snapshot epochs 3 and later,
// and a point in epoch 4 needs epoch 2 for go.
func TestQueryShelleyStakeSnapshots_PinnedGoSnapshotPrunedRejected(
	t *testing.T,
) {
	t.Parallel()

	ls := newStakeSnapshotsLedger(t, repeatedBytes(28, 0x22), 10, 10)
	_, err := ls.queryShelleyStakeSnapshots(
		stakeSnapshotsQuery(), QueryPoint{Slot: 450}, nil,
	)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestQueryShelleyStakeSnapshots_PinnedPointUsesItsProtocolVersion covers
// the PV11 rule that drops an explicitly requested pool with no stake: the
// pinned epoch ran PV10, which keeps it, while the live epoch runs PV11.
func TestQueryShelleyStakeSnapshots_PinnedPointUsesItsProtocolVersion(
	t *testing.T,
) {
	t.Parallel()

	ls := newStakeSnapshotsLedger(t, repeatedBytes(28, 0x23), 10, 11)
	idle := repeatedBytes(28, 0x24)
	key := lcommon.NewBlake2b224(idle)

	pinned, err := ls.queryShelleyStakeSnapshots(
		stakeSnapshotsQuery(idle), QueryPoint{Slot: 550}, nil,
	)
	require.NoError(t, err)
	require.Contains(t, stakeSnapshotsResult(t, pinned).PoolSnapshots, key)

	live, err := ls.queryShelleyStakeSnapshots(
		stakeSnapshotsQuery(idle), QueryPoint{}, nil,
	)
	require.NoError(t, err)
	require.NotContains(t, stakeSnapshotsResult(t, live).PoolSnapshots, key)
}

func shelleyLeafQuery(query any) *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{Query: query},
	}
}

// TestQuery_PinnedPointReachesUtxoByAddressAccountStateAndStakeSnapshots
// sends all three queries through Query with a point on chain, so a dispatch
// that dropped the point would answer from the live tip instead.
func TestQuery_PinnedPointReachesUtxoByAddressAccountStateAndStakeSnapshots(
	t *testing.T,
) {
	t.Parallel()

	pool := repeatedBytes(28, 0x25)
	ls := newStakeSnapshotsLedger(t, pool, 10, 10)
	db := ls.db
	hash := repeatedBytes(32, 0x55)
	seedBlockAtSlot(t, ls, 550, hash)
	at := QueryPoint{Slot: 550, Hash: hash}

	require.NoError(t, db.Metadata().SetNetworkState(10, 900, 100, nil))
	require.NoError(t, db.Metadata().SetNetworkState(20, 800, 600, nil))
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xA9}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	spent := seedAddressUtxoAt(t, db, addr, 0x05, 100, 5_000_000)
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		nil,
		[]dbtypes.UtxoKey{{TxId: spent, OutputIdx: 0}},
		600,
	))

	result, err := ls.Query(
		shelleyLeafQuery(&olocalstatequery.ShelleyUtxoByAddressQuery{
			Addrs: []ledger.Address{addr},
		}),
		at,
	)
	require.NoError(t, err)
	reply := result.([]any)[0]
	utxos, ok := reply.(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok)
	require.Len(t, utxos, 1, "the output spent at 600 was live at 550")

	result, err = ls.Query(
		shelleyLeafQuery(&olocalstatequery.ShelleyAccountStateQuery{}),
		at,
	)
	require.NoError(t, err)
	require.Equal(t, int64(10), accountState(t, result).Treasury)

	result, err = ls.Query(shelleyLeafQuery(stakeSnapshotsQuery()), at)
	require.NoError(t, err)
	snapshot := stakeSnapshotsResult(t, result).
		PoolSnapshots[lcommon.NewBlake2b224(pool)]
	require.NotNil(t, snapshot)
	require.Equal(t, uint64(500), snapshot.StakeMark)
}

// TestVerifyPointQueryable_RejectsPointWhoseGoSnapshotIsPruned covers the
// Acquire side of GetStakeSnapshots' go-snapshot floor: a point in epoch 4,
// whose mark snapshot is retained at live epoch 6 but whose go snapshot is
// not, must be refused at Acquire rather than accepted and then failing
// GetStakeSnapshots after the session can no longer report a failure.
func TestVerifyPointQueryable_RejectsPointWhoseGoSnapshotIsPruned(
	t *testing.T,
) {
	t.Parallel()

	ls := newStakeSnapshotsLedger(t, repeatedBytes(28, 0x26), 10, 10)
	require.NoError(t, ls.db.Metadata().SetNetworkState(0, 0, 0, nil))
	pruned := repeatedBytes(32, 0x61)
	retained := repeatedBytes(32, 0x62)
	seedBlockAtSlot(t, ls, 450, pruned)
	seedBlockAtSlot(t, ls, 550, retained)

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 450, Hash: pruned})
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
	require.NoError(
		t,
		ls.VerifyPointQueryable(nil, QueryPoint{Slot: 550, Hash: retained}),
	)
}
