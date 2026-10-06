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
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
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

// newStakePoolsLedger seeds epochs 0-6 of 100 slots, a pool registered at
// slot 100 and another at slot 600, and a tip at slot 650.
func newStakePoolsLedger(t *testing.T) (*LedgerState, []byte, []byte) {
	t.Helper()
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	epochs := map[uint64]uint64{}
	for epoch := uint64(0); epoch <= 6; epoch++ {
		epochs[epoch*100] = epoch
	}
	seedEpochs(t, ls, epochs)
	early := repeatedBytes(28, 0x31)
	late := repeatedBytes(28, 0x32)
	for _, p := range []struct {
		hash []byte
		slot uint64
	}{{early, 100}, {late, 600}} {
		require.NoError(t, db.Metadata().ImportPool(
			&models.Pool{
				PoolKeyHash: p.hash,
				VrfKeyHash:  repeatedBytes(32, p.hash[0]),
			},
			&models.PoolRegistration{
				PoolKeyHash: p.hash,
				VrfKeyHash:  repeatedBytes(32, p.hash[0]),
				AddedSlot:   p.slot,
				Pledge:      dbtypes.Uint64(1),
				Cost:        dbtypes.Uint64(1),
			},
			nil,
		))
	}
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))
	return ls, early, late
}

func stakePoolIDs(t *testing.T, result any) []ledger.PoolId {
	t.Helper()
	set, ok := result.([]any)[0].(cbor.Set)
	require.True(t, ok)
	ids := make([]ledger.PoolId, 0, len(set))
	for _, v := range set {
		id, ok := v.(ledger.PoolId)
		require.True(t, ok)
		ids = append(ids, id)
	}
	return ids
}

func poolID(hash []byte) ledger.PoolId {
	return ledger.PoolId(lcommon.NewBlake2b224(hash))
}

func TestQueryShelleyStakePools_PinnedPointAnswersAtThatPoint(t *testing.T) {
	t.Parallel()

	ls, early, late := newStakePoolsLedger(t)

	pinned, err := ls.queryShelleyStakePools(QueryPoint{Slot: 300}, nil)
	require.NoError(t, err)
	require.Equal(
		t, []ledger.PoolId{poolID(early)}, stakePoolIDs(t, pinned),
		"the pool registered at slot 600 did not exist at slot 300",
	)

	live, err := ls.queryShelleyStakePools(QueryPoint{}, nil)
	require.NoError(t, err)
	require.ElementsMatch(
		t, []ledger.PoolId{poolID(early), poolID(late)}, stakePoolIDs(t, live),
	)
}

func TestQueryShelleyStakePools_PinnedSlotWithoutEpochDataRejected(
	t *testing.T,
) {
	t.Parallel()

	ls, _, _ := newStakePoolsLedger(t)
	_, err := ls.queryShelleyStakePools(QueryPoint{Slot: 5_000}, nil)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestQuery_PinnedPointReachesStakePools sends GetStakePools through Query
// with a point on chain, so a dispatch that dropped the point would answer
// from the tip instead.
func TestQuery_PinnedPointReachesStakePools(t *testing.T) {
	t.Parallel()

	ls, early, _ := newStakePoolsLedger(t)
	hash := repeatedBytes(32, 0x56)
	seedBlockAtSlot(t, ls, 300, hash)

	result, err := ls.Query(
		shelleyLeafQuery(&olocalstatequery.ShelleyStakePoolsQuery{}),
		QueryPoint{Slot: 300, Hash: hash},
	)
	require.NoError(t, err)
	require.Equal(t, []ledger.PoolId{poolID(early)}, stakePoolIDs(t, result))
}

// seedStakeCertAt stores a transaction at slot carrying one stake
// registration or deregistration certificate for stakeKey, with deposit as
// that certificate's deposit or refund.
func seedStakeCertAt(
	t *testing.T,
	db *database.Database,
	stakeKey []byte,
	register bool,
	slot uint64,
	deposit uint64,
) {
	t.Helper()
	cred := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(stakeKey),
	}
	txId := make([]byte, 32)
	copy(txId, stakeKey[:4])
	txId[30], txId[31] = byte(slot>>8), byte(slot)
	txBuilder := mockledger.NewTransactionBuilder()
	txBuilder.WithId(txId)
	txBuilder.WithValid(true)
	input, err := mockledger.NewSimpleTransactionInput(txId, 0)
	require.NoError(t, err)
	txBuilder.WithInputs(input)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(
			"addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd",
		).
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	txBuilder.WithOutputs(output)
	if register {
		txBuilder.WithCertificates(&lcommon.StakeRegistrationCertificate{
			StakeCredential: cred,
		})
	} else {
		txBuilder.WithCertificates(&lcommon.StakeDeregistrationCertificate{
			StakeCredential: cred,
		})
	}
	tx, err := txBuilder.Build()
	require.NoError(t, err)
	blockHash := make([]byte, 32)
	copy(blockHash, txId)
	require.NoError(t, db.SetTransactionMetadataOnly(
		tx,
		ocommon.NewPoint(slot, blockHash),
		0,
		map[int]uint64{0: deposit},
		nil,
	))
}

func stakeDeposits(
	t *testing.T,
	result any,
) olocalstatequery.StakeDelegDepositsResult {
	t.Helper()
	reply := result.([]any)[0]
	deposits, ok := reply.(olocalstatequery.StakeDelegDepositsResult)
	require.True(t, ok)
	return deposits
}

func stakeQueryCred(stakeKey []byte) olocalstatequery.StakeCredential {
	return olocalstatequery.StakeCredential{
		Tag:   0,
		Bytes: lcommon.NewBlake2b224(stakeKey),
	}
}

func TestQueryShelleyStakeDelegDeposits_PinnedPointAnswersAtThatPoint(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	cycled := repeatedBytes(28, 0x41)
	late := repeatedBytes(28, 0x42)
	seedStakeCertAt(t, db, cycled, true, 100, 2_000_000)
	seedStakeCertAt(t, db, cycled, false, 500, 2_000_000)
	seedStakeCertAt(t, db, cycled, true, 700, 3_000_000)
	seedStakeCertAt(t, db, late, true, 600, 2_000_000)
	creds := []olocalstatequery.StakeCredential{
		stakeQueryCred(cycled),
		stakeQueryCred(late),
	}

	for _, tc := range []struct {
		name string
		at   QueryPoint
		want olocalstatequery.StakeDelegDepositsResult
	}{
		{
			"registered, before the later account", QueryPoint{Slot: 300},
			olocalstatequery.StakeDelegDepositsResult{creds[0]: 2_000_000},
		},
		{
			"after the deregistration", QueryPoint{Slot: 550},
			olocalstatequery.StakeDelegDepositsResult{},
		},
		{
			"tip, after the re-registration", QueryPoint{},
			olocalstatequery.StakeDelegDepositsResult{
				creds[0]: 3_000_000,
				creds[1]: 2_000_000,
			},
		},
	} {
		result, err := ls.queryShelleyStakeDelegDeposits(creds, tc.at, nil)
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.want, stakeDeposits(t, result), tc.name)
	}
}

// TestQueryShelleyStakeDelegDeposits_PinnedPointSkipsLongHistory puts many
// events after the pinned point, so only the event in force at the point may
// decide the answer.
func TestQueryShelleyStakeDelegDeposits_PinnedPointSkipsLongHistory(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	key := repeatedBytes(28, 0x43)
	seedStakeCertAt(t, db, key, true, 100, 2_000_000)
	for i := range 18 {
		seedStakeCertAt(
			t, db, key, i%2 == 1, uint64(400+10*i), 5_000_000,
		)
	}
	cred := stakeQueryCred(key)

	result, err := ls.queryShelleyStakeDelegDeposits(
		[]olocalstatequery.StakeCredential{cred},
		QueryPoint{Slot: 300},
		nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		olocalstatequery.StakeDelegDepositsResult{cred: 2_000_000},
		stakeDeposits(t, result),
	)
}

// TestQuery_PinnedPointReachesStakeDelegDeposits sends GetStakeDelegDeposits
// through Query with a point on chain, so a dispatch that dropped the point
// would answer from the tip instead.
func TestQuery_PinnedPointReachesStakeDelegDeposits(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	key := repeatedBytes(28, 0x44)
	seedStakeCertAt(t, db, key, true, 100, 2_000_000)
	seedStakeCertAt(t, db, key, false, 500, 2_000_000)
	hash := repeatedBytes(32, 0x57)
	seedBlockAtSlot(t, ls, 300, hash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))
	cred := stakeQueryCred(key)

	result, err := ls.Query(
		shelleyLeafQuery(&olocalstatequery.ShelleyStakeDelegDepositsQuery{
			Creds: cbor.NewSetType(
				[]olocalstatequery.StakeCredential{cred}, true,
			),
		}),
		QueryPoint{Slot: 300, Hash: hash},
	)
	require.NoError(t, err)
	require.Equal(
		t,
		olocalstatequery.StakeDelegDepositsResult{cred: 2_000_000},
		stakeDeposits(t, result),
	)
}

// TestQueryShelleyStakeDelegDeposits_ImportedBaseline covers a credential
// imported from a ledger snapshot: it has no registration certificate, only
// the import baseline, until a later deregistration certificate.
func TestQueryShelleyStakeDelegDeposits_ImportedBaseline(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	key := repeatedBytes(28, 0x45)
	deposit := dbtypes.Uint64(2_000_000)
	require.NoError(t, db.Metadata().ImportAccount(&models.Account{
		StakingKey:    key,
		CredentialTag: 0,
		Active:        true,
		AddedSlot:     200,
		ImportDeposit: &deposit,
	}, nil))
	cred := stakeQueryCred(key)
	creds := []olocalstatequery.StakeCredential{cred}

	registered := olocalstatequery.StakeDelegDepositsResult{cred: 2_000_000}
	none := olocalstatequery.StakeDelegDepositsResult{}
	check := func(
		at QueryPoint,
		want olocalstatequery.StakeDelegDepositsResult,
		msg string,
	) {
		t.Helper()
		result, err := ls.queryShelleyStakeDelegDeposits(creds, at, nil)
		require.NoError(t, err, msg)
		require.Equal(t, want, stakeDeposits(t, result), msg)
	}
	check(QueryPoint{Slot: 100}, none, "before the import baseline")
	check(QueryPoint{Slot: 300}, registered, "after the import baseline")
	check(QueryPoint{}, registered, "tip, baseline only")

	seedStakeCertAt(t, db, key, false, 500, 2_000_000)
	check(QueryPoint{Slot: 400}, registered, "before the deregistration")
	check(QueryPoint{Slot: 600}, none, "after the deregistration")
	check(QueryPoint{}, none, "tip, after the deregistration")
}
