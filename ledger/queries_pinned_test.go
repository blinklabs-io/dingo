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
	require.NoError(t, db.CreateUtxo(t.Context(), nil, &models.Utxo{
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
		t.Context(), []ledger.Address{addr}, at, nil,
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
		t.Context(),
		nil,
		[]dbtypes.UtxoKey{{TxId: spentLater, OutputIdx: 0}},
		500,
	))
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		t.Context(),
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
		t.Context(), []ledger.Address{addr}, QueryPoint{Slot: 149_999}, nil,
	)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
	_, err = ls.queryShelleyUtxoByAddress(
		t.Context(), []ledger.Address{addr}, QueryPoint{Slot: 150_000}, nil,
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
		result, err := ls.queryShelleyAccountState(t.Context(), tc.at, nil)
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

	_, err := ls.queryShelleyAccountState(t.Context(), QueryPoint{Slot: 99}, nil)
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
			t.Context(), stakeSnapshotsQuery(), tc.at, nil,
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
		t.Context(), stakeSnapshotsQuery(), QueryPoint{Slot: 450}, nil,
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
		t.Context(), stakeSnapshotsQuery(idle), QueryPoint{Slot: 550}, nil,
	)
	require.NoError(t, err)
	require.Contains(t, stakeSnapshotsResult(t, pinned).PoolSnapshots, key)

	live, err := ls.queryShelleyStakeSnapshots(
		t.Context(), stakeSnapshotsQuery(idle), QueryPoint{}, nil,
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
		t.Context(),
		nil,
		[]dbtypes.UtxoKey{{TxId: spent, OutputIdx: 0}},
		600,
	))

	result, err := ls.Query(
		t.Context(),
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
		t.Context(),
		shelleyLeafQuery(&olocalstatequery.ShelleyAccountStateQuery{}),
		at,
	)
	require.NoError(t, err)
	require.Equal(t, int64(10), accountState(t, result).Treasury)

	result, err = ls.Query(t.Context(), shelleyLeafQuery(stakeSnapshotsQuery()), at)
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

	err := ls.VerifyPointQueryable(t.Context(), nil, QueryPoint{Slot: 450, Hash: pruned})
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
	require.NoError(
		t,
		ls.VerifyPointQueryable(t.Context(), nil, QueryPoint{Slot: 550, Hash: retained}),
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

	pinned, err := ls.queryShelleyStakePools(context.Background(), QueryPoint{Slot: 300}, nil)
	require.NoError(t, err)
	require.Equal(
		t, []ledger.PoolId{poolID(early)}, stakePoolIDs(t, pinned),
		"the pool registered at slot 600 did not exist at slot 300",
	)

	live, err := ls.queryShelleyStakePools(context.Background(), QueryPoint{}, nil)
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
	_, err := ls.queryShelleyStakePools(context.Background(), QueryPoint{Slot: 5_000}, nil)
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
		context.Background(),
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
	cred := stakeKeyCredential(stakeKey)
	var cert lcommon.Certificate = &lcommon.StakeDeregistrationCertificate{
		StakeCredential: cred,
	}
	if register {
		cert = &lcommon.StakeRegistrationCertificate{StakeCredential: cred}
	}
	seedAccountTxAt(t, db, stakeKey, slot, map[int]uint64{0: deposit},
		func(tx *mockledger.MockTransaction) {
			tx.WithCertificates(cert)
		})
}

func stakeKeyCredential(stakeKey []byte) lcommon.Credential {
	return lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(stakeKey),
	}
}

// seedAccountTxAt writes one valid transaction at slot whose ID derives from
// stakeKey and slot, so each (stakeKey, slot) pair is a distinct transaction.
func seedAccountTxAt(
	t *testing.T,
	db *database.Database,
	stakeKey []byte,
	slot uint64,
	deposits map[int]uint64,
	build func(tx *mockledger.MockTransaction),
) {
	t.Helper()
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
	build(txBuilder)
	tx, err := txBuilder.Build()
	require.NoError(t, err)
	blockHash := make([]byte, 32)
	copy(blockHash, txId)
	require.NoError(t, db.SetTransactionMetadataOnly(
		context.Background(),
		tx,
		ocommon.NewPoint(slot, blockHash),
		0,
		deposits,
		nil,
		9,
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
		result, err := ls.queryShelleyStakeDelegDeposits(context.Background(), creds, tc.at, nil)
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
		context.Background(),
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
		context.Background(),
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
		result, err := ls.queryShelleyStakeDelegDeposits(context.Background(), creds, at, nil)
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

// TestQueryShelleyStakeDelegDeposits_SameSlotDeregistrationSupersedesBaseline
// covers a deregistration certificate in the import baseline's own slot,
// which DATABASE.md's account_import_baseline rule says supersedes it.
func TestQueryShelleyStakeDelegDeposits_SameSlotDeregistrationSupersedesBaseline(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	key := repeatedBytes(28, 0x46)
	deposit := dbtypes.Uint64(2_000_000)
	require.NoError(t, db.Metadata().ImportAccount(&models.Account{
		StakingKey:    key,
		CredentialTag: 0,
		Active:        true,
		AddedSlot:     200,
		ImportDeposit: &deposit,
	}, nil))
	seedStakeCertAt(t, db, key, false, 200, 2_000_000)
	creds := []olocalstatequery.StakeCredential{stakeQueryCred(key)}

	for _, at := range []QueryPoint{{Slot: 200}, {Slot: 300}, {}} {
		result, err := ls.queryShelleyStakeDelegDeposits(context.Background(), creds, at, nil)
		require.NoError(t, err)
		require.Empty(t, stakeDeposits(t, result), "slot %d", at.Slot)
	}
}

func delegationsAndRewards(
	t *testing.T,
	result any,
) (map[olocalstatequery.StakeCredential]ledger.Blake2b224, map[olocalstatequery.StakeCredential]uint64) {
	t.Helper()
	reply := result.([]any)[0].([]any)
	delegations, ok := reply[0].(map[olocalstatequery.StakeCredential]ledger.Blake2b224)
	require.True(t, ok)
	rewards, ok := reply[1].(map[olocalstatequery.StakeCredential]uint64)
	require.True(t, ok)
	return delegations, rewards
}

func seedPoolDelegationAt(
	t *testing.T,
	db *database.Database,
	stakeKey []byte,
	pool []byte,
	slot uint64,
) {
	t.Helper()
	seedAccountTxAt(t, db, stakeKey, slot, nil,
		func(tx *mockledger.MockTransaction) {
			tx.WithCertificates(&lcommon.StakeDelegationCertificate{
				StakeCredential: new(stakeKeyCredential(stakeKey)),
				PoolKeyHash:     lcommon.NewBlake2b224(pool),
			})
		})
}

func seedVoteDelegationAt(
	t *testing.T,
	db *database.Database,
	stakeKey []byte,
	drep lcommon.Drep,
	slot uint64,
) {
	t.Helper()
	seedAccountTxAt(t, db, stakeKey, slot, nil,
		func(tx *mockledger.MockTransaction) {
			tx.WithCertificates(&lcommon.VoteDelegationCertificate{
				StakeCredential: stakeKeyCredential(stakeKey),
				Drep:            drep,
			})
		})
}

func TestQueryShelleyFilteredDelegationAndRewardAccounts_PinnedPointAnswersAtThatPoint(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	moved := repeatedBytes(28, 0x51)
	left := repeatedBytes(28, 0x52)
	firstPool := repeatedBytes(28, 0xA1)
	secondPool := repeatedBytes(28, 0xA2)
	seedStakeCertAt(t, db, moved, true, 100, 2_000_000)
	seedStakeCertAt(t, db, left, true, 100, 2_000_000)
	seedPoolDelegationAt(t, db, moved, firstPool, 200)
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(), 0, moved, 5_000_000, 300, repeatedBytes(32, 0xC1), nil,
	))
	seedPoolDelegationAt(t, db, moved, secondPool, 400)
	seedStakeCertAt(t, db, left, false, 450, 2_000_000)
	require.NoError(t, db.Metadata().ApplyAccountRewardWithdrawal(
		0, moved, 5_000_000, 500, repeatedBytes(32, 0xC3), nil,
	))
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(), 0, moved, 1_000_000, 600, repeatedBytes(32, 0xC2), nil,
	))
	movedCred, leftCred := stakeQueryCred(moved), stakeQueryCred(left)
	creds := []olocalstatequery.StakeCredential{movedCred, leftCred}
	type want = struct {
		delegations map[olocalstatequery.StakeCredential]ledger.Blake2b224
		rewards     map[olocalstatequery.StakeCredential]uint64
	}
	tip := want{
		delegations: map[olocalstatequery.StakeCredential]ledger.Blake2b224{
			movedCred: lcommon.NewBlake2b224(secondPool),
		},
		rewards: map[olocalstatequery.StakeCredential]uint64{
			movedCred: 1_000_000,
		},
	}

	for _, tc := range []struct {
		name string
		at   QueryPoint
		want want
	}{
		{
			name: "registered, not delegated",
			at:   QueryPoint{Slot: 150},
			want: want{
				delegations: map[olocalstatequery.StakeCredential]ledger.Blake2b224{},
				rewards: map[olocalstatequery.StakeCredential]uint64{
					movedCred: 0,
					leftCred:  0,
				},
			},
		},
		{
			name: "first pool, credited",
			at:   QueryPoint{Slot: 350},
			want: want{
				delegations: map[olocalstatequery.StakeCredential]ledger.Blake2b224{
					movedCred: lcommon.NewBlake2b224(firstPool),
				},
				rewards: map[olocalstatequery.StakeCredential]uint64{
					movedCred: 5_000_000,
					leftCred:  0,
				},
			},
		},
		{
			name: "second pool, other deregistered",
			at:   QueryPoint{Slot: 450},
			want: want{
				delegations: map[olocalstatequery.StakeCredential]ledger.Blake2b224{
					movedCred: lcommon.NewBlake2b224(secondPool),
				},
				rewards: map[olocalstatequery.StakeCredential]uint64{
					movedCred: 5_000_000,
				},
			},
		},
		{
			name: "withdrawn",
			at:   QueryPoint{Slot: 550},
			want: want{
				delegations: map[olocalstatequery.StakeCredential]ledger.Blake2b224{
					movedCred: lcommon.NewBlake2b224(secondPool),
				},
				rewards: map[olocalstatequery.StakeCredential]uint64{
					movedCred: 0,
				},
			},
		},
		{name: "pinned at the tip", at: QueryPoint{Slot: 700}, want: tip},
		{name: "unpinned", at: QueryPoint{}, want: tip},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ls.queryShelleyFilteredDelegationAndRewardAccounts(
				context.Background(),
				creds, tc.at, nil,
			)
			require.NoError(t, err)
			delegations, rewards := delegationsAndRewards(t, result)
			require.Equal(t, tc.want.delegations, delegations)
			require.Equal(t, tc.want.rewards, rewards)
		})
	}
}

func voteDelegatees(
	t *testing.T,
	result any,
) olocalstatequery.FilteredVoteDelegateesResult {
	t.Helper()
	delegatees, ok := result.([]any)[0].(olocalstatequery.FilteredVoteDelegateesResult)
	require.True(t, ok)
	return delegatees
}

func TestQueryShelleyFilteredVoteDelegatees_PinnedPointAnswersAtThatPoint(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	moved := repeatedBytes(28, 0x53)
	left := repeatedBytes(28, 0x54)
	drep := lcommon.Drep{
		Type:       lcommon.DrepTypeAddrKeyHash,
		Credential: repeatedBytes(28, 0xD1),
	}
	abstain := lcommon.Drep{Type: lcommon.DrepTypeAbstain}
	seedStakeCertAt(t, db, moved, true, 100, 2_000_000)
	seedStakeCertAt(t, db, left, true, 100, 2_000_000)
	seedVoteDelegationAt(t, db, moved, drep, 200)
	seedVoteDelegationAt(t, db, left, drep, 200)
	seedStakeCertAt(t, db, left, false, 300, 2_000_000)
	seedVoteDelegationAt(t, db, moved, abstain, 400)
	creds := []lcommon.Credential{
		stakeKeyCredential(moved),
		stakeKeyCredential(left),
	}
	movedCred, leftCred := stakeQueryCred(moved), stakeQueryCred(left)
	tip := olocalstatequery.FilteredVoteDelegateesResult{movedCred: abstain}

	for _, tc := range []struct {
		name string
		at   QueryPoint
		want olocalstatequery.FilteredVoteDelegateesResult
	}{
		{
			name: "registered, not delegated",
			at:   QueryPoint{Slot: 150},
			want: olocalstatequery.FilteredVoteDelegateesResult{},
		},
		{
			name: "both delegated",
			at:   QueryPoint{Slot: 250},
			want: olocalstatequery.FilteredVoteDelegateesResult{
				movedCred: drep,
				leftCred:  drep,
			},
		},
		{
			name: "other deregistered",
			at:   QueryPoint{Slot: 350},
			want: olocalstatequery.FilteredVoteDelegateesResult{
				movedCred: drep,
			},
		},
		{name: "pinned at the tip", at: QueryPoint{Slot: 500}, want: tip},
		{name: "unpinned", at: QueryPoint{}, want: tip},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ls.queryShelleyFilteredVoteDelegatees(
				context.Background(),
				creds, tc.at, nil,
			)
			require.NoError(t, err)
			require.Equal(t, tc.want, voteDelegatees(t, result))
		})
	}
}

// TestQueryShelleyFilteredVoteDelegatees_PinnedPointKeepsPV10Clear covers a
// write no certificate records: the PV10 HARDFORK rule clearing a delegation
// to an unregistered DRep. The certificate history still names the DRep, so a
// point after the clear must read the account row it left.
func TestQueryShelleyFilteredVoteDelegatees_PinnedPointKeepsPV10Clear(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	key := repeatedBytes(28, 0x55)
	drep := lcommon.Drep{
		Type:       lcommon.DrepTypeAddrKeyHash,
		Credential: repeatedBytes(28, 0xD2),
	}
	seedStakeCertAt(t, db, key, true, 100, 2_000_000)
	seedVoteDelegationAt(t, db, key, drep, 200)
	cleared, err := db.ClearDanglingDRepDelegations(context.Background(), 300, nil)
	require.NoError(t, err)
	require.Equal(t, 1, cleared)
	seedPoolDelegationAt(t, db, key, repeatedBytes(28, 0xA5), 400)
	creds := []lcommon.Credential{stakeKeyCredential(key)}
	cred := stakeQueryCred(key)

	for _, tc := range []struct {
		name string
		at   QueryPoint
		want olocalstatequery.FilteredVoteDelegateesResult
	}{
		{
			name: "before the clear",
			at:   QueryPoint{Slot: 250},
			want: olocalstatequery.FilteredVoteDelegateesResult{cred: drep},
		},
		{
			name: "after the clear",
			at:   QueryPoint{Slot: 350},
			want: olocalstatequery.FilteredVoteDelegateesResult{},
		},
		{
			name: "unpinned",
			at:   QueryPoint{},
			want: olocalstatequery.FilteredVoteDelegateesResult{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ls.queryShelleyFilteredVoteDelegatees(
				context.Background(),
				creds, tc.at, nil,
			)
			require.NoError(t, err)
			require.Equal(t, tc.want, voteDelegatees(t, result))
		})
	}
}

// TestQueryShelleyFilteredDelegations_PinnedPointImportedBaseline covers an
// account imported from a ledger snapshot, whose pool and DRep come from the
// import baseline until a later certificate replaces them.
func TestQueryShelleyFilteredDelegations_PinnedPointImportedBaseline(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	key := repeatedBytes(28, 0x56)
	importedPool := repeatedBytes(28, 0xA3)
	laterPool := repeatedBytes(28, 0xA4)
	importedDrep := repeatedBytes(28, 0xD3)
	deposit := dbtypes.Uint64(2_000_000)
	seedStakeCertAt(t, db, key, true, 200, 2_000_000)
	seedPoolDelegationAt(t, db, key, repeatedBytes(28, 0xA2), 200)
	seedVoteDelegationAt(t, db, key, lcommon.Drep{
		Type:       lcommon.DrepTypeAddrKeyHash,
		Credential: repeatedBytes(28, 0xD2),
	}, 200)
	require.NoError(t, db.Metadata().ImportAccount(&models.Account{
		StakingKey:    key,
		CredentialTag: 0,
		Pool:          importedPool,
		Drep:          importedDrep,
		DrepType:      models.DrepTypeAddrKeyHash,
		Reward:        3_000_000,
		Active:        true,
		AddedSlot:     200,
		ImportDeposit: &deposit,
	}, nil))
	seedPoolDelegationAt(t, db, key, laterPool, 400)
	abstain := lcommon.Drep{Type: lcommon.DrepTypeAbstain}
	seedVoteDelegationAt(t, db, key, abstain, 410)
	cred := stakeQueryCred(key)

	result, err := ls.queryShelleyFilteredDelegationAndRewardAccounts(
		context.Background(),
		[]olocalstatequery.StakeCredential{cred}, QueryPoint{Slot: 300}, nil,
	)
	require.NoError(t, err)
	delegations, rewards := delegationsAndRewards(t, result)
	require.Equal(t, map[olocalstatequery.StakeCredential]ledger.Blake2b224{
		cred: lcommon.NewBlake2b224(importedPool),
	}, delegations)
	require.Equal(t, map[olocalstatequery.StakeCredential]uint64{
		cred: 3_000_000,
	}, rewards)

	result, err = ls.queryShelleyFilteredVoteDelegatees(
		context.Background(),
		[]lcommon.Credential{stakeKeyCredential(key)}, QueryPoint{Slot: 300}, nil,
	)
	require.NoError(t, err)
	require.Equal(t, olocalstatequery.FilteredVoteDelegateesResult{
		cred: {Type: lcommon.DrepTypeAddrKeyHash, Credential: importedDrep},
	}, voteDelegatees(t, result))

	result, err = ls.queryShelleyFilteredDelegationAndRewardAccounts(
		context.Background(),
		[]olocalstatequery.StakeCredential{cred}, QueryPoint{Slot: 500}, nil,
	)
	require.NoError(t, err)
	delegations, _ = delegationsAndRewards(t, result)
	require.Equal(t, map[olocalstatequery.StakeCredential]ledger.Blake2b224{
		cred: lcommon.NewBlake2b224(laterPool),
	}, delegations)
}

func TestQuery_PinnedPointReachesFilteredDelegationsAndVoteDelegatees(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	key := repeatedBytes(28, 0x57)
	pool := repeatedBytes(28, 0xA5)
	drep := lcommon.Drep{
		Type:       lcommon.DrepTypeAddrKeyHash,
		Credential: repeatedBytes(28, 0xD4),
	}
	seedStakeCertAt(t, db, key, true, 100, 2_000_000)
	seedPoolDelegationAt(t, db, key, pool, 500)
	seedVoteDelegationAt(t, db, key, drep, 510)
	hash := repeatedBytes(32, 0x58)
	seedBlockAtSlot(t, ls, 300, hash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0C)),
	}, nil))
	cred := stakeQueryCred(key)
	at := QueryPoint{Slot: 300, Hash: hash}

	result, err := ls.Query(
		context.Background(),
		shelleyLeafQuery(
			&olocalstatequery.ShelleyFilteredDelegationAndRewardAccountsQuery{
				Creds: cbor.NewSetType(
					[]olocalstatequery.StakeCredential{cred}, true,
				),
			},
		),
		at,
	)
	require.NoError(t, err)
	delegations, rewards := delegationsAndRewards(t, result)
	require.Empty(t, delegations)
	require.Equal(t, map[olocalstatequery.StakeCredential]uint64{cred: 0}, rewards)

	result, err = ls.Query(
		context.Background(),
		shelleyLeafQuery(&olocalstatequery.ShelleyFilteredVoteDelegateesQuery{
			Credentials: cbor.NewSetType(
				[]lcommon.Credential{stakeKeyCredential(key)}, true,
			),
		}),
		at,
	)
	require.NoError(t, err)
	require.Empty(t, voteDelegatees(t, result))
}

// TestVerifyPointQueryable_RejectsPointBelowMithrilTrustBoundary covers a point
// below the latest Mithril import's snapshot slot, whose history the import
// does not hold, and the snapshot slot itself, which it does.
func TestVerifyPointQueryable_RejectsPointBelowMithrilTrustBoundary(
	t *testing.T,
) {
	t.Parallel()

	ls := newStakeSnapshotsLedger(t, repeatedBytes(28, 0x27), 10, 10)
	require.NoError(t, ls.db.Metadata().SetNetworkState(0, 0, 0, nil))
	below := QueryPoint{Slot: 550, Hash: repeatedBytes(32, 0x63)}
	boundary := QueryPoint{Slot: 560, Hash: repeatedBytes(32, 0x64)}
	seedBlockAtSlot(t, ls, below.Slot, below.Hash)
	seedBlockAtSlot(t, ls, boundary.Slot, boundary.Hash)
	require.NoError(t, ls.VerifyPointQueryable(context.Background(), nil, below))

	ls.mithrilLedgerSlot = boundary.Slot
	require.ErrorIs(
		t,
		ls.VerifyPointQueryable(context.Background(), nil, below),
		ErrHistoricalStateUnavailable,
	)
	require.NoError(t, ls.VerifyPointQueryable(context.Background(), nil, boundary))
}

func seedProposalAt(
	t *testing.T,
	db *database.Database,
	marker byte,
	addedSlot uint64,
) *models.GovernanceProposal {
	t.Helper()
	govAction, err := cbor.Encode([]any{uint64(lcommon.GovActionTypeInfo)})
	require.NoError(t, err)
	proposal := &models.GovernanceProposal{
		TxHash:        repeatedBytes(32, marker),
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ExpiresEpoch:  10,
		AnchorURL:     "https://example.com/proposal.json",
		AnchorHash:    repeatedBytes(32, marker+1),
		Deposit:       100_000_000,
		ReturnAddress: append([]byte{0xe0}, repeatedBytes(28, marker+2)...),
		GovActionCbor: govAction,
		AddedSlot:     addedSlot,
	}
	require.NoError(t, db.SetGovernanceProposal(t.Context(), proposal, nil))
	return proposal
}

// proposalsAt returns the proposals GetProposals reports at, keyed by the
// first byte of their transaction hash, with each one's DRep votes.
func proposalsAt(
	t *testing.T,
	ls *LedgerState,
	at QueryPoint,
) map[byte]map[olocalstatequery.StakeCredential]lcommon.Vote {
	t.Helper()
	result, err := ls.queryShelleyGetProposals(t.Context(), nil, at, nil)
	require.NoError(t, err)
	proposals, ok := result.([]any)[0].(olocalstatequery.ProposalsResult)
	require.True(t, ok)
	ret := make(map[byte]map[olocalstatequery.StakeCredential]lcommon.Vote)
	for _, proposal := range proposals {
		ret[proposal.Id.TransactionId[0]] = proposal.DRepVotes
	}
	return ret
}

func TestQueryShelleyGetProposals_PinnedPointAnswersAtThatPoint(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	enacted := seedProposalAt(t, db, 0x71, 100)
	dropped := seedProposalAt(t, db, 0x81, 100)
	seedProposalAt(t, db, 0x91, 300)
	voter := repeatedBytes(28, 0x75)
	require.NoError(t, db.SetGovernanceVote(t.Context(), &models.GovernanceVote{
		ProposalID:      enacted.ID,
		VoterType:       models.VoterTypeDRep,
		VoterCredential: voter,
		Vote:            models.VoteYes,
		AddedSlot:       150,
	}, nil))
	require.NoError(t, db.SetGovernanceVote(t.Context(), &models.GovernanceVote{
		ProposalID:      enacted.ID,
		VoterType:       models.VoterTypeDRep,
		VoterCredential: voter,
		Vote:            models.VoteNo,
		AddedSlot:       150,
		VoteUpdatedSlot: new(uint64(350)),
	}, nil))
	expiredEpoch, expiredSlot := uint64(3), uint64(300)
	droppedEpoch, droppedSlot := uint64(4), uint64(400)
	dropped.ExpiredEpoch, dropped.ExpiredSlot = &expiredEpoch, &expiredSlot
	dropped.DroppedEpoch, dropped.DroppedSlot = &droppedEpoch, &droppedSlot
	require.NoError(t, db.SetGovernanceProposal(t.Context(), dropped, nil))
	enactedEpoch, enactedSlot := uint64(5), uint64(500)
	enacted.EnactedEpoch, enacted.EnactedSlot = &enactedEpoch, &enactedSlot
	require.NoError(t, db.SetGovernanceProposal(t.Context(), enacted, nil))

	cred := olocalstatequery.StakeCredential{
		Tag:   0,
		Bytes: lcommon.NewBlake2b224(voter),
	}
	none := map[olocalstatequery.StakeCredential]lcommon.Vote{}
	yes := map[olocalstatequery.StakeCredential]lcommon.Vote{
		cred: lcommon.Vote(models.VoteYes),
	}
	no := map[olocalstatequery.StakeCredential]lcommon.Vote{
		cred: lcommon.Vote(models.VoteNo),
	}
	tip := map[byte]map[olocalstatequery.StakeCredential]lcommon.Vote{
		0x91: none,
	}
	for _, tc := range []struct {
		name string
		at   QueryPoint
		want map[byte]map[olocalstatequery.StakeCredential]lcommon.Vote
	}{
		{
			name: "before any vote",
			at:   QueryPoint{Slot: 120},
			want: map[byte]map[olocalstatequery.StakeCredential]lcommon.Vote{
				0x71: none, 0x81: none,
			},
		},
		{
			name: "first vote",
			at:   QueryPoint{Slot: 200},
			want: map[byte]map[olocalstatequery.StakeCredential]lcommon.Vote{
				0x71: yes, 0x81: none,
			},
		},
		{
			name: "replaced vote, third proposal, expired not dropped",
			at:   QueryPoint{Slot: 380},
			want: map[byte]map[olocalstatequery.StakeCredential]lcommon.Vote{
				0x71: no, 0x81: none, 0x91: none,
			},
		},
		{
			name: "dropped",
			at:   QueryPoint{Slot: 450},
			want: map[byte]map[olocalstatequery.StakeCredential]lcommon.Vote{
				0x71: no, 0x91: none,
			},
		},
		{name: "enacted", at: QueryPoint{Slot: 600}, want: tip},
		{name: "unpinned", at: QueryPoint{}, want: tip},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, proposalsAt(t, ls, tc.at))
		})
	}
}

func drepAnchorAt(url string, marker byte) *lcommon.GovAnchor {
	anchor := &lcommon.GovAnchor{Url: url}
	copy(anchor.DataHash[:], repeatedBytes(32, marker))
	return anchor
}

// seedDRepAt writes one DRep certificate at slot and, for a registration or
// update, the activity renewal the ledger applies with it.
func seedDRepAt(
	t *testing.T,
	db *database.Database,
	drep []byte,
	cert lcommon.Certificate,
	slot uint64,
	epoch uint64,
	deposit uint64,
) {
	t.Helper()
	seedAccountTxAt(t, db, drep, slot, map[int]uint64{0: deposit},
		func(tx *mockledger.MockTransaction) {
			tx.WithCertificates(cert)
		})
	switch cert.(type) {
	case *lcommon.RegistrationDrepCertificate, *lcommon.UpdateDrepCertificate:
		require.NoError(t, db.UpdateDRepActivity(t.Context(), 0, drep, epoch, 20, slot, nil))
	}
}

func drepStates(
	t *testing.T,
	result any,
) olocalstatequery.DRepStateResult {
	t.Helper()
	states, ok := result.([]any)[0].(olocalstatequery.DRepStateResult)
	require.True(t, ok)
	return states
}

func TestQueryShelleyDRepState_PinnedPointAnswersAtThatPoint(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	retiring := repeatedBytes(28, 0xD5)
	late := repeatedBytes(28, 0xD6)
	retiringCred := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(retiring),
	}
	lateCred := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(late),
	}
	firstAnchor := drepAnchorAt("https://example.com/first.json", 0xA6)
	secondAnchor := drepAnchorAt("https://example.com/second.json", 0xA7)
	seedDRepAt(t, db, retiring, &lcommon.RegistrationDrepCertificate{
		DrepCredential: retiringCred,
		Amount:         500_000_000,
		Anchor:         firstAnchor,
	}, 100, 1, 500_000_000)
	early := repeatedBytes(28, 0x5A)
	later := repeatedBytes(28, 0x5B)
	moved := repeatedBytes(28, 0x5C)
	seedStakeCertAt(t, db, early, true, 110, 2_000_000)
	seedStakeCertAt(t, db, later, true, 110, 2_000_000)
	seedStakeCertAt(t, db, moved, true, 110, 2_000_000)
	drep := lcommon.Drep{
		Type:       lcommon.DrepTypeAddrKeyHash,
		Credential: retiring,
	}
	seedVoteDelegationAt(t, db, early, drep, 200)
	seedVoteDelegationAt(t, db, moved, drep, 220)
	seedDRepAt(t, db, retiring, &lcommon.UpdateDrepCertificate{
		DrepCredential: retiringCred,
		Anchor:         secondAnchor,
	}, 300, 3, 0)
	seedDRepAt(t, db, late, &lcommon.RegistrationDrepCertificate{
		DrepCredential: lateCred,
		Amount:         500_000_000,
	}, 350, 3, 500_000_000)
	require.NoError(t, db.UpdateDRepActivity(t.Context(), 0, retiring, 4, 20, 400, nil))
	seedVoteDelegationAt(t, db, later, drep, 450)
	seedVoteDelegationAt(
		t, db, moved, lcommon.Drep{Type: lcommon.DrepTypeAbstain}, 550,
	)
	seedDRepAt(t, db, retiring, &lcommon.DeregistrationDrepCertificate{
		DrepCredential: retiringCred,
		Amount:         500_000_000,
	}, 600, 6, 500_000_000)
	// A vote renews the second DRep's expiry without touching its row.
	require.NoError(t, db.UpdateDRepActivity(t.Context(), 0, late, 6, 20, 650, nil))

	retiringKey := olocalstatequery.StakeCredential{
		Tag:   0,
		Bytes: lcommon.NewBlake2b224(retiring),
	}
	lateKey := olocalstatequery.StakeCredential{
		Tag:   0,
		Bytes: lcommon.NewBlake2b224(late),
	}
	entry := func(
		expiry uint64,
		anchor *lcommon.GovAnchor,
		delegators ...[]byte,
	) olocalstatequery.DRepStateEntry {
		e := olocalstatequery.DRepStateEntry{
			Expiry:  expiry,
			Anchor:  anchor,
			Deposit: 500_000_000,
		}
		for _, key := range delegators {
			e.Delegators = append(e.Delegators, stakeQueryCred(key))
		}
		return e
	}
	lateEntry := entry(23, nil)
	tip := olocalstatequery.DRepStateResult{lateKey: entry(26, nil)}
	for _, tc := range []struct {
		name  string
		creds []lcommon.Credential
		at    QueryPoint
		want  olocalstatequery.DRepStateResult
	}{
		{
			name: "registered",
			at:   QueryPoint{Slot: 150},
			want: olocalstatequery.DRepStateResult{
				retiringKey: entry(21, firstAnchor),
			},
		},
		{
			name: "first delegator",
			at:   QueryPoint{Slot: 250},
			want: olocalstatequery.DRepStateResult{
				retiringKey: entry(21, firstAnchor, early, moved),
			},
		},
		{
			name: "updated, second DRep",
			at:   QueryPoint{Slot: 380},
			want: olocalstatequery.DRepStateResult{
				retiringKey: entry(23, secondAnchor, early, moved),
				lateKey:     lateEntry,
			},
		},
		{
			name:  "restricted to one DRep",
			creds: []lcommon.Credential{retiringCred},
			at:    QueryPoint{Slot: 380},
			want: olocalstatequery.DRepStateResult{
				retiringKey: entry(23, secondAnchor, early, moved),
			},
		},
		{
			name:  "restricted, named twice",
			creds: []lcommon.Credential{retiringCred, retiringCred},
			at:    QueryPoint{Slot: 380},
			want: olocalstatequery.DRepStateResult{
				retiringKey: entry(23, secondAnchor, early, moved),
			},
		},
		{
			name:  "restricted to a DRep not yet registered",
			creds: []lcommon.Credential{lateCred},
			at:    QueryPoint{Slot: 300},
			want:  olocalstatequery.DRepStateResult{},
		},
		{
			name: "renewed by a vote, second delegator",
			at:   QueryPoint{Slot: 500},
			want: olocalstatequery.DRepStateResult{
				retiringKey: entry(24, secondAnchor, early, later, moved),
				lateKey:     lateEntry,
			},
		},
		{name: "deregistered", at: QueryPoint{Slot: 700}, want: tip},
		{name: "unpinned", at: QueryPoint{}, want: tip},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ls.queryShelleyDRepState(t.Context(), tc.creds, tc.at, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, drepStates(t, result))
		})
	}
}

// TestRestoreDrepStateAtSlot_RestoresExpiryFromHistory covers rollback of a
// DRep's expiry: a vote renews it without a certificate or a drep.added_slot
// change, and a registration's expiry used to be reset to 0 on rollback.
func TestRestoreDrepStateAtSlot_RestoresExpiryFromHistory(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		rollbackTo uint64
		want       uint64
	}{
		{name: "past a vote only", rollbackTo: 350, want: 23},
		{name: "past an update certificate", rollbackTo: 250, want: 21},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			key := repeatedBytes(28, 0xD7)
			cred := lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.NewBlake2b224(key),
			}
			seedDRepAt(t, db, key, &lcommon.RegistrationDrepCertificate{
				DrepCredential: cred,
				Amount:         500_000_000,
			}, 100, 1, 500_000_000)
			seedDRepAt(t, db, key, &lcommon.UpdateDrepCertificate{
				DrepCredential: cred,
			}, 300, 3, 0)
			require.NoError(t, db.UpdateDRepActivity(t.Context(), 0, key, 4, 20, 400, nil))

			require.NoError(t, db.RestoreDrepStateAtSlot(t.Context(), tc.rollbackTo, nil))
			drep, err := db.GetDrepByCredential(t.Context(), 0, key, true, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, drep.ExpiryEpoch)
		})
	}
}

func TestQuery_PinnedPointReachesDRepStateAndProposals(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	key := repeatedBytes(28, 0xD8)
	seedDRepAt(t, db, key, &lcommon.RegistrationDrepCertificate{
		DrepCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.NewBlake2b224(key),
		},
		Amount: 500_000_000,
	}, 500, 5, 500_000_000)
	seedProposalAt(t, db, 0xA1, 500)
	hash := repeatedBytes(32, 0x59)
	seedBlockAtSlot(t, ls, 300, hash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0E)),
	}, nil))
	at := QueryPoint{Slot: 300, Hash: hash}

	result, err := ls.Query(
		t.Context(),
		shelleyLeafQuery(&olocalstatequery.ShelleyDRepStateQuery{
			Credentials: cbor.NewSetType([]lcommon.Credential{}, true),
		}),
		at,
	)
	require.NoError(t, err)
	require.Empty(t, drepStates(t, result))

	result, err = ls.Query(
		t.Context(),
		shelleyLeafQuery(&olocalstatequery.ShelleyGetProposalsQuery{
			ActionIds: cbor.NewSetType([]lcommon.GovActionId{}, true),
		}),
		at,
	)
	require.NoError(t, err)
	proposals, ok := result.([]any)[0].(olocalstatequery.ProposalsResult)
	require.True(t, ok)
	require.Empty(t, proposals)
}
