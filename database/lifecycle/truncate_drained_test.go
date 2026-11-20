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

package lifecycle_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// praosChain creates n Praos blocks (IDs 1..n, slot id*10) with the tip at
// the last one, so a truncate to one of them reads its nonce.
func praosChain(t *testing.T, n uint64) (*database.Database, []models.Block) {
	t.Helper()
	db := newTestDB(t)
	blocks := make([]models.Block, 0, n)
	for id := uint64(1); id <= n; id++ {
		block := testBlock(id, byte(id))
		block.Type = babbage.BlockTypeBabbage
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}
	last := blocks[len(blocks)-1]
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
		BlockNumber: last.Number,
	}, nil))
	return db, blocks
}

// latchWithoutCache writes the drain latch the way an earlier run of the
// node did, leaving db's cached mask as a freshly opened database has it.
func latchWithoutCache(
	t *testing.T,
	db *database.Database,
	families ...nodesettings.Family,
) {
	t.Helper()
	var mask nodesettings.FamilyMask
	for _, family := range families {
		mask = mask.With(family)
	}
	require.NoError(t, db.Metadata().SetNodeSettingsGates(
		t.Context(),
		nodesettings.Values{
			nodesettings.LedgerStateDrainedGateName: nodesettings.
				EncodeFamilyMask(mask),
		},
		0, 0,
	))
	require.False(t, db.FamilyDrained(families[0]), "the cache must be cold")
}

func nonceAt(id uint64, checkpoint bool) badgerutxo.Nonce {
	return badgerutxo.Nonce{
		Point:      blockPoint(id),
		Nonce:      bytes.Repeat([]byte{0xa0 + byte(id)}, 32),
		Checkpoint: checkpoint,
	}
}

func TestTruncateOfADrainedNoncesFamilyReadsTheSet(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		nonces  []badgerutxo.Nonce
		wantErr string
	}{
		{
			name: "the set holds the target nonce",
			nonces: []badgerutxo.Nonce{
				nonceAt(1, true), nonceAt(2, false), nonceAt(3, false),
				nonceAt(4, false), nonceAt(5, false),
			},
		},
		{
			name: "the target nonce is pruned and a checkpoint survives",
			nonces: []badgerutxo.Nonce{
				nonceAt(1, true), nonceAt(4, false), nonceAt(5, false),
			},
		},
		{
			name:    "no target nonce and no checkpoint",
			nonces:  []badgerutxo.Nonce{nonceAt(4, false), nonceAt(5, false)},
			wantErr: "no earlier checkpoint exists",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, blocks := praosChain(t, 5)
			store := openUtxoSetStore(t, t.TempDir())
			applyBlocks(t, store, 1, 5)
			require.NoError(t, store.LoadNonces(tc.nonces))
			latchWithoutCache(t, db, nodesettings.FamilyNonces)

			_, err := lifecycle.Truncate(
				context.Background(), db, blocks[2], 0, false, 0,
				lifecycle.WithTruncateUtxoSet(
					dblifecycle.TruncateUtxoSet{Store: store},
				),
			)

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			tip, err := db.GetTip(nil)
			require.NoError(t, err)
			require.Equal(t, blocks[2].Slot, tip.Point.Slot)
			requireSetAt(t, store, 3)
			// The set's rollback ran before the read: nothing past the
			// target is left to serve a later start.
			rows, err := store.NoncesInSlotRange(0, 1000)
			require.NoError(t, err)
			for _, row := range rows {
				require.LessOrEqual(t, row.Point.Slot, blocks[2].Slot)
			}
		})
	}
}

// fakeSet is a TruncateUtxoSet that serves no family's reads.
type fakeSet struct{}

func (fakeSet) Tip() (ocommon.Point, bool, error) {
	return ocommon.Point{}, false, nil
}

func (fakeSet) JournalFloor() (ocommon.Point, bool, error) {
	return ocommon.Point{}, false, nil
}

func (fakeSet) CanRollbackTo(ocommon.Point) (bool, error) { return true, nil }

func (fakeSet) RollbackTo(ocommon.Point) error { return nil }

func TestTruncateRefusesADrainedFamilyTheSetCannotServe(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		family   nodesettings.Family
		options  []lifecycle.TruncateOption
		wantText string
	}{
		{
			name:     "no set opened",
			family:   nodesettings.FamilyNonces,
			wantText: "no live UTxO set was opened",
		},
		{
			name:   "a set that serves no nonces",
			family: nodesettings.FamilyNonces,
			options: []lifecycle.TruncateOption{
				lifecycle.WithTruncateUtxoSet(fakeSet{}),
			},
			wantText: "the set does not serve nonces",
		},
		{
			name:   "a set that serves no protocol parameters",
			family: nodesettings.FamilyPParamsUpdates,
			options: []lifecycle.TruncateOption{
				lifecycle.WithTruncateUtxoSet(fakeSet{}),
			},
			wantText: "the set does not serve protocol parameters",
		},
		{
			name:   "a set that serves no epochs",
			family: nodesettings.FamilyEpochs,
			options: []lifecycle.TruncateOption{
				lifecycle.WithTruncateUtxoSet(fakeSet{}),
			},
			wantText: "the set does not serve epochs",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, blocks := praosChain(t, 5)
			latchWithoutCache(t, db, tc.family)

			_, err := lifecycle.Truncate(
				context.Background(), db, blocks[2], 0, false, 0,
				tc.options...,
			)

			require.ErrorIs(t, err, lifecycle.ErrDrainedFamilyUnserved)
			require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
			require.ErrorContains(t, err, tc.wantText)
			for _, b := range blocks {
				_, err := db.BlockByIndex(b.ID, nil)
				require.NoError(
					t, err, "block %d was removed by a refused truncate", b.ID,
				)
			}
		})
	}
}

func TestTruncateOfADrainedNetworkFamilyNeedsNoSetReads(t *testing.T) {
	t.Parallel()
	db, blocks := praosChain(t, 5)
	require.NoError(t, db.SetBlockNonce(
		blocks[2].Hash,
		blocks[2].Slot,
		bytes.Repeat([]byte{0xc2}, 32),
		true,
		nil,
	))
	latchWithoutCache(t, db, nodesettings.FamilyNetworkDonations)

	_, err := lifecycle.Truncate(
		context.Background(), db, blocks[2], 0, false, 0,
		lifecycle.WithTruncateUtxoSet(fakeSet{}),
	)

	require.NoError(t, err)
}

// A truncate of a drained live-stake family makes no read of the set: the
// rollback refreshes no live stake of a family with no rows.
func TestTruncateOfADrainedLiveStakeFamilyNeedsNoSetReads(t *testing.T) {
	t.Parallel()
	db, blocks := praosChain(t, 5)
	require.NoError(t, db.SetBlockNonce(
		blocks[2].Hash,
		blocks[2].Slot,
		bytes.Repeat([]byte{0xc2}, 32),
		true,
		nil,
	))
	latchWithoutCache(t, db, nodesettings.FamilyLiveStake)

	_, err := lifecycle.Truncate(
		context.Background(), db, blocks[2], 0, false, 0,
		lifecycle.WithTruncateUtxoSet(fakeSet{}),
	)

	require.NoError(t, err)
}

// A truncate of a drained era-switch family with the epochs family still in
// SQL returns the switched epoch's row to its source era, although the set's
// own rollback has already dropped its record.
func TestTruncateOfADrainedEraSwitchRevertsTheSQLEpochRow(t *testing.T) {
	t.Parallel()
	db, blocks := praosChain(t, 5)
	require.NoError(t, db.SetBlockNonce(
		blocks[2].Hash, blocks[2].Slot, bytes.Repeat([]byte{0xc2}, 32), true, nil,
	))
	require.NoError(t, db.SetEpoch(0, 1, nil, nil, nil, nil, 6, 1_000, 10_000, nil))
	store := openUtxoSetStore(t, t.TempDir())
	applyBlocks(t, store, 1, 5)
	require.NoError(t, store.LoadEpochState(nil, nil, &badgerutxo.EraSwitch{
		Epoch: 1, Slot: blocks[3].Slot, FromEra: 5, ToEra: 6,
	}))
	latchWithoutCache(t, db, nodesettings.FamilyEraSwitch)

	_, err := lifecycle.Truncate(
		context.Background(), db, blocks[2], 0, false, 0,
		lifecycle.WithTruncateUtxoSet(dblifecycle.TruncateUtxoSet{Store: store}),
	)

	require.NoError(t, err)
	sw, err := store.EraSwitch()
	require.NoError(t, err)
	require.Nil(t, sw, "the set dropped its own record")
	row, err := db.GetEpoch(1, nil)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, uint(5), row.EraId)
}

// A truncate of a drained committee or op-certs family reads nothing from the
// set and runs no statement that touches the family's tables, in the
// certificate, committee, constitution and pool rollbacks.
func TestTruncateOfADrainedCommitteeOrOpCertsTouchesNoTable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		family nodesettings.Family
		tables []string
	}{
		{
			nodesettings.FamilyCommitteeConstitution,
			[]string{
				"auth_committee_hot", "resign_committee_cold",
				"committee_member", "committee_quorum", "constitution",
			},
		},
		{nodesettings.FamilyOpCerts, []string{"pool_opcert_sequence"}},
		{
			nodesettings.FamilyRewardOutputs,
			[]string{
				"reward_account_output", "reward_pool_output",
				"reward_credit_round",
			},
		},
	} {
		t.Run(tc.family.String(), func(t *testing.T) {
			t.Parallel()
			guard := statedrain.NewTripGuard()
			db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
				Config:         &database.Config{DataDir: t.TempDir()},
				StatementGuard: guard,
			})
			require.NoError(t, err)
			blocks := make([]models.Block, 0, 5)
			for id := uint64(1); id <= 5; id++ {
				block := testBlock(id, byte(id))
				block.Type = babbage.BlockTypeBabbage
				require.NoError(t, db.BlockCreate(block, nil))
				blocks = append(blocks, block)
			}
			last := blocks[len(blocks)-1]
			require.NoError(t, db.SetTip(ochainsync.Tip{
				Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
				BlockNumber: last.Number,
			}, nil))
			require.NoError(t, db.SetBlockNonce(
				blocks[2].Hash, blocks[2].Slot,
				bytes.Repeat([]byte{0xc2}, 32), true, nil,
			))
			latchWithoutCache(t, db, tc.family)
			guard.Arm(tc.family, tc.tables)

			_, err = lifecycle.Truncate(
				context.Background(), db, blocks[2], 0, false, 0,
				lifecycle.WithTruncateUtxoSet(fakeSet{}),
			)

			require.NoError(t, err)
			require.Empty(t, guard.Trips())
			tip, err := db.GetTip(nil)
			require.NoError(t, err)
			require.Equal(t, blocks[2].Slot, tip.Point.Slot)
		})
	}
}

func TestTruncateOfADrainedGovernanceFamilyNeedsNoSetReads(t *testing.T) {
	t.Parallel()
	db, blocks := praosChain(t, 5)
	require.NoError(t, db.SetBlockNonce(
		blocks[2].Hash,
		blocks[2].Slot,
		bytes.Repeat([]byte{0xc2}, 32),
		true,
		nil,
	))
	latchWithoutCache(t, db, nodesettings.FamilyGovernance)

	_, err := lifecycle.Truncate(
		context.Background(), db, blocks[2], 0, false, 0,
		lifecycle.WithTruncateUtxoSet(fakeSet{}),
	)

	require.NoError(t, err)
	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, blocks[2].Slot, tip.Point.Slot)
}

func pparamUpdateRow(slot, epoch uint64, genesis, cbor byte) badgerutxo.RowPut {
	key := binary.BigEndian.AppendUint64(
		[]byte{badgerutxo.PParamUpdateTag},
		slot,
	)
	key = binary.BigEndian.AppendUint32(key, 0)
	key = append(key, genesis)
	return badgerutxo.RowPut{
		Slot:  slot,
		Key:   key,
		Value: append(binary.BigEndian.AppendUint64(nil, epoch), cbor),
	}
}

// A truncate of a drained pparams-updates family succeeds on a database with
// no protocol parameter rows: the set rolls its own rows and markers back,
// and the database's reads of them come from the set afterwards.
func TestTruncateOfADrainedPParamsFamilyRollsTheSetBack(t *testing.T) {
	t.Parallel()
	db, blocks := praosChain(t, 5)
	store := openUtxoSetStore(t, t.TempDir())
	require.NoError(t, store.LoadPParams(nil))
	require.NoError(t, store.LoadPParamUpdates(
		func(func(k, v []byte) error) error { return nil },
	))
	require.NoError(t, store.LoadMarkers(nil))
	applyBlocks(t, store, 1, 3)
	// Block 4 writes a row, a proposal and the markers real data was
	// confirmed with at epoch 1, which starts after the target block.
	p4 := blockPoint(4)
	_, err := store.ApplyLedgerBatch(&p4, badgerutxo.LedgerBatch{
		Changes: []badgerutxo.Change{{
			Point: p4,
			Added: []badgerutxo.Output{{
				Ref:  badgerutxo.NewRef(bytes.Repeat([]byte{4}, 32), 0),
				Cbor: []byte{0x80, 4},
			}},
		}},
		Epochs: []badgerutxo.EpochRow{
			{Epoch: 0, StartSlot: 0, Value: []byte{0}},
			{Epoch: 1, StartSlot: 35, Value: []byte{1}},
		},
		PParams: []badgerutxo.PParamsRow{
			{Era: 6, Epoch: 0, AddedSlot: 20, Cbor: []byte{1}},
			{Era: 6, Epoch: 1, AddedSlot: 45, Cbor: []byte{2}},
		},
		Rows: []badgerutxo.RowPut{
			pparamUpdateRow(25, 0, 1, 0xa1),
			pparamUpdateRow(45, 1, 2, 0xa2),
		},
		Markers: []badgerutxo.MarkerWrite{
			{Name: badgerutxo.SyntheticV2Marker, Value: "false"},
			{Name: badgerutxo.SyntheticV2ClearedMarker, Value: "1"},
		},
	})
	require.NoError(t, err)
	applyBlocks(t, store, 5, 5)
	require.NoError(t, db.SetBlockNonce(
		blocks[2].Hash,
		blocks[2].Slot,
		bytes.Repeat([]byte{0xc2}, 32),
		true,
		nil,
	))
	latchWithoutCache(t, db, nodesettings.FamilyPParamsUpdates)

	_, err = lifecycle.Truncate(
		context.Background(), db, blocks[2], 0, false, 0,
		lifecycle.WithTruncateUtxoSet(
			dblifecycle.TruncateUtxoSet{Store: store},
		),
	)

	require.NoError(t, err)
	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, blocks[2].Slot, tip.Point.Slot)
	requireSetAt(t, store, 3)
	rows, err := store.PParamsForEra(6)
	require.NoError(t, err)
	require.Len(t, rows, 1, "the row added after the target is gone")
	require.Equal(t, uint64(20), rows[0].AddedSlot)
	var proposals int
	require.NoError(t, utxoset.EachPParamUpdate(
		store, func(models.PParamUpdate) error { proposals++; return nil },
	))
	require.Equal(
		t,
		1,
		proposals,
		"the proposal added after the target is gone",
	)
	for _, name := range []string{
		badgerutxo.SyntheticV2Marker, badgerutxo.SyntheticV2ClearedMarker,
	} {
		_, ok, err := store.Marker(name)
		require.NoError(t, err)
		require.False(
			t,
			ok,
			"%s: the target is before the confirming epoch",
			name,
		)
	}
	// The database answers from the set that bound at the truncate.
	v, err := db.GetSyncState(database.SyntheticV2CostModelSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, v)
	listed, err := db.ListPParamsForEra(6, nil)
	require.NoError(t, err)
	require.Len(t, listed, 1)
}

// A truncate of drained accounts and dreps families reads nothing from the
// set: the rollback skips their restores and deletes.
func TestTruncateOfDrainedAccountFamiliesNeedsNoSetReads(t *testing.T) {
	t.Parallel()
	db, blocks := praosChain(t, 5)
	require.NoError(t, db.SetBlockNonce(
		blocks[2].Hash, blocks[2].Slot, bytes.Repeat([]byte{0xc2}, 32), true, nil,
	))
	latchWithoutCache(t, db, nodesettings.FamilyAccounts, nodesettings.FamilyDReps)

	_, err := lifecycle.Truncate(
		context.Background(), db, blocks[2], 0, false, 0,
		lifecycle.WithTruncateUtxoSet(fakeSet{}),
	)

	require.NoError(t, err)
	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, blocks[2].Slot, tip.Point.Slot)
}

// A truncate of a drained pools-history family reads nothing of it from the
// set: the rollback only skips the family's deletes.
func TestTruncateOfADrainedPoolsFamilyNeedsNoSetReads(t *testing.T) {
	t.Parallel()
	db, blocks := praosChain(t, 5)
	require.NoError(t, db.SetBlockNonce(
		blocks[2].Hash,
		blocks[2].Slot,
		bytes.Repeat([]byte{0xc2}, 32),
		true,
		nil,
	))
	latchWithoutCache(t, db, nodesettings.FamilyPoolsHistory)

	_, err := lifecycle.Truncate(
		context.Background(), db, blocks[2], 0, false, 0,
		lifecycle.WithTruncateUtxoSet(fakeSet{}),
	)

	require.NoError(t, err)
	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, blocks[2].Slot, tip.Point.Slot)
	require.True(t, db.FamilyDrained(nodesettings.FamilyPoolsHistory),
		"the truncate reloads the latch")
}
