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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

const (
	replayBoundaryLength   = uint(2)
	replayBoundaryEndedEra = uint64(5)
	replayBoundaryNewEpoch = replayBoundaryEndedEra + 1
)

// boundaryReplayEffects is every durable and in-memory effect a normal
// successor NEWEPOCH leaves behind, read back after the rollover commits.
type boundaryReplayEffects struct {
	epoch         models.Epoch
	adaPots       *models.RewardAdaPots
	rewardMark    *models.RewardSnapshot
	stakeInputs   []*models.RewardStakeInput
	summary       *models.EpochSummary
	markRows      []*models.PoolStakeSnapshot
	network       *models.NetworkState
	pparamsRows   int
	ledgerEpoch   uint64
	currentEpoch  uint64
	snapshotCalls int
}

func normalizeReplayEpoch(epoch models.Epoch) models.Epoch {
	epoch.ID = 0
	return epoch
}

func normalizeReplayMarkRows(
	rows []*models.PoolStakeSnapshot,
) []*models.PoolStakeSnapshot {
	out := make([]*models.PoolStakeSnapshot, 0, len(rows))
	for _, row := range rows {
		cp := *row
		cp.ID = 0
		out = append(out, &cp)
	}
	return out
}

func normalizeReplayStakeInputs(
	rows []*models.RewardStakeInput,
) []*models.RewardStakeInput {
	out := make([]*models.RewardStakeInput, 0, len(rows))
	for _, row := range rows {
		cp := *row
		cp.ID = 0
		out = append(out, &cp)
	}
	return out
}

// boundaryReplayHarness is a real ledger over a real chain and metadata
// store, with the production snapshot manager wired into the rollover hooks.
type boundaryReplayHarness struct {
	t         *testing.T
	fixture   *chainsyncRollbackFixture
	ls        *LedgerState
	poolHash  []byte
	snapCalls int
	next      models.Epoch
}

func newBoundaryReplayHarness(t *testing.T) *boundaryReplayHarness {
	t.Helper()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	h := &boundaryReplayHarness{t: t, fixture: fixture, ls: ls}
	ls.config.CardanoNodeConfig.ShelleyGenesisHash = "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d"

	for epochID := uint64(0); epochID <= replayBoundaryEndedEra; epochID++ {
		require.NoError(t, ls.db.SetEpoch(
			epochID*uint64(replayBoundaryLength), epochID,
			nil, nil, nil, nil,
			eras.ShelleyEraDesc.Id, 1000, replayBoundaryLength, nil,
		))
	}
	epochs, err := ls.db.GetEpochs(nil)
	require.NoError(t, err)
	require.Len(t, epochs, int(replayBoundaryNewEpoch))

	rat := func() *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(1, 2)} }
	ls.currentEra = eras.ShelleyEraDesc
	ls.currentPParams = &shelley.ShelleyProtocolParameters{
		ProtocolMajor:    shelley.MinProtocolVersionShelley,
		MinFeeA:          44,
		NOpt:             1,
		A0:               rat(),
		Rho:              rat(),
		Tau:              rat(),
		Decentralization: rat(),
	}
	ls.epochCache = epochs
	ls.currentEpoch = epochs[len(epochs)-1]
	ls.ledgerEpoch = ls.currentEpoch.EpochId
	seedEmptyRewardBasisForRollover(
		t, ls.db, ls.currentEpoch, ls.currentPParams,
	)

	h.poolHash = bytes.Repeat([]byte{0xd1}, 28)
	ownerKey := bytes.Repeat([]byte{0x71}, 28)
	delegatorKey := bytes.Repeat([]byte{0x72}, 28)
	margin := &types.Rat{Rat: big.NewRat(1, 100)}
	require.NoError(t, ls.db.ImportPool(
		context.Background(), nil,
		&models.Pool{
			PoolKeyHash:   h.poolHash,
			VrfKeyHash:    make([]byte, 32),
			Pledge:        5_000_000,
			Cost:          340_000_000,
			Margin:        margin,
			RewardAccount: ownerKey,
		},
		&models.PoolRegistration{
			PoolKeyHash:   h.poolHash,
			AddedSlot:     0,
			Pledge:        5_000_000,
			Cost:          340_000_000,
			Margin:        margin,
			VrfKeyHash:    make([]byte, 32),
			RewardAccount: ownerKey,
			Owners: []models.PoolRegistrationOwner{
				{KeyHash: append([]byte(nil), ownerKey...)},
			},
		},
	))
	for i, key := range [][]byte{ownerKey, delegatorKey} {
		require.NoError(t, ls.db.CreateAccount(
			context.Background(), nil, &models.Account{
				StakingKey: key,
				Pool:       h.poolHash,
				AddedSlot:  0,
				Active:     true,
			},
		))
		h.addUtxo(byte(0x10+i), key, 20_000_000, 0)
	}

	mgr := snapshot.NewManager(ls.db, nil, nil)
	ls.SetEpochBoundarySnapshotStakeHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return mgr.ComputeEpochBoundarySnapshot(
				context.Background(), txn, evt,
			)
		},
	)
	ls.SetEpochBoundarySnapshotHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			h.snapCalls++
			return mgr.CaptureEpochBoundarySnapshot(
				context.Background(), txn, evt,
			)
		},
	)
	ls.SetCurrentBoundarySPOStakeHook(
		func(
			txn *database.Txn,
			evt event.EpochTransitionEvent,
			incomingEraID uint,
		) ([]*models.PoolStakeSnapshot, error) {
			return mgr.CurrentBoundarySPOStakeRows(
				context.Background(), txn, evt, incomingEraID,
			)
		},
	)
	epochBoundaryBenchWireDeferred(ls, mgr)

	h.next = models.Epoch{
		EpochId:       replayBoundaryNewEpoch,
		StartSlot:     replayBoundaryNewEpoch * uint64(replayBoundaryLength),
		SlotLength:    1000,
		LengthInSlots: replayBoundaryLength,
		EraId:         eras.ShelleyEraDesc.Id,
	}
	return h
}

func (h *boundaryReplayHarness) addUtxo(
	id byte,
	stakingKey []byte,
	amount uint64,
	slot uint64,
) {
	h.t.Helper()
	require.NoError(h.t, h.ls.db.CreateUtxo(
		context.Background(), nil, &models.Utxo{
			TxId:       bytes.Repeat([]byte{id}, 32),
			OutputIdx:  0,
			StakingKey: stakingKey,
			Amount:     types.Uint64(amount),
			AddedSlot:  slot,
		},
	))
}

// applyBoundary runs the successor rollover the way ledgerProcessBlocks does:
// it hands the rollover the ledger's own epoch label, commits, and then
// publishes the result into the in-memory state.
func (h *boundaryReplayHarness) applyBoundary() boundaryReplayEffects {
	h.t.Helper()
	ls := h.ls
	h.snapCalls = 0
	var result *EpochRolloverResult
	txn := ls.db.Transaction(context.Background(), true)
	require.NoError(h.t, txn.Do(func(txn *database.Txn) error {
		var err error
		result, err = ls.processEpochRolloverForTargetWithClassicPParamsContext(
			context.Background(),
			txn,
			ls.currentEpoch,
			h.next,
			h.next,
			ls.ledgerEpoch,
			ls.currentEra,
			ls.currentPParams,
			false,
			false,
			nil,
		)
		return err
	}))
	require.NotNil(h.t, result)
	ls.Lock()
	ls.epochCache = result.NewEpochCache
	ls.currentEpoch = result.NewCurrentEpoch
	ls.currentEra = result.NewCurrentEra
	ls.currentPParams = result.NewCurrentPParams
	ls.ledgerEpoch = result.newLedgerEpoch
	ls.checkpointWrittenForEpoch = result.CheckpointWrittenForEpoch
	ls.Unlock()
	return h.readEffects()
}

func (h *boundaryReplayHarness) readEffects() boundaryReplayEffects {
	h.t.Helper()
	meta := h.ls.db.Metadata()
	var e boundaryReplayEffects
	epoch, err := h.ls.db.GetEpoch(replayBoundaryNewEpoch, nil)
	require.NoError(h.t, err)
	if epoch != nil {
		e.epoch = normalizeReplayEpoch(*epoch)
	}
	e.adaPots, err = meta.GetRewardAdaPots(replayBoundaryNewEpoch, nil)
	require.NoError(h.t, err)
	if e.adaPots != nil {
		e.adaPots.ID = 0
	}
	e.rewardMark, err = meta.GetRewardSnapshot(
		replayBoundaryNewEpoch, "mark", nil,
	)
	require.NoError(h.t, err)
	if e.rewardMark != nil {
		e.rewardMark.ID = 0
	}
	inputs, err := meta.GetRewardStakeInputs(replayBoundaryNewEpoch, nil)
	require.NoError(h.t, err)
	e.stakeInputs = normalizeReplayStakeInputs(inputs)
	e.summary, err = meta.GetEpochSummary(replayBoundaryNewEpoch, nil)
	require.NoError(h.t, err)
	if e.summary != nil {
		e.summary.ID = 0
	}
	marks, err := h.ls.db.GetPoolStakeSnapshotsByEpoch(
		replayBoundaryNewEpoch, models.PoolStakeSnapshotTypeMark, nil,
	)
	require.NoError(h.t, err)
	e.markRows = normalizeReplayMarkRows(marks)
	e.network, err = meta.GetNetworkState(nil)
	require.NoError(h.t, err)
	if e.network != nil {
		e.network.ID = 0
	}
	e.ledgerEpoch = h.ls.ledgerEpoch
	e.currentEpoch = h.ls.currentEpoch.EpochId
	e.snapshotCalls = h.snapCalls
	return e
}

func (h *boundaryReplayHarness) rollbackBeforeBoundary() {
	h.t.Helper()
	require.NoError(h.t, h.ls.rollbackWithBlocksAndIntent(
		context.Background(),
		h.fixture.ancestorTip.Point,
		nil,
		false,
		false,
		false,
	))
}

// A rollback across epoch E followed by re-application of E's first block
// must run E's EPOCH effects again. cardano-ledger restores the whole
// NewEpochState snapshot, so the re-applied boundary is a fresh successor
// NEWEPOCH; a skipped effect would split the chain from the Haskell node.
func TestRollbackAcrossEpochBoundaryReplaysEpochEffects(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// forkStake is the extra stake the re-applied chain carries at the
		// boundary; the original chain carries origStake.
		forkStake uint64
	}{
		{name: "same chain", forkStake: 7_000_000},
		{name: "fork with different stake", forkStake: 9_000_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newBoundaryReplayHarness(t)
			delegatorKey := bytes.Repeat([]byte{0x72}, 28)
			boundarySlot := h.next.StartSlot
			require.Greater(t, boundarySlot, h.fixture.ancestorTip.Point.Slot)

			// Stake credited by a block of the epoch being closed, above the
			// rollback point, so the rollback deletes it.
			const origStake = uint64(7_000_000)
			h.addUtxo(0x33, delegatorKey, origStake, boundarySlot-1)
			preBoundaryPParams, err := cbor.Encode(h.ls.currentPParams)
			require.NoError(t, err)
			first := h.applyBoundary()

			require.Equal(t, replayBoundaryNewEpoch, first.currentEpoch)
			require.Equal(t, replayBoundaryNewEpoch, first.ledgerEpoch)
			require.Equal(t, 1, first.snapshotCalls,
				"the first application must run the SNAP capture")
			require.NotNil(t, first.adaPots,
				"the successor rollover persists the ADA pots")
			require.NotNil(t, first.summary)
			require.NotEmpty(t, first.markRows)
			require.NotNil(t, first.rewardMark)
			require.Equal(t, models.PoolStakeSnapshotTypeMark,
				first.markRows[0].SnapshotType)
			firstStake := uint64(first.markRows[0].TotalStake)

			h.rollbackBeforeBoundary()

			epoch, err := h.ls.db.GetEpoch(replayBoundaryNewEpoch, nil)
			require.NoError(t, err)
			require.Nil(t, epoch, "the rollback must delete epoch E's row")
			pots, err := h.ls.db.Metadata().GetRewardAdaPots(
				replayBoundaryNewEpoch, nil,
			)
			require.NoError(t, err)
			require.Nil(t, pots, "the rollback must delete epoch E's pots")
			staleMarks, err := h.ls.db.GetPoolStakeSnapshotsByEpoch(
				replayBoundaryNewEpoch, models.PoolStakeSnapshotTypeMark, nil,
			)
			require.NoError(t, err)
			require.Empty(t, staleMarks,
				"the rollback must delete epoch E's mark snapshot rows")
			staleSummary, err := h.ls.db.Metadata().GetEpochSummary(
				replayBoundaryNewEpoch, nil,
			)
			require.NoError(t, err)
			require.Nil(t, staleSummary,
				"the rollback must delete epoch E's summary")
			require.Equal(t, replayBoundaryEndedEra, h.ls.currentEpoch.EpochId)
			require.True(t, h.ls.checkpointWrittenForEpoch,
				"the rollback target's epoch already has its checkpoint row, "+
					"so replay must not write a second one")
			rewoundPParams, err := cbor.Encode(h.ls.currentPParams)
			require.NoError(t, err)
			require.Equal(t, preBoundaryPParams, rewoundPParams,
				"the rollback must restore the pre-boundary protocol parameters")

			h.addUtxo(0x34, delegatorKey, tc.forkStake, boundarySlot-1)
			second := h.applyBoundary()

			require.Equal(t, 1, second.snapshotCalls,
				"the re-applied boundary must run the SNAP capture again; "+
					"zero means the jump path skipped every EPOCH effect")
			require.Equal(t, first.currentEpoch, second.currentEpoch)
			require.Equal(t, first.ledgerEpoch, second.ledgerEpoch)
			require.NotNil(t, second.adaPots)
			require.NotEmpty(t, second.markRows)
			require.Equal(t, first.epoch, second.epoch)
			require.Equal(t, first.adaPots, second.adaPots)
			require.Equal(t, first.network, second.network)
			require.Equal(t, first.stakeInputs != nil, second.stakeInputs != nil)

			wantStake := firstStake - origStake + tc.forkStake
			require.Equal(t, wantStake, uint64(second.markRows[0].TotalStake),
				"the mark snapshot must reflect the re-applied chain, not "+
					"the rolled-back one")
			require.Len(t, second.markRows, len(first.markRows),
				"the rolled-back boundary's mark rows must not be duplicated")
			if tc.forkStake == origStake {
				require.Equal(t, first.markRows, second.markRows)
				require.Equal(t, first.rewardMark, second.rewardMark)
				require.Equal(t, first.summary, second.summary)
				require.Equal(t, first.stakeInputs, second.stakeInputs)
			}
		})
	}
}
