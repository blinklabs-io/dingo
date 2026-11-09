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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// midEpochEraK is the security parameter of the mid-epoch era cases. It puts
// the voting deadline 2 * ceil(3k/f) = 240 slots before the end of a
// 1000-slot epoch, so slots below the deadline do not count.
const midEpochEraK = 2

// midEpochEraGapStart is the first slot of epoch 3, the epoch the cases'
// blocks are in. The Babbage ledger's epoch label is 0 and epochs 1 and 2
// are empty, so the pending update names epoch 1 as the transition epoch.
const midEpochEraGapStart = 3 * boundaryEraTestEpochLength

// midEpochEraDeadline is the first slot of epoch 3 past its voting deadline.
const midEpochEraDeadline = midEpochEraGapStart + boundaryEraTestEpochLength - 240

// midEpochEraCase is a Babbage ledger whose last block is in epoch 0, with
// one block there past the deadline (fewer than k) and a quorum-met classic
// update to major 9 pending. The boundary block and followers are in epoch 3.
func midEpochEraCase(
	boundarySlot uint64,
	followers ...boundaryEraFollower,
) boundaryEraCase {
	return boundaryEraCase{
		pparamsMajor:       8,
		classicUpdateMajor: 9,
		votingBlocks:       1,
		babbageBody:        true,
		blockSlot:          boundarySlot,
		followers:          followers,
		configure: func(cfg *cardano.CardanoNodeConfig) {
			cfg.ShelleyGenesis().SecurityParam = midEpochEraK
			cfg.ShelleyGenesis().EpochLength = boundaryEraTestEpochLength
		},
	}
}

func processMidEpochEraBlocks(
	t *testing.T,
	ls *LedgerState,
	blocks []gledger.Block,
) error {
	t.Helper()
	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: blocks}
	close(results)
	return ls.ledgerProcessBlocksFromSource(context.Background(), results)
}

func requireMidEpochEraTip(
	t *testing.T,
	ls *LedgerState,
	block gledger.Block,
) {
	t.Helper()
	assert.Equal(t, block.SlotNumber(), ls.currentTip.Point.Slot)
	assert.Equal(t, block.Hash().Bytes(), ls.currentTip.Point.Hash)
}

// TestMidEpochEraFollowsKnownTransition pins when the hard-fork combinator
// moves the ledger into the next era inside an epoch that follows empty
// epochs. It reads the transition from the ledger state before the block
// (Shelley/ShelleyHFEras.hs shelleyTransition via HardFork/Combinator/State.hs
// extendToSlot): the pending update names the next era's major and
// shelleyAfterVoting >= k, where the count restarts in every slot epoch
// (Shelley/Ledger/Ledger.hs applyChainTickLedgerResult) and counts blocks at
// or past the epoch's own voting deadline. The transition epoch is the
// successor of the ledger epoch label, which is behind the block's epoch, so
// the block that follows the k-th such block is a Conway block and a Babbage
// block there is refused. The k-th block itself is still a Babbage block.
func TestMidEpochEraFollowsKnownTransition(t *testing.T) {
	t.Parallel()

	past := uint64(midEpochEraDeadline)
	babbage := func(slot uint64) boundaryEraFollower {
		return boundaryEraFollower{babbageBody: true, slot: slot}
	}
	conwayAt := func(slot uint64) boundaryEraFollower {
		return boundaryEraFollower{slot: slot}
	}

	t.Run("k-th block past the deadline is still a babbage block", func(t *testing.T) {
		t.Parallel()
		ls, blocks := newBoundaryEraLedgerChain(t, midEpochEraCase(
			past, babbage(past+1),
		))
		require.NoError(t, processMidEpochEraBlocks(t, ls, blocks))
		assert.Equal(t, eras.BabbageEraDesc.Id, ls.currentEra.Id)
		requireMidEpochEraTip(t, ls, blocks[1])
	})
	t.Run("conway block before the k-th is refused", func(t *testing.T) {
		t.Parallel()
		ls, blocks := newBoundaryEraLedgerChain(t, midEpochEraCase(
			past, conwayAt(past+1),
		))
		err := processMidEpochEraBlocks(t, ls, blocks)
		require.ErrorIs(t, err, errRestartLedgerPipeline)
		assert.Equal(t, eras.BabbageEraDesc.Id, ls.currentEra.Id)
		requireMidEpochEraTip(t, ls, blocks[0])
	})
	t.Run("conway block after k blocks past the deadline is accepted", func(t *testing.T) {
		t.Parallel()
		ls, blocks := newBoundaryEraLedgerChain(t, midEpochEraCase(
			past, babbage(past+1), conwayAt(past+2),
		))
		require.NoError(t, processMidEpochEraBlocks(t, ls, blocks))
		assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)
		assert.Equal(t, uint64(3), ls.currentEpoch.EpochId)
		assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEpoch.EraId)
		ver, err := GetProtocolVersion(ls.currentPParams)
		require.NoError(t, err)
		assert.Equal(t, uint(8), ver.Major,
			"the translation carries the major, as across an empty epoch")
		requireMidEpochEraTip(t, ls, blocks[2])
	})
	t.Run("babbage block after k blocks past the deadline is refused", func(t *testing.T) {
		t.Parallel()
		ls, blocks := newBoundaryEraLedgerChain(t, midEpochEraCase(
			past, babbage(past+1), babbage(past+2),
		))
		err := processMidEpochEraBlocks(t, ls, blocks)
		require.ErrorIs(t, err, errRestartLedgerPipeline)
		assert.Equal(t, eras.BabbageEraDesc.Id, ls.currentEra.Id)
		requireMidEpochEraTip(t, ls, blocks[1])
	})
	t.Run("blocks before the deadline do not count", func(t *testing.T) {
		t.Parallel()
		ls, blocks := newBoundaryEraLedgerChain(t, midEpochEraCase(
			midEpochEraGapStart+100,
			babbage(midEpochEraGapStart+101),
			babbage(midEpochEraGapStart+102),
		))
		require.NoError(t, processMidEpochEraBlocks(t, ls, blocks))
		assert.Equal(t, eras.BabbageEraDesc.Id, ls.currentEra.Id)
		requireMidEpochEraTip(t, ls, blocks[2])
	})
}

// TestMidEpochEraSwitchRollsBack pins that rolling back to the last block
// before the switch restores the Babbage ledger, its parameters and its
// epoch row, so the chain can continue in either era, and that a reload from
// the database after the switch comes up in Conway.
func TestMidEpochEraSwitchRollsBack(t *testing.T) {
	t.Parallel()

	past := uint64(midEpochEraDeadline)
	followers := []boundaryEraFollower{
		{babbageBody: true, slot: past + 1},
		{slot: past + 2},
	}

	t.Run("a reload after the switch is conway", func(t *testing.T) {
		t.Parallel()
		ls, blocks := newBoundaryEraLedgerChain(t, midEpochEraCase(
			past, followers...,
		))
		require.NoError(t, processMidEpochEraBlocks(t, ls, blocks))
		require.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)

		require.NoError(t, ls.loadEpochs(t.Context(), nil))
		assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)
		assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEpoch.EraId)
	})
	t.Run("rollback to the block before the switch restores babbage", func(t *testing.T) {
		t.Parallel()
		ls, blocks := newBoundaryEraLedgerChain(t, midEpochEraCase(
			past, followers...,
		))
		require.NoError(t, processMidEpochEraBlocks(t, ls, blocks))
		require.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)

		before := blocks[1]
		require.NoError(t, ls.rollback(
			t.Context(),
			ocommon.NewPoint(before.SlotNumber(), before.Hash().Bytes()),
		))
		assert.Equal(t, eras.BabbageEraDesc.Id, ls.currentEra.Id)
		assert.Equal(t, eras.BabbageEraDesc.Id, ls.currentEpoch.EraId)
		ver, err := GetProtocolVersion(ls.currentPParams)
		require.NoError(t, err)
		assert.Equal(t, uint(8), ver.Major)
		requireMidEpochEraTip(t, ls, before)
		conwayRow, err := ls.db.GetPParams(
			ls.currentEpoch.EpochId,
			eras.ConwayEraDesc.Id,
			eras.ConwayEraDesc.DecodePParamsFunc,
			nil,
		)
		require.NoError(t, err)
		assert.Nil(t, conwayRow,
			"the translated parameters must go with the rolled-back block")

		require.NoError(t, ls.loadEpochs(t.Context(), nil))
		assert.Equal(t, eras.BabbageEraDesc.Id, ls.currentEra.Id,
			"a reload after the rollback must not find the abandoned switch")
	})
}

// TestMidEpochEraIsKnownBeforeTheBlockIsApplied pins that everything which
// decides the era of a block ahead of its application agrees with the apply
// path once the applied tip knows the transition: the forger builds the next
// block from ProtocolParamsForSlot, and the leader schedule and the leader
// check take the consensus mode from EraIDForEpoch. A forger that built the
// block after the k-th one in Babbage would produce a block the ledger
// refuses, and a leader check in the old era's mode would judge the block
// under the wrong VRF derivation. The decision follows the applied tip, so a
// rollback across the point that made it known withdraws it.
func TestMidEpochEraIsKnownBeforeTheBlockIsApplied(t *testing.T) {
	t.Parallel()

	past := uint64(midEpochEraDeadline)
	ls, blocks := newBoundaryEraLedgerChain(t, midEpochEraCase(
		past,
		boundaryEraFollower{babbageBody: true, slot: past + 1},
	))
	requireBabbage := func(t *testing.T, slot uint64) {
		t.Helper()
		_, ok := ls.ProtocolParamsForSlot(slot).(*babbage.BabbageProtocolParameters)
		assert.True(t, ok, "slot %d must be forged with Babbage parameters", slot)
		eraID, err := ls.EraIDForEpoch(3)
		require.NoError(t, err)
		assert.Equal(t, eras.BabbageEraDesc.Id, eraID)
	}
	requireConway := func(t *testing.T, slot uint64) {
		t.Helper()
		_, ok := ls.ProtocolParamsForSlot(slot).(*conway.ConwayProtocolParameters)
		assert.True(t, ok, "slot %d must be forged with Conway parameters", slot)
		eraID, err := ls.EraIDForEpoch(3)
		require.NoError(t, err)
		assert.Equal(t, eras.ConwayEraDesc.Id, eraID)
	}

	require.NoError(t, processMidEpochEraBlocks(t, ls, blocks[:1]))
	requireBabbage(t, past+1)

	require.NoError(t, processMidEpochEraBlocks(t, ls, blocks[1:]))
	require.Equal(t, eras.BabbageEraDesc.Id, ls.currentEra.Id)
	requireConway(t, past+2)

	require.NoError(t, ls.rollback(
		t.Context(),
		ocommon.NewPoint(blocks[0].SlotNumber(), blocks[0].Hash().Bytes()),
	))
	requireBabbage(t, past+1)
}

// TestMidEpochEraKeepsPendingMajorOutOfConway pins that the update to major 9
// which was pending when the ledger was translated stays unenacted in Conway.
// Conway has no classic update proposals (the translation drops them), so the
// next epoch boundary neither raises the major nor reads the pending update
// as a second hard fork out of Babbage.
func TestMidEpochEraKeepsPendingMajorOutOfConway(t *testing.T) {
	t.Parallel()

	past := uint64(midEpochEraDeadline)
	nextEpoch := uint64(4 * boundaryEraTestEpochLength)
	ls, blocks := newBoundaryEraLedgerChain(t, midEpochEraCase(
		past,
		boundaryEraFollower{babbageBody: true, slot: past + 1},
		boundaryEraFollower{slot: past + 2},
		boundaryEraFollower{slot: nextEpoch + 1},
	))
	require.NoError(t, processMidEpochEraBlocks(t, ls, blocks))
	assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)
	assert.Equal(t, uint64(4), ls.currentEpoch.EpochId)
	ver, err := GetProtocolVersion(ls.currentPParams)
	require.NoError(t, err)
	assert.Equal(t, uint(8), ver.Major)
	requireMidEpochEraTip(t, ls, blocks[3])
}
