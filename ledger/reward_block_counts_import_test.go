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
	"log/slog"
	"strconv"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
)

// A bootstrapped node applies no block at or below its trust anchor, so every
// slot of an epoch that ended below the anchor is uncountable. The blocks were
// nonetheless minted, and the reference credits their rewards, so the counts
// have to come from the snapshot's own BlocksMade rather than from a floor of
// zero.
func TestRewardBlockCountsMergesImportedCountsAcrossTheAnchor(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		performanceEpoch = uint64(2)
		epochStartSlot   = uint64(100)
		epochLength      = 100
		anchorSlot       = uint64(150)
	)
	poolKey := rewardCalcHash(0x81)
	otherPoolKey := rewardCalcHash(0x82)
	retiredPoolKey := rewardCalcHash(0x83)
	var poolID, otherPoolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)
	copy(otherPoolID[:], otherPoolKey)

	require.NoError(t, meta.SetEpoch(
		epochStartSlot,
		performanceEpoch,
		nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id,
		1,
		epochLength,
		nil,
	))
	require.NoError(t, meta.SetSyncState(
		mithrilLedgerSlotSyncKey,
		strconv.FormatUint(anchorSlot, 10),
		nil,
	))
	// Blocks this node applied itself, all strictly above the anchor.
	for _, slot := range []uint64{160, 170} {
		require.NoError(t, db.UpdatePoolOpCertSequence(poolID, slot, slot, nil))
	}
	require.NoError(t, db.UpdatePoolOpCertSequence(otherPoolID, 180, 180, nil))
	// Blocks the snapshot reports for the same epoch, minted at or below the
	// anchor. retiredPoolKey is not one of the pools asked about, but its
	// blocks still belong to the epoch total that every pool's beta divides by.
	require.NoError(t, meta.SaveImportedPoolBlockCounts(
		[]models.ImportedPoolBlockCount{
			{
				Epoch:          performanceEpoch,
				PoolKeyHash:    poolKey,
				BlocksProduced: 5,
				CapturedSlot:   anchorSlot,
			},
			{
				Epoch:          performanceEpoch,
				PoolKeyHash:    otherPoolKey,
				BlocksProduced: 3,
				CapturedSlot:   anchorSlot,
			},
			{
				Epoch:          performanceEpoch,
				PoolKeyHash:    retiredPoolKey,
				BlocksProduced: 2,
				CapturedSlot:   anchorSlot,
			},
		},
		nil,
	))
	require.NoError(t, meta.SaveImportedEpochBlockTotal(
		performanceEpoch,
		5+3+2,
		anchorSlot,
		nil,
	))

	counts, total, known, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{
			{PoolKeyHash: poolKey},
			{PoolKeyHash: otherPoolKey},
		},
		nil,
	)
	require.NoError(t, err)
	require.True(t, known)
	assert.Equal(t, uint64(2+5), counts[string(poolKey)])
	assert.Equal(t, uint64(1+3), counts[string(otherPoolKey)])
	assert.Equal(t, uint64(3+10), total)
}

// Zero blocks and no block history are different answers. The first is a real
// epoch outcome; the second is an epoch this node cannot count, and reading it
// as zero gives every pool zero performance and credits every delegator
// nothing while reporting a completed round.
func TestRewardBlockCountsUnknownWhenAnchorHidesTheEpoch(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		performanceEpoch = uint64(2)
		epochStartSlot   = uint64(100)
		epochLength      = 100
	)
	poolKey := rewardCalcHash(0x84)

	require.NoError(t, meta.SetEpoch(
		epochStartSlot,
		performanceEpoch,
		nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id,
		1,
		epochLength,
		nil,
	))
	// The anchor sits past the end of the epoch, so none of it is observable.
	require.NoError(t, meta.SetSyncState(
		mithrilLedgerSlotSyncKey,
		strconv.FormatUint(epochStartSlot+epochLength, 10),
		nil,
	))

	_, _, known, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{{PoolKeyHash: poolKey}},
		nil,
	)
	require.NoError(t, err)
	require.False(
		t,
		known,
		"an epoch that ended below the anchor with no imported counts has "+
			"unknown block counts, not zero",
	)
}

// The imported counts are consulted only for an epoch the anchor actually
// covers. A node that never bootstrapped counts its own blocks exactly as it
// did before.
func TestRewardBlockCountsIgnoresImportedCountsAboveTheAnchor(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		performanceEpoch = uint64(2)
		epochStartSlot   = uint64(100)
		epochLength      = 100
	)
	poolKey := rewardCalcHash(0x85)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	require.NoError(t, meta.SetEpoch(
		epochStartSlot,
		performanceEpoch,
		nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id,
		1,
		epochLength,
		nil,
	))
	require.NoError(t, meta.SetSyncState(
		mithrilLedgerSlotSyncKey,
		strconv.FormatUint(epochStartSlot-1, 10),
		nil,
	))
	require.NoError(t, db.UpdatePoolOpCertSequence(poolID, 1, 120, nil))
	require.NoError(t, meta.SaveImportedPoolBlockCounts(
		[]models.ImportedPoolBlockCount{
			{
				Epoch:          performanceEpoch,
				PoolKeyHash:    poolKey,
				BlocksProduced: 7,
				CapturedSlot:   epochStartSlot - 1,
			},
		},
		nil,
	))
	require.NoError(t, meta.SaveImportedEpochBlockTotal(
		performanceEpoch,
		7,
		epochStartSlot-1,
		nil,
	))

	counts, total, known, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{{PoolKeyHash: poolKey}},
		nil,
	)
	require.NoError(t, err)
	require.True(t, known)
	assert.Equal(t, uint64(1), counts[string(poolKey)])
	assert.Equal(t, uint64(1), total)
}

// The round-level consequence. seedRewardPrecomputeTimingState places ten
// blocks for the single pool inside performance epoch 2; putting the anchor
// past that epoch removes every one of them from the node's reach.
func TestStakeRewardRoundDeclinedWhenAnchorHidesTheBlockCounts(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	var logs bytes.Buffer
	ls.config.Logger = slog.New(slog.NewTextHandler(&logs, nil))

	require.NoError(t, db.Metadata().SetSyncState(
		mithrilLedgerSlotSyncKey,
		"199",
		nil,
	))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(txn, 4, 1_200, 1_200, true)
	require.NoError(t, err)
	require.False(
		t,
		ok,
		"a round whose performance epoch cannot be counted must be declined, "+
			"not distributed as zero",
	)
	require.Nil(t, app)
	assert.Contains(t, logs.String(), "no block counts for the performance epoch")
}

// A recorded anchor sits at or above slot 0 and so covers epoch 0, the
// performance epoch of both bootstrap rounds. Those rounds must still run:
// they distribute no pool or account rewards but do move the ADA pots, and
// declining one would leave treasury and reserves at their genesis values for
// the life of the chain. They are safe because epoch 0's mark snapshot holds
// no pools, and an empty pool set is answered before the anchor is consulted;
// the reference agrees that zero rather than unknown is the answer there,
// since NEWEPOCH's initialRules construct the genesis state with BlocksMade
// Map.empty. This pins that, rather than proving a fix.
func TestBootstrapStakeRewardRoundSurvivesAMithrilAnchor(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetEpoch(
		0, 0, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	pparamsCbor, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		pparamsCbor, 0, 0, eras.ShelleyEraDesc.Id, nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        0,
		Reserves:     100_000_000,
		CapturedSlot: 0,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:           0,
		SnapshotType:    "mark",
		CapturedSlot:    0,
		BoundarySlot:    0,
		ProtocolVersion: 7,
	}, nil))
	require.NoError(t, meta.SetSyncState(
		mithrilLedgerSlotSyncKey,
		"50",
		nil,
	))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(txn, 1, 100, 100, true)
	require.NoError(t, err)
	require.True(
		t,
		ok,
		"an anchor covers epoch 0 by construction; the bootstrap round still "+
			"has to move the pots",
	)
	require.NotNil(t, app)
	assert.True(t, app.epochs.bootstrap)
	assert.Empty(t, app.poolOutputs)
	assert.Empty(t, app.accountOutputs)
}

// The imported counts are not an approximation: for the same epoch they
// reproduce the distribution the node would have computed from its own block
// history, pool output for pool output and account output for account output.
func TestStakeRewardRoundFromImportedBlockCountsMatchesObservedHistory(
	t *testing.T,
) {
	observed := stakeRewardApplicationForTest(t, false)
	imported := stakeRewardApplicationForTest(t, true)

	require.Len(t, observed.poolOutputs, 1)
	require.Len(t, imported.poolOutputs, len(observed.poolOutputs))
	for i, want := range observed.poolOutputs {
		got := imported.poolOutputs[i]
		assert.Equal(t, want.PoolKeyHash, got.PoolKeyHash)
		assert.Equal(t, want.TotalReward, got.TotalReward)
		assert.Equal(t, want.LeaderReward, got.LeaderReward)
		assert.Equal(
			t,
			want.ApparentPerformance.String(),
			got.ApparentPerformance.String(),
		)
	}
	require.NotEmpty(t, observed.accountOutputs)
	require.Len(t, imported.accountOutputs, len(observed.accountOutputs))
	for i, want := range observed.accountOutputs {
		got := imported.accountOutputs[i]
		assert.Equal(t, want.StakingKey, got.StakingKey)
		assert.Equal(t, want.RewardType, got.RewardType)
		assert.Equal(t, want.Amount, got.Amount)
	}
	assert.Positive(t, uint64(observed.poolOutputs[0].TotalReward))
	assert.Equal(t, observed.effectiveRewards, imported.effectiveRewards)
}

// stakeRewardApplicationForTest computes the epoch 4 reward round twice over
// the same state: once from the ten blocks the node itself applied in epoch 2,
// and once with those blocks hidden behind a trust anchor and supplied instead
// as the snapshot's imported counts for the same epoch.
func stakeRewardApplicationForTest(
	t *testing.T,
	fromImport bool,
) *stakeRewardApplication {
	t.Helper()
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()
	if fromImport {
		require.NoError(t, meta.SetSyncState(
			mithrilLedgerSlotSyncKey,
			"199",
			nil,
		))
		require.NoError(t, meta.SaveImportedPoolBlockCounts(
			[]models.ImportedPoolBlockCount{
				{
					Epoch:          2,
					PoolKeyHash:    rewardCalcHash(0x4a),
					BlocksProduced: 10,
					CapturedSlot:   199,
				},
			},
			nil,
		))
		require.NoError(t, meta.SaveImportedEpochBlockTotal(2, 10, 199, nil))
	}
	txn := db.Transaction(false)
	t.Cleanup(func() { _ = txn.Rollback() })
	app, ok, err := ls.calculateStakeRewardApplication(txn, 4, 1_200, 1_200, true)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, app)
	return app
}
