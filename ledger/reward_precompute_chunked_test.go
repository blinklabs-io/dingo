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
	"encoding/binary"
	"math/big"
	"sort"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// chunkedFixtureCredential deterministically derives a unique 28-byte
// credential or pool-key hash from a domain tag and index, so a multi-pool
// fixture can mint as many distinct hashes as it needs without collisions.
// runChunkedStakeRewardPrecompute runs the chunked precompute for one round
// and drops the handled flag.
func (ls *LedgerState) runChunkedStakeRewardPrecompute(
	newEpoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
) error {
	_, err := ls.runChunkedStakeRewardPrecomputeRound(
		newEpoch, capturedSlot, boundarySlot,
	)
	return err
}

func chunkedFixtureCredential(domain byte, index uint64) []byte {
	hash := make([]byte, 28)
	hash[0] = domain
	binary.BigEndian.PutUint64(hash[20:], index)
	return hash
}

// seedMultiPoolRewardPrecomputeFixture writes a reward round with poolCount
// pools, each with delegatorsPerPool ordinary member delegators plus its
// owner, directly onto the metadata store -- the same level
// seedRewardPrecomputeTimingInputs seeds its single-pool fixture at. It
// exists so the chunked precompute can be exercised across more than one
// chunk with a small rewardPrecomputeChunkPoolsOverride.
func seedMultiPoolRewardPrecomputeFixture(
	t *testing.T,
	poolCount, delegatorsPerPool int,
	protocolMajor uint,
) (*LedgerState, *database.Database) {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	seedMultiPoolRewardInputs(
		t,
		db,
		poolCount,
		delegatorsPerPool,
		protocolMajor,
	)
	return ls, db
}

// seedMultiPoolRewardInputs writes seedMultiPoolRewardPrecomputeFixture's
// reward round into db, on any metadata backend.
func seedMultiPoolRewardInputs(
	t *testing.T,
	db *database.Database,
	poolCount, delegatorsPerPool int,
	protocolMajor uint,
) {
	t.Helper()
	meta := db.Metadata()

	const (
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
	)

	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(1, 10),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    protocolMajor,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	require.NoError(t, meta.SetEpoch(
		100, performanceEpoch, nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	require.NoError(t, meta.SetEpoch(
		200, potsEpoch, nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, 1_000, nil,
	))
	require.NoError(t, db.SetPParams(
		pparamsCbor, 100, performanceEpoch, eras.ShelleyEraDesc.Id, nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Fees:         1_000,
		CapturedSlot: 200,
	}, nil))

	var poolInputs []*models.RewardPoolInput
	var stakeInputs []*models.RewardStakeInput
	var totalActiveStake uint64
	var totalDelegators uint64

	for p := range poolCount {
		poolKey := chunkedFixtureCredential(0x40, uint64(p)+1)
		rewardAccount := chunkedFixtureCredential(0x50, uint64(p)+1)
		var poolID lcommon.PoolKeyHash
		copy(poolID[:], poolKey)
		require.NoError(t, db.UpdatePoolOpCertSequence(poolID, 1, 140, nil))
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey: rewardAccount,
			Active:     true,
		}))

		ownerStake := uint64(500)
		delegatedStake := ownerStake
		stakeInputs = append(stakeInputs, &models.RewardStakeInput{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			Stake:         types.Uint64(ownerStake),
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		})
		totalDelegators++

		for d := range delegatorsPerPool {
			member := chunkedFixtureCredential(
				0x60, uint64(p)*uint64(delegatorsPerPool)+uint64(d)+1,
			)
			require.NoError(t, db.CreateAccount(nil, &models.Account{
				StakingKey: member,
				Active:     true,
			}))
			stake := uint64(100 + d*7)
			stakeInputs = append(stakeInputs, &models.RewardStakeInput{
				Epoch:         rewardSnapshotEpoch,
				PoolKeyHash:   poolKey,
				CredentialTag: 0,
				StakingKey:    member,
				Stake:         types.Uint64(stake),
				Registered:    true,
				CapturedSlot:  100,
				BoundarySlot:  100,
			})
			delegatedStake += stake
			totalDelegators++
		}

		poolInputs = append(poolInputs, &models.RewardPoolInput{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin: &types.Rat{
				Rat: big.NewRat(int64(p%10), 100),
			},
			Pledge:         types.Uint64(ownerStake),
			Cost:           types.Uint64(1),
			DelegatedStake: types.Uint64(delegatedStake),
			OwnerStake:     types.Uint64(ownerStake),
			DelegatorCount: uint64(delegatorsPerPool) + 1,
			CapturedSlot:   100,
			BoundarySlot:   100,
		})
		totalActiveStake += delegatedStake
	}
	// GetRewardPoolInputs returns rows ordered by pool_key_hash ascending;
	// keep the in-memory fixture in the same order so index-based assertions
	// (chunk boundaries, "pool N of the batch") read naturally.
	sort.Slice(poolInputs, func(i, j int) bool {
		return string(
			poolInputs[i].PoolKeyHash,
		) < string(
			poolInputs[j].PoolKeyHash,
		)
	})

	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: types.Uint64(totalActiveStake),
		TotalPoolCount:   uint64(poolCount),
		TotalDelegators:  totalDelegators,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  protocolMajor,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs(poolInputs, nil))
	require.NoError(t, meta.SaveRewardStakeInputs(stakeInputs, nil))
}

// rewardPrecomputeOutputsSnapshot is the comparable, order-independent shape
// of a chunked or monolithic precompute's persisted result, used to prove the
// two are exactly equal regardless of chunk size.
type rewardPrecomputeOutputsSnapshot struct {
	pots           models.RewardAdaPots
	poolOutputs    map[string]models.RewardPoolOutput
	accountOutputs map[string]models.RewardAccountOutput
}

func snapshotRewardPrecomputeOutputs(
	t *testing.T,
	db *database.Database,
	rewardSnapshotEpoch, potsEpoch uint64,
) rewardPrecomputeOutputsSnapshot {
	t.Helper()
	meta := db.Metadata()

	pots, err := meta.GetRewardAdaPots(potsEpoch, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	poolByKey := make(map[string]models.RewardPoolOutput, len(poolOutputs))
	for _, output := range poolOutputs {
		cp := *output
		cp.ID = 0
		poolByKey[string(output.PoolKeyHash)] = cp
	}

	accountOutputs, err := meta.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	accountByKey := make(
		map[string]models.RewardAccountOutput,
		len(accountOutputs),
	)
	for _, output := range accountOutputs {
		cp := *output
		cp.ID = 0
		key := strconv.Itoa(int(output.CredentialTag)) + ":" +
			string(output.StakingKey) + ":" + string(output.PoolKeyHash) +
			":" + output.RewardType
		accountByKey[key] = cp
	}

	potsCopy := *pots
	potsCopy.ID = 0
	return rewardPrecomputeOutputsSnapshot{
		pots:           potsCopy,
		poolOutputs:    poolByKey,
		accountOutputs: accountByKey,
	}
}

// TestChunkedRewardPrecomputeMatchesAcrossChunkSizes proves the chunked
// precompute's persisted result -- pool outputs, account outputs, and the
// reward_ada_pots.Rewards marker -- is exactly the same regardless of how
// many pools are processed per chunk, including a single chunk covering the
// whole pool set (the pre-chunking shape). This is the ledger-level
// counterpart to ledger/rewards' TestChunkedRewardSplitMatchesCalculate: that
// test proves the arithmetic is batch-size invariant, this one proves the
// persisted database state is too.
func TestChunkedRewardPrecomputeMatchesAcrossChunkSizes(t *testing.T) {
	t.Parallel()

	const (
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		eventBoundarySlot   = uint64(200)
	)
	poolCount, delegatorsPerPool := 7, 5

	var reference rewardPrecomputeOutputsSnapshot
	for i, chunkSize := range []int{1, 2, 3, 100} {
		ls, db := seedMultiPoolRewardPrecomputeFixture(
			t, poolCount, delegatorsPerPool, 7,
		)
		ls.rewardPrecomputeChunkPoolsOverride = chunkSize

		require.NoError(t, ls.runChunkedStakeRewardPrecompute(
			rewardSnapshotEpoch+3, eventBoundarySlot, 1_200,
		))

		snapshot := snapshotRewardPrecomputeOutputs(
			t, db, rewardSnapshotEpoch, potsEpoch,
		)
		require.Len(t, snapshot.poolOutputs, poolCount)
		require.Len(
			t, snapshot.accountOutputs, poolCount*(delegatorsPerPool+1),
		)
		if i == 0 {
			reference = snapshot
			continue
		}
		require.Equal(
			t, reference, snapshot,
			"chunk size %d produced a different result than chunk size 1",
			chunkSize,
		)
	}
}

// TestChunkedRewardPrecomputeMatchesMonolithicCalculation proves the
// chunked precompute reproduces exactly what the pre-existing, still-current
// monolithic path (calculateStakeRewardApplication +
// applyStakeRewardApplication, what applyStakeRewards falls back to inline at
// the boundary) persists for the same reward round: every pool output, every
// account output, and the credited account_reward_delta/network_state
// mutations. The rewards-package test proves the arithmetic is equivalent in
// isolation; this proves the ledger-level orchestration around it (which
// pool-input/stake-input rows feed which rewards.Pool, which accounts count
// as active/prefiltered) wires the chunked path to the same real inputs.
func TestChunkedRewardPrecomputeMatchesMonolithicCalculation(t *testing.T) {
	t.Parallel()

	const (
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		newEpoch            = rewardSnapshotEpoch + 3
		capturedSlot        = uint64(200)
		boundarySlot        = uint64(1_200)
	)
	poolCount, delegatorsPerPool := 8, 6

	monolithic, monolithicDB := seedMultiPoolRewardPrecomputeFixture(
		t, poolCount, delegatorsPerPool, 7,
	)
	writeTxn := monolithicDB.Transaction(true)
	require.NoError(t, writeTxn.Do(func(txn *database.Txn) error {
		app, ok, err := monolithic.calculateStakeRewardApplication(
			txn, newEpoch, capturedSlot, boundarySlot, true,
		)
		require.NoError(t, err)
		require.True(t, ok, "fixture must produce a calculable reward round")
		return monolithic.applyStakeRewardApplication(txn, app, boundarySlot)
	}))
	monolithicResult := snapshotRewardPrecomputeOutputs(
		t, monolithicDB, rewardSnapshotEpoch, potsEpoch,
	)

	chunked, chunkedDB := seedMultiPoolRewardPrecomputeFixture(
		t, poolCount, delegatorsPerPool, 7,
	)
	chunked.rewardPrecomputeChunkPoolsOverride = 3
	require.NoError(t, chunked.runChunkedStakeRewardPrecompute(
		newEpoch, capturedSlot, boundarySlot,
	))
	chunkedResult := snapshotRewardPrecomputeOutputs(
		t, chunkedDB, rewardSnapshotEpoch, potsEpoch,
	)

	require.Len(t, monolithicResult.poolOutputs, poolCount)
	require.Equal(t, monolithicResult, chunkedResult)
}

// TestChunkedRewardPrecomputeResumesAfterInterruption proves the resumable
// half of the design: a run stopped partway through (simulating a process
// restart between chunks) picks back up from the persisted cursor on its
// next invocation rather than recomputing already-committed pools, and the
// final result is identical to an uninterrupted run.
func TestChunkedRewardPrecomputeResumesAfterInterruption(t *testing.T) {
	t.Parallel()

	const (
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		eventBoundarySlot   = uint64(200)
	)
	poolCount, delegatorsPerPool := 9, 4

	ls, db := seedMultiPoolRewardPrecomputeFixture(
		t, poolCount, delegatorsPerPool, 7,
	)
	ls.rewardPrecomputeChunkPoolsOverride = 2

	// Drive exactly one chunk step directly and stop, rather than running
	// the full loop: a process crash always lands between two committed
	// transactions, never mid-transaction (the transaction itself is atomic),
	// so calling stakeRewardPrecomputeChunkStep once and abandoning the round
	// is the faithful simulation, not an injected panic.
	round, ok, err := ls.resolveStakeRewardPrecomputeRound(
		rewardSnapshotEpoch+3, eventBoundarySlot, 1_200,
	)
	require.NoError(t, err)
	require.True(t, ok)
	done, err := ls.stakeRewardPrecomputeChunkStep(round)
	require.NoError(t, err)
	require.False(t, done, "fixture must need more than one chunk to complete")

	partialPoolOutputs, err := db.Metadata().GetRewardPoolOutputs(
		rewardSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	require.Len(
		t,
		partialPoolOutputs,
		2,
		"only the first chunk should have committed",
	)

	pots, err := db.Metadata().GetRewardAdaPots(potsEpoch, nil)
	require.NoError(t, err)
	require.Equal(
		t, types.Uint64(0), pots.Rewards,
		"the round is not complete, so its pots.Rewards marker must not be set",
	)

	// Resume: a fresh call (as a restarted process's first attempt would
	// make) must continue from the cursor rather than recomputing pools 1-2.
	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		rewardSnapshotEpoch+3, eventBoundarySlot, 1_200,
	))

	resumed := snapshotRewardPrecomputeOutputs(
		t, db, rewardSnapshotEpoch, potsEpoch,
	)
	require.Len(t, resumed.poolOutputs, poolCount)
	require.Len(
		t, resumed.accountOutputs, poolCount*(delegatorsPerPool+1),
	)

	// An uninterrupted run over a fresh, identically-seeded database must
	// reach the same result.
	uninterrupted, uninterruptedDB := seedMultiPoolRewardPrecomputeFixture(
		t, poolCount, delegatorsPerPool, 7,
	)
	uninterrupted.rewardPrecomputeChunkPoolsOverride = 2
	require.NoError(t, uninterrupted.runChunkedStakeRewardPrecompute(
		rewardSnapshotEpoch+3, eventBoundarySlot, 1_200,
	))
	reference := snapshotRewardPrecomputeOutputs(
		t, uninterruptedDB, rewardSnapshotEpoch, potsEpoch,
	)
	require.Equal(t, reference, resumed)
}

// TestChunkedRewardPrecomputeRestartsOnGenerationChange proves the other
// half of "repairable": a rollback that bumps rewardInputGeneration between
// two chunks must not let the next chunk keep extending the abandoned run,
// and a later resumption attempt must discard the abandoned generation's
// partial rows rather than mixing them with the new one's.
func TestChunkedRewardPrecomputeRestartsOnGenerationChange(t *testing.T) {
	t.Parallel()

	const (
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		eventBoundarySlot   = uint64(200)
	)
	poolCount, delegatorsPerPool := 6, 3

	ls, db := seedMultiPoolRewardPrecomputeFixture(
		t, poolCount, delegatorsPerPool, 7,
	)
	ls.rewardPrecomputeChunkPoolsOverride = 2

	stopped := false
	ls.rewardPrecomputeChunkHook = func(processed, total int) {
		if !stopped {
			stopped = true
			ls.rewardInputGeneration.Add(1)
		}
	}
	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		rewardSnapshotEpoch+3, eventBoundarySlot, 1_200,
	))

	// The bump happened after the first chunk committed and the run then
	// observed it at the top of the second chunk step, so it stopped with
	// exactly the first chunk's rows on disk and no completion marker.
	partial, err := db.Metadata().GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, partial, 2)
	pots, err := db.Metadata().GetRewardAdaPots(potsEpoch, nil)
	require.NoError(t, err)
	require.Equal(t, types.Uint64(0), pots.Rewards)

	// A fresh attempt at the current (post-bump) generation must discard the
	// abandoned generation's rows and produce a complete, correct result --
	// not 2 stale rows plus 4 fresh ones.
	ls.rewardPrecomputeChunkHook = nil
	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		rewardSnapshotEpoch+3, eventBoundarySlot, 1_200,
	))
	final := snapshotRewardPrecomputeOutputs(
		t, db, rewardSnapshotEpoch, potsEpoch,
	)
	require.Len(t, final.poolOutputs, poolCount)
	require.Len(t, final.accountOutputs, poolCount*(delegatorsPerPool+1))
}
