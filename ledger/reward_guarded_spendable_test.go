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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file covers dingo #3021: the CIP-0163 reward-crediting guard decision
// (rewardOutputGuarded) must be persisted onto RewardAccountOutput.Guarded,
// the same way Spendable already is, so a reader of reward_account_output
// (the Blockfrost account reward-history endpoint) can tell an uncredited
// guarded row from a credited one without re-deriving activation state and
// inactivity windows itself. It intentionally does not touch any pre-existing
// ledger/*_test.go file; see applyGuardExpiredLeaderScenario in
// reward_calculation_test.go for the sibling test that pins the guard's
// crediting/ADA-conservation behavior this file assumes as already correct.

// guardedRewardScenario identifies the fixture setupGuardedRewardScenario
// seeds: a single pool with an expired reward (leader) account and an active
// member delegator, matching the Task 10 reward-crediting guard fixture.
type guardedRewardScenario struct {
	newEpoch            uint64
	rewardSnapshotEpoch uint64
	potsEpoch           uint64
	boundarySlot        uint64
	poolKey             []byte
	rewardAccount       []byte
	member              []byte
}

// setupGuardedRewardScenario seeds a fresh ledger/database with a single
// pool, an expired reward (leader) account (ExpirationEpoch 1, strictly
// before the reward snapshot epoch 2), and an active member delegator
// (ExpirationEpoch 0/unset). It returns the ledger, database, and scenario
// identity so callers can invoke applyStakeRewards themselves and inspect
// the persisted RewardAccountOutput rows, or roll the reward state back.
func setupGuardedRewardScenario(
	t *testing.T,
	gateEnabled bool,
) (*LedgerState, *database.Database, guardedRewardScenario) {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	ls.config.DelegatorInactivityEnabled = gateEnabled
	meta := db.Metadata()

	sc := guardedRewardScenario{
		newEpoch:            5,
		rewardSnapshotEpoch: 2,
		potsEpoch:           4,
		boundarySlot:        500,
		poolKey:             rewardCalcHash(0x91),
		rewardAccount:       rewardCalcHash(0x92),
		member:              rewardCalcHash(0x93),
	}
	const performanceEpoch = uint64(3)

	var poolID lcommon.PoolKeyHash
	copy(poolID[:], sc.poolKey)

	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	require.NoError(t, meta.SetEpoch(100, performanceEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	require.NoError(t, meta.SetEpoch(200, sc.potsEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			poolID,
			i+1,
			140+i,
			nil,
		))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        sc.potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            sc.rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      sc.rewardSnapshotEpoch,
			PoolKeyHash:                sc.poolKey,
			RewardAccount:              sc.rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         sc.rewardSnapshotEpoch,
			PoolKeyHash:   sc.poolKey,
			CredentialTag: 0,
			StakingKey:    sc.rewardAccount,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         sc.rewardSnapshotEpoch,
			PoolKeyHash:   sc.poolKey,
			CredentialTag: 0,
			StakingKey:    sc.member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	gormDB := rewardCalcGormDB(t, db)
	pool := models.Pool{PoolKeyHash: sc.poolKey}
	require.NoError(t, gormDB.Create(&pool).Error)
	require.NoError(t, gormDB.Create(&models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: sc.poolKey,
		AddedSlot:   0,
	}).Error)
	// The reward (leader) account is expired as of the snapshot epoch (2):
	// ExpirationEpoch 1 is nonzero and strictly before 2.
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:      sc.rewardAccount,
		Pool:            sc.poolKey,
		Active:          true,
		ExpirationEpoch: 1,
	}))
	// The member delegator is active (unset expiration).
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:      sc.member,
		Pool:            sc.poolKey,
		Active:          true,
		ExpirationEpoch: 0,
	}))
	rewardCalcSeedStakeCert(
		t, db, 1, sc.rewardAccount, 0, 250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t, db, 2, sc.member, 0, 250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)

	return ls, db, sc
}

// rewardAccountOutputsByStakingKey indexes a slice of RewardAccountOutput
// rows by staking key for lookups in the tests below.
func rewardAccountOutputsByStakingKey(
	outputs []*models.RewardAccountOutput,
) map[string]*models.RewardAccountOutput {
	ret := make(map[string]*models.RewardAccountOutput, len(outputs))
	for _, output := range outputs {
		ret[string(output.StakingKey)] = output
	}
	return ret
}

// TestApplyStakeRewardsPersistsGuardedFlagOnAccountOutput is the dingo #3021
// persistence test: when the delegator-inactivity gate is on, an expired
// reward account's output row must be persisted with Guarded = true even
// though it remains Spendable = true (the guard is a distinct reason for
// withholding the reward from spendable/deregistered), and the active
// member's row must be Guarded = false. The persisted flag must line up with
// what was actually credited to the account balance.
func TestApplyStakeRewardsPersistsGuardedFlagOnAccountOutput(t *testing.T) {
	ls, db, sc := setupGuardedRewardScenario(t, true)
	meta := db.Metadata()

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, sc.newEpoch, sc.boundarySlot)
	}))

	outputs, err := meta.GetRewardAccountOutputs(sc.rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, outputs, 2)
	byKey := rewardAccountOutputsByStakingKey(outputs)

	leader := byKey[string(sc.rewardAccount)]
	require.NotNil(t, leader)
	assert.True(
		t, leader.Spendable,
		"leader row remains marked spendable: the guard is a separate reason for withholding than deregistration",
	)
	assert.True(
		t, leader.Guarded,
		"an expired reward account's output must be persisted as guarded",
	)

	member := byKey[string(sc.member)]
	require.NotNil(t, member)
	assert.True(t, member.Spendable)
	assert.False(
		t, member.Guarded,
		"the active member's output must not be guarded",
	)

	// The persisted flag must line up with what was actually credited.
	rewardOwner, err := db.GetAccountByCredential(0, sc.rewardAccount, true, nil)
	require.NoError(t, err)
	assert.Equal(
		t, uint64(0), uint64(rewardOwner.Reward),
		"the guarded leader must not have been credited",
	)
	rewardMember, err := db.GetAccountByCredential(0, sc.member, true, nil)
	require.NoError(t, err)
	assert.Greater(
		t, uint64(rewardMember.Reward), uint64(0),
		"the unguarded member must have been credited",
	)
}

// TestApplyStakeRewardsGateOffNeverSetsGuardedFlag pins that Guarded stays
// false for every row when the delegator-inactivity gate is off, keeping
// that path byte-identical to pre-CIP behavior: the same "expired" account
// data is credited normally, exactly as
// TestApplyStakeRewardsGuardsExpiredRewardAccount's gate-off case already
// pins for the crediting/ADA-conservation side.
func TestApplyStakeRewardsGateOffNeverSetsGuardedFlag(t *testing.T) {
	ls, db, sc := setupGuardedRewardScenario(t, false)
	meta := db.Metadata()

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, sc.newEpoch, sc.boundarySlot)
	}))

	outputs, err := meta.GetRewardAccountOutputs(sc.rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, outputs, 2)
	for _, output := range outputs {
		assert.False(
			t, output.Guarded,
			"gate off must never persist guarded=true (staking key %x)",
			output.StakingKey,
		)
	}

	rewardOwner, err := db.GetAccountByCredential(0, sc.rewardAccount, true, nil)
	require.NoError(t, err)
	assert.Greater(
		t, uint64(rewardOwner.Reward), uint64(0),
		"gate off must still credit the nominally-expired reward account",
	)
}

// TestPrecomputedRewardApplicationPersistsGuardedFlag covers the reused
// precompute path: precomputeStakeRewardsAfterEpochTransition (and the
// synchronous precomputeStakeRewards helper used here) persists the pool and
// account output rows before the CIP-0163 guard is ever computed -- the
// guard set is only built inside applyStakeRewardApplication, which the
// precompute path never calls -- so the precomputed rows start with the
// zero-value Guarded = false. This pins that the synchronous application
// path, which reuses that precomputed application instead of recalculating
// (precomputedStakeRewardApplication), still reconciles Guarded against the
// current guard set and re-saves the row rather than leaving the
// precompute's stale false in place.
func TestPrecomputedRewardApplicationPersistsGuardedFlag(t *testing.T) {
	ls, db, sc := setupGuardedRewardScenario(t, true)
	meta := db.Metadata()

	precomputeTxn := db.Transaction(true)
	require.NoError(t, precomputeTxn.Do(func(txn *database.Txn) error {
		return ls.precomputeStakeRewards(
			txn, sc.newEpoch, sc.boundarySlot, sc.boundarySlot,
		)
	}))

	preOutputs, err := meta.GetRewardAccountOutputs(sc.rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, preOutputs, 2)
	for _, output := range preOutputs {
		require.False(
			t, output.Guarded,
			"precompute runs before the guard is known and must not set it",
		)
	}

	applyTxn := db.Transaction(true)
	require.NoError(t, applyTxn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, sc.newEpoch, sc.boundarySlot)
	}))

	outputs, err := meta.GetRewardAccountOutputs(sc.rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	byKey := rewardAccountOutputsByStakingKey(outputs)
	leader := byKey[string(sc.rewardAccount)]
	require.NotNil(t, leader)
	assert.True(
		t, leader.Guarded,
		"reusing a precomputed application must still persist the guard decision",
	)
	member := byKey[string(sc.member)]
	require.NotNil(t, member)
	assert.False(t, member.Guarded)

	rewardOwner, err := db.GetAccountByCredential(0, sc.rewardAccount, true, nil)
	require.NoError(t, err)
	assert.Equal(
		t, uint64(0), uint64(rewardOwner.Reward),
		"the guard must still prevent crediting on the reused precompute path",
	)
}

// TestApplyStakeRewardsGuardedFlagRecomputesAfterRollback is the rollback
// correctness test for dingo #3021, in the same spirit as
// TestDeleteRewardStateAfterSlotUnaffectedByAPIModeRetention
// (ledger/snapshot/rotation_reward_retention_mode_test.go) pins retention:
// DeleteRewardStateAfterSlot removes the previously-persisted guarded row
// (it is an unconditional slot-based delete; Guarded plays no special role),
// and a subsequent recomputation must not carry the guard decision forward
// as though it were still true. It re-derives Guarded fresh from current
// state, so an account renewed on the surviving fork before the recompute
// runs (as recomputeAccountExpirationsAfterRollback would do) is credited
// and its row is persisted with Guarded = false, not stuck at the
// pre-rollback true.
func TestApplyStakeRewardsGuardedFlagRecomputesAfterRollback(t *testing.T) {
	ls, db, sc := setupGuardedRewardScenario(t, true)
	meta := db.Metadata()

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, sc.newEpoch, sc.boundarySlot)
	}))

	before, err := meta.GetRewardAccountOutputs(sc.rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	beforeByKey := rewardAccountOutputsByStakingKey(before)
	beforeLeader := beforeByKey[string(sc.rewardAccount)]
	require.NotNil(t, beforeLeader)
	require.True(
		t, beforeLeader.Guarded,
		"sanity: the guard must apply before the rollback",
	)

	// Roll back to a slot between the snapshot/pool-input capture (100) and
	// the reward output boundary (sc.boundarySlot, 500): this removes the
	// reward_ada_pots row (captured_slot 300) and both pool/account output
	// rows (captured_slot/boundary_slot 500), while leaving the reward
	// snapshot and pool/stake inputs (captured_slot/boundary_slot 100)
	// untouched -- the same partial removal an epoch-boundary-crossing
	// rollback produces in practice.
	const rollbackSlot = uint64(200)
	require.NoError(t, meta.DeleteRewardStateAfterSlot(rollbackSlot, nil))

	rolledBack, err := meta.GetRewardAccountOutputs(sc.rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Empty(
		t, rolledBack,
		"rollback must remove the previously guarded rows regardless of Guarded",
	)
	pots, err := meta.GetRewardAdaPots(sc.potsEpoch, nil)
	require.NoError(t, err)
	require.Nil(
		t, pots,
		"rollback must remove the reward ADA pots row above the rollback slot",
	)

	// Simulate the surviving fork renewing the reward account before the
	// recompute runs (what recomputeAccountExpirationsAfterRollback does in
	// practice): it is no longer expired as of the snapshot epoch.
	require.NoError(t, rewardCalcGormDB(t, db).
		Model(&models.Account{}).
		Where("credential_tag = ? AND staking_key = ?", 0, sc.rewardAccount).
		Update("expiration_epoch", 0).Error)

	// Reseed only what the rollback removed; the snapshot and pool/stake
	// inputs survived untouched and are reused as-is.
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        sc.potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))

	recomputeTxn := db.Transaction(true)
	require.NoError(t, recomputeTxn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, sc.newEpoch, sc.boundarySlot)
	}))

	after, err := meta.GetRewardAccountOutputs(sc.rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, after, 2)
	afterByKey := rewardAccountOutputsByStakingKey(after)
	afterLeader := afterByKey[string(sc.rewardAccount)]
	require.NotNil(t, afterLeader)
	assert.False(
		t, afterLeader.Guarded,
		"recompute after rollback must not carry forward a stale guarded=true once the account was renewed",
	)

	rewardOwner, err := db.GetAccountByCredential(0, sc.rewardAccount, true, nil)
	require.NoError(t, err)
	assert.Greater(
		t, uint64(rewardOwner.Reward), uint64(0),
		"the renewed reward account must be credited on recompute",
	)
}
