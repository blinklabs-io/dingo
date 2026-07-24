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

// This file is Task 11 of CIP-0163 Mechanism B: an end-to-end lifecycle test
// for reward-account inactivity expiry. CIP-0163 is ahead of cardano-ledger,
// so there is no cardano-node DevNet reference for this behavior -- this test
// is the primary integration validation that the per-layer mechanisms (Tasks
// 1-10) compose correctly on a single reward account as it moves through its
// full lifecycle.
//
// Scope note: this is a COMPOSED lifecycle test, not a full multi-epoch block
// replay. There is no harness in this package that drives block-by-block
// chain application through real epoch rollovers with certificate-bearing
// blocks (the closest candidates -- newRenewTestLedger in
// account_expiry_renew_test.go and newRewardCalculationTestLedger in
// reward_calculation_test.go -- both call the relevant LedgerState methods
// directly against a seeded database rather than replaying blocks). This test
// reuses those exact harnesses and calls the same production methods
// (renewWitnessedAccountExpirations, GetStakeByPoolsAtSlot,
// applyStakeRewards) that the per-layer tests already exercise individually,
// but drives them in lifecycle order against a single account/pool so the
// composition itself -- not just each layer in isolation -- is under test.

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// Epoch labels for the lifecycle. DelegatorInactivity is short (2 epochs) so
// the whole lifecycle -- register, expire, and reactivate -- fits in a small
// number of epoch labels. The reward-crediting stage's epoch offsets follow
// stakeRewardEpochsForNewEpoch (snapshot = newEpoch-3, performance =
// newEpoch-2, pots = newEpoch-1), chosen so its snapshot epoch lines up with
// delegatorInactivityE2EExcludeEpoch: the same point in the timeline where
// the stake-snapshot query (Task 8) excludes this account is also the
// snapshot epoch the reward-crediting guard (Task 10) judges it against.
const (
	delegatorInactivityE2EInactivity = uint64(2)
	// registerEpoch: the credential registers and delegates (witnesses).
	delegatorInactivityE2ERegisterEpoch = uint64(0)
	// includeEpoch: still active (expiration 2 >= 1); gate genuinely on
	// (nonzero) so this is not the "gate off" sentinel.
	delegatorInactivityE2EIncludeEpoch = uint64(1)
	// excludeEpoch: past expiration (2 < 3): now expired.
	delegatorInactivityE2EExcludeEpoch = uint64(3)
	// Reward-crediting guard epochs: newEpoch 6 -> snapshot 3 (matches
	// excludeEpoch), performance 4, pots 5.
	delegatorInactivityE2ERewardNewEpoch   = uint64(6)
	delegatorInactivityE2ERewardSnapshotEp = uint64(3)
	delegatorInactivityE2EPerformanceEpoch = uint64(4)
	delegatorInactivityE2EPotsEpoch        = uint64(5)
	delegatorInactivityE2EBoundarySlot     = uint64(600)
	// rewitnessEpoch: a withdrawal is witnessed once already expired.
	delegatorInactivityE2ERewitnessEpoch = uint64(3)
	// reincludeEpoch: active again (new expiration 5 >= 4).
	delegatorInactivityE2EReincludeEpoch = uint64(4)
	// querySlot is held fixed across every stake-snapshot query in this test:
	// only the CIP-0163 epoch argument changes between stages, not the chain
	// slot, since this is a composed test rather than a real multi-slot
	// chain replay.
	delegatorInactivityE2EQuerySlot = uint64(1000)
)

// delegatorInactivityLifecycleResult captures the observable state at each
// stage of the CIP-0163 lifecycle so the gate-on and gate-off assertions can
// be written against a single shared run.
type delegatorInactivityLifecycleResult struct {
	expirationAfterRegister  uint64
	stakeAtInclude           uint64
	delegatorsAtInclude      uint64
	stakeAtExclude           uint64
	delegatorsAtExclude      uint64
	leaderRewardAfterGuard   uint64
	memberRewardAfterGuard   uint64
	expirationAfterRewitness uint64
	stakeAtReinclude         uint64
	delegatorsAtReinclude    uint64
}

// e2eExpiryArg mirrors snapshot.Manager.expiryEpoch: callers pass the epoch
// itself as the CIP-0163 gate argument when the gate is enabled, or the
// sentinel 0 (gate off, byte-identical to pre-CIP behavior) when it is not.
func e2eExpiryArg(gateEnabled bool, epoch uint64) uint64 {
	if !gateEnabled {
		return 0
	}
	return epoch
}

// runDelegatorInactivityLifecycleScenario drives a single reward-account
// credential through the full CIP-0163 lifecycle against one shared
// LedgerState/database:
//
//  1. Register+delegate (witness): create the account row the same way
//     block application would after a registration+delegation certificate,
//     then call renewWitnessedAccountExpirations (the same hook
//     ledger/delta.go wires into block application) as if a
//     StakeRegistrationDelegationCertificate had witnessed it. Query the
//     Task 8 stake-snapshot aggregation (GetStakeByPoolsAtSlot) to confirm
//     the delegated stake is included.
//  2. Advance with no further witness past expiration: query the same
//     aggregation at a later epoch to confirm the now-expired credential's
//     stake is excluded, then run the Task 10 reward-crediting guard
//     (applyStakeRewards) against a second credential carrying the same
//     expiration to confirm an account in this same expired state is not
//     credited (see the leaderCred comment below for why it is a separate
//     credential rather than a literal reuse of cred).
//  3. Witness again (a reward withdrawal): call
//     renewWitnessedAccountExpirations again to confirm the expiration is
//     renewed forward, then re-query the stake snapshot to confirm the
//     credential is included again.
//
// gateEnabled controls both the renewal hook and the query arguments
// (mirroring how the snapshot manager derives its expiryEpoch argument), so
// calling this twice with true and false produces the gate-on lifecycle and
// the gate-off baseline from the exact same sequence of operations.
func runDelegatorInactivityLifecycleScenario(
	t *testing.T,
	gateEnabled bool,
) delegatorInactivityLifecycleResult {
	t.Helper()
	var res delegatorInactivityLifecycleResult

	ls, db := newRewardCalculationTestLedger(t)
	ls.config.DelegatorInactivityEnabled = gateEnabled
	ls.config.DelegatorInactivity = delegatorInactivityE2EInactivity
	meta := db.Metadata()
	gormDB := rewardCalcGormDB(t, db)

	poolKey := rewardCalcHash(0x91)
	cred := renewTestCred(0x92)
	member := rewardCalcHash(0x93)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	pool := models.Pool{PoolKeyHash: poolKey}
	require.NoError(t, gormDB.Create(&pool).Error)
	require.NoError(t, gormDB.Create(&models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: poolKey,
		AddedSlot:   0,
	}).Error)

	// ---- Stage 1: register + delegate (witness) ----
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    cred,
		CredentialTag: 0,
		Pool:          poolKey,
		Active:        true,
	}))
	require.NoError(t, gormDB.Create(&models.Utxo{
		TxId:       bytes.Repeat([]byte{0x71}, 32),
		OutputIdx:  0,
		StakingKey: cred,
		Amount:     1_000_000,
		AddedSlot:  0,
	}).Error)

	regDelegCred := lcommon.Credential{
		CredType:   0,
		Credential: lcommon.NewBlake2b224(cred),
	}
	regDelegTx := mockledger.NewTransactionBuilder()
	regDelegTx.WithValid(true)
	regDelegTx.WithCertificates(&lcommon.StakeRegistrationDelegationCertificate{
		StakeCredential: regDelegCred,
		PoolKeyHash:     poolID,
	})
	runRenew(t, ls, db, delegatorInactivityE2ERegisterEpoch, regDelegTx)

	acct, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	res.expirationAfterRegister = acct.ExpirationEpoch

	stakes, delegators, err := meta.GetStakeByPoolsAtSlot(
		[][]byte{poolKey},
		delegatorInactivityE2EQuerySlot,
		e2eExpiryArg(gateEnabled, delegatorInactivityE2EIncludeEpoch),
		delegatorInactivityE2EInactivity,
		nil,
	)
	require.NoError(t, err)
	res.stakeAtInclude = stakes[string(poolKey)]
	res.delegatorsAtInclude = delegators[string(poolKey)]

	// ---- Stage 2: advance with no further witness past expiration ----
	stakes, delegators, err = meta.GetStakeByPoolsAtSlot(
		[][]byte{poolKey},
		delegatorInactivityE2EQuerySlot,
		e2eExpiryArg(gateEnabled, delegatorInactivityE2EExcludeEpoch),
		delegatorInactivityE2EInactivity,
		nil,
	)
	require.NoError(t, err)
	res.stakeAtExclude = stakes[string(poolKey)]
	res.delegatorsAtExclude = delegators[string(poolKey)]

	// "Not credited": a second credential (leaderCred), carrying the exact
	// expiration res.expirationAfterRegister just observed on cred, plays the
	// pool's own reward (leader) account for the Task 10 reward-crediting
	// guard via applyStakeRewards, at a snapshot epoch matching excludeEpoch.
	// It must be a separate credential rather than reusing cred: seeding a
	// StakeRegistration certificate below (rewardCalcSeedStakeCert) gives the
	// credential real certificate history, which would make the Task 8
	// account-fallback CTE's "no certificate history" guard in
	// stakequery.go's delegationFallbackBlockTables exclude cred from its own
	// later stake-snapshot queries (stage 3's reinclusion check) -- an
	// artifact of this test harness, not of the CIP-0163 mechanism under
	// test. This is otherwise the same scaffolding shape as
	// TestApplyStakeRewardsGuardsExpiredRewardAccount's
	// applyGuardExpiredLeaderScenario helper (reused pattern, not a new
	// mechanism): pparams, performance/pots epochs, ADA pots, the reward
	// snapshot row, and the pool/stake reward inputs the precompute step
	// reads.
	leaderCred := rewardCalcHash(0x94)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:      leaderCred,
		CredentialTag:   0,
		Pool:            poolKey,
		Active:          true,
		ExpirationEpoch: res.expirationAfterRegister,
	}))

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
	require.NoError(t, meta.SetEpoch(
		100, delegatorInactivityE2EPerformanceEpoch,
		nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	require.NoError(t, meta.SetEpoch(
		200, delegatorInactivityE2EPotsEpoch,
		nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			poolID, i+1, 140+i, nil,
		))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor, 100, delegatorInactivityE2EPerformanceEpoch,
		eras.ShelleyEraDesc.Id, nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        delegatorInactivityE2EPotsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            delegatorInactivityE2ERewardSnapshotEp,
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
			Epoch:                      delegatorInactivityE2ERewardSnapshotEp,
			PoolKeyHash:                poolKey,
			RewardAccount:              leaderCred,
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
			Epoch:         delegatorInactivityE2ERewardSnapshotEp,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    leaderCred,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         delegatorInactivityE2ERewardSnapshotEp,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    member,
		CredentialTag: 0,
		Pool:          poolKey,
		Active:        true,
	}))
	rewardCalcSeedStakeCert(
		t, db, 1, leaderCred, 0, 250, uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t, db, 2, member, 0, 250, uint(lcommon.CertificateTypeStakeRegistration),
	)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(
			txn,
			delegatorInactivityE2ERewardNewEpoch,
			delegatorInactivityE2EBoundarySlot,
		)
	}))

	rewardOwner, err := db.GetAccountByCredential(0, leaderCred, true, nil)
	require.NoError(t, err)
	res.leaderRewardAfterGuard = uint64(rewardOwner.Reward)
	rewardMember, err := db.GetAccountByCredential(0, member, true, nil)
	require.NoError(t, err)
	res.memberRewardAfterGuard = uint64(rewardMember.Reward)

	// ---- Stage 3: witness again (withdrawal) -> renewed + reactivated ----
	rewardAddr := renewTestRewardAddress(t, 0xE1, 0x92) // same fill byte as cred
	withdrawTx := mockledger.NewTransactionBuilder()
	withdrawTx.WithValid(true)
	withdrawTx.WithWithdrawals(map[*lcommon.Address]uint64{rewardAddr: 1})
	runRenew(t, ls, db, delegatorInactivityE2ERewitnessEpoch, withdrawTx)

	acct, err = db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	res.expirationAfterRewitness = acct.ExpirationEpoch

	stakes, delegators, err = meta.GetStakeByPoolsAtSlot(
		[][]byte{poolKey},
		delegatorInactivityE2EQuerySlot,
		e2eExpiryArg(gateEnabled, delegatorInactivityE2EReincludeEpoch),
		delegatorInactivityE2EInactivity,
		nil,
	)
	require.NoError(t, err)
	res.stakeAtReinclude = stakes[string(poolKey)]
	res.delegatorsAtReinclude = delegators[string(poolKey)]

	return res
}

// TestDelegatorInactivityEndToEnd is the CIP-0163 Mechanism B Task 11
// end-to-end lifecycle test. See the file doc comment for scope (composed
// lifecycle, not full block replay).
func TestDelegatorInactivityEndToEnd(t *testing.T) {
	t.Run("GateOn", testDelegatorInactivityEndToEndGateOn)
	t.Run("GateOff", testDelegatorInactivityEndToEndGateOff)
}

// testDelegatorInactivityEndToEndGateOn walks a reward-account credential
// through register -> included -> expire -> excluded+not-credited ->
// witness -> renewed+included with the CIP-0163 gate on.
func testDelegatorInactivityEndToEndGateOn(t *testing.T) {
	res := runDelegatorInactivityLifecycleScenario(t, true)

	require.Equal(t,
		delegatorInactivityE2ERegisterEpoch+delegatorInactivityE2EInactivity,
		res.expirationAfterRegister,
		"registration+delegation witness must set the inactivity window",
	)
	require.Equal(t, uint64(1_000_000), res.stakeAtInclude,
		"newly delegated stake must be included while active")
	require.Equal(t, uint64(1), res.delegatorsAtInclude)

	require.Equal(t, uint64(0), res.stakeAtExclude,
		"expired delegator stake must be excluded from the pool snapshot")
	require.Equal(t, uint64(0), res.delegatorsAtExclude)

	require.Equal(t, uint64(0), res.leaderRewardAfterGuard,
		"expired reward account must not be credited")
	require.Greater(t, res.memberRewardAfterGuard, uint64(0),
		"active member must still be credited")

	require.Equal(t,
		delegatorInactivityE2ERewitnessEpoch+delegatorInactivityE2EInactivity,
		res.expirationAfterRewitness,
		"a withdrawal witness must renew the expiration forward",
	)
	require.Equal(t, uint64(1_000_000), res.stakeAtReinclude,
		"reactivated account's stake must be included again")
	require.Equal(t, uint64(1), res.delegatorsAtReinclude)
}

// testDelegatorInactivityEndToEndGateOff runs the identical sequence with the
// CIP-0163 gate off: the expiration is never set and the credential's stake
// stays included and credited throughout, matching pre-CIP behavior.
func testDelegatorInactivityEndToEndGateOff(t *testing.T) {
	res := runDelegatorInactivityLifecycleScenario(t, false)

	require.Equal(t, uint64(0), res.expirationAfterRegister,
		"gate off must never set an expiration")
	require.Equal(t, uint64(1_000_000), res.stakeAtInclude)
	require.Equal(t, uint64(1), res.delegatorsAtInclude)

	// Baseline unchanged: the same "advance past expiration" point that
	// excludes with the gate on must still include with the gate off.
	require.Equal(t, uint64(1_000_000), res.stakeAtExclude,
		"gate off must keep the delegator included at every point in the timeline")
	require.Equal(t, uint64(1), res.delegatorsAtExclude)

	require.Greater(t, res.leaderRewardAfterGuard, uint64(0),
		"gate off must credit the reward account (pre-CIP behavior)")
	require.Greater(t, res.memberRewardAfterGuard, uint64(0))

	require.Equal(t, uint64(0), res.expirationAfterRewitness,
		"gate off must never set an expiration, even after another witness")
	require.Equal(t, uint64(1_000_000), res.stakeAtReinclude)
	require.Equal(t, uint64(1), res.delegatorsAtReinclude)
}
