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

package sqlite

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

// seedStakeDelegationCert writes the certs + transaction rows the historical
// stake CTE joins against, plus the typed certificate row itself.
func seedStakeDelegationCert(
	t *testing.T,
	db *gorm.DB,
	txID uint,
	slot uint64,
	stakeKey []byte,
	poolKeyHash []byte,
) {
	t.Helper()
	require.NoError(t, createTestTransaction(db, txID, slot))
	cert := models.Certificate{
		TransactionID: txID,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		Slot:          slot,
	}
	require.NoError(t, db.Create(&cert).Error)
	require.NoError(t, db.Create(&models.StakeDelegation{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		PoolKeyHash:   poolKeyHash,
		AddedSlot:     slot,
		CertificateID: cert.ID,
	}).Error)
}

// seedVoteDelegationCert writes a DRep-only vote-delegation certificate and
// bumps the mutable account.added_slot exactly the way the sqlite certificate
// processor does (see the VoteDelegationCertificate case in transaction.go).
// vote_delegation is neither a stake-delegation nor a registration source, so
// the account-derived fallback rows in the historical CTE must not treat the
// bumped added_slot as registration/delegation evidence.
func seedVoteDelegationCert(
	t *testing.T,
	db *gorm.DB,
	txID uint,
	slot uint64,
	stakeKey []byte,
	drep []byte,
) {
	t.Helper()
	require.NoError(t, createTestTransaction(db, txID, slot))
	cert := models.Certificate{
		TransactionID: txID,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeVoteDelegation),
		Slot:          slot,
	}
	require.NoError(t, db.Create(&cert).Error)
	require.NoError(t, db.Create(&models.VoteDelegation{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Drep:          drep,
		AddedSlot:     slot,
		CertificateID: cert.ID,
	}).Error)
	require.NoError(t, db.Model(&models.Account{}).
		Where("credential_tag = ? AND staking_key = ?", 0, stakeKey).
		Updates(map[string]any{
			"drep":       drep,
			"added_slot": slot,
		}).Error)
}

// TestGetStakeByPoolsAtSlotKeepsCredentialAfterVoteDelegation covers the
// dropped-credential defect in the historical stake CTE's account fallback. A
// Mithril-imported (or Shelley-genesis-staked) credential has no local
// registration certificate, so its registration state is synthesized from the
// live account row. A DRep-only vote delegation bumps the mutable
// account.added_slot past the credential's stake-delegation certificate, which
// used to make latest_delegation.added_slot > latest_registration.added_slot
// false and drop the whole credential — and all of its stake — out of
// active_delegation.
func TestGetStakeByPoolsAtSlotKeepsCredentialAfterVoteDelegation(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	db := store.DB()
	pool := bytes.Repeat([]byte{0xF1}, 28)
	stakeKey := bytes.Repeat([]byte{0x31}, 28)
	drep := bytes.Repeat([]byte{0x71}, 28)

	// Imported account: live row only, no registration certificate history.
	require.NoError(t, db.Create(&models.Account{
		StakingKey:  stakeKey,
		Pool:        pool,
		Active:      true,
		AddedSlot:   10,
		CreatedSlot: 10,
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       bytes.Repeat([]byte{0x61}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     1_000,
		AddedSlot:  20,
	}).Error)
	// On-chain stake delegation at slot 100 (real certificate history).
	seedStakeDelegationCert(t, db, 9001, 100, stakeKey, pool)
	// DRep-only vote delegation at slot 150 bumps account.added_slot to 150.
	seedVoteDelegationCert(t, db, 9002, 150, stakeKey, drep)

	stakes, delegators, err := store.GetStakeByPoolsAtSlot(
		[][]byte{pool}, 200, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1_000), stakes[string(pool)],
		"a DRep-only vote delegation must not drop the credential's stake")
	require.Equal(t, uint64(1), delegators[string(pool)],
		"a DRep-only vote delegation must not drop the credential")
}

// TestGetStakeByPoolsAtSlotKeepsCredentialMutatedAfterSlot covers the second
// face of the same defect: the account fallback's visibility gate. A credential
// whose account row is mutated after the requested slot (here by a DRep-only
// vote delegation) demonstrably existed at that slot — account.created_slot
// proves it — so the mutation must not hide it from a historical
// reconstruction. This is the shape the epoch-boundary fallback capture hits,
// because it reconstructs the boundary after live account state has already
// advanced past it.
func TestGetStakeByPoolsAtSlotKeepsCredentialMutatedAfterSlot(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	db := store.DB()
	pool := bytes.Repeat([]byte{0xF2}, 28)
	stakeKey := bytes.Repeat([]byte{0x32}, 28)
	drep := bytes.Repeat([]byte{0x72}, 28)

	require.NoError(t, db.Create(&models.Account{
		StakingKey:  stakeKey,
		Pool:        pool,
		Active:      true,
		AddedSlot:   10,
		CreatedSlot: 10,
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       bytes.Repeat([]byte{0x62}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     2_000,
		AddedSlot:  20,
	}).Error)
	seedStakeDelegationCert(t, db, 9101, 100, stakeKey, pool)
	// Mutation lands after the reconstruction slot.
	seedVoteDelegationCert(t, db, 9102, 250, stakeKey, drep)

	stakes, delegators, err := store.GetStakeByPoolsAtSlot(
		[][]byte{pool}, 200, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2_000), stakes[string(pool)],
		"an account mutation after the slot must not hide the credential")
	require.Equal(t, uint64(1), delegators[string(pool)],
		"an account mutation after the slot must not hide the credential")
}

// TestGetStakeByPoolsAtSlotFloorsNegativeHistoricalReward covers the
// total_stake unsigned wrap. The historical reward reconstruction subtracts
// every later credit from the live balance; when the journal retains more
// credit than the live balance can account for (a pruned or imported journal),
// the intermediate goes negative and used to be scanned straight into a uint64,
// turning a tiny stake into a near-2^64 one.
func TestGetStakeByPoolsAtSlotFloorsNegativeHistoricalReward(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	db := store.DB()
	pool := bytes.Repeat([]byte{0xF3}, 28)
	stakeKey := bytes.Repeat([]byte{0x33}, 28)

	require.NoError(t, db.Create(&models.Account{
		StakingKey:  stakeKey,
		Pool:        pool,
		Active:      true,
		AddedSlot:   10,
		CreatedSlot: 10,
		Reward:      types.Uint64(10),
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       bytes.Repeat([]byte{0x63}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     5,
		AddedSlot:  20,
	}).Error)
	// A journal credit larger than the live balance: reconstructing slot 100
	// yields 10 - 100 = -90 before flooring.
	require.NoError(t, db.Create(&models.AccountRewardDelta{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		TxHash:        bytes.Repeat([]byte{0x91}, 32),
		Amount:        types.Uint64(100),
		AddedSlot:     200,
		Withdrawal:    false,
	}).Error)

	stakes, delegators, err := store.GetStakeByPoolsAtSlot(
		[][]byte{pool}, 100, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(5), stakes[string(pool)],
		"a negative reconstructed reward must floor at zero, not wrap")
	require.Equal(t, uint64(1), delegators[string(pool)])
}

// TestEpochBoundaryStakeRetainsBoundaryRewardUpdate covers the whole-epoch
// divergence between the two mark-snapshot capture paths.
//
// cardano-ledger applies the delayed reward update before SNAP, so a mark
// snapshot includes that epoch's rewards; the authoritative capture, reading the
// live aggregate at the SNAP point, does too. dingo records the update at the
// boundary slot — one past the snapshot slot — so the plain
// "subtract everything after slot" reconstruction used by the fallback capture
// removed a whole epoch of rewards from every delegator.
//
// The epoch-boundary query must retain that credit and still exclude what
// cardano-ledger applies after SNAP (POOLREAP refunds, MIR, treasury
// withdrawals, proposal refunds), plus anything past the boundary. The plain
// "stake at slot" query must be unchanged.
func TestEpochBoundaryStakeRetainsBoundaryRewardUpdate(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	db := store.DB()
	pool := bytes.Repeat([]byte{0xF4}, 28)
	stakeKey := bytes.Repeat([]byte{0x34}, 28)

	const (
		snapshotSlot = uint64(199)
		boundarySlot = uint64(200)
	)

	require.NoError(t, db.Create(&models.Account{
		StakingKey:  stakeKey,
		Pool:        pool,
		Active:      true,
		AddedSlot:   10,
		CreatedSlot: 10,
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       bytes.Repeat([]byte{0x64}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     100,
		AddedSlot:  10,
	}).Error)

	// Pre-SNAP: the delayed reward update, applied at the boundary slot.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, stakeKey, 50, boundarySlot,
		bytes.Repeat([]byte{0xa1}, 32), nil,
	))
	// Post-SNAP: a boundary credit cardano-ledger applies after SNAP.
	require.NoError(t, store.AddPostSnapshotAccountRewardByCredential(
		0, stakeKey, 7, boundarySlot,
		bytes.Repeat([]byte{0xa2}, 32), nil,
	))
	// Well past the boundary: never part of this snapshot.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, stakeKey, 3, 300,
		bytes.Repeat([]byte{0xa3}, 32), nil,
	))

	stakes, delegators, err := store.GetStakeByPoolsAtSlot(
		[][]byte{pool}, snapshotSlot, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(100), stakes[string(pool)],
		"a plain stake-at-slot query must exclude every later credit")
	require.Equal(t, uint64(1), delegators[string(pool)])

	stakes, delegators, err = store.GetEpochBoundaryStakeByPools(
		[][]byte{pool}, snapshotSlot, boundarySlot, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(150), stakes[string(pool)],
		"the boundary query must retain the pre-SNAP reward update and drop the rest")
	require.Equal(t, uint64(1), delegators[string(pool)])

	inputs, err := store.GetEpochBoundaryRewardStakeInputsForPools(
		[][]byte{pool}, snapshotSlot, boundarySlot, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(150), uint64(inputs[0].Stake),
		"the reward basis must agree with the leader-election pool total")
}

// TestEpochBoundaryStakeHandlesBoundaryWithdrawal covers the interaction between
// the retained boundary reward update and a withdrawal in the boundary block.
// The boundary block is applied after the rollover, so its withdrawal is
// post-boundary: its recorded previous balance already includes the reward
// update, and reconstruction must recover that balance rather than the cleared
// one.
func TestEpochBoundaryStakeHandlesBoundaryWithdrawal(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	db := store.DB()
	pool := bytes.Repeat([]byte{0xF5}, 28)
	stakeKey := bytes.Repeat([]byte{0x35}, 28)

	const (
		snapshotSlot = uint64(199)
		boundarySlot = uint64(200)
	)

	require.NoError(t, db.Create(&models.Account{
		StakingKey:  stakeKey,
		Pool:        pool,
		Active:      true,
		AddedSlot:   10,
		CreatedSlot: 10,
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       bytes.Repeat([]byte{0x65}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     100,
		AddedSlot:  10,
	}).Error)

	require.NoError(t, store.AddAccountRewardByCredential(
		0, stakeKey, 50, boundarySlot,
		bytes.Repeat([]byte{0xb1}, 32), nil,
	))
	require.NoError(t, store.AddPostSnapshotAccountRewardByCredential(
		0, stakeKey, 7, boundarySlot,
		bytes.Repeat([]byte{0xb2}, 32), nil,
	))
	// A withdrawal in the boundary block clears the whole balance.
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, stakeKey, 57, boundarySlot,
		bytes.Repeat([]byte{0xb3}, 32), nil,
	))

	stakes, _, err := store.GetEpochBoundaryStakeByPools(
		[][]byte{pool}, snapshotSlot, boundarySlot, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(150), stakes[string(pool)],
		"a boundary-block withdrawal must not erase the retained reward update")
}

// TestEpochBoundaryStakeIncludesPreSnapshotCreditsOnly pins which epoch-boundary
// reward-account credits a mark snapshot contains, for every kind dingo applies
// at a boundary. The reference sequence is NEWEPOCH = applyRUpd, MIR, EPOCH and
// EPOCH = SNAP, POOLREAP, ratification/enactment, so:
//
//   - the delayed reward update and MIR credits precede SNAP and are INCLUDED,
//   - POOLREAP deposit refunds, enacted treasury withdrawals and
//     proposal-deposit refunds follow SNAP and are EXCLUDED.
//
// Each credit is written through the same store method its ledger rule uses, so
// this pins the include/exclude split rather than just re-asserting the flag.
// ledger.TestBoundaryCreditVisibility_* pin that each rule reaches the right
// method.
func TestEpochBoundaryStakeIncludesPreSnapshotCreditsOnly(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	db := store.DB()
	pool := bytes.Repeat([]byte{0xF6}, 28)
	stakeKey := bytes.Repeat([]byte{0x36}, 28)

	const (
		snapshotSlot = uint64(199)
		boundarySlot = uint64(200)
	)

	require.NoError(t, db.Create(&models.Account{
		StakingKey:  stakeKey,
		Pool:        pool,
		Active:      true,
		AddedSlot:   10,
		CreatedSlot: 10,
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       bytes.Repeat([]byte{0x66}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     1_000,
		AddedSlot:  10,
	}).Error)

	// Pre-SNAP: delayed reward update (ledger.applyStakeRewards) and MIR
	// (governance.CreditRegisteredRewardAccountBeforeSnapshot).
	for i, amount := range []uint64{50, 3} {
		require.NoError(t, store.AddAccountRewardByCredential(
			0, stakeKey, amount, boundarySlot,
			bytes.Repeat([]byte{byte(0xe0 + i)}, 32), nil,
		))
	}
	// Post-SNAP: POOLREAP refund, treasury withdrawal, proposal-deposit refund
	// (all governance.CreditRegisteredRewardAccountAfterSnapshot).
	for i, amount := range []uint64{7, 11, 13} {
		require.NoError(t, store.AddPostSnapshotAccountRewardByCredential(
			0, stakeKey, amount, boundarySlot,
			bytes.Repeat([]byte{byte(0xf0 + i)}, 32), nil,
		))
	}

	stakes, _, err := store.GetEpochBoundaryStakeByPools(
		[][]byte{pool}, snapshotSlot, boundarySlot, 0, 0, nil,
	)
	require.NoError(t, err)
	// 1000 utxo + 50 reward update + 3 MIR; the 7 + 11 + 13 post-SNAP credits
	// are excluded.
	require.Equal(t, uint64(1_053), stakes[string(pool)],
		"only pre-SNAP boundary credits belong in the mark snapshot")

	inputs, err := store.GetEpochBoundaryRewardStakeInputsForPools(
		[][]byte{pool}, snapshotSlot, boundarySlot, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(1_053), uint64(inputs[0].Stake),
		"the reward basis must apply the same include/exclude split")
}
