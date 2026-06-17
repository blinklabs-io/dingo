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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// mirCred28 builds a 28-byte stake credential filled with seed.
func mirCred28(seed byte) []byte {
	out := make([]byte, 28)
	for i := range out {
		out[i] = seed
	}
	return out
}

// newMIRTestLedger reuses the poolreap helper (same DB setup).
func newMIRTestLedger(t *testing.T) (*LedgerState, *database.Database, *gorm.DB) {
	t.Helper()
	return newPoolreapTestLedger(t)
}

// seedMIRDistribution inserts a MoveInstantaneousRewards row with one or more
// credential→amount reward rows, simulating a distribution MIR cert.
func seedMIRDistribution(
	t *testing.T,
	gdb *gorm.DB,
	pot uint,
	addedSlot uint64,
	rewards []models.MoveInstantaneousRewardsReward,
) {
	t.Helper()
	mir := &models.MoveInstantaneousRewards{
		Pot:       pot,
		AddedSlot: addedSlot,
	}
	require.NoError(t, gdb.Create(mir).Error)
	for i := range rewards {
		rewards[i].MIRID = mir.ID
		require.NoError(t, gdb.Create(&rewards[i]).Error)
	}
}

// seedMIRPotTransfer inserts a MoveInstantaneousRewards row representing a
// pot-to-pot transfer (OtherPot > 0, no credential rows).
func seedMIRPotTransfer(
	t *testing.T,
	gdb *gorm.DB,
	sourcePot uint,
	amount uint64,
	addedSlot uint64,
) {
	t.Helper()
	mir := &models.MoveInstantaneousRewards{
		Pot:       sourcePot,
		OtherPot:  types.Uint64(amount),
		AddedSlot: addedSlot,
	}
	require.NoError(t, gdb.Create(mir).Error)
}

func runApplyMIRCerts(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	epochStartSlot, boundarySlot uint64,
) {
	t.Helper()
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyMIRCerts(txn, epochStartSlot, boundarySlot)
	}))
}

// TestApplyMIRCerts_DistributionFromReserves_RegisteredAccount verifies that a
// MIR cert distributing from reserves credits the registered reward account and
// debits reserves.
func TestApplyMIRCerts_DistributionFromReserves_RegisteredAccount(t *testing.T) {
	ls, db, gdb := newMIRTestLedger(t)

	const (
		epochStartSlot = uint64(0)
		boundarySlot   = uint64(1_000)
		mirAmount      = uint64(750)
	)
	cred := mirCred28(0x11)
	seedMIRDistribution(t, gdb, mirPotReserves, 500, []models.MoveInstantaneousRewardsReward{
		{Credential: cred, Amount: types.Uint64(mirAmount)},
	})
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Reward:     0,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 10_000, 50, nil))

	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)

	account, err := db.GetAccount(cred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, mirAmount, uint64(account.Reward),
		"registered account should receive MIR reward")

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(9_250), uint64(state.Reserves),
		"reserves debited by MIR amount")
	assert.Equal(t, uint64(1_000), uint64(state.Treasury),
		"treasury untouched for reserves distribution")
}

// TestApplyMIRCerts_DistributionFromTreasury_RegisteredAccount verifies a MIR
// cert from the treasury credits the account and debits the treasury.
func TestApplyMIRCerts_DistributionFromTreasury_RegisteredAccount(t *testing.T) {
	ls, db, gdb := newMIRTestLedger(t)

	const (
		epochStartSlot = uint64(0)
		boundarySlot   = uint64(1_000)
		mirAmount      = uint64(200)
	)
	cred := mirCred28(0x22)
	seedMIRDistribution(t, gdb, mirPotTreasury, 500, []models.MoveInstantaneousRewardsReward{
		{Credential: cred, Amount: types.Uint64(mirAmount)},
	})
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Reward:     0,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(5_000, 8_000, 50, nil))

	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)

	account, err := db.GetAccount(cred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, mirAmount, uint64(account.Reward),
		"registered account should receive MIR reward from treasury")

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(4_800), uint64(state.Treasury),
		"treasury debited by MIR amount")
	assert.Equal(t, uint64(8_000), uint64(state.Reserves),
		"reserves untouched for treasury distribution")
}

// TestApplyMIRCerts_DistributionUnregisteredAccount verifies that an
// unregistered credential is silently skipped — no pot debit, no error.
func TestApplyMIRCerts_DistributionUnregisteredAccount(t *testing.T) {
	ls, db, gdb := newMIRTestLedger(t)

	cred := mirCred28(0x33) // no Account row seeded
	seedMIRDistribution(t, gdb, mirPotReserves, 500, []models.MoveInstantaneousRewardsReward{
		{Credential: cred, Amount: types.Uint64(400)},
	})
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	runApplyMIRCerts(t, ls, db, 0, 1_000)

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(5_000), uint64(state.Reserves),
		"unregistered credential — reserves untouched")
	assert.Equal(t, uint64(1_000), uint64(state.Treasury),
		"unregistered credential — treasury untouched")
	assert.Equal(t, uint64(50), state.Slot,
		"no boundary state row written when nothing is distributed")
}

// TestApplyMIRCerts_PotTransferReservesToTreasury verifies that a pot-to-pot
// MIR with sourcePot=Reserves moves coins from reserves to treasury.
func TestApplyMIRCerts_PotTransferReservesToTreasury(t *testing.T) {
	ls, db, gdb := newMIRTestLedger(t)

	const transfer = uint64(2_000)
	seedMIRPotTransfer(t, gdb, mirPotReserves, transfer, 500)
	require.NoError(t, db.Metadata().SetNetworkState(3_000, 10_000, 50, nil))

	runApplyMIRCerts(t, ls, db, 0, 1_000)

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(5_000), uint64(state.Treasury),
		"treasury increased by pot transfer")
	assert.Equal(t, uint64(8_000), uint64(state.Reserves),
		"reserves decreased by pot transfer")
}

// TestApplyMIRCerts_PotTransferTreasuryToReserves verifies sourcePot=Treasury
// moves coins from treasury to reserves.
func TestApplyMIRCerts_PotTransferTreasuryToReserves(t *testing.T) {
	ls, db, gdb := newMIRTestLedger(t)

	const transfer = uint64(1_500)
	seedMIRPotTransfer(t, gdb, mirPotTreasury, transfer, 500)
	require.NoError(t, db.Metadata().SetNetworkState(4_000, 6_000, 50, nil))

	runApplyMIRCerts(t, ls, db, 0, 1_000)

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(2_500), uint64(state.Treasury),
		"treasury decreased by pot transfer")
	assert.Equal(t, uint64(7_500), uint64(state.Reserves),
		"reserves increased by pot transfer")
}

// TestApplyMIRCerts_OutsideEpochRange verifies that a MIR cert submitted
// before epochStartSlot or at/after boundarySlot is not applied.
func TestApplyMIRCerts_OutsideEpochRange(t *testing.T) {
	ls, db, gdb := newMIRTestLedger(t)

	cred := mirCred28(0x44)
	// addedSlot=50 is before epochStartSlot=100
	seedMIRDistribution(t, gdb, mirPotReserves, 50, []models.MoveInstantaneousRewardsReward{
		{Credential: cred, Amount: types.Uint64(500)},
	})
	// addedSlot=1000 equals boundarySlot — excluded (half-open interval)
	seedMIRDistribution(t, gdb, mirPotReserves, 1_000, []models.MoveInstantaneousRewardsReward{
		{Credential: cred, Amount: types.Uint64(300)},
	})
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Reward:     0,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 10, nil))

	// epoch range: [100, 1000)
	runApplyMIRCerts(t, ls, db, 100, 1_000)

	account, err := db.GetAccount(cred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"certs outside epoch range should not be applied")
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(5_000), uint64(state.Reserves),
		"reserves should be unchanged for out-of-range certs")
}

// TestApplyMIRCerts_Rollback verifies that reward credits and pot debits are
// reversed by deleting AccountRewardDelta and NetworkState rows after slot,
// and re-application produces the same outcome.
func TestApplyMIRCerts_Rollback(t *testing.T) {
	ls, db, gdb := newMIRTestLedger(t)

	const (
		epochStartSlot = uint64(0)
		boundarySlot   = uint64(1_000)
		preBoundary    = uint64(500)
		mirAmount      = uint64(600)
	)
	cred := mirCred28(0x55)
	seedMIRDistribution(t, gdb, mirPotReserves, 200, []models.MoveInstantaneousRewardsReward{
		{Credential: cred, Amount: types.Uint64(mirAmount)},
	})
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Reward:     0,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 8_000, 50, nil))

	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)

	account, err := db.GetAccount(cred, false, nil)
	require.NoError(t, err)
	require.Equal(t, mirAmount, uint64(account.Reward), "MIR reward applied")
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(7_400), uint64(state.Reserves),
		"reserves debited after apply")

	// Roll back past the boundary: drop the reward credit and treasury row.
	require.NoError(t, db.DeleteAccountRewardsAfterSlot(preBoundary, nil))
	require.NoError(t, db.DeleteNetworkStateAfterSlot(preBoundary, nil))

	account, err = db.GetAccount(cred, false, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"reward credit reverted on rollback")
	state, err = db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(8_000), uint64(state.Reserves),
		"reserves restored to pre-boundary value")

	// Re-apply must be deterministic.
	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)
	account, err = db.GetAccount(cred, false, nil)
	require.NoError(t, err)
	assert.Equal(t, mirAmount, uint64(account.Reward),
		"re-applied MIR reward is deterministic")
	state, err = db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(7_400), uint64(state.Reserves))
}

// TestApplyMIRCerts_NoOp verifies that an epoch with no MIR certs leaves
// state completely untouched.
func TestApplyMIRCerts_NoOp(t *testing.T) {
	ls, db, _ := newMIRTestLedger(t)

	require.NoError(t, db.Metadata().SetNetworkState(2_000, 9_000, 50, nil))

	runApplyMIRCerts(t, ls, db, 0, 1_000)

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(2_000), uint64(state.Treasury))
	assert.Equal(t, uint64(9_000), uint64(state.Reserves))
	assert.Equal(t, uint64(50), state.Slot,
		"no boundary row written when no MIR certs exist")
}
