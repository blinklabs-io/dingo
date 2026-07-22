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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func reapCred28(seed byte) []byte {
	out := make([]byte, 28)
	for i := range out {
		out[i] = seed
	}
	return out
}

// newPoolreapTestLedger builds a LedgerState backed by an in-memory sqlite
// metadata store and returns the gorm handle for seeding pool/account rows.
func newPoolreapTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database, *gorm.DB) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	store, ok := db.Metadata().(*sqlite.MetadataStoreSqlite)
	require.True(t, ok, "expected sqlite metadata store")
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	return ls, db, store.DB()
}

// seedRetiringPool inserts a pool with a registration (reward account +
// deposit) and a retirement at retireEpoch, so GetPoolsRetiringAtEpoch will
// return it at that epoch boundary.
func seedRetiringPool(
	t *testing.T,
	gdb *gorm.DB,
	keyHash, rewardAccount []byte,
	deposit, regSlot, retireEpoch, retireSlot uint64,
) {
	t.Helper()
	pool := &models.Pool{PoolKeyHash: keyHash, RewardAccount: rewardAccount}
	require.NoError(t, gdb.Create(pool).Error)
	require.NoError(t, gdb.Create(&models.PoolRegistration{
		PoolID:        pool.ID,
		PoolKeyHash:   keyHash,
		RewardAccount: rewardAccount,
		DepositAmount: types.Uint64(deposit),
		AddedSlot:     regSlot,
	}).Error)
	require.NoError(t, gdb.Create(&models.PoolRetirement{
		PoolID:      pool.ID,
		PoolKeyHash: keyHash,
		Epoch:       retireEpoch,
		AddedSlot:   retireSlot,
	}).Error)
}

func runApplyPoolRetirements(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	newEpoch, boundarySlot uint64,
) {
	t.Helper()
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyPoolRetirements(txn, newEpoch, boundarySlot)
	}))
}

// TestApplyPoolRetirements_CreditsRegisteredRewardAccount: a pool retiring at
// the new epoch with a registered, active reward account has its deposit
// refunded to that account; the treasury is untouched.
func TestApplyPoolRetirements_CreditsRegisteredRewardAccount(t *testing.T) {
	ls, db, gdb := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
	)
	rewardAccount := reapCred28(0x11)
	seedRetiringPool(t, gdb, reapCred28(0xAA), rewardAccount, deposit, 100, newEpoch, 200)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		Reward:     types.Uint64(0),
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	runApplyPoolRetirements(t, ls, db, newEpoch, boundarySlot)

	account, err := db.GetAccountByCredential(0, rewardAccount, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, deposit, uint64(account.Reward),
		"deposit refunded to the registered reward account")

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(1_000), uint64(state.Treasury),
		"treasury untouched when deposit is refunded to an account")
	assert.Equal(t, uint64(50), state.Slot,
		"no boundary network-state row when nothing goes to treasury")
}

// TestApplyPoolRetirements_UnregisteredAccountToTreasury: a pool retiring with
// no reward account, and one with an inactive account, both route their
// deposit to the treasury.
func TestApplyPoolRetirements_UnregisteredAccountToTreasury(t *testing.T) {
	ls, db, gdb := newPoolreapTestLedger(t)

	const (
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
	)
	// Pool with no reward account at all.
	seedRetiringPool(t, gdb, reapCred28(0xBB), reapCred28(0x22), 500, 100, newEpoch, 200)
	// Pool whose reward account exists but is inactive (deregistered). The
	// Account.Active column defaults to true, so create it then flip the
	// column the way a deregistration would, rather than relying on the
	// zero value (which GORM replaces with the default on insert).
	inactive := reapCred28(0x33)
	seedRetiringPool(t, gdb, reapCred28(0xCC), inactive, 700, 100, newEpoch, 200)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: inactive,
		Reward:     types.Uint64(0),
		Active:     true,
	}))
	require.NoError(t, gdb.Model(&models.Account{}).
		Where("staking_key = ?", inactive).
		Update("active", false).Error)
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	runApplyPoolRetirements(t, ls, db, newEpoch, boundarySlot)

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(2_200), uint64(state.Treasury),
		"both deposits (500+700) added to treasury")
	assert.Equal(t, uint64(5_000), uint64(state.Reserves),
		"reserves untouched by deposit refunds")
	assert.Equal(t, boundarySlot, state.Slot,
		"treasury update written at the boundary slot")

	// The inactive account was not credited.
	account, err := db.GetAccountByCredential(0, inactive, true, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"inactive account is not credited")
}

// TestApplyPoolRetirements_WrongEpoch: a pool whose retirement epoch is not the
// new epoch is left untouched.
func TestApplyPoolRetirements_WrongEpoch(t *testing.T) {
	ls, db, gdb := newPoolreapTestLedger(t)

	rewardAccount := reapCred28(0x11)
	// Retires at epoch 6, but we process the boundary into epoch 5.
	seedRetiringPool(t, gdb, reapCred28(0xAA), rewardAccount, 500, 100, 6, 200)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		Reward:     types.Uint64(0),
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	runApplyPoolRetirements(t, ls, db, 5, 1_000)

	account, err := db.GetAccountByCredential(0, rewardAccount, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"pool retiring at a later epoch is not refunded yet")
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_000), uint64(state.Treasury))
}

// TestApplyPoolRetirements_Rollback exercises the acceptance scenario: applying
// the boundary refunds (one to a reward account, one to the treasury), then
// rolling back past the boundary restores the prior reward balance and
// treasury so re-application is deterministic.
func TestApplyPoolRetirements_Rollback(t *testing.T) {
	ls, db, gdb := newPoolreapTestLedger(t)

	const (
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
		preBoundary  = uint64(500)
	)
	registered := reapCred28(0x11)
	seedRetiringPool(t, gdb, reapCred28(0xAA), registered, 500, 100, newEpoch, 200)
	seedRetiringPool(t, gdb, reapCred28(0xBB), reapCred28(0x22), 300, 100, newEpoch, 200)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: registered,
		Reward:     types.Uint64(0),
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	runApplyPoolRetirements(t, ls, db, newEpoch, boundarySlot)

	account, err := db.GetAccountByCredential(0, registered, false, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(500), uint64(account.Reward),
		"registered pool deposit refunded")
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1_300), uint64(state.Treasury),
		"unregistered pool deposit (300) added to treasury")

	// Roll back past the boundary: reward credit and treasury row are dropped.
	require.NoError(t, db.DeleteAccountRewardsAfterSlot(preBoundary, nil))
	require.NoError(t, db.DeleteNetworkStateAfterSlot(preBoundary, nil))

	account, err = db.GetAccountByCredential(0, registered, false, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"reward credit reverted on rollback")
	state, err = db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_000), uint64(state.Treasury),
		"treasury restored to pre-boundary value")
	assert.Equal(t, uint64(50), state.Slot)

	// Re-applying the boundary reproduces the same effects (determinism).
	runApplyPoolRetirements(t, ls, db, newEpoch, boundarySlot)
	account, err = db.GetAccountByCredential(0, registered, false, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), uint64(account.Reward),
		"re-applied refund is deterministic")
	state, err = db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_300), uint64(state.Treasury))
}
