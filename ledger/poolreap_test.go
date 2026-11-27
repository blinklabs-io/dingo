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
	"database/sql"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func reapCred28(seed byte) []byte {
	out := make([]byte, 28)
	for i := range out {
		out[i] = seed
	}
	return out
}

// newPoolreapTestLedger builds a LedgerState backed by an in-memory sqlite
// metadata store and returns the raw SQL handle for seeding pool/account rows.
func newPoolreapTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database, *sql.DB) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	fxAttachLedgerDB(t, ls)
	return ls, db, raw
}

func runApplyPoolRetirements(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	newEpoch, boundarySlot uint64,
) {
	t.Helper()
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.engine().ApplyPoolRetirements(context.Background(), txn, newEpoch, boundarySlot)
	}))
}

// reapAccount reads the account of a key-hash credential, active or not.
func reapAccount(
	t *testing.T,
	db *database.Database,
	stakingKey []byte,
) *models.Account {
	t.Helper()
	account, err := seedDB(t, db).GetAccountByCredential(
		context.Background(), 0, stakingKey, true, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	return account
}

// touchReapAccount commits the account unchanged as the block at point, so a
// rollback below point has a later write of the account to undo.
func touchReapAccount(
	t *testing.T,
	db *database.Database,
	stakingKey []byte,
	point ocommon.Point,
) {
	t.Helper()
	account := reapAccount(t, db, stakingKey)
	account.AddedSlot = point.Slot
	fxCommitAt(t, db, point, LedgerChanges{Accounts: []AccountChange{{
		Slot: point.Slot, Account: *account,
	}}})
}

// fxReapBlock applies the reap of the boundary into newEpoch as the block at
// boundarySlot, so the set journals it and a rollback below the boundary
// undoes it.
func fxReapBlock(
	t *testing.T,
	ls *LedgerState,
	newEpoch, boundarySlot uint64,
) {
	t.Helper()
	fxBlock(
		t, ls, fxPoint(boundarySlot),
		func(_ ledgerdb.LedgerDB, txn ledgerdb.Txn) error {
			return ls.engine().ApplyPoolRetirements(
				context.Background(), txn, newEpoch, boundarySlot,
			)
		},
	)
}

// TestApplyPoolRetirements_CreditsRegisteredRewardAccount: a pool retiring at
// the new epoch with a registered, active reward account has its deposit
// refunded to that account; the treasury is untouched.
func TestApplyPoolRetirements_CreditsRegisteredRewardAccount(t *testing.T) {
	t.Parallel()

	ls, db, _ := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
	)
	rewardAccount := reapCred28(0x11)
	fxRetiringPool(
		t, db,
		reapCred28(0xAA),
		rewardAccount,
		deposit,
		100,
		newEpoch,
		200,
	)
	require.NoError(
		t,
		seedCreateAccount(t, db, context.Background(), nil, &models.Account{
			StakingKey: rewardAccount,
			Reward:     types.Uint64(0),
			Active:     true,
		}),
	)
	require.NoError(t, seedDB(t, db).SetNetworkState(1_000, 5_000, 50, nil))

	fxReapBlock(t, ls, newEpoch, boundarySlot)

	account, err := seedDB(t, db).GetAccountByCredential(
		context.Background(),
		0,
		rewardAccount,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, deposit, uint64(account.Reward),
		"deposit refunded to the registered reward account")

	state, err := seedDB(t, db).GetNetworkState(nil)
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
	t.Parallel()

	ls, db, _ := newPoolreapTestLedger(t)

	const (
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
	)
	// Pool with no reward account at all.
	fxRetiringPool(
		t, db,
		reapCred28(0xBB),
		reapCred28(0x22),
		500,
		100,
		newEpoch,
		200,
	)
	// Pool whose reward account exists but is inactive (deregistered). The
	// test creates it active and then flips the column through the same update
	// path a deregistration uses.
	inactive := reapCred28(0x33)
	fxRetiringPool(
		t, db,
		reapCred28(0xCC),
		inactive,
		700,
		100,
		newEpoch,
		200,
	)
	require.NoError(
		t,
		seedCreateAccount(t, db, context.Background(), nil, &models.Account{
			StakingKey: inactive,
			Reward:     types.Uint64(0),
			Active:     true,
		}),
	)
	fxSetAccountActive(t, db, 0, inactive, false)
	require.NoError(t, seedDB(t, db).SetNetworkState(1_000, 5_000, 50, nil))

	fxReapBlock(t, ls, newEpoch, boundarySlot)

	state, err := seedDB(t, db).GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(2_200), uint64(state.Treasury),
		"both deposits (500+700) added to treasury")
	assert.Equal(t, uint64(5_000), uint64(state.Reserves),
		"reserves untouched by deposit refunds")
	assert.Equal(t, boundarySlot, state.Slot,
		"treasury update written at the boundary slot")

	// The inactive account was not credited.
	account, err := seedDB(t, db).GetAccountByCredential(
		context.Background(),
		0,
		inactive,
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"inactive account is not credited")
}

// TestApplyPoolRetirements_WrongEpoch: a pool whose retirement epoch is not the
// new epoch is left untouched.
func TestApplyPoolRetirements_WrongEpoch(t *testing.T) {
	t.Parallel()

	ls, db, _ := newPoolreapTestLedger(t)

	rewardAccount := reapCred28(0x11)
	// Retires at epoch 6, but we process the boundary into epoch 5.
	fxRetiringPool(
		t, db, reapCred28(0xAA), rewardAccount, 500, 100, 6, 200)
	require.NoError(
		t,
		seedCreateAccount(t, db, context.Background(), nil, &models.Account{
			StakingKey: rewardAccount,
			Reward:     types.Uint64(0),
			Active:     true,
		}),
	)
	require.NoError(t, seedDB(t, db).SetNetworkState(1_000, 5_000, 50, nil))

	fxReapBlock(t, ls, 5, 1_000)

	account, err := seedDB(t, db).GetAccountByCredential(
		context.Background(),
		0,
		rewardAccount,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"pool retiring at a later epoch is not refunded yet")
	state, err := seedDB(t, db).GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_000), uint64(state.Treasury))
}

// TestApplyPoolRetirements_Rollback exercises the acceptance scenario: applying
// the boundary refunds (one to a reward account, one to the treasury), then
// rolling back past the boundary restores the prior reward balance and
// treasury so re-application is deterministic.
func TestApplyPoolRetirements_Rollback(t *testing.T) {
	t.Parallel()

	ls, db, _ := newPoolreapTestLedger(t)

	const (
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
		preBoundary  = uint64(500)
	)
	registered := reapCred28(0x11)
	fxRetiringPool(
		t, db,
		reapCred28(0xAA),
		registered,
		500,
		100,
		newEpoch,
		200,
	)
	fxRetiringPool(
		t, db,
		reapCred28(0xBB),
		reapCred28(0x22),
		300,
		100,
		newEpoch,
		200,
	)
	require.NoError(
		t,
		seedCreateAccount(t, db, context.Background(), nil, &models.Account{
			StakingKey: registered,
			Reward:     types.Uint64(0),
			Active:     true,
		}),
	)
	require.NoError(t, seedDB(t, db).SetNetworkState(1_000, 5_000, 50, nil))
	fxTip(t, ls, fxPoint(preBoundary))

	fxReapBlock(t, ls, newEpoch, boundarySlot)

	account, err := seedDB(t, db).GetAccountByCredential(
		context.Background(),
		0,
		registered,
		false,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(500), uint64(account.Reward),
		"registered pool deposit refunded")
	state, err := seedDB(t, db).GetNetworkState(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1_300), uint64(state.Treasury),
		"unregistered pool deposit (300) added to treasury")

	// Roll back past the boundary: the set undoes the reward credit and the
	// treasury update.
	fxRollback(t, ls, fxPoint(preBoundary))

	account, err = seedDB(t, db).GetAccountByCredential(
		context.Background(),
		0,
		registered,
		false,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"reward credit reverted on rollback")
	state, err = seedDB(t, db).GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_000), uint64(state.Treasury),
		"treasury restored to pre-boundary value")
	assert.Equal(t, uint64(50), state.Slot)

	// Re-applying the boundary reproduces the same effects (determinism).
	fxReapBlock(t, ls, newEpoch, boundarySlot)
	account, err = seedDB(t, db).GetAccountByCredential(
		context.Background(),
		0,
		registered,
		false,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), uint64(account.Reward),
		"re-applied refund is deterministic")
	state, err = seedDB(t, db).GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_300), uint64(state.Treasury))
}

// TestApplyPoolRetirements_ClearsDelegationsToReapedPool covers the delegation
// half of POOLREAP. cardano-ledger removes delegations pointing at a reaped
// pool (`delegations ⋫ retired`, Shelley spec Fig. 41); keeping them lets the
// stake return to the pool distribution the moment the pool re-registers, so
// the node's total active stake exceeds the network's and every other pool's
// VRF leader threshold is computed too small.
//
// A pool retiring at a different epoch keeps its delegators, so the clear is
// scoped to the pools actually reaped at this boundary rather than to every
// pool carrying a retirement certificate.
func TestApplyPoolRetirements_ClearsDelegationsToReapedPool(t *testing.T) {
	t.Parallel()

	ls, db, _ := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
	)
	reaped := reapCred28(0xAA)
	surviving := reapCred28(0xBB)
	fxRetiringPool(
		t, db, reaped, reapCred28(0x11), deposit, 100, newEpoch, 200,
	)
	fxRetiringPool(
		t, db, surviving, reapCred28(0x12), deposit, 100, newEpoch+1, 200,
	)

	delegator := reapCred28(0x21)
	other := reapCred28(0x22)
	require.NoError(
		t,
		seedCreateAccount(t, db, context.Background(), nil, &models.Account{
			StakingKey: delegator,
			Pool:       reaped,
			AddedSlot:  300,
			Active:     true,
		}),
	)
	require.NoError(
		t,
		seedCreateAccount(t, db, context.Background(), nil, &models.Account{
			StakingKey: other,
			Pool:       surviving,
			AddedSlot:  300,
			Active:     true,
		}),
	)
	require.NoError(t, seedDB(t, db).SetNetworkState(1_000, 5_000, 50, nil))

	fxReapBlock(t, ls, newEpoch, boundarySlot)

	reapedDelegator, err := seedDB(t, db).GetAccountByCredential(
		context.Background(),
		0,
		delegator,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, reapedDelegator)
	assert.Empty(t, reapedDelegator.Pool,
		"delegation to the reaped pool must not survive the boundary")

	untouched, err := seedDB(t, db).GetAccountByCredential(
		context.Background(),
		0,
		other,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, untouched)
	assert.Equal(t, surviving, untouched.Pool,
		"a pool retiring at a later epoch keeps its delegators")
}

// TestApplyPoolRetirements_ClearsLiveStakeAttributionForReapedPool covers the
// aggregate the boundary snapshot actually reads. GetLiveStakeInputsForPools
// selects on reward_live_stake.pool_key_hash, which mirrors account.pool but is
// only recomputed when refreshRewardLiveStakeAggregate runs for the credential;
// a reap triggers no such refresh, so clearing the account alone would leave
// the stale attribution feeding the very stake distribution this fixes.
func TestApplyPoolRetirements_ClearsLiveStakeAttributionForReapedPool(
	t *testing.T,
) {
	t.Parallel()

	ls, db, _ := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
	)
	reaped := reapCred28(0xAA)
	surviving := reapCred28(0xBB)
	fxRetiringPool(
		t, db, reaped, reapCred28(0x11), deposit, 100, newEpoch, 200,
	)
	fxRetiringPool(
		t, db, surviving, reapCred28(0x12), deposit, 100, newEpoch+1, 200,
	)

	delegator := reapCred28(0x21)
	other := reapCred28(0x22)
	for _, seed := range []struct {
		key  []byte
		pool []byte
	}{{delegator, reaped}, {other, surviving}} {
		require.NoError(
			t,
			seedCreateAccount(t, db, context.Background(), nil, &models.Account{
				StakingKey: seed.key,
				Pool:       seed.pool,
				AddedSlot:  300,
				Active:     true,
			}),
		)
		txID := make([]byte, 32)
		copy(txID, seed.key)
		seedTestStakedOutput(t, ls, txID, 0, seed.key, 7)
	}
	require.NoError(t, seedDB(t, db).SetNetworkState(1_000, 5_000, 50, nil))

	fxReapBlock(t, ls, newEpoch, boundarySlot)

	inputs, err := seedDB(t, db).GetLiveStakeInputsForPools(
		[][]byte{reaped, surviving}, 0, nil,
	)
	require.NoError(t, err)
	for _, input := range inputs {
		assert.NotEqual(t, reaped, input.PoolKeyHash,
			"reaped pool must contribute no live stake input")
	}
	require.Len(t, inputs, 1,
		"only the surviving pool's delegator remains attributed")
	assert.Equal(t, surviving, inputs[0].PoolKeyHash)
	assert.Equal(t, other, inputs[0].StakingKey)
}

// TestApplyPoolRetirements_ClearedDelegationIsRollbackSafe pins the slot the
// clear is stamped with. RestoreAccountStateAtSlot only revisits accounts whose
// added_slot is past the rollback target and re-derives the delegation from the
// surviving certificates, so the clear must advance added_slot to the boundary
// — otherwise a rollback to before the reap leaves the account un-delegated
// with no certificate saying so.
func TestApplyPoolRetirements_ClearedDelegationIsRollbackSafe(t *testing.T) {
	t.Parallel()

	ls, db, _ := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
	)
	reaped := reapCred28(0xAA)
	fxRetiringPool(
		t, db, reaped, reapCred28(0x11), deposit, 100, newEpoch, 200,
	)
	delegator := reapCred28(0x21)
	require.NoError(
		t,
		seedCreateAccount(t, db, context.Background(), nil, &models.Account{
			StakingKey: delegator,
			Pool:       reaped,
			AddedSlot:  300,
			Active:     true,
		}),
	)
	require.NoError(t, seedDB(t, db).SetNetworkState(1_000, 5_000, 50, nil))

	fxReapBlock(t, ls, newEpoch, boundarySlot)

	account, err := seedDB(t, db).GetAccountByCredential(
		context.Background(),
		0,
		delegator,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, boundarySlot, account.AddedSlot,
		"the clear is a boundary write and must be reachable by rollback")
}

// TestRollbackAfterPoolReapKeepsDelegationCleared covers the rollback half of
// the reap. A reap writes no certificate, so nothing but the reap itself says
// the delegation is gone: an account written again after the reap and then
// rolled back to a point still past it must stay undelegated, or the stake
// the reap removed returns to the pool distribution.
//
// A rollback to before the reap is the opposite case and must restore the
// delegation; that is TestRollbackBeforePoolReapRestoresDelegation below.
func TestRollbackAfterPoolReapKeepsDelegationCleared(t *testing.T) {
	t.Parallel()

	ls, db, _ := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
		// The account is written again after the reap, and the rollback
		// target sits between the reap and that write.
		laterSlot    = uint64(1_500)
		rollbackSlot = uint64(1_200)
	)
	reaped := reapCred28(0xAA)
	fxRetiringPool(
		t, db, reaped, reapCred28(0x11), deposit, 100, newEpoch, 200,
	)
	delegator := reapCred28(0x21)
	seedDelegatedAccount(t, db, delegator, reaped, 300)
	require.NoError(t, seedDB(t, db).SetNetworkState(1_000, 5_000, 50, nil))

	fxReapBlock(t, ls, newEpoch, boundarySlot)
	fxTip(t, ls, fxPoint(rollbackSlot))
	touchReapAccount(t, db, delegator, fxPoint(laterSlot))

	fxRollback(t, ls, fxPoint(rollbackSlot))

	assert.Empty(t, reapAccount(t, db, delegator).Pool,
		"a rollback landing after the reap must not restore the delegation")
	active, err := seedDB(t, db).GetActivePoolKeyHashes(
		context.Background(), nil,
	)
	require.NoError(t, err)
	assert.Empty(t, active, "the pool stays reaped")
}

// TestRollbackBeforePoolReapRestoresDelegation is the other direction:
// rolling back past the boundary undoes the reap along with every other write
// it made, so the pool is registered and the delegation stands again.
func TestRollbackBeforePoolReapRestoresDelegation(t *testing.T) {
	t.Parallel()

	ls, db, _ := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
		rollbackSlot = uint64(900)
	)
	reaped := reapCred28(0xAA)
	rewardAccount := reapCred28(0x11)
	fxRetiringPool(
		t, db, reaped, rewardAccount, deposit, 100, newEpoch, 200,
	)
	require.NoError(
		t,
		seedCreateAccount(t, db, context.Background(), nil, &models.Account{
			StakingKey: rewardAccount,
			Active:     true,
		}),
	)
	delegator := reapCred28(0x21)
	seedDelegatedAccount(t, db, delegator, reaped, 300)
	require.NoError(t, seedDB(t, db).SetNetworkState(1_000, 5_000, 50, nil))
	fxTip(t, ls, fxPoint(rollbackSlot))

	fxReapBlock(t, ls, newEpoch, boundarySlot)
	require.Empty(t, reapAccount(t, db, delegator).Pool)
	require.Equal(t, deposit, uint64(reapAccount(t, db, rewardAccount).Reward))

	fxRollback(t, ls, fxPoint(rollbackSlot))

	assert.Equal(t, reaped, reapAccount(t, db, delegator).Pool,
		"rolling back past the reap restores the delegation it removed")
	assert.Zero(t, uint64(reapAccount(t, db, rewardAccount).Reward),
		"the deposit refund is undone with the reap")
	retiring, err := seedDB(t, db).GetPoolsRetiringAtEpoch(
		context.Background(), newEpoch, boundarySlot, nil,
	)
	require.NoError(t, err)
	require.Len(t, retiring, 1, "the pool is registered and retiring again")
	assert.Equal(t, reaped, retiring[0].PoolKeyHash)
}

// seedDelegatedAccount creates an active account delegated to pool since
// certSlot.
func seedDelegatedAccount(
	t *testing.T,
	db *database.Database,
	stakingKey, pool []byte,
	certSlot uint64,
) {
	t.Helper()
	require.NoError(
		t,
		seedCreateAccount(t, db, context.Background(), nil, &models.Account{
			StakingKey:  stakingKey,
			Pool:        pool,
			AddedSlot:   certSlot,
			CreatedSlot: 1,
			Active:      true,
		}),
	)
}

// TestPoolReapHonoursSupersedingRetirement covers a pool whose retirement
// certificate was replaced by a later one for a further-out epoch.
// cardano-ledger keeps only the latest retirement, so the earlier epoch's
// boundary reaps nothing, and a rollback across later writes of a delegator
// leaves its delegation in place.
func TestPoolReapHonoursSupersedingRetirement(t *testing.T) {
	t.Parallel()

	ls, db, _ := newPoolreapTestLedger(t)

	const (
		deposit       = uint64(500)
		firstEpoch    = uint64(5)
		firstBoundary = uint64(1_000)
		secondEpoch   = uint64(9)
		rollbackSlot  = uint64(1_200)
	)
	pool := reapCred28(0xAA)
	fxRetiringPool(
		t, db, pool, reapCred28(0x11), deposit, 100, firstEpoch, 200,
	)
	// A later certificate moves the retirement out to a further epoch.
	seedStakePoolRetirement(t, ls, pool, secondEpoch, 300)

	delegator := reapCred28(0x21)
	seedDelegatedAccount(t, db, delegator, pool, 400)
	require.NoError(t, seedDB(t, db).SetNetworkState(1_000, 5_000, 50, nil))

	// The first boundary reaps nothing.
	fxReapBlock(t, ls, firstEpoch, firstBoundary)
	require.Equal(t, pool, reapAccount(t, db, delegator).Pool,
		"a superseded retirement must not reap the pool")
	state, err := seedDB(t, db).GetNetworkState(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1_000), uint64(state.Treasury),
		"no deposit is refunded at the superseded epoch")

	fxTip(t, ls, fxPoint(rollbackSlot))
	touchReapAccount(t, db, delegator, fxPoint(rollbackSlot+500))
	fxRollback(t, ls, fxPoint(rollbackSlot))

	assert.Equal(t, pool, reapAccount(t, db, delegator).Pool,
		"the rollback must not clear a delegation the reap never cleared")
	retiring, err := seedDB(t, db).GetPoolsRetiringAtEpoch(
		context.Background(), secondEpoch, 5_000, nil,
	)
	require.NoError(t, err)
	require.Len(t, retiring, 1)
	assert.Equal(t, pool, retiring[0].PoolKeyHash)
}
