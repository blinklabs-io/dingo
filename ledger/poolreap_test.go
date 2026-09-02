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
	"database/sql"
	"io"
	"log/slog"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
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
	return ls, db, raw
}

// seedRetiringPool inserts a pool with a registration (reward account +
// deposit) and a retirement at retireEpoch, so GetPoolsRetiringAtEpoch will
// return it at that epoch boundary.
func seedRetiringPool(
	t *testing.T,
	raw *sql.DB,
	keyHash, rewardAccount []byte,
	deposit, regSlot, retireEpoch, retireSlot uint64,
) {
	t.Helper()
	result, err := raw.Exec(`
INSERT INTO pool (pool_key_hash, reward_account) VALUES (?, ?)`,
		keyHash, rewardAccount,
	)
	require.NoError(t, err)
	poolID, err := result.LastInsertId()
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO pool_registration (
    pool_id, pool_key_hash, reward_account, deposit_amount, added_slot
) VALUES (?, ?, ?, ?, ?)`,
		poolID,
		keyHash,
		rewardAccount,
		strconv.FormatUint(deposit, 10),
		regSlot,
	)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO pool_retirement (pool_id, pool_key_hash, epoch, added_slot)
VALUES (?, ?, ?, ?)`,
		poolID, keyHash, retireEpoch, retireSlot,
	)
	require.NoError(t, err)
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
	seedRetiringPool(
		t,
		gdb,
		reapCred28(0xAA),
		rewardAccount,
		deposit,
		100,
		newEpoch,
		200,
	)
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
	seedRetiringPool(
		t,
		gdb,
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
	seedRetiringPool(
		t,
		gdb,
		reapCred28(0xCC),
		inactive,
		700,
		100,
		newEpoch,
		200,
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: inactive,
		Reward:     types.Uint64(0),
		Active:     true,
	}))
	_, err := gdb.Exec(
		"UPDATE account SET active = FALSE WHERE staking_key = ?",
		inactive,
	)
	require.NoError(t, err)
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
	seedRetiringPool(
		t,
		gdb,
		reapCred28(0xAA),
		registered,
		500,
		100,
		newEpoch,
		200,
	)
	seedRetiringPool(
		t,
		gdb,
		reapCred28(0xBB),
		reapCred28(0x22),
		300,
		100,
		newEpoch,
		200,
	)
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

// TestApplyPoolRetirements_ClearsDelegationsToReapedPool covers the delegation
// half of POOLREAP. cardano-ledger removes delegations pointing at a reaped
// pool (`delegations ⋫ retired`, Shelley spec Fig. 41); keeping them lets the
// stake return to the pool distribution the moment the pool re-registers, so
// the node's total active stake exceeds the network's and every other pool's
// VRF leader threshold is computed too small (dingo #3794).
//
// A pool retiring at a different epoch keeps its delegators, so the clear is
// scoped to the pools actually reaped at this boundary rather than to every
// pool carrying a retirement certificate.
func TestApplyPoolRetirements_ClearsDelegationsToReapedPool(t *testing.T) {
	ls, db, gdb := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
	)
	reaped := reapCred28(0xAA)
	surviving := reapCred28(0xBB)
	seedRetiringPool(
		t, gdb, reaped, reapCred28(0x11), deposit, 100, newEpoch, 200,
	)
	seedRetiringPool(
		t, gdb, surviving, reapCred28(0x12), deposit, 100, newEpoch+1, 200,
	)

	delegator := reapCred28(0x21)
	other := reapCred28(0x22)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: delegator,
		Pool:       reaped,
		AddedSlot:  300,
		Active:     true,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: other,
		Pool:       surviving,
		AddedSlot:  300,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	runApplyPoolRetirements(t, ls, db, newEpoch, boundarySlot)

	reapedDelegator, err := db.GetAccountByCredential(0, delegator, false, nil)
	require.NoError(t, err)
	require.NotNil(t, reapedDelegator)
	assert.Empty(t, reapedDelegator.Pool,
		"delegation to the reaped pool must not survive the boundary")

	untouched, err := db.GetAccountByCredential(0, other, false, nil)
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
	ls, db, gdb := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
	)
	reaped := reapCred28(0xAA)
	surviving := reapCred28(0xBB)
	seedRetiringPool(
		t, gdb, reaped, reapCred28(0x11), deposit, 100, newEpoch, 200,
	)
	seedRetiringPool(
		t, gdb, surviving, reapCred28(0x12), deposit, 100, newEpoch+1, 200,
	)

	delegator := reapCred28(0x21)
	other := reapCred28(0x22)
	for _, seed := range []struct {
		key  []byte
		pool []byte
	}{{delegator, reaped}, {other, surviving}} {
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey: seed.key,
			Pool:       seed.pool,
			AddedSlot:  300,
			Active:     true,
		}))
		// CreateAccount already seeds the aggregate row; give it the stake
		// and pool attribution a delegated account would carry.
		_, err := gdb.Exec(`
UPDATE reward_live_stake
SET pool_key_hash = ?, utxo_stake = '7', total_stake = '7',
    registered = TRUE, pool_delegation_slot = 300, updated_slot = 300,
    calculation_version = ?
WHERE credential_tag = 0 AND staking_key = ?`,
			seed.pool, models.RewardStakeCalculationVersion, seed.key,
		)
		require.NoError(t, err)
	}
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	runApplyPoolRetirements(t, ls, db, newEpoch, boundarySlot)

	inputs, err := db.Metadata().GetLiveStakeInputsForPools(
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
	ls, db, gdb := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
	)
	reaped := reapCred28(0xAA)
	seedRetiringPool(
		t, gdb, reaped, reapCred28(0x11), deposit, 100, newEpoch, 200,
	)
	delegator := reapCred28(0x21)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: delegator,
		Pool:       reaped,
		AddedSlot:  300,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	runApplyPoolRetirements(t, ls, db, newEpoch, boundarySlot)

	account, err := db.GetAccountByCredential(0, delegator, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, boundarySlot, account.AddedSlot,
		"the clear is a boundary write and must be reachable by rollback")
}

// TestRestoreAccountStateDoesNotRevivePoolReapedBeforeRollback covers the
// rollback half of the reap. RestoreAccountStateAtSlot re-derives account.pool
// from the latest delegation certificate at or before the rollback slot, and a
// reap writes no certificate — so an account touched again after the reap and
// then rolled back to a point still past it had its delegation to the reaped
// pool restored, putting the stake the reap removed straight back into the pool
// distribution (dingo #3794).
//
// A rollback to before the reap is the opposite case and must restore the
// delegation, which the certificate derivation already does; that is covered by
// TestRestoreAccountStateRevivesDelegationRolledBackBeforeReap below.
func TestRestoreAccountStateDoesNotRevivePoolReapedBeforeRollback(t *testing.T) {
	ls, db, gdb := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
		// The account is modified again after the reap, and the rollback
		// target sits between the reap and that modification.
		laterSlot    = uint64(1_500)
		rollbackSlot = uint64(1_200)
	)
	reaped := reapCred28(0xAA)
	seedRetiringPool(
		t, gdb, reaped, reapCred28(0x11), deposit, 100, newEpoch, 200,
	)
	seedReapTestEpoch(t, gdb, newEpoch, boundarySlot)

	delegator := reapCred28(0x21)
	seedDelegatedAccount(t, db, gdb, delegator, reaped, 300)
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	runApplyPoolRetirements(t, ls, db, newEpoch, boundarySlot)

	// Something touches the account after the reap, so the rollback revisits
	// it. Only the modification slot matters here.
	_, err := gdb.Exec(
		`UPDATE account SET added_slot = ? WHERE staking_key = ?`,
		laterSlot, delegator,
	)
	require.NoError(t, err)

	require.NoError(t, db.Metadata().RestoreAccountStateAtSlot(
		rollbackSlot, nil,
	))

	account, err := db.GetAccountByCredential(0, delegator, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Empty(t, account.Pool,
		"a rollback landing after the reap must not restore the delegation")
}

// TestRestoreAccountStateRevivesDelegationRolledBackBeforeReap is the other
// direction: rolling back past the boundary undoes the reap along with every
// other write it made, so the delegation certificate is authoritative again.
func TestRestoreAccountStateRevivesDelegationRolledBackBeforeReap(t *testing.T) {
	ls, db, gdb := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(1_000)
		rollbackSlot = uint64(900)
	)
	reaped := reapCred28(0xAA)
	seedRetiringPool(
		t, gdb, reaped, reapCred28(0x11), deposit, 100, newEpoch, 200,
	)
	seedReapTestEpoch(t, gdb, newEpoch, boundarySlot)

	delegator := reapCred28(0x21)
	seedDelegatedAccount(t, db, gdb, delegator, reaped, 300)
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	runApplyPoolRetirements(t, ls, db, newEpoch, boundarySlot)
	require.NoError(t, db.Metadata().RestoreAccountStateAtSlot(
		rollbackSlot, nil,
	))

	account, err := db.GetAccountByCredential(0, delegator, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, reaped, account.Pool,
		"rolling back past the reap restores the delegation it removed")
}

// seedReapTestEpoch gives the retirement epoch a row, so the reap boundary can
// be resolved from the certificate's epoch the way the ledger resolves it.
func seedReapTestEpoch(
	t *testing.T,
	raw *sql.DB,
	epoch, startSlot uint64,
) {
	t.Helper()
	_, err := raw.Exec(`
INSERT INTO epoch (epoch_id, start_slot, era_id, slot_length, length_in_slots)
VALUES (?, ?, 6, 1000, 500)`,
		epoch, startSlot,
	)
	require.NoError(t, err)
}

// seedDelegatedAccount creates an account delegated to pool with a matching
// registration and stake_delegation certificate, so a rollback can re-derive
// its state from certificate history the way a replayed account's would be.
func seedDelegatedAccount(
	t *testing.T,
	db *database.Database,
	raw *sql.DB,
	stakingKey, pool []byte,
	certSlot uint64,
) {
	t.Helper()
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:  stakingKey,
		Pool:        pool,
		AddedSlot:   certSlot,
		CreatedSlot: 1,
		Active:      true,
	}))
	_, err := raw.Exec(`
INSERT INTO registration (staking_key, credential_tag, added_slot)
VALUES (?, 0, 1)`,
		stakingKey,
	)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO stake_delegation (
    staking_key, credential_tag, pool_key_hash, added_slot
) VALUES (?, 0, ?, ?)`,
		stakingKey, pool, certSlot,
	)
	require.NoError(t, err)
}

// TestRestoreAccountStateHonoursSupersedingRetirement covers a pool whose
// retirement certificate was replaced by a later one for a further-out epoch.
// cardano-ledger keeps only the latest retirement, so the earlier epoch's
// boundary reaps nothing — GetPoolsRetiringAtEpoch encodes exactly that by
// selecting the latest retirement per pool and requiring its epoch to match the
// boundary. The rollback derivation has to agree, or it clears a delegation the
// forward path never cleared.
func TestRestoreAccountStateHonoursSupersedingRetirement(t *testing.T) {
	ls, db, gdb := newPoolreapTestLedger(t)

	const (
		deposit        = uint64(500)
		firstEpoch     = uint64(5)
		firstBoundary  = uint64(1_000)
		secondEpoch    = uint64(9)
		secondBoundary = uint64(5_000)
		rollbackSlot   = uint64(1_200)
	)
	pool := reapCred28(0xAA)
	seedRetiringPool(
		t, gdb, pool, reapCred28(0x11), deposit, 100, firstEpoch, 200,
	)
	// A later certificate moves the retirement out to a further epoch.
	poolID := poolIDForKeyHash(t, gdb, pool)
	_, err := gdb.Exec(`
INSERT INTO pool_retirement (pool_id, pool_key_hash, epoch, added_slot)
VALUES (?, ?, ?, ?)`,
		poolID, pool, secondEpoch, 300,
	)
	require.NoError(t, err)
	seedReapTestEpoch(t, gdb, firstEpoch, firstBoundary)
	seedReapTestEpoch(t, gdb, secondEpoch, secondBoundary)

	delegator := reapCred28(0x21)
	seedDelegatedAccount(t, db, gdb, delegator, pool, 400)
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	// The forward path agrees the first boundary reaps nothing.
	runApplyPoolRetirements(t, ls, db, firstEpoch, firstBoundary)
	account, err := db.GetAccountByCredential(0, delegator, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Equal(t, pool, account.Pool,
		"a superseded retirement must not reap the pool")

	// Force the rollback to revisit the account, then confirm the derivation
	// reaches the same conclusion.
	_, err = gdb.Exec(
		`UPDATE account SET added_slot = ? WHERE staking_key = ?`,
		rollbackSlot+500, delegator,
	)
	require.NoError(t, err)
	require.NoError(t, db.Metadata().RestoreAccountStateAtSlot(
		rollbackSlot, nil,
	))

	account, err = db.GetAccountByCredential(0, delegator, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, pool, account.Pool,
		"the rollback must not clear a delegation the reap never cleared")
}

// poolIDForKeyHash returns the pool row id seedRetiringPool created.
func poolIDForKeyHash(t *testing.T, raw *sql.DB, keyHash []byte) int64 {
	t.Helper()
	var id int64
	require.NoError(t, raw.QueryRow(
		`SELECT id FROM pool WHERE pool_key_hash = ?`, keyHash,
	).Scan(&id))
	return id
}
