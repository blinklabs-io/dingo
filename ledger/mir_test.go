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
	"database/sql"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
func newMIRTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database, *sql.DB) {
	t.Helper()
	return newPoolreapTestLedger(t)
}

// seedMIRDistribution inserts a MoveInstantaneousRewards row with one or more
// credential→amount reward rows, simulating a distribution MIR cert.
func seedMIRDistribution(
	t *testing.T,
	raw *sql.DB,
	pot uint,
	addedSlot uint64,
	rewards []models.MoveInstantaneousRewardsReward,
) {
	t.Helper()
	result, err := raw.Exec(`
INSERT INTO move_instantaneous_rewards (pot, added_slot, other_pot)
VALUES (?, ?, '0')`,
		pot, addedSlot,
	)
	require.NoError(t, err)
	mirID, err := result.LastInsertId()
	require.NoError(t, err)
	for i := range rewards {
		_, err = raw.Exec(`
INSERT INTO move_instantaneous_rewards_reward (
    mir_id, credential, credential_tag, amount
) VALUES (?, ?, ?, ?)`,
			mirID,
			rewards[i].Credential,
			rewards[i].CredentialTag,
			rewards[i].Amount.String(),
		)
		require.NoError(t, err)
	}
}

// seedMIRPotTransfer inserts a MoveInstantaneousRewards row representing a
// pot-to-pot transfer (OtherPot > 0, no credential rows).
func seedMIRPotTransfer(
	t *testing.T,
	raw *sql.DB,
	sourcePot uint,
	amount uint64,
	addedSlot uint64,
) {
	t.Helper()
	_, err := raw.Exec(`
INSERT INTO move_instantaneous_rewards (pot, added_slot, other_pot)
VALUES (?, ?, ?)`,
		sourcePot,
		addedSlot,
		strconv.FormatUint(amount, 10),
	)
	require.NoError(t, err)
}

// runApplyMIRCerts and applyMIRCertsErr default to an Alonzo-era boundary, so
// existing MIR tests exercise the additive (era >= Alonzo) fold unless a test
// cares about the era and calls the …Era variant below directly.
func runApplyMIRCerts(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	epochStartSlot, boundarySlot uint64,
) {
	t.Helper()
	runApplyMIRCertsEra(
		t, ls, db, epochStartSlot, boundarySlot, alonzo.EraIdAlonzo,
	)
}

func runApplyMIRCertsEra(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	epochStartSlot, boundarySlot uint64,
	epochEraID uint,
) {
	t.Helper()
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyMIRCerts(txn, epochStartSlot, boundarySlot, epochEraID)
	}))
}

func applyMIRCertsErr(
	ls *LedgerState,
	db *database.Database,
	epochStartSlot, boundarySlot uint64,
) error {
	return applyMIRCertsErrEra(
		ls, db, epochStartSlot, boundarySlot, alonzo.EraIdAlonzo,
	)
}

func applyMIRCertsErrEra(
	ls *LedgerState,
	db *database.Database,
	epochStartSlot, boundarySlot uint64,
	epochEraID uint,
) error {
	txn := db.Transaction(true)
	return txn.Do(func(txn *database.Txn) error {
		return ls.applyMIRCerts(txn, epochStartSlot, boundarySlot, epochEraID)
	})
}

// TestApplyMIRCerts_UnknownPotDiscardsBoundary verifies that an unknown-pot
// distribution certificate discards the whole boundary rather than merely
// being skipped: an otherwise-valid credit at the same boundary must not be
// applied either. A lone unknown-pot row with no reward entries can't tell
// discard apart from skip, since both leave an empty boundary and the same
// unchanged state; seeding a valid credit alongside it separates them.
func TestApplyMIRCerts_UnknownPotDiscardsBoundary(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)
	cred := mirCred28(0x15)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		400,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(500)},
		},
	)
	seedMIRDistribution(t, gdb, 2, 500, nil)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 10_000, 50, nil))

	assert.NoError(t, applyMIRCertsErr(ls, db, 0, 1_000))

	account, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"an unknown pot elsewhere in the boundary discards the whole "+
			"boundary, including an otherwise-valid credit")

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(10_000), uint64(state.Reserves))
}

// TestApplyMIRCerts_DistributionFromReserves_RegisteredAccount verifies that a
// MIR cert distributing from reserves credits the registered reward account and
// debits reserves.
func TestApplyMIRCerts_DistributionFromReserves_RegisteredAccount(
	t *testing.T,
) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	const (
		epochStartSlot = uint64(0)
		boundarySlot   = uint64(1_000)
		mirAmount      = uint64(750)
	)
	cred := mirCred28(0x11)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		500,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(mirAmount)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Reward:     0,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 10_000, 50, nil))

	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)

	account, err := db.GetAccountByCredential(0, cred, false, nil)
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

// TestApplyMIRCerts_MultipleDistributionsSameAccount verifies that distinct MIR
// certs crediting the same account from the same pot at one epoch boundary are
// folded into the single credit cardano-ledger's InstantaneousRewards map
// produces, rather than one journal event per certificate. It runs at
// runApplyMIRCerts's default Alonzo-era boundary, where the reference folds
// additively; see TestApplyMIRCerts_PreAlonzoSameCredentialReplaces for the
// pre-Alonzo case, where the reference instead keeps only the later amount.
func TestApplyMIRCerts_MultipleDistributionsSameAccount(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	const (
		epochStartSlot = uint64(0)
		boundarySlot   = uint64(1_000)
		firstAmount    = uint64(300)
		secondAmount   = uint64(450)
	)
	cred := mirCred28(0x12)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		200,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(firstAmount)},
		},
	)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		400,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(secondAmount)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Reward:     0,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 10_000, 50, nil))

	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)

	account, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, firstAmount+secondAmount, uint64(account.Reward))

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(9_250), uint64(state.Reserves))

	require.Len(
		t,
		boundaryRewardSourceHashes(t, gdb, cred, boundarySlot),
		1,
	)
}

// TestApplyMIRCerts_PreAlonzoSameCredentialReplaces verifies that below
// Alonzo, two MIR certs crediting the same account from the same pot at one
// epoch boundary credit only the later certificate's amount rather than
// their sum. cardano-ledger's delegTransition folds with plain, left-biased
// Map.union pre-Alonzo (majors 2-4), replacing rather than combining a
// pending entry; only Alonzo onward switches to the additive
// Map.unionWith (<>) that TestApplyMIRCerts_MultipleDistributionsSameAccount
// exercises.
func TestApplyMIRCerts_PreAlonzoSameCredentialReplaces(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	const (
		epochStartSlot = uint64(0)
		boundarySlot   = uint64(1_000)
		firstAmount    = uint64(300)
		secondAmount   = uint64(450)
	)
	cred := mirCred28(0x13)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		200,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(firstAmount)},
		},
	)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		400,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(secondAmount)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Reward:     0,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 10_000, 50, nil))

	runApplyMIRCertsEra(
		t, ls, db, epochStartSlot, boundarySlot, shelley.EraIdShelley,
	)

	account, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, secondAmount, uint64(account.Reward),
		"pre-Alonzo replaces with the later certificate's amount, not the sum")

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(9_550), uint64(state.Reserves))
}

// TestApplyMIRCerts_ReservesAndTreasuryStayDistinct verifies that a reserves
// credit and a treasury credit to the same account at one epoch boundary remain
// distinct journal events. cardano-ledger keeps iRReserves and iRTreasury as
// separate maps, so folding is per pot and both credits must survive the
// journal's (tx_hash, credential, slot) idempotency key.
func TestApplyMIRCerts_ReservesAndTreasuryStayDistinct(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	const (
		epochStartSlot = uint64(0)
		boundarySlot   = uint64(1_000)
		reservesAmount = uint64(300)
		treasuryAmount = uint64(450)
	)
	cred := mirCred28(0x1A)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		200,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(reservesAmount)},
		},
	)
	seedMIRDistribution(
		t,
		gdb,
		mirPotTreasury,
		400,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(treasuryAmount)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Reward:     0,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 10_000, 50, nil))

	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)

	account, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, reservesAmount+treasuryAmount, uint64(account.Reward))

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(10_000-reservesAmount), uint64(state.Reserves))
	assert.Equal(t, uint64(1_000-treasuryAmount), uint64(state.Treasury))

	hashes := boundaryRewardSourceHashes(t, gdb, cred, boundarySlot)
	require.Len(t, hashes, 2)
	assert.NotEqual(t, string(hashes[0]), string(hashes[1]))
}

// boundaryRewardSourceHashes returns the reward journal discriminators written
// for a credential at one boundary slot, in insertion order.
func boundaryRewardSourceHashes(
	t *testing.T,
	gdb *sql.DB,
	credential []byte,
	boundarySlot uint64,
) [][]byte {
	t.Helper()
	rows, err := gdb.Query(`
SELECT tx_hash FROM account_reward_delta
WHERE credential_tag = ? AND staking_key = ? AND added_slot = ?
ORDER BY id ASC`,
		0, credential, boundarySlot,
	)
	require.NoError(t, err)
	defer rows.Close()
	var hashes [][]byte
	for rows.Next() {
		var hash []byte
		require.NoError(t, rows.Scan(&hash))
		hashes = append(hashes, hash)
	}
	require.NoError(t, rows.Err())
	return hashes
}

// TestApplyMIRCerts_DistributionTotalBeyondEveryPotIsNoOp verifies that folded
// credits whose per-pot total no longer fits uint64 discard the boundary rather
// than failing it. cardano-ledger folds over unbounded Coin, so a total larger
// than the pot reaches its no-op branch; failing would wedge the node, since
// the stored certificates are re-read and re-fail on every retry.
func TestApplyMIRCerts_DistributionTotalBeyondEveryPotIsNoOp(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	maxUint := ^uint64(0)
	credA := mirCred28(0x13)
	credB := mirCred28(0x14)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		200,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: credA, Amount: new(big.Int).SetUint64(maxUint)},
			{Credential: credB, Amount: new(big.Int).SetUint64(1)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: credA,
		Active:     true,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: credB,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, maxUint, 50, nil))

	require.NoError(t, applyMIRCertsErr(ls, db, 0, 1_000),
		"a total larger than every pot must not fail the epoch boundary")

	accountA, err := db.GetAccountByCredential(0, credA, false, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(accountA.Reward))
	accountB, err := db.GetAccountByCredential(0, credB, false, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(accountB.Reward))
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.Equal(t, maxUint, uint64(state.Reserves))
	require.Equal(t, uint64(50), state.Slot)
}

// TestApplyMIRCerts_DistributionFromTreasury_RegisteredAccount verifies a MIR
// cert from the treasury credits the account and debits the treasury.
func TestApplyMIRCerts_DistributionFromTreasury_RegisteredAccount(
	t *testing.T,
) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	const (
		epochStartSlot = uint64(0)
		boundarySlot   = uint64(1_000)
		mirAmount      = uint64(200)
	)
	cred := mirCred28(0x22)
	seedMIRDistribution(
		t,
		gdb,
		mirPotTreasury,
		500,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(mirAmount)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Reward:     0,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(5_000, 8_000, 50, nil))

	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)

	account, err := db.GetAccountByCredential(0, cred, false, nil)
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
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	cred := mirCred28(0x33) // no Account row seeded
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		500,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(400)},
		},
	)
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
	t.Parallel()

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
	t.Parallel()

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

// TestApplyMIRCerts_PotTransferOverflow verifies that an inbound transfer
// that would overflow the destination pot's uint64 balance discards the
// boundary as a no-op rather than failing the epoch rollover, the same as an
// over-budget distribution: a hard error here would wedge the node, since the
// stored certificate is re-read and re-fails on every deterministic retry.
func TestApplyMIRCerts_PotTransferOverflow(t *testing.T) {
	t.Parallel()

	maxUint := ^uint64(0)

	t.Run("reserves to treasury", func(t *testing.T) {
		ls, db, gdb := newMIRTestLedger(t)
		seedMIRPotTransfer(t, gdb, mirPotReserves, 1, 500)
		require.NoError(t, db.Metadata().SetNetworkState(maxUint, 1, 50, nil))

		require.NoError(t, applyMIRCertsErr(ls, db, 0, 1_000),
			"pot overflow must not fail the epoch boundary")
		state, stateErr := db.Metadata().GetNetworkState(nil)
		require.NoError(t, stateErr)
		require.Equal(t, maxUint, uint64(state.Treasury))
		require.Equal(t, uint64(1), uint64(state.Reserves))
		require.Equal(t, uint64(50), state.Slot)
	})

	t.Run("treasury to reserves", func(t *testing.T) {
		ls, db, gdb := newMIRTestLedger(t)
		seedMIRPotTransfer(t, gdb, mirPotTreasury, 1, 500)
		require.NoError(t, db.Metadata().SetNetworkState(1, maxUint, 50, nil))

		require.NoError(t, applyMIRCertsErr(ls, db, 0, 1_000),
			"pot overflow must not fail the epoch boundary")
		state, stateErr := db.Metadata().GetNetworkState(nil)
		require.NoError(t, stateErr)
		require.Equal(t, uint64(1), uint64(state.Treasury))
		require.Equal(t, maxUint, uint64(state.Reserves))
		require.Equal(t, uint64(50), state.Slot)
	})
}

// TestApplyMIRCerts_PotTransferOverflowLogsDiscardReason verifies that the
// inbound-overflow case names the actual overflow through boundary.discard
// instead of the generic over-budget warning: without that, an operator
// would only see "reserves_distributed=0 treasury_distributed=0" for a
// boundary that was not actually over budget.
func TestApplyMIRCerts_PotTransferOverflowLogsDiscardReason(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)
	var logBuf bytes.Buffer
	ls.config.Logger = slog.New(slog.NewTextHandler(&logBuf, nil))

	maxUint := ^uint64(0)
	seedMIRPotTransfer(t, gdb, mirPotReserves, 1, 500)
	require.NoError(t, db.Metadata().SetNetworkState(maxUint, 1, 50, nil))

	require.NoError(t, applyMIRCertsErr(ls, db, 0, 1_000))

	assertLogContains(t, &logBuf, []string{
		"discarding uncreditable MIR at epoch boundary",
		"would overflow treasury",
	})
}

// TestApplyMIRCerts_UnknownSourcePotIsNoOp verifies that a pot-to-pot
// transfer naming a source pot other than reserves (0) or treasury (1)
// discards the whole boundary rather than failing the epoch rollover or
// merely being skipped: an otherwise-valid credit at the same boundary must
// not be applied either. A lone unknown-pot transfer with nothing else at
// the boundary can't tell discard apart from skip, since both leave an
// empty boundary and the same unchanged state; seeding a valid credit
// alongside it separates them, the same way
// TestApplyMIRCerts_UnknownPotDiscardsBoundary does for the equivalent
// distribution-certificate case.
func TestApplyMIRCerts_UnknownSourcePotIsNoOp(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)
	cred := mirCred28(0x16)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		400,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(500)},
		},
	)
	seedMIRPotTransfer(t, gdb, 2, 500, 500)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 10_000, 50, nil))

	require.NoError(t, applyMIRCertsErr(ls, db, 0, 1_000),
		"unknown source pot must not fail the epoch boundary")

	account, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"an unknown source pot elsewhere in the boundary discards the whole "+
			"boundary, including an otherwise-valid credit")

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_000), uint64(state.Treasury))
	assert.Equal(t, uint64(10_000), uint64(state.Reserves))
}

// TestApplyMIRCerts_TransferTotalOverflowIsNoOp verifies that two pot-to-pot
// transfers from the same source pot in one epoch, whose combined total no
// longer fits uint64, discard the boundary as a no-op rather than failing
// the epoch rollover.
func TestApplyMIRCerts_TransferTotalOverflowIsNoOp(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)
	var logBuf bytes.Buffer
	ls.config.Logger = slog.New(slog.NewTextHandler(&logBuf, nil))

	maxUint := ^uint64(0)
	seedMIRPotTransfer(t, gdb, mirPotReserves, maxUint, 200)
	seedMIRPotTransfer(t, gdb, mirPotReserves, maxUint, 400)
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 10_000, 50, nil))

	require.NoError(t, applyMIRCertsErr(ls, db, 0, 1_000),
		"a transfer total overflow must not fail the epoch boundary")

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_000), uint64(state.Treasury))
	assert.Equal(t, uint64(10_000), uint64(state.Reserves))

	// Removing addTransfer's overflow guard would still leave reservesOut and
	// reservesIn/treasuryIn wrapped to a smaller value, and the boundary would
	// fall into the generic over-budget no-op with the same untouched state
	// asserted above. Only the discard log line distinguishes the two, so pin
	// it directly.
	assertLogContains(t, &logBuf, []string{
		"discarding uncreditable MIR at epoch boundary",
		"MIR pot transfer total overflow",
	})
}

// TestApplyMIRCerts_OutsideEpochRange verifies that a MIR cert submitted
// before epochStartSlot or at/after boundarySlot is not applied.
func TestApplyMIRCerts_OutsideEpochRange(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	cred := mirCred28(0x44)
	// addedSlot=50 is before epochStartSlot=100
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		50,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(500)},
		},
	)
	// addedSlot=1000 equals boundarySlot — excluded (half-open interval)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		1_000,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(300)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Reward:     0,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 10, nil))

	// epoch range: [100, 1000)
	runApplyMIRCerts(t, ls, db, 100, 1_000)

	account, err := db.GetAccountByCredential(0, cred, false, nil)
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
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	const (
		epochStartSlot = uint64(0)
		boundarySlot   = uint64(1_000)
		preBoundary    = uint64(500)
		mirAmount      = uint64(600)
	)
	cred := mirCred28(0x55)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		200,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(mirAmount)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Reward:     0,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 8_000, 50, nil))

	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)

	account, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	require.Equal(t, mirAmount, uint64(account.Reward), "MIR reward applied")
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(7_400), uint64(state.Reserves),
		"reserves debited after apply")

	// Roll back past the boundary: drop the reward credit and treasury row.
	require.NoError(t, db.DeleteAccountRewardsAfterSlot(preBoundary, nil))
	require.NoError(t, db.DeleteNetworkStateAfterSlot(preBoundary, nil))

	account, err = db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"reward credit reverted on rollback")
	state, err = db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(8_000), uint64(state.Reserves),
		"reserves restored to pre-boundary value")

	// Re-apply must be deterministic.
	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)
	account, err = db.GetAccountByCredential(0, cred, false, nil)
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
	t.Parallel()

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

// TestApplyMIRCerts_OverBudgetReservesIsNoOp verifies that a distribution
// exceeding the reserves pot is skipped for the epoch rather than failing the
// boundary. cardano-ledger's MIR rule compares the registered-account total
// against the available pot and returns the epoch state unchanged when it does
// not fit, so an over-budget certificate must not abort the rollover.
func TestApplyMIRCerts_OverBudgetReservesIsNoOp(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	cred := mirCred28(0x61)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		500,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(750)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 500, 50, nil))

	require.NoError(t, applyMIRCertsErr(ls, db, 0, 1_000),
		"over-budget MIR must not fail the epoch boundary")

	account, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"no credit applied for an over-budget distribution")
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), uint64(state.Reserves), "reserves untouched")
	assert.Equal(t, uint64(1_000), uint64(state.Treasury), "treasury untouched")
	assert.Equal(t, uint64(50), state.Slot,
		"no boundary state row written when the distribution is skipped")
}

// TestApplyMIRCerts_ExactBudgetApplies verifies the boundary case where the
// registered total exactly equals the pot: cardano-ledger uses `totR <=
// availableReserves`, so an exact-budget certificate is applied and drains the
// pot to zero.
func TestApplyMIRCerts_ExactBudgetApplies(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	cred := mirCred28(0x62)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		500,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(750)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 750, 50, nil))

	runApplyMIRCerts(t, ls, db, 0, 1_000)

	account, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(750), uint64(account.Reward),
		"exact-budget distribution is applied")
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(
		t,
		uint64(0),
		uint64(state.Reserves),
		"reserves drained to zero",
	)
	assert.Equal(t, uint64(1_000), state.Slot,
		"boundary state row written at the boundary slot")
}

// TestApplyMIRCerts_OverBudgetIsAggregateAcrossCerts verifies that the pot
// check covers every certificate applied at the boundary rather than each
// certificate individually. cardano-ledger accumulates all MIR credits into
// iRReserves/iRTreasury and performs one comparison, so an affordable
// certificate must not be applied when a sibling pushes the epoch total over
// the pot.
func TestApplyMIRCerts_OverBudgetIsAggregateAcrossCerts(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	credA := mirCred28(0x63)
	credB := mirCred28(0x64)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		200,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: credA, Amount: new(big.Int).SetUint64(600)},
		},
	)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		300,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: credB, Amount: new(big.Int).SetUint64(600)},
		},
	)
	for _, cred := range [][]byte{credA, credB} {
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey: cred,
			Active:     true,
		}))
	}
	// Either cert alone fits in 1000; together they do not.
	require.NoError(t, db.Metadata().SetNetworkState(0, 1_000, 50, nil))

	require.NoError(t, applyMIRCertsErr(ls, db, 0, 1_000))

	for _, cred := range [][]byte{credA, credB} {
		account, err := db.GetAccountByCredential(0, cred, false, nil)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), uint64(account.Reward),
			"no cert applied when the epoch total exceeds the pot")
	}
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_000), uint64(state.Reserves), "reserves untouched")
	assert.Equal(t, uint64(50), state.Slot)
}

// TestApplyMIRCerts_BudgetExcludesUnregisteredCredentials verifies that the pot
// comparison is made against the registered-account total only. cardano-ledger
// restricts the credit map with `Map.intersection accountsMap` before folding,
// so credits to unregistered credentials must not make an affordable
// distribution look over budget.
func TestApplyMIRCerts_BudgetExcludesUnregisteredCredentials(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	registered := mirCred28(0x65)
	unregistered := mirCred28(0x66)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		500,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: registered, Amount: new(big.Int).SetUint64(400)},
			{Credential: unregistered, Amount: new(big.Int).SetUint64(5_000)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: registered,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(0, 1_000, 50, nil))

	runApplyMIRCerts(t, ls, db, 0, 1_000)

	account, err := db.GetAccountByCredential(0, registered, false, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(400), uint64(account.Reward),
		"registered credit applied; unregistered amount is not budgeted")
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(600), uint64(state.Reserves),
		"reserves debited only by the registered total")
}

// TestApplyMIRCerts_OverBudgetTreasuryBlocksReservesDistribution verifies the
// all-or-nothing shape of the rule: cardano-ledger requires `totR <=
// availableReserves && totT <= availableTreasury` and returns the account state
// unchanged when either fails, so an over-budget treasury distribution also
// suppresses an affordable reserves distribution.
func TestApplyMIRCerts_OverBudgetTreasuryBlocksReservesDistribution(
	t *testing.T,
) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	reservesCred := mirCred28(0x67)
	treasuryCred := mirCred28(0x68)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		200,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: reservesCred, Amount: new(big.Int).SetUint64(100)},
		},
	)
	seedMIRDistribution(
		t,
		gdb,
		mirPotTreasury,
		300,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: treasuryCred, Amount: new(big.Int).SetUint64(9_000)},
		},
	)
	for _, cred := range [][]byte{reservesCred, treasuryCred} {
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey: cred,
			Active:     true,
		}))
	}
	require.NoError(t, db.Metadata().SetNetworkState(500, 1_000, 50, nil))

	require.NoError(t, applyMIRCertsErr(ls, db, 0, 1_000))

	for _, cred := range [][]byte{reservesCred, treasuryCred} {
		account, err := db.GetAccountByCredential(0, cred, false, nil)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), uint64(account.Reward),
			"neither pot is distributed when one is over budget")
	}
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_000), uint64(state.Reserves))
	assert.Equal(t, uint64(500), uint64(state.Treasury))
	assert.Equal(t, uint64(50), state.Slot)
}

// TestApplyMIRCerts_PotTransferCountsTowardAvailablePot verifies that a
// pot-to-pot transfer in the same epoch is folded into the available pot before
// the distribution check, matching cardano-ledger's `availableReserves =
// reserves + deltaReserves`.
func TestApplyMIRCerts_PotTransferCountsTowardAvailablePot(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	cred := mirCred28(0x69)
	// Treasury alone cannot cover 900, but gains 500 from reserves first.
	seedMIRPotTransfer(t, gdb, mirPotReserves, 500, 200)
	seedMIRDistribution(
		t,
		gdb,
		mirPotTreasury,
		300,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(900)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(600, 2_000, 50, nil))

	runApplyMIRCerts(t, ls, db, 0, 1_000)

	account, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(900), uint64(account.Reward),
		"distribution fits once the pot transfer is applied")
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(200), uint64(state.Treasury),
		"treasury: 600 + 500 transferred - 900 distributed")
	assert.Equal(t, uint64(1_500), uint64(state.Reserves),
		"reserves: 2000 - 500 transferred")
}

// TestApplyMIRCerts_OverBudgetDropsPotTransfer verifies that the no-op branch
// discards the epoch's pot-to-pot transfers as well. cardano-ledger's else
// branch returns the original ChainAccountState, so the deltas that were only
// ever folded into `available` are not written.
func TestApplyMIRCerts_OverBudgetDropsPotTransfer(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	cred := mirCred28(0x6a)
	seedMIRPotTransfer(t, gdb, mirPotReserves, 500, 200)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		300,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: new(big.Int).SetUint64(5_000)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(0, 1_000, 50, nil))

	require.NoError(t, applyMIRCertsErr(ls, db, 0, 1_000))

	account, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), uint64(account.Reward))
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_000), uint64(state.Reserves),
		"pot transfer discarded along with the over-budget distribution")
	assert.Equal(t, uint64(0), uint64(state.Treasury))
	assert.Equal(t, uint64(50), state.Slot)
}

// TestApplyMIRCerts_PotTransferLargerThanPotIsNoOp verifies that a pot-to-pot
// transfer exceeding its source pot is skipped rather than failing the
// boundary. cardano-ledger computes `reserves + deltaReserves` over unbounded
// Coin, so a net outflow larger than the pot makes `totR <= availableReserves`
// false and the rule takes its no-op branch.
func TestApplyMIRCerts_PotTransferLargerThanPotIsNoOp(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	seedMIRPotTransfer(t, gdb, mirPotReserves, 5_000, 200)
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 800, 50, nil))

	require.NoError(t, applyMIRCertsErr(ls, db, 0, 1_000),
		"an unaffordable pot transfer must not fail the epoch boundary")

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(800), uint64(state.Reserves), "reserves untouched")
	assert.Equal(t, uint64(1_000), uint64(state.Treasury), "treasury untouched")
	assert.Equal(t, uint64(50), state.Slot,
		"no boundary state row written for a skipped transfer")
}

// TestApplyMIRCerts_NegativeDeltaReducesEarlierCredit proves a negative MIR
// delta is applied rather than rejected or truncated. gouroboros decodes a MIR
// reward as delta_coin, and cardano-ledger's DELEG rule accumulates the
// certificates of an epoch into one InstantaneousRewards entry per credential,
// so a later negative delta reduces an earlier positive one and the boundary
// credits only the fold.
func TestApplyMIRCerts_NegativeDeltaReducesEarlierCredit(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	const (
		epochStartSlot = uint64(0)
		boundarySlot   = uint64(1_000)
		reserves       = uint64(10_000)
	)
	cred := mirCred28(0x71)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		200,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: big.NewInt(1_000)},
		},
	)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		400,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: big.NewInt(-400)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Active:     true,
	}))
	require.NoError(
		t,
		db.Metadata().SetNetworkState(1_000, reserves, 50, nil),
	)

	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)

	account, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(600), uint64(account.Reward),
		"the account is credited the fold of the epoch's deltas")

	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, reserves-600, uint64(state.Reserves),
		"reserves are debited only what was credited")
	assert.Equal(t, uint64(1_000), uint64(state.Treasury))

	require.Len(
		t,
		boundaryRewardSourceHashes(t, gdb, cred, boundarySlot),
		1,
		"the folded credit is one journal event, not one per certificate",
	)
}

// TestApplyMIRCerts_NegativeDeltaCancellingCreditWritesNothing verifies that
// deltas netting to zero neither credit the account nor move the pot.
func TestApplyMIRCerts_NegativeDeltaCancellingCreditWritesNothing(
	t *testing.T,
) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	cred := mirCred28(0x72)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		200,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: big.NewInt(500)},
		},
	)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		400,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: big.NewInt(-500)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Active:     true,
	}))
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 10_000, 50, nil))

	runApplyMIRCerts(t, ls, db, 0, 1_000)

	account, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), uint64(account.Reward))
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(10_000), uint64(state.Reserves))
	assert.Equal(t, uint64(50), state.Slot,
		"no boundary state row written when nothing moves")
	assert.Empty(t, boundaryRewardSourceHashes(t, gdb, cred, 1_000))
}

// TestApplyMIRCerts_NegativeDeltaExcludedFromBudget proves the pot capacity
// check is computed over the signed fold. Summing the deltas as unsigned
// magnitudes would make this boundary look over-budget and drop a distribution
// the pot can cover.
func TestApplyMIRCerts_NegativeDeltaExcludedFromBudget(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	cred := mirCred28(0x73)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		200,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: big.NewInt(900)},
		},
	)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		400,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: big.NewInt(-400)},
		},
	)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred,
		Active:     true,
	}))
	// 500 fits; the 900 of the first certificate on its own does not.
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 500, 50, nil))

	runApplyMIRCerts(t, ls, db, 0, 1_000)

	account, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), uint64(account.Reward))
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), uint64(state.Reserves))
}

// TestApplyMIRCerts_NetNegativeDeltaDiscardsBoundary pins the behaviour for a
// credential whose deltas net below zero. cardano-ledger cannot reach this
// state: DELEG rejects the transaction with MIRProducesNegativeUpdate. Dingo
// does not run that accumulation check at transaction ingestion, so the
// boundary discards the whole set rather than crediting a debit the reward
// account cannot carry, and the epoch rollover still succeeds.
func TestApplyMIRCerts_NetNegativeDeltaDiscardsBoundary(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	negativeCred := mirCred28(0x74)
	otherCred := mirCred28(0x75)
	seedMIRDistribution(
		t,
		gdb,
		mirPotReserves,
		200,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: negativeCred, Amount: big.NewInt(-100)},
			{Credential: otherCred, Amount: big.NewInt(300)},
		},
	)
	seedMIRPotTransfer(t, gdb, mirPotTreasury, 250, 300)
	for _, cred := range [][]byte{negativeCred, otherCred} {
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey: cred,
			Active:     true,
		}))
	}
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 10_000, 50, nil))

	require.NoError(t, applyMIRCertsErr(ls, db, 0, 1_000),
		"an uncreditable MIR fold must not fail the epoch boundary")

	for _, cred := range [][]byte{negativeCred, otherCred} {
		account, err := db.GetAccountByCredential(0, cred, false, nil)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), uint64(account.Reward),
			"the whole boundary is discarded, not just the negative entry")
	}
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(10_000), uint64(state.Reserves))
	assert.Equal(t, uint64(1_000), uint64(state.Treasury),
		"the pot transfer at the same boundary is discarded too")
	assert.Equal(t, uint64(50), state.Slot)
}

// withMIRCutoffEpoch gives ls a Shelley genesis with k=2160 and f=1/20, so a
// 129,600-slot stability window, and publishes epoch as the only cached
// epoch, which is what LedgerView.MIRDelegState needs to compute the cutoff.
func withMIRCutoffEpoch(t *testing.T, ls *LedgerState, epoch models.Epoch) {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.05,
		"securityParam": 2160,
		"systemStart": "2017-09-23T21:44:51Z"
	}`)))
	ls.config.CardanoNodeConfig = cfg
	ls.Lock()
	ls.currentEpoch = epoch
	ls.epochCache = []models.Epoch{epoch}
	ls.publishSnapshotsLocked()
	ls.Unlock()
}

func mirDelegState(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	epochStartSlot uint64,
	slot uint64,
	additive bool,
) eras.MIRDelegState {
	t.Helper()
	var state eras.MIRDelegState
	txn := db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		lv := &LedgerView{ls: ls, txn: txn, epochStartSlot: epochStartSlot}
		var err error
		state, err = lv.MIRDelegState(slot, additive)
		return err
	}))
	return state
}

// TestLedgerView_MIRDelegState_Cutoff pins the cutoff to the worked example in
// blinklabs-io/dingo#4362: a boundary at 1,000,000 and a 129,600-slot window
// put it at 870,400, whichever slot of the epoch asks.
func TestLedgerView_MIRDelegState_Cutoff(t *testing.T) {
	t.Parallel()

	ls, db, _ := newMIRTestLedger(t)
	withMIRCutoffEpoch(t, ls, models.Epoch{
		EpochId:       1,
		StartSlot:     568_000,
		LengthInSlots: 432_000,
	})
	for _, slot := range []uint64{568_000, 870_399, 870_400, 999_999} {
		state := mirDelegState(t, ls, db, 568_000, slot, true)
		assert.Equal(t, uint64(870_400), state.Cutoff, "slot %d", slot)
	}
}

// TestLedgerView_MIRDelegState_CutoffRequiresGenesis proves the cutoff fails
// closed rather than falling back to a default window when the Shelley
// genesis is missing.
func TestLedgerView_MIRDelegState_CutoffRequiresGenesis(t *testing.T) {
	t.Parallel()

	ls, db, _ := newMIRTestLedger(t)
	epoch := models.Epoch{StartSlot: 0, LengthInSlots: 432_000}
	ls.Lock()
	ls.currentEpoch = epoch
	ls.epochCache = []models.Epoch{epoch}
	ls.publishSnapshotsLocked()
	ls.Unlock()
	txn := db.Transaction(false)
	err := txn.Do(func(txn *database.Txn) error {
		lv := &LedgerView{ls: ls, txn: txn}
		_, err := lv.MIRDelegState(100, true)
		return err
	})
	require.ErrorContains(t, err, "cardano node config is not set")
}

// TestLedgerView_MIRDelegState_Pending verifies the query sees MIR
// certificates already committed earlier in the current epoch: it folds deltas
// per credential within [epochStartSlot, slot] inclusive and excludes anything
// outside that window on either side.
func TestLedgerView_MIRDelegState_Pending(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)
	withMIRCutoffEpoch(
		t,
		ls,
		models.Epoch{StartSlot: 100, LengthInSlots: 432_000},
	)

	cred := mirCred28(0x77)
	other := mirCred28(0x78)
	// Before epochStartSlot=100 — excluded.
	seedMIRDistribution(t, gdb, mirPotReserves, 50,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: big.NewInt(1_000)},
		})
	// Within the window, two certs on the same credential.
	seedMIRDistribution(t, gdb, mirPotReserves, 150,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: big.NewInt(10)},
		})
	seedMIRDistribution(t, gdb, mirPotReserves, 200,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: big.NewInt(-3)},
			{Credential: other, Amount: big.NewInt(5)},
		})
	// After slot=200 — excluded, as if not yet validated.
	seedMIRDistribution(t, gdb, mirPotReserves, 201,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: big.NewInt(999)},
		})

	credKey := eras.MIRCredentialKey{Credential: lcommon.NewBlake2b224(cred)}
	otherKey := eras.MIRCredentialKey{Credential: lcommon.NewBlake2b224(other)}

	additive := mirDelegState(t, ls, db, 100, 200, true)
	assert.Equal(t, big.NewInt(7), additive.Pending[credKey],
		"10 at slot 150 plus -3 at slot 200, excluding slot 50 and slot 201")
	assert.Equal(t, big.NewInt(5), additive.Pending[otherKey])

	replaced := mirDelegState(t, ls, db, 100, 200, false)
	assert.Equal(t, big.NewInt(-3), replaced.Pending[credKey],
		"before protocol version 5 the later certificate replaces the earlier")
}

// TestLedgerView_MIRDelegState_PotsAndTransfers verifies the query reports the
// chain account pots and nets the epoch's pot transfers into signed deltas.
func TestLedgerView_MIRDelegState_PotsAndTransfers(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)
	withMIRCutoffEpoch(
		t,
		ls,
		models.Epoch{StartSlot: 100, LengthInSlots: 432_000},
	)
	require.NoError(t,
		db.Metadata().SetNetworkState(1_000, 5_000, 100, nil))
	seedMIRPotTransfer(t, gdb, mirPotReserves, 300, 150)
	seedMIRPotTransfer(t, gdb, mirPotTreasury, 40, 160)
	seedMIRPotTransfer(t, gdb, mirPotReserves, 7, 50)

	state := mirDelegState(t, ls, db, 100, 200, true)
	assert.Equal(t, uint64(5_000), state.Reserves)
	assert.Equal(t, uint64(1_000), state.Treasury)
	assert.Equal(t, big.NewInt(-260), state.DeltaReserves)
	assert.Equal(t, big.NewInt(260), state.DeltaTreasury)
}

// TestLedgerView_MIRDelegState_PinnedSurvivesConcurrentRollover addresses a
// human review finding on PR #4415: the query must read lv.epochStartSlot, a
// value pinned once when this view was built for a specific transaction's
// validation, never a fresh read of LedgerState.currentEpoch -- because a
// concurrent writer can roll the epoch over while this transaction's
// validation is still in flight. A view built fresh after the rollover
// (ls.NewView, exactly what a caller must not do mid-validation) reproduces
// the bug the review flagged: the rolled-over epoch's later start slot
// exceeds the still-valid slot and hides every pending delta.
func TestLedgerView_MIRDelegState_PinnedSurvivesConcurrentRollover(
	t *testing.T,
) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)
	withMIRCutoffEpoch(
		t,
		ls,
		models.Epoch{StartSlot: 100, LengthInSlots: 432_000},
	)

	cred := mirCred28(0x7A)
	seedMIRDistribution(t, gdb, mirPotReserves, 150,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: big.NewInt(10)},
		})

	txn := db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		pinned := &LedgerView{ls: ls, txn: txn, epochStartSlot: 100}

		ls.Lock()
		ls.currentEpoch = models.Epoch{StartSlot: 500}
		ls.publishSnapshotsLocked()
		ls.Unlock()

		pinnedState, err := pinned.MIRDelegState(150, true)
		require.NoError(t, err)
		key := eras.MIRCredentialKey{Credential: lcommon.NewBlake2b224(cred)}
		assert.Equal(
			t,
			big.NewInt(10),
			pinnedState.Pending[key],
			"a pinned view must still see epoch A's own certificates after a concurrent rollover to epoch B",
		)

		freshState, err := ls.NewView(txn).MIRDelegState(150, true)
		require.NoError(t, err)
		assert.Empty(
			t,
			freshState.Pending,
			"documents why a validation call must reuse its own pinned view, never build a fresh one mid-flight",
		)
		return nil
	}))
}

// TestLedgerView_MIRDelegState_BeforeEpochStart verifies that a validating
// slot before the current epoch's start (a stale/overlay call) returns no
// pending deltas rather than an inverted or negative range.
func TestLedgerView_MIRDelegState_BeforeEpochStart(t *testing.T) {
	t.Parallel()

	ls, db, _ := newMIRTestLedger(t)
	withMIRCutoffEpoch(
		t,
		ls,
		models.Epoch{StartSlot: 0, LengthInSlots: 432_000},
	)
	state := mirDelegState(t, ls, db, 100, 50, true)
	assert.Empty(t, state.Pending)
}

// TestLedgerView_MIRDelegState_SeparatesPots proves the query keys its totals
// by source pot as well as credential, matching the reference's separate
// iRReserves and iRTreasury maps.
func TestLedgerView_MIRDelegState_SeparatesPots(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)
	withMIRCutoffEpoch(
		t,
		ls,
		models.Epoch{StartSlot: 100, LengthInSlots: 432_000},
	)

	cred := mirCred28(0x79)
	seedMIRDistribution(t, gdb, mirPotReserves, 150,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: big.NewInt(100)},
		})
	seedMIRDistribution(t, gdb, mirPotTreasury, 150,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: cred, Amount: big.NewInt(-50)},
		})

	state := mirDelegState(t, ls, db, 100, 200, true)
	reservesKey := eras.MIRCredentialKey{
		Credential: lcommon.NewBlake2b224(cred),
		Pot:        mirPotReserves,
	}
	treasuryKey := eras.MIRCredentialKey{
		Credential: lcommon.NewBlake2b224(cred),
		Pot:        mirPotTreasury,
	}
	assert.Equal(t, big.NewInt(100), state.Pending[reservesKey],
		"the reserves cert must not be netted against the treasury cert")
	assert.Equal(t, big.NewInt(-50), state.Pending[treasuryKey])
}
