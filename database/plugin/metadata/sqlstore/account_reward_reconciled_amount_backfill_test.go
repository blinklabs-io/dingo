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

package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// testReconciliationDiscriminatorPrefix mirrors account.go's unexported
// reconciliationDiscriminatorPrefix. It cannot be imported directly: this
// test builds the pre-v20 row shape by hand (see below), and account.go's
// current ReconcileAccountRewardBalance can no longer produce that shape
// itself now that it always populates reconciled_amount.
const testReconciliationDiscriminatorPrefix = "dingo:reconcile:"

// TestAccountRewardReconciledAmountBackfillRecoversPreV20Reconciliation pins
// the v21 migration (dingo #4529) that backfills reconciled_amount for
// ReconcileAccountRewardBalance rows written before v20 added that column.
//
// v20 (account-reward-delta-reconciled-amount) added the column but never
// backfilled it, so on an upgraded database every pre-v20 reconciliation row
// still carries reconciled_amount = NULL. historicalRewardsBatch's
// resolvedWithdrawalBalance falls back to previous_reward for such a row --
// exactly the pre-correction balance the correction proved wrong -- which
// reproduces the small, persistent, always-understated historical/
// epoch-boundary stake read described in dingo #4529 for every boundary
// before the correction's slot.
//
// This test builds that exact pre-v20 database shape by hand (raw SQL,
// migrated only through v20), proves the bug reproduces, runs the v21
// backfill, and proves both the column and the historical read are now
// correct.
func TestAccountRewardReconciledAmountBackfillRecoversPreV20Reconciliation(
	t *testing.T,
) {
	t.Parallel()

	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:reconciled_backfill_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 21)

	runTo := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker:   migrations.NewProcessLocker(),
		}
		require.NoError(t, runner.Run(context.Background()))
	}

	// v1-v20: the schema carries reconciled_amount (v20's own ADD COLUMN),
	// but nothing has backfilled it yet -- exactly an upgraded pre-v21
	// database.
	runTo(registry[:20])

	key := bytes.Repeat([]byte{0x41}, 28)
	realHash := bytes.Repeat([]byte{0xd2}, 32)
	discriminator := append(
		[]byte(testReconciliationDiscriminatorPrefix),
		realHash...,
	)

	_, err = db.Exec(
		"INSERT INTO account (staking_key, credential_tag, active, reward) "+
			"VALUES (?, ?, TRUE, ?)",
		key, 0, "1000000",
	)
	require.NoError(t, err)

	// The pre-v20 ReconcileAccountRewardBalance write for a correction at
	// slot 150: a canonical peer proved the true balance was 5_000_014, not
	// dingo's own 5_000_000. reconciled_amount did not exist yet, so it is
	// absent here (NULL once v20's ADD COLUMN applies) -- amount and previous_reward
	// both hold the pre-correction balance, exactly as
	// ReconcileAccountRewardBalance still writes them today. The row is
	// keyed on the reconciliationDiscriminator (prefix + real tx hash), not
	// the real hash itself; see account.go.
	_, err = db.Exec(
		`INSERT INTO account_reward_delta (
		    staking_key, credential_tag, tx_hash, amount, previous_reward,
		    added_slot, withdrawal
		) VALUES (?, ?, ?, ?, ?, ?, TRUE)`,
		key, 0, discriminator, "5000000", "5000000", 150,
	)
	require.NoError(t, err)

	// The real withdrawal row the reconciliation's retry committed for the
	// same credential and slot, through ApplyAccountRewardWithdrawal: its
	// amount is the withdrawal's own on-chain claimed amount, which is
	// exactly what the reconciliation corrected account.reward to.
	_, err = db.Exec(
		`INSERT INTO account_reward_delta (
		    staking_key, credential_tag, tx_hash, amount, previous_reward,
		    added_slot, withdrawal
		) VALUES (?, ?, ?, ?, ?, ?, TRUE)`,
		key, 0, realHash, "5000014", "5000014", 150,
	)
	require.NoError(t, err)

	selected := map[historicalRewardKey]struct{}{
		{tag: 0, key: string(key)}: {},
	}
	ref := historicalRewardKey{tag: 0, key: string(key)}

	// Before the v21 backfill: reconciled_amount is NULL, so a boundary
	// before the correction's slot resolves through previous_reward -- the
	// wrong, pre-correction balance. This is the bug (dingo #4529)
	// reproducing; without the fix this assertion is the one that should
	// (and does, when checked against registry[:20] alone) hold.
	before, err := historicalRewardsAtBoundary(
		context.Background(), db, 120, 0, selected,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(5_000_000),
		before[ref],
		"without the backfill, a pre-correction boundary resolves to the "+
			"wrong, pre-correction balance (5_000_000) instead of the "+
			"corrected one (5_000_014)",
	)

	// Run the v21 backfill.
	runTo(registry)

	var reconciled sql.NullString
	require.NoError(t, db.QueryRow(
		"SELECT reconciled_amount FROM account_reward_delta WHERE tx_hash = ?",
		discriminator,
	).Scan(&reconciled))
	require.True(
		t,
		reconciled.Valid,
		"the backfill must populate reconciled_amount for the poisoned row",
	)
	require.Equal(t, "5000014", reconciled.String)

	// The ordinary real withdrawal row must be left alone: it never needed
	// reconciled_amount and the backfill must not touch it.
	var untouched sql.NullString
	require.NoError(t, db.QueryRow(
		"SELECT reconciled_amount FROM account_reward_delta WHERE tx_hash = ?",
		realHash,
	).Scan(&untouched))
	require.False(t, untouched.Valid)

	after, err := historicalRewardsAtBoundary(
		context.Background(), db, 120, 0, selected,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(5_000_014),
		after[ref],
		"after the backfill, the same boundary must resolve to the "+
			"recovered corrected balance",
	)
}

// TestAccountRewardReconciledAmountBackfillSkipsOrdinaryWithdrawals proves the
// backfill's discriminator check: an ordinary withdrawal row with
// reconciled_amount NULL (the normal, expected state for every real
// withdrawal) must never be mistaken for a poisoned reconciliation row and
// must be left untouched, even though it matches the same
// `withdrawal = TRUE AND reconciled_amount IS NULL` scan predicate.
func TestAccountRewardReconciledAmountBackfillSkipsOrdinaryWithdrawals(
	t *testing.T,
) {
	t.Parallel()

	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:reconciled_backfill_ordinary_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	runTo := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker:   migrations.NewProcessLocker(),
		}
		require.NoError(t, runner.Run(context.Background()))
	}

	// Seed the ordinary withdrawal row before the backfill runs, exactly
	// like the poisoned-row test, so the backfill's scan (`withdrawal = TRUE
	// AND reconciled_amount IS NULL`) actually considers this row rather
	// than finding an empty table.
	runTo(registry[:20])

	key := bytes.Repeat([]byte{0x42}, 28)
	txHash := bytes.Repeat([]byte{0xab}, 32)

	_, err = db.Exec(
		"INSERT INTO account (staking_key, credential_tag, active, reward) "+
			"VALUES (?, ?, TRUE, ?)",
		key, 0, "0",
	)
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO account_reward_delta (
		    staking_key, credential_tag, tx_hash, amount, previous_reward,
		    added_slot, withdrawal
		) VALUES (?, ?, ?, ?, ?, ?, TRUE)`,
		key, 0, txHash, "2000000", "2000000", 300,
	)
	require.NoError(t, err)

	runTo(registry)

	var reconciled sql.NullString
	require.NoError(t, db.QueryRow(
		"SELECT reconciled_amount FROM account_reward_delta WHERE tx_hash = ?",
		txHash,
	).Scan(&reconciled))
	require.False(
		t,
		reconciled.Valid,
		"an ordinary withdrawal row must be left with reconciled_amount NULL",
	)
}
