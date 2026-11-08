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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestPV10ReverseReconciliationPreservesCanonicalHistoryAndIsIdempotent(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	old := bytes.Repeat([]byte{0xc1}, 28)
	current := bytes.Repeat([]byte{0xc2}, 28)
	stake := bytes.Repeat([]byte{0xc3}, 28)
	for _, drep := range [][]byte{old, current} {
		require.NoError(
			t,
			store.CreateDrep(
				nil,
				&models.Drep{Credential: drep, AddedSlot: 1, Active: true},
			),
		)
	}
	require.NoError(
		t,
		store.ImportAccount(
			&models.Account{
				StakingKey: stake,
				Drep:       current,
				AddedSlot:  100,
				Active:     true,
			},
			nil,
		),
	)
	db, ctx, err := store.dbFromTxn(nil)
	require.NoError(t, err)
	require.NoError(t, addDrepDelegator(ctx, db, 0, old, 0, stake, 50))
	require.NoError(t, addDrepDelegator(ctx, db, 0, current, 0, stake, 60))
	var canonicalID uint64
	require.NoError(
		t,
		db.QueryRowContext(ctx, "SELECT id FROM drep_delegator WHERE drep_credential = ?", current).
			Scan(&canonicalID),
	)
	for range 2 {
		cleared, err := store.ClearDanglingDRepDelegations(100, nil)
		require.NoError(t, err)
		require.Zero(t, cleared)
		var total int
		require.NoError(
			t,
			db.QueryRowContext(ctx, "SELECT count(*) FROM drep_delegator").
				Scan(&total),
		)
		require.Equal(t, 2, total)
		var id, added uint64
		require.NoError(
			t,
			db.QueryRowContext(ctx, "SELECT id,added_slot FROM drep_delegator WHERE drep_credential = ? AND removed_slot IS NULL", current).
				Scan(&id, &added),
		)
		require.Equal(t, canonicalID, id)
		require.Equal(t, uint64(60), added)
		var removed uint64
		require.NoError(
			t,
			db.QueryRowContext(ctx, "SELECT removed_slot FROM drep_delegator WHERE drep_credential = ?", old).
				Scan(&removed),
		)
		require.Equal(t, uint64(100), removed)
	}
	require.NoError(t, store.RestoreDrepStateAtSlot(90, nil))
	var active int
	require.NoError(
		t,
		db.QueryRowContext(ctx, "SELECT count(*) FROM drep_delegator WHERE removed_slot IS NULL").
			Scan(&active),
	)
	require.Equal(
		t,
		2,
		active,
		"rolling back the boundary restores the historical PV9 reverse set",
	)
}

func TestReverseReconciliationSkipsLegacyMissingStakeCredentials(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	drep := bytes.Repeat([]byte{0xd1}, 28)
	stake := bytes.Repeat([]byte{0xd2}, 28)
	require.NoError(
		t,
		store.CreateDrep(
			nil,
			&models.Drep{Credential: drep, AddedSlot: 1, Active: true},
		),
	)
	db, ctx, err := store.dbFromTxn(nil)
	require.NoError(t, err)
	for _, key := range []any{nil, []byte{}, stake} {
		_, err := db.ExecContext(
			ctx,
			`INSERT INTO account (staking_key, credential_tag, drep, drep_type, reward, active, added_slot) VALUES (?, 0, ?, 0, 700, TRUE, 50)`,
			key,
			drep,
		)
		require.NoError(t, err)
	}
	for range 2 {
		cleared, err := store.ClearDanglingDRepDelegations(100, nil)
		require.NoError(t, err)
		require.Zero(t, cleared)
		var count int
		require.NoError(
			t,
			db.QueryRowContext(ctx, "SELECT count(*) FROM drep_delegator").
				Scan(&count),
		)
		require.Equal(t, 1, count)
		var actual []byte
		require.NoError(
			t,
			db.QueryRowContext(ctx, "SELECT stake_credential FROM drep_delegator").
				Scan(&actual),
		)
		require.Equal(t, stake, actual)
		var accounts, rewards int
		require.NoError(
			t,
			db.QueryRowContext(ctx, "SELECT count(*), sum(reward) FROM account WHERE drep = ? AND added_slot = 50", drep).
				Scan(&accounts, &rewards),
		)
		require.Equal(t, 3, accounts)
		require.Equal(t, 2100, rewards)
	}
}
