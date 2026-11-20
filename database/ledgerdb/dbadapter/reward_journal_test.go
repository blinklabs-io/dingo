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

package dbadapter_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// The adapter lists the reward journal in the order it was written, with each
// amount, slot and flag as the store holds them, and refuses once the accounts
// family is drained.
func TestAdapterListsTheRewardJournalInWriteOrder(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(
		`INSERT INTO account_reward_delta
		(staking_key, credential_tag, tx_hash, amount, added_slot, withdrawal, post_snapshot)
		VALUES (?, 0, ?, '30', 20, 0, 0), (?, 1, ?, '10', 10, 1, 0), (?, 0, ?, '6', 20, 0, 1)`,
		[]byte{1}, []byte{0xa1}, []byte{2}, []byte{0xa2}, []byte{1}, []byte{0xa3},
	)
	require.NoError(t, err)

	adapter := dbadapter.New(db)
	var got []ledgerdb.RewardJournalRow
	require.NoError(t, adapter.EachRewardJournalRow(nil, func(r ledgerdb.RewardJournalRow) error {
		got = append(got, r)
		return nil
	}))
	require.Equal(t, []ledgerdb.RewardJournalRow{
		{StakingKey: []byte{1}, TxHash: []byte{0xa1}, Amount: 30, Slot: 20},
		{StakingKey: []byte{2}, CredentialTag: 1, TxHash: []byte{0xa2}, Amount: 10, Slot: 10, Withdrawal: true},
		{StakingKey: []byte{1}, TxHash: []byte{0xa3}, Amount: 6, Slot: 20, PostSnapshot: true},
	}, got)

	require.NoError(t, db.RecordDrainedFamily(t.Context(), nodesettings.FamilyDReps))
	require.NoError(t, db.RecordDrainedFamily(t.Context(), nodesettings.FamilyAccounts))
	err = adapter.EachRewardJournalRow(nil, func(ledgerdb.RewardJournalRow) error { return nil })
	require.ErrorIs(t, err, database.ErrFamilyDrained)
}
